import { BILLING_CURRENCIES, BILLING_METADATA, type BillingCurrency } from "@openbot/contracts/billing";
import { isOneOf } from "@openbot/contracts/runtime-values";
import { Effect, Option, Result, Schema } from "effect";
import { runApiEffect } from "./effect-runtime";

/** Stripe changes response shapes by API version, so every request names the version this code reads. */
export const STRIPE_API_VERSION = "2025-03-31.basil";
const STRIPE_API_ORIGIN = "https://api.stripe.com";
const STRIPE_TIMEOUT_MS = 10_000;
/** Stripe's recommended tolerance for the webhook timestamp. */
const SIGNATURE_TOLERANCE_SECONDS = 300;
/**
 * A Checkout page stays open this long. Stripe accepts 30 minutes to 24 hours from its own clock, so this
 * keeps a margin for clock skew and request time.
 */
const CHECKOUT_LIFETIME_SECONDS = 35 * 60;

export type StripeFetch = (input: string, init: RequestInit) => Promise<Response>;

/**
 * A Stripe request failed. The message holds only Stripe's error `type` and `code`, never the key, the
 * request body or Stripe's own message, which can repeat customer data.
 */
export class StripeRequestError extends Schema.TaggedError<StripeRequestError>()("StripeRequestError", {
  status: Schema.Number,
  type: Schema.NullOr(Schema.String),
  code: Schema.NullOr(Schema.String),
  message: Schema.String,
}) {
  constructor(status: number, type: string | null, code: string | null) {
    super({
      status,
      type,
      code,
      message: `Stripe request failed with ${status} (${type ?? "unknown"}${code ? `/${code}` : ""}).`,
    });
  }
}
class StripeTransportError extends Schema.TaggedError<StripeTransportError>()("StripeTransportError", {}) {}

const Metadata = Schema.Record(Schema.String, Schema.String).pipe(Schema.withDecodingDefault(Effect.succeed({})));
const CurrencyOptions = Schema.optional(
  Schema.Record(Schema.String, Schema.Struct({ unit_amount: Schema.optional(Schema.NullOr(Schema.Int)) })),
);
const stripeErrorSchema = Schema.Struct({
  error: Schema.Struct({ type: Schema.optional(Schema.String), code: Schema.optional(Schema.String) }),
});
const sessionSchema = Schema.Struct({ id: Schema.String, url: Schema.NullOr(Schema.String) });
const customerSchema = Schema.Struct({ id: Schema.String, deleted: Schema.optional(Schema.Boolean) });
const checkoutSessionSchema = Schema.Struct({
  id: Schema.String,
  url: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  subscription: Schema.optional(Schema.NullOr(Schema.String)),
});
export type StripeCheckoutSession = typeof checkoutSessionSchema.Type;
const priceSchema = Schema.Struct({
  id: Schema.String,
  lookup_key: Schema.NullOr(Schema.String),
  currency: Schema.String,
  unit_amount: Schema.NullOr(Schema.Int),
  currency_options: CurrencyOptions,
});
export type StripePrice = typeof priceSchema.Type;
const priceListSchema = Schema.Struct({ data: Schema.Array(priceSchema) });
const subscriptionSchema = Schema.Struct({
  id: Schema.String,
  customer: Schema.Union([Schema.String, Schema.Struct({ id: Schema.String })]),
  status: Schema.String,
  currency: Schema.String,
  cancel_at_period_end: Schema.Boolean,
  cancel_at: Schema.optional(Schema.NullOr(Schema.Int)),
  metadata: Metadata,
  current_period_end: Schema.optional(Schema.Int),
  items: Schema.Struct({
    data: Schema.Array(
      Schema.Struct({
        current_period_end: Schema.optional(Schema.Int),
        quantity: Schema.optional(Schema.Int),
        price: Schema.Struct({
          id: Schema.String,
          lookup_key: Schema.NullOr(Schema.String),
          metadata: Metadata,
          currency: Schema.String,
          unit_amount: Schema.NullOr(Schema.Int),
          currency_options: CurrencyOptions,
        }),
      }),
    ),
  }),
});
export type StripeSubscription = typeof subscriptionSchema.Type;
const eventSchema = Schema.Struct({
  id: Schema.String,
  type: Schema.String,
  created: Schema.Int,
  data: Schema.Struct({ object: Schema.Record(Schema.String, Schema.Unknown) }),
});
export type StripeEvent = typeof eventSchema.Type;

export function isBillingCurrency(value: unknown): value is BillingCurrency {
  return isOneOf(BILLING_CURRENCIES, value);
}

export class StripeClient {
  constructor(
    private readonly secretKey: string,
    private readonly fetcher: StripeFetch,
  ) {}

  /**
   * A Customer Portal page. With a flow it opens on the plan change or the cancel page of one
   * subscription, and goes back to `returnUrl` when that step is done.
   */
  createPortalSession(input: {
    customerId: string;
    returnUrl: string;
    flow?: { type: "subscription_update" | "subscription_cancel"; subscriptionId: string };
  }): Promise<string> {
    return runApiEffect(this.#createPortalSessionEffect(input));
  }

  readonly #createPortalSessionEffect = Effect.fn("StripeClient.createPortalSession")(function* (
    this: StripeClient,
    input: {
      customerId: string;
      returnUrl: string;
      flow?: { type: "subscription_update" | "subscription_cancel"; subscriptionId: string };
    },
  ): Effect.fn.Return<string, StripeRequestError | StripeTransportError> {
    const body = new URLSearchParams({ customer: input.customerId, return_url: input.returnUrl });
    if (input.flow) {
      body.set("flow_data[type]", input.flow.type);
      body.set(`flow_data[${input.flow.type}][subscription]`, input.flow.subscriptionId);
      body.set("flow_data[after_completion][type]", "redirect");
      body.set("flow_data[after_completion][redirect][return_url]", input.returnUrl);
    }
    const session = yield* this.#requestEffect("POST", "/v1/billing_portal/sessions", body, sessionSchema);
    if (!session.url) return yield* new StripeRequestError(200, "invalid_response", "missing_url");
    return session.url;
  });

  /** The subscription, with the Price amount in each currency, so the plan's own currency has a price. */
  getSubscription(subscriptionId: string): Promise<StripeSubscription> {
    return runApiEffect(this.#getSubscriptionEffect(subscriptionId));
  }

  readonly #getSubscriptionEffect = Effect.fn("StripeClient.getSubscription")(function* (
    this: StripeClient,
    subscriptionId: string,
  ): Effect.fn.Return<StripeSubscription, StripeRequestError | StripeTransportError> {
    const query = new URLSearchParams({ "expand[]": "items.data.price.currency_options" });
    return yield* this.#requestEffect(
      "GET",
      `/v1/subscriptions/${encodeURIComponent(subscriptionId)}?${query}`,
      null,
      subscriptionSchema,
    );
  });

  /** The active Prices with these lookup keys, with the amount in each currency. */
  listPricesByLookupKeys(lookupKeys: readonly string[]): Promise<StripePrice[]> {
    return runApiEffect(this.#listPricesByLookupKeysEffect(lookupKeys));
  }

  readonly #listPricesByLookupKeysEffect = Effect.fn("StripeClient.listPricesByLookupKeys")(function* (
    this: StripeClient,
    lookupKeys: readonly string[],
  ): Effect.fn.Return<StripePrice[], StripeRequestError | StripeTransportError> {
    const query = new URLSearchParams({ active: "true", limit: "100" });
    for (const key of lookupKeys) query.append("lookup_keys[]", key);
    query.append("expand[]", "data.currency_options");
    return [...(yield* this.#requestEffect("GET", `/v1/prices?${query}`, null, priceListSchema)).data];
  });

  /** The same idempotency key gives the same customer, so two requests at once make one customer. */
  createCustomer(input: { userId: string; email: string }, idempotencyKey: string): Promise<string> {
    return runApiEffect(this.#createCustomerEffect(input, idempotencyKey));
  }

  readonly #createCustomerEffect = Effect.fn("StripeClient.createCustomer")(function* (
    this: StripeClient,
    input: { userId: string; email: string },
    idempotencyKey: string,
  ): Effect.fn.Return<string, StripeRequestError | StripeTransportError> {
    const body = new URLSearchParams({ email: input.email, [`metadata[${BILLING_METADATA.userId}]`]: input.userId });
    return (yield* this.#requestEffect("POST", "/v1/customers", body, customerSchema, idempotencyKey)).id;
  });

  /**
   * True when the customer was deleted in Stripe. Stripe still returns a deleted customer, with
   * `deleted`. A customer of another Stripe account, or of test data that was reset, does not exist.
   */
  isCustomerDeleted(customerId: string): Promise<boolean> {
    return runApiEffect(this.#isCustomerDeletedEffect(customerId));
  }

  readonly #isCustomerDeletedEffect = Effect.fn("StripeClient.isCustomerDeleted")(function* (
    this: StripeClient,
    customerId: string,
  ): Effect.fn.Return<boolean, StripeRequestError | StripeTransportError> {
    const path = `/v1/customers/${encodeURIComponent(customerId)}`;
    const result = yield* Effect.result(this.#requestEffect("GET", path, null, customerSchema));
    if (Result.isFailure(result)) {
      if (result.failure instanceof StripeRequestError && result.failure.status === 404) return true;
      return yield* result.failure;
    }
    return result.success.deleted === true;
  });

  /**
   * A subscription Checkout for one server. The metadata goes on the session and on the subscription, so
   * the webhook links the subscription to the account and the server. Each call makes a new page, so it
   * takes no idempotency key: the caller closes the previous page.
   */
  createCheckoutSession(input: {
    customerId: string;
    priceId: string;
    currency: BillingCurrency;
    userId: string;
    serverId: string;
    successUrl: string;
    cancelUrl: string;
    nowSeconds: number;
  }): Promise<{ id: string; url: string }> {
    return runApiEffect(this.#createCheckoutSessionEffect(input));
  }

  readonly #createCheckoutSessionEffect = Effect.fn("StripeClient.createCheckoutSession")(function* (
    this: StripeClient,
    input: {
      customerId: string;
      priceId: string;
      currency: BillingCurrency;
      userId: string;
      serverId: string;
      successUrl: string;
      cancelUrl: string;
      nowSeconds: number;
    },
  ): Effect.fn.Return<{ id: string; url: string }, StripeRequestError | StripeTransportError> {
    const body = new URLSearchParams({
      mode: "subscription",
      customer: input.customerId,
      "line_items[0][price]": input.priceId,
      "line_items[0][quantity]": "1",
      currency: input.currency,
      // Card collection stays on, so a plan that a code discounts only at the start can still renew.
      allow_promotion_codes: "true",
      client_reference_id: input.serverId,
      success_url: input.successUrl,
      cancel_url: input.cancelUrl,
      expires_at: String(input.nowSeconds + CHECKOUT_LIFETIME_SECONDS),
    });
    for (const prefix of ["metadata", "subscription_data[metadata]"]) {
      body.set(`${prefix}[${BILLING_METADATA.userId}]`, input.userId);
      body.set(`${prefix}[${BILLING_METADATA.serverId}]`, input.serverId);
    }
    const session = yield* this.#requestEffect("POST", "/v1/checkout/sessions", body, checkoutSessionSchema);
    if (!session.url) return yield* new StripeRequestError(200, "invalid_response", "missing_url");
    return { id: session.id, url: session.url };
  });

  /**
   * Closes a Checkout page. Returns the page in its final status: `expired`, or `complete` for a page
   * that the user paid. Stripe refuses to expire a session that is not open, so the service then reads it.
   */
  expireCheckoutSession(sessionId: string): Promise<StripeCheckoutSession> {
    return runApiEffect(this.#expireCheckoutSessionEffect(sessionId));
  }

  readonly #expireCheckoutSessionEffect = Effect.fn("StripeClient.expireCheckoutSession")(function* (
    this: StripeClient,
    sessionId: string,
  ): Effect.fn.Return<StripeCheckoutSession, StripeRequestError | StripeTransportError> {
    const path = `/v1/checkout/sessions/${encodeURIComponent(sessionId)}`;
    const result = yield* Effect.result(
      this.#requestEffect("POST", `${path}/expire`, new URLSearchParams(), checkoutSessionSchema),
    );
    if (Result.isSuccess(result)) return result.success;
    if (!(result.failure instanceof StripeRequestError) || result.failure.status !== 400) return yield* result.failure;
    return yield* this.#requestEffect("GET", path, null, checkoutSessionSchema);
  });

  /** Cancels a subscription now. Stripe gives no refund and no credit for the rest of the period. */
  cancelSubscription(subscriptionId: string): Promise<StripeSubscription> {
    return runApiEffect(this.#cancelSubscriptionEffect(subscriptionId));
  }

  readonly #cancelSubscriptionEffect = Effect.fn("StripeClient.cancelSubscription")(function* (
    this: StripeClient,
    subscriptionId: string,
  ): Effect.fn.Return<StripeSubscription, StripeRequestError | StripeTransportError> {
    return yield* this.#requestEffect(
      "DELETE",
      `/v1/subscriptions/${encodeURIComponent(subscriptionId)}`,
      null,
      subscriptionSchema,
    );
  });

  readonly #requestEffect = Effect.fn("StripeClient.request")(function* <T>(
    this: StripeClient,
    method: "GET" | "POST" | "DELETE",
    path: string,
    body: URLSearchParams | null,
    schema: Schema.Decoder<T>,
    idempotencyKey?: string,
  ): Effect.fn.Return<T, StripeRequestError | StripeTransportError> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.secretKey}`,
      "Stripe-Version": STRIPE_API_VERSION,
    };
    if (body) headers["Content-Type"] = "application/x-www-form-urlencoded";
    if (idempotencyKey) headers["Idempotency-Key"] = idempotencyKey;
    const response = yield* Effect.tryPromise({
      try: (signal) =>
        this.fetcher(`${STRIPE_API_ORIGIN}${path}`, {
          method,
          headers,
          ...(body ? { body: body.toString() } : {}),
          signal: AbortSignal.any([signal, AbortSignal.timeout(STRIPE_TIMEOUT_MS)]),
        }),
      catch: () => new StripeTransportError({}),
    });
    const value = yield* Effect.tryPromise({
      try: () => response.json(),
      catch: () => new StripeTransportError({}),
    }).pipe(Effect.catch(() => Effect.succeed(null)));
    if (!response.ok) {
      const parsed = Schema.decodeUnknownOption(stripeErrorSchema)(value);
      return yield* new StripeRequestError(
        response.status,
        Option.isSome(parsed) ? (parsed.value.error.type ?? null) : null,
        Option.isSome(parsed) ? (parsed.value.error.code ?? null) : null,
      );
    }
    return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
      Effect.mapError(() => new StripeRequestError(response.status, "invalid_response", null)),
    );
  });
}

/** The subscription's period end in milliseconds, from the item or, for older API versions, the subscription. */
export function subscriptionPeriodEnd(subscription: StripeSubscription): number | null {
  const seconds = subscription.items.data[0]?.current_period_end ?? subscription.current_period_end;
  return seconds === undefined ? null : seconds * 1_000;
}

/**
 * The list price of one period in the subscription's currency, in minor units. A multi-currency Price
 * holds its base currency amount in `unit_amount` and the other currencies in `currency_options`.
 * Null for a Price with no fixed amount, such as a tiered Price.
 */
export function subscriptionAmount(subscription: StripeSubscription): number | null {
  const item = subscription.items.data[0];
  if (!item) return null;
  const currency = subscription.currency.toLowerCase();
  const unitAmount =
    item.price.currency.toLowerCase() === currency
      ? item.price.unit_amount
      : (item.price.currency_options?.[currency]?.unit_amount ?? null);
  if (unitAmount === null) return null;
  const amount = unitAmount * (item.quantity ?? 1);
  return Number.isSafeInteger(amount) && amount >= 0 ? amount : null;
}

export function subscriptionCustomerId(subscription: StripeSubscription): string {
  return typeof subscription.customer === "string" ? subscription.customer : subscription.customer.id;
}

/** Decodes a webhook body that `verifyStripeSignature` accepted. */
export function parseStripeEvent(payload: string): StripeEvent | null {
  try {
    const parsed = Schema.decodeUnknownOption(eventSchema)(JSON.parse(payload));
    return Option.isSome(parsed) ? parsed.value : null;
  } catch {
    return null;
  }
}

/**
 * Checks a `Stripe-Signature` header (`t=<seconds>,v1=<hex>[,v1=<hex>]`) against the raw body.
 * During a secret rotation Stripe sends one `v1` for each secret, so any match is enough.
 */
export async function verifyStripeSignature(
  payload: string,
  header: string | null,
  secret: string,
  now = Date.now(),
  toleranceSeconds = SIGNATURE_TOLERANCE_SECONDS,
): Promise<boolean> {
  if (!header || !secret) return false;
  let timestamp: string | null = null;
  const signatures: string[] = [];
  for (const part of header.split(",")) {
    const separator = part.indexOf("=");
    if (separator < 0) continue;
    const name = part.slice(0, separator).trim();
    const value = part.slice(separator + 1).trim();
    if (name === "t") timestamp = value;
    else if (name === "v1" && /^[0-9a-f]{64}$/u.test(value)) signatures.push(value);
  }
  if (!timestamp || !/^\d{1,12}$/u.test(timestamp) || signatures.length === 0) return false;
  if (Math.abs(now / 1_000 - Number(timestamp)) > toleranceSeconds) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const expected = new Uint8Array(
    await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(`${timestamp}.${payload}`)),
  );
  let matched = false;
  for (const signature of signatures) {
    // Every candidate is compared in full, so the time does not show which one matched.
    if (constantTimeEqual(expected, hexBytes(signature))) matched = true;
  }
  return matched;
}

function hexBytes(value: string): Uint8Array {
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function constantTimeEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  let difference = 0;
  for (let index = 0; index < left.byteLength; index += 1) difference |= (left[index] ?? 0) ^ (right[index] ?? 0);
  return difference === 0;
}
