import {
  type BillingPortalRequest,
  type BillingState,
  parseBillingSessionUrl,
  parseBillingState,
} from "@openbot/contracts/billing";
import { sourceText } from "@openbot/i18n/source";
import { Effect, type Layer } from "effect";
import { AccountServicePlatform, runAccountEffect } from "./account-service-platform";

export interface BillingAuthClient {
  requestAuthorized<T>(path: string, init: RequestInit, decoder: (value: unknown) => T, timeoutMs?: number): Promise<T>;
}

/**
 * The Stripe plan of each server that the account pays for. The renderer never sends a URL: this
 * service gets the Customer Portal URL from the account server and opens it only when it is an https
 * Stripe page.
 */
export class BillingDesktopService {
  readonly #platform: Layer.Layer<AccountServicePlatform>;

  constructor(auth: BillingAuthClient, openExternal: (url: string) => Promise<void>) {
    this.#platform = AccountServicePlatform.layer(auth, openExternal);
  }

  getState(): Promise<BillingState> {
    return runAccountEffect(readBillingState(), this.#platform);
  }

  openPortal(request: BillingPortalRequest): Promise<void> {
    return runAccountEffect(openBillingPortal(request), this.#platform);
  }
}

const readBillingState = Effect.fn("BillingDesktop.getState")(function* () {
  const platform = yield* AccountServicePlatform;
  return yield* platform.request("/v1/me/billing/", { method: "GET" }, decodeBillingState);
});

const openBillingPortal = Effect.fn("BillingDesktop.openPortal")(function* (request: BillingPortalRequest) {
  const platform = yield* AccountServicePlatform;
  const url = yield* platform.request(
    "/v1/me/billing/portal",
    { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(request) },
    decodeSessionUrl,
  );
  yield* platform.openPage(url);
});

function decodeBillingState(value: unknown): BillingState {
  const state = parseBillingState(value);
  if (!state) throw new Error(sourceText("error.billing.invalidResponse"));
  return state;
}

function decodeSessionUrl(value: unknown): string {
  const url = parseBillingSessionUrl(value);
  if (!url) throw new Error(sourceText("error.billing.notStripePage"));
  return url;
}
