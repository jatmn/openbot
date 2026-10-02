import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import { Context, Effect, Layer, Schema } from "effect";
import { runApiEffect } from "./effect-runtime";

const BOAT_API_URL = "https://boat.dev/api/v1";
const REQUEST_TIMEOUT_MS = 15_000;
/**
 * boat gives the sandbox `env` to its tool environment and to the create-time setup script, not to
 * systemd. The template's helper copies the two hosted variables to a file that the OpenBot service
 * waits for. See `scripts/hosting/openbot-hosted-env`.
 */
const HOSTED_SERVER_SETUP_SCRIPT = "exec /opt/OpenBot/hosted/openbot-hosted-env\n";

export type BoatSandboxType = "small" | "default" | "large";

const BoatSandboxState = Schema.Literals([
  "init",
  "provisioning",
  "provisioned",
  "cloning",
  "ready",
  "idle",
  "running",
  "archiving",
  "archived",
  "error",
  "cancelled",
]);
export type BoatSandboxState = typeof BoatSandboxState.Type;
const BoatSandbox = Schema.Struct({ id: Schema.String, state: BoatSandboxState });
export type BoatSandbox = typeof BoatSandbox.Type;

/** Provider messages and raw causes can contain credentials and are not retained. */
export class BoatApiError extends Schema.TaggedError<BoatApiError>()("BoatApiError", {
  status: Schema.Number,
  code: Schema.String,
  message: Schema.String,
}) {
  constructor(status: number, code: string) {
    super({ status, code, message: `boat request failed: ${status} ${code}` });
  }
}

export type BoatFetch = (input: string, init: RequestInit) => Promise<Response>;

export interface BoatClientOptions {
  apiKey: string;
  fetch?: BoatFetch | undefined;
  baseUrl?: string;
}

class BoatTransport extends Context.Service<
  BoatTransport,
  {
    fetch: BoatFetch;
    apiKey: string;
    baseUrl: string;
  }
>()("@openbot/auth-api/BoatTransport") {}

const boatRequest = Effect.fn("BoatClient.request")(function* (
  method: string,
  path: string,
  options: { body?: unknown; headers?: Record<string, string> } = {},
) {
  const transport = yield* BoatTransport;
  const headers: Record<string, string> = {
    Authorization: `Bearer ${transport.apiKey}`,
    Accept: "application/json",
    ...options.headers,
  };
  if (options.body !== undefined) headers["Content-Type"] = "application/json";
  const response = yield* Effect.tryPromise({
    try: (signal) =>
      transport.fetch(`${transport.baseUrl}${path}`, {
        method,
        headers,
        ...(options.body === undefined ? {} : { body: JSON.stringify(options.body) }),
        signal: AbortSignal.any([signal, AbortSignal.timeout(REQUEST_TIMEOUT_MS)]),
      }),
    catch: () => new BoatApiError(503, "network_error"),
  });
  const text = yield* Effect.tryPromise({
    try: () => response.text(),
    catch: () => new BoatApiError(503, "network_error"),
  });
  const payload = text
    ? yield* Effect.try({ try: (): unknown => JSON.parse(text), catch: () => null }).pipe(
        Effect.catch(() => Effect.succeed(null)),
      )
    : null;
  if (!response.ok) {
    const code = isDynamicRecord(payload) && isString(payload.code) ? payload.code : "http_error";
    return yield* new BoatApiError(response.status, code);
  }
  return payload;
});

const parseSandbox = Effect.fn("BoatClient.decodeSandbox")((value: unknown) =>
  Schema.decodeUnknownEffect(BoatSandbox)(
    isDynamicRecord(value) && isDynamicRecord(value.sandbox) ? value.sandbox : value,
  ).pipe(Effect.mapError(() => new BoatApiError(502, "invalid_response"))),
);

export class BoatClient {
  readonly #transport: Layer.Layer<BoatTransport>;

  constructor(options: BoatClientOptions) {
    this.#transport = Layer.succeed(BoatTransport)({
      apiKey: options.apiKey,
      // A wrapped global fetch preserves its workerd receiver.
      fetch: options.fetch ?? ((input, init) => fetch(input, init)),
      baseUrl: options.baseUrl ?? BOAT_API_URL,
    });
  }

  createSandbox(input: {
    type: BoatSandboxType;
    from: string;
    env: Record<string, string>;
    ttlSeconds: number;
    idempotencyKey: string;
  }): Promise<BoatSandbox> {
    return this.#run(
      boatRequest("POST", "/sandboxes", {
        body: {
          type: input.type,
          from: input.from,
          env: input.env,
          noEnv: true,
          ttlSeconds: input.ttlSeconds,
          setupScript: HOSTED_SERVER_SETUP_SCRIPT,
        },
        headers: { "Idempotency-Key": input.idempotencyKey },
      }).pipe(Effect.flatMap(parseSandbox)),
    );
  }

  getSandbox(sandboxId: string): Promise<BoatSandbox> {
    return this.#run(
      boatRequest("GET", `/sandboxes/${encodeURIComponent(sandboxId)}`).pipe(Effect.flatMap(parseSandbox)),
    );
  }

  /** Restore the saved disk; boat refuses a smaller machine that cannot hold it. */
  resumeSandbox(sandboxId: string, ttlSeconds: number, type?: BoatSandboxType): Promise<void> {
    return this.#run(
      boatRequest("POST", `/sandboxes/${encodeURIComponent(sandboxId)}/resume`, {
        body: type === undefined ? { ttlSeconds } : { ttlSeconds, type },
      }).pipe(Effect.asVoid),
    );
  }

  extendSandbox(sandboxId: string, ttlSeconds: number): Promise<void> {
    return this.#run(
      boatRequest("PATCH", `/sandboxes/${encodeURIComponent(sandboxId)}`, { body: { ttlSeconds } }).pipe(Effect.asVoid),
    );
  }

  renameSandbox(sandboxId: string, name: string): Promise<void> {
    return this.#run(
      boatRequest("PATCH", `/sandboxes/${encodeURIComponent(sandboxId)}`, { body: { name } }).pipe(Effect.asVoid),
    );
  }

  /** Boat saves the disk before stopping. A failed save must not force a stop. */
  stopSandbox(sandboxId: string): Promise<void> {
    return this.#run(
      boatRequest("POST", `/sandboxes/${encodeURIComponent(sandboxId)}/stop`, { body: {} }).pipe(Effect.asVoid),
    );
  }

  /** A sandbox that is already gone counts as deleted. */
  deleteSandbox(sandboxId: string): Promise<void> {
    return this.#run(
      boatRequest("DELETE", `/sandboxes/${encodeURIComponent(sandboxId)}`, {
        headers: { "X-Ascii-Confirm-Delete": sandboxId },
      }).pipe(
        Effect.asVoid,
        Effect.catch((error) => (error.status === 404 ? Effect.void : Effect.fail(error))),
      ),
    );
  }

  #run<A>(operation: Effect.Effect<A, BoatApiError, BoatTransport>): Promise<A> {
    return runApiEffect(operation.pipe(Effect.provide(this.#transport)));
  }
}

export const isBoatState = Schema.is(BoatSandboxState);

/** Checks a boat webhook signature: hex HMAC-SHA256 of `delivery.timestamp.body`. */
export async function verifyBoatWebhookSignature(input: {
  secret: string;
  deliveryId: string;
  timestamp: string;
  signature: string;
  body: string;
  now: number;
}): Promise<boolean> {
  const seconds = Number(input.timestamp);
  if (!Number.isInteger(seconds) || Math.abs(input.now - seconds * 1_000) > 5 * 60_000) return false;
  const provided = input.signature.startsWith("v1=") ? input.signature.slice(3) : "";
  if (!/^[0-9a-f]{64}$/iu.test(provided)) return false;
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(input.secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  const bytes = new Uint8Array(32);
  for (let index = 0; index < 32; index += 1)
    bytes[index] = Number.parseInt(provided.slice(index * 2, index * 2 + 2), 16);
  return crypto.subtle.verify(
    "HMAC",
    key,
    bytes,
    new TextEncoder().encode(`${input.deliveryId}.${input.timestamp}.${input.body}`),
  );
}
