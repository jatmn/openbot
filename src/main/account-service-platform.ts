import { Context, Effect, Layer, Result, Schema } from "effect";

export interface AccountRequestClient {
  requestAuthorized<T>(path: string, init: RequestInit, decoder: (value: unknown) => T, timeoutMs?: number): Promise<T>;
}

class AccountRequestFailure extends Schema.TaggedError<AccountRequestFailure>()("AccountRequestFailure", {
  cause: Schema.Defect(),
}) {}
class AccountPageFailure extends Schema.TaggedError<AccountPageFailure>()("AccountPageFailure", {
  cause: Schema.Defect(),
}) {}
export type AccountServiceFailure = AccountRequestFailure | AccountPageFailure;

/** Desktop account services share these injected I/O boundaries, including test implementations. */
export class AccountServicePlatform extends Context.Service<
  AccountServicePlatform,
  {
    request<T>(
      path: string,
      init: RequestInit,
      decode: (value: unknown) => T,
      timeoutMs?: number,
    ): Effect.Effect<T, AccountRequestFailure>;
    openPage(url: string): Effect.Effect<void, AccountPageFailure>;
  }
>()("openbot/main/AccountServicePlatform") {
  static layer(auth: AccountRequestClient, openExternal: (url: string) => Promise<void>) {
    return Layer.succeed(
      AccountServicePlatform,
      AccountServicePlatform.of({
        request: <T>(path: string, init: RequestInit, decode: (value: unknown) => T, timeoutMs?: number) =>
          Effect.tryPromise({
            try: () => auth.requestAuthorized(path, init, decode, timeoutMs),
            catch: (cause) => new AccountRequestFailure({ cause }),
          }),
        openPage: (url) =>
          Effect.tryPromise({ try: () => openExternal(url), catch: (cause) => new AccountPageFailure({ cause }) }),
      }),
    );
  }
}

/** No resources are acquired by this layer. Preserve native account failures at the IPC boundary. */
export async function runAccountEffect<A>(
  operation: Effect.Effect<A, AccountServiceFailure, AccountServicePlatform>,
  platform: Layer.Layer<AccountServicePlatform>,
): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation.pipe(Effect.provide(platform))));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
