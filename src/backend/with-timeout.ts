import { Effect, Result, Schema } from "effect";

/** The rejection of `withTimeout`, so a caller can tell its own timer from an error `work` threw. */
export class TimeoutError extends Error {}

/**
 * Settles as `work` settles, or rejects with `message` after `timeoutMs`. The timer is cleared in
 * both cases. `work` itself keeps running: the caller only stops waiting for it.
 */
export async function withTimeout<T>(work: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  // This boundary borrows work that is already running; timeout only stops this waiter.
  const result = await Effect.runPromise(
    Effect.result(
      Effect.tryPromise({
        try: () => work,
        catch: (cause) => new WaitFailed({ cause }),
      }).pipe(
        Effect.timeoutOrElse({
          duration: timeoutMs,
          orElse: () => Effect.fail(new WaitFailed({ cause: new TimeoutError(message) })),
        }),
      ),
    ),
  );
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}

class WaitFailed extends Schema.TaggedError<WaitFailed>()("WaitFailed", { cause: Schema.Defect() }) {}
