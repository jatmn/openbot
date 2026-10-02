import { Effect, Result, Schema } from "effect";

/** Preserves channel errors at the existing command and provider boundaries. */
export class ChannelOperationError extends Schema.TaggedError<ChannelOperationError>()("ChannelOperationError", {
  cause: Schema.Defect(),
}) {}

export function channelFailure(cause: unknown): ChannelOperationError {
  return cause instanceof ChannelOperationError ? cause : new ChannelOperationError({ cause });
}
export function channelCall<A>(operation: () => A | Promise<A>): Effect.Effect<A, ChannelOperationError> {
  return Effect.tryPromise({ try: () => Promise.resolve(operation()), catch: channelFailure });
}
export const channelSync = <A>(operation: () => A) => Effect.try({ try: operation, catch: channelFailure });

export async function runChannelEffect<A>(operation: Effect.Effect<A, ChannelOperationError>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}

/** Lets a native catch keep loop control and inspect the original operational error. */
export function channelResult<A>(result: Result.Result<A, ChannelOperationError>): A {
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
