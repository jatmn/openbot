import { Effect, Result, Schema } from "effect";

/** Expected failures from tool validation and injected storage or provider adapters. */
export class ToolOperationFailed extends Schema.TaggedError<ToolOperationFailed>()("ToolOperationFailed", {
  cause: Schema.Defect(),
}) {}

export function toolIo<A>(run: () => Promise<A>): Effect.Effect<A, ToolOperationFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new ToolOperationFailed({ cause }) });
}

export function toolStep<A>(run: () => A): Effect.Effect<A, ToolOperationFailed> {
  return Effect.try({ try: run, catch: (cause) => new ToolOperationFailed({ cause }) });
}

export async function runTool<A>(effect: Effect.Effect<A, ToolOperationFailed>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(effect));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
