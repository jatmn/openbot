import { Effect, Result, Schema } from "effect";

/** Internal storage failure. Public facades retain the released native error contract. */
export class StoredStateFailure extends Schema.TaggedError<StoredStateFailure>()("StoredStateFailure", {
  cause: Schema.Defect(),
}) {}

export function storedIO<A>(operation: (signal: AbortSignal) => Promise<A>): Effect.Effect<A, StoredStateFailure> {
  return Effect.tryPromise({ try: operation, catch: (cause) => new StoredStateFailure({ cause }) });
}

export function storedSync<A>(operation: () => A): Effect.Effect<A, StoredStateFailure> {
  return Effect.try({ try: operation, catch: (cause) => new StoredStateFailure({ cause }) });
}

export async function runStored<A>(operation: Effect.Effect<A, StoredStateFailure>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
