import { Effect, Result, Schema } from "effect";

/** Preserves attachment failures at the existing Promise interfaces. */
export class AttachmentOperationError extends Schema.TaggedError<AttachmentOperationError>()(
  "AttachmentOperationError",
  {
    cause: Schema.Defect(),
  },
) {}

export function attachmentFailure(cause: unknown): AttachmentOperationError {
  return cause instanceof AttachmentOperationError ? cause : new AttachmentOperationError({ cause });
}
export function attachmentCall<A>(operation: () => A | Promise<A>): Effect.Effect<A, AttachmentOperationError> {
  return Effect.tryPromise({ try: () => Promise.resolve(operation()), catch: attachmentFailure });
}
export const attachmentSync = <A>(operation: () => A) => Effect.try({ try: operation, catch: attachmentFailure });

export async function runAttachmentEffect<A>(operation: Effect.Effect<A, AttachmentOperationError>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}

/** Lets a native catch keep loop control and inspect the original operational error. */
export function attachmentResult<A>(result: Result.Result<A, AttachmentOperationError>): A {
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
