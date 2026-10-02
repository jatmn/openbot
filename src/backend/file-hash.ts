import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { Effect, Result, Schema, Stream } from "effect";

export class FileHashFailure extends Schema.TaggedError<FileHashFailure>()("FileHashFailure", {
  cause: Schema.Defect(),
}) {}

/** Reads a file as a stream and releases it on completion or interruption. */
export const sha256FileEffect = Effect.fn("FileHash.sha256")(function* (path: string) {
  const hash = createHash("sha256");
  yield* Effect.acquireUseRelease(
    Effect.sync(() => createReadStream(path)),
    (stream) =>
      Stream.fromAsyncIterable(stream, (cause) => new FileHashFailure({ cause })).pipe(
        Stream.runForEach((chunk) =>
          Effect.sync(() => {
            hash.update(chunk);
          }),
        ),
      ),
    (stream) =>
      Effect.sync(() => {
        stream.destroy();
      }),
  );
  return hash.digest("hex");
});

export async function sha256File(path: string): Promise<string> {
  const result = await Effect.runPromise(Effect.result(sha256FileEffect(path)));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
