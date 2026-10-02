import { open } from "node:fs/promises";
import { resolve } from "node:path";
import { AVATAR_MIME_TYPES, isValidAvatarImage } from "@openbot/contracts/avatar-images";
import { AVATAR_IMAGE_LIMITS } from "@openbot/contracts/input-limits";
import type { AvatarImageInput } from "@openbot/contracts/ipc";
import { Effect, Result, Schema } from "effect";

const SIZE_ERROR =
  "The avatar exceeds 512 KB. Resize or compress a copy with your available tools, then retry with its path.";

/** Reads a prepared local avatar without changing the source file. */
export async function loadAvatarFile(path: string, workspacePath: string): Promise<AvatarImageInput> {
  const result = await Effect.runPromise(Effect.result(loadAvatarFileEffect(path, workspacePath)));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}

export class AvatarFileFailed extends Schema.TaggedError<AvatarFileFailed>()("AvatarFileFailed", {
  cause: Schema.Defect(),
}) {}

export const loadAvatarFileEffect = Effect.fn("AvatarFile.load")(function* (path: string, workspacePath: string) {
  const source = yield* readSourceEffect(resolve(workspacePath, path));
  const mimeType = AVATAR_MIME_TYPES.find((type) => isValidAvatarImage(type, source));
  if (!mimeType)
    return yield* new AvatarFileFailed({ cause: new Error("Choose a valid PNG, JPEG, or WebP avatar image.") });
  return { mimeType, bytes: source };
});

const readSourceEffect = Effect.fn("AvatarFile.readSource")((path: string) =>
  Effect.acquireUseRelease(
    avatarIo(() => open(path, "r")).pipe(
      Effect.mapError(
        () =>
          new AvatarFileFailed({
            cause: new Error("OpenBot could not open the avatar file. Use an existing local image path."),
          }),
      ),
    ),
    (file) =>
      Effect.gen(function* () {
        const metadata = yield* avatarIo(() => file.stat());
        if (!metadata.isFile())
          return yield* new AvatarFileFailed({ cause: new Error("The avatar path must refer to a regular file.") });
        if (metadata.size > AVATAR_IMAGE_LIMITS.storedBytes)
          return yield* new AvatarFileFailed({ cause: new Error(SIZE_ERROR) });
        // Read at most the limit plus one byte, even if the file grows after stat.
        const buffer = Buffer.alloc(AVATAR_IMAGE_LIMITS.storedBytes + 1);
        let length = 0;
        while (length < buffer.length) {
          const offset = length;
          const { bytesRead } = yield* avatarIo(() => file.read(buffer, offset, buffer.length - offset, null));
          if (bytesRead === 0) break;
          length += bytesRead;
        }
        if (length > AVATAR_IMAGE_LIMITS.storedBytes)
          return yield* new AvatarFileFailed({ cause: new Error(SIZE_ERROR) });
        return buffer.subarray(0, length);
      }),
    (file) => avatarIo(() => file.close()),
  ),
);

function avatarIo<A>(run: () => Promise<A>): Effect.Effect<A, AvatarFileFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new AvatarFileFailed({ cause }) });
}
