import { randomUUID } from "node:crypto";
import { constants } from "node:fs";
import { lstat, open, rename, unlink } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { Effect, Result, Schema } from "effect";
import { z } from "zod";
import type { HostManagerConfig, HostTenantStatus, HostUpdateState } from "../../packages/contracts/src/host-manager";
import { isMissingFileError } from "../backend/file-errors";

export const HOST_MANAGER_DIRECTORY = "/Library/Application Support/OpenBot/HostManager";
export const HOST_POLL_MS = 5_000;
export const HOST_HEARTBEAT_TIMEOUT_MS = 20_000;
export const HOST_IDLE_GRACE_MS = 300_000;
const version = z
  .string()
  .regex(/^\d+\.\d+\.\d+$/)
  .max(64);
export const hostConfigSchema: z.ZodType<HostManagerConfig> = z
  .object({
    managed: z.boolean(),
    tenants: z
      .array(z.number().int().min(501))
      .min(1)
      .max(100)
      .refine((uids) => new Set(uids).size === uids.length),
  })
  .strict();
export const hostStateSchema: z.ZodType<HostUpdateState> = z
  .object({
    phase: z.enum(["idle", "downloading", "waiting", "stopping", "installing", "released", "aborted", "failed"]),
    cycle: z.string().max(64),
    version: version.nullable(),
    updatedAt: z.number().nonnegative(),
    error: z.string().max(300).nullable(),
  })
  .strict();
export const tenantStatusSchema: z.ZodType<HostTenantStatus> = z
  .object({
    uid: z.number().int().min(501),
    pid: z.number().int().positive(),
    currentVersion: version,
    heartbeatAt: z.number().nonnegative(),
    safeToRestart: z.boolean(),
    idleSince: z.number().nonnegative().nullable(),
    cycle: z.string().max(64),
    healthy: z.boolean(),
  })
  .strict();

export class HostProtocolFileError extends Schema.TaggedError<HostProtocolFileError>()("HostProtocolFileError", {
  cause: Schema.Defect(),
}) {}
const fileCall = <A>(operation: () => Promise<A>) =>
  Effect.tryPromise({
    try: operation,
    catch: (cause) => new HostProtocolFileError({ cause }),
  });
async function runFileEffect<A>(operation: Effect.Effect<A, HostProtocolFileError>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}

/** Each ancestor is immutable to tenants. Never accept a symlink as a directory. */
export function verifyHostDirectory(path: string, hostUid = 0): Promise<void> {
  return runFileEffect(verifyHostDirectoryEffect(path, hostUid));
}
export const verifyHostDirectoryEffect: (path: string, hostUid?: number) => Effect.Effect<void, HostProtocolFileError> =
  Effect.fn("HostFiles.verifyDirectory")(function* (
    path: string,
    hostUid = 0,
  ): Effect.fn.Return<void, HostProtocolFileError> {
    const absolute = resolve(path);
    const parent = dirname(absolute);
    if (parent !== absolute) yield* verifyHostDirectoryEffect(parent, hostUid);
    const info = yield* fileCall(() => lstat(absolute));
    // Tests use a private directory below the OS temporary directory. Production always uses UID 0.
    if (!info.isDirectory() || (info.uid !== 0 && info.uid !== hostUid) || (info.mode & 0o022) !== 0) {
      return yield* new HostProtocolFileError({
        cause: new Error("Host directory must have a trusted owner and no group or public write permission."),
      });
    }
  });

export function readOwnedJson<T>(path: string, uid: number, schema: z.ZodType<T>): Promise<T> {
  return runFileEffect(readOwnedJsonEffect(path, uid, schema));
}
export const readOwnedJsonEffect = Effect.fn("HostFiles.readOwnedJson")(function* <T>(
  path: string,
  uid: number,
  schema: z.ZodType<T>,
): Effect.fn.Return<T, HostProtocolFileError> {
  return yield* Effect.acquireUseRelease(
    fileCall(() => open(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK)),
    (handle) =>
      Effect.gen(function* () {
        const info = yield* fileCall(() => handle.stat());
        if (!info.isFile() || info.uid !== uid || info.nlink !== 1 || (info.mode & 0o022) !== 0 || info.size > 8192) {
          return yield* new HostProtocolFileError({
            cause: new Error("Invalid host protocol file ownership, type, permissions or size."),
          });
        }
        // Bound the read even if the tenant grows its file after fstat.
        const buffer = Buffer.alloc(8193);
        const { bytesRead } = yield* fileCall(() => handle.read(buffer, 0, buffer.length, 0));
        if (bytesRead > 8192)
          return yield* new HostProtocolFileError({ cause: new Error("Host protocol file is too large.") });
        return yield* Effect.try({
          try: () => schema.parse(JSON.parse(buffer.subarray(0, bytesRead).toString("utf8"))),
          catch: (cause) => new HostProtocolFileError({ cause }),
        });
      }),
    (handle) => fileCall(() => handle.close()),
  );
});

/** Rename replaces the directory entry; it never opens an existing destination or symlink. */
export function writeProtocolJson(
  path: string,
  value: HostUpdateState | HostTenantStatus | HostManagerConfig,
): Promise<void> {
  return runFileEffect(writeProtocolJsonEffect(path, value));
}
export const writeProtocolJsonEffect = Effect.fn("HostFiles.writeProtocolJson")(function* (
  path: string,
  value: HostUpdateState | HostTenantStatus | HostManagerConfig,
): Effect.fn.Return<void, HostProtocolFileError> {
  yield* Effect.acquireUseRelease(
    Effect.sync(() => join(dirname(path), `.write-${randomUUID()}`)),
    (temporary) =>
      Effect.gen(function* () {
        yield* Effect.acquireUseRelease(
          fileCall(() => open(temporary, "wx", 0o644)),
          (handle) =>
            Effect.gen(function* () {
              yield* fileCall(() => handle.chmod(0o644));
              yield* fileCall(() => handle.writeFile(`${JSON.stringify(value)}\n`));
              yield* fileCall(() => handle.sync());
            }),
          (handle) => fileCall(() => handle.close()),
        );
        yield* fileCall(() => rename(temporary, path));
        yield* Effect.acquireUseRelease(
          fileCall(() => open(dirname(path), constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW)),
          (parent) => fileCall(() => parent.sync()),
          (parent) => fileCall(() => parent.close()),
        );
      }),
    // A successful rename already removes this path. Other cleanup failures remain visible.
    (temporary) =>
      fileCall(() => unlink(temporary)).pipe(
        Effect.catch((failure) => (isMissingFileError(failure.cause) ? Effect.void : Effect.fail(failure))),
      ),
  );
});

export function readHostConfig(directory = HOST_MANAGER_DIRECTORY, hostUid = 0): Promise<HostManagerConfig | null> {
  return runFileEffect(readHostConfigEffect(directory, hostUid));
}
export const readHostConfigEffect = Effect.fn("HostFiles.readConfig")(function* (
  directory = HOST_MANAGER_DIRECTORY,
  hostUid = 0,
): Effect.fn.Return<HostManagerConfig | null, HostProtocolFileError> {
  return yield* Effect.gen(function* () {
    yield* verifyHostDirectoryEffect(directory, hostUid);
    return yield* readOwnedJsonEffect(join(directory, "config.json"), hostUid, hostConfigSchema);
  }).pipe(Effect.catch((failure) => (isMissingFileError(failure.cause) ? Effect.succeed(null) : Effect.fail(failure))));
});

export function verifyTenantDirectory(directory: string, uid: number, hostUid = 0): Promise<string> {
  return runFileEffect(verifyTenantDirectoryEffect(directory, uid, hostUid));
}
export const verifyTenantDirectoryEffect = Effect.fn("HostFiles.verifyTenantDirectory")(function* (
  directory: string,
  uid: number,
  hostUid = 0,
): Effect.fn.Return<string, HostProtocolFileError> {
  yield* verifyHostDirectoryEffect(join(directory, "tenants"), hostUid);
  const path = join(directory, "tenants", String(uid));
  const info = yield* fileCall(() => lstat(path));
  // The parent is root-owned, so a tenant cannot replace this directory with a symlink.
  if (!info.isDirectory() || info.uid !== uid || (info.mode & 0o077) !== 0) {
    return yield* new HostProtocolFileError({ cause: new Error("Invalid tenant status directory.") });
  }
  return path;
});
