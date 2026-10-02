import type { UpdatePreference, UpdatePreferenceChange } from "@openbot/contracts/ipc";
import { isBoolean, isDynamicRecord } from "@openbot/contracts/runtime-values";
import { Effect } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import { readPreferenceFile, runPreference, writePreferenceFile } from "./preference-file";

const DEFAULT_PREFERENCE: UpdatePreference = { autoDownload: true, allowRemoteUpdates: true, autoInstall: false };

// `allowRemoteUpdates` and `autoInstall` came later in the same version 1 file. A file without them
// is one written before remote updates existed, and an older app that reads a newer file ignores them.
export function readUpdatePreference(path: string): Promise<UpdatePreference> {
  return runPreference(readUpdatePreferenceEffect(path));
}
const readUpdatePreferenceEffect = Effect.fn("readUpdatePreference")((path: string) =>
  readPreferenceFile(path, (parsed): UpdatePreference => {
    if (!isDynamicRecord(parsed) || parsed.version !== 1 || !isBoolean(parsed.autoDownload)) {
      return { ...DEFAULT_PREFERENCE };
    }
    return {
      autoDownload: parsed.autoDownload,
      allowRemoteUpdates: isBoolean(parsed.allowRemoteUpdates)
        ? parsed.allowRemoteUpdates
        : DEFAULT_PREFERENCE.allowRemoteUpdates,
      autoInstall: isBoolean(parsed.autoInstall) ? parsed.autoInstall : DEFAULT_PREFERENCE.autoInstall,
    };
  }).pipe(
    Effect.catch((failure) => {
      const error = failure.cause;
      if (isMissingFileError(error) || error instanceof SyntaxError) return Effect.succeed({ ...DEFAULT_PREFERENCE });
      return Effect.fail(failure);
    }),
  ),
);

/**
 * Writes are serialized because each one renames its own temporary file into place. Two quick toggles
 * would otherwise race, and the earlier rename could land last and persist the value the user just
 * turned off. Chaining also keeps the replies in invocation order, so the renderer adopts the latest.
 */
let pendingWrite: Promise<unknown> = Promise.resolve();

export function writeUpdatePreference(path: string, change: UpdatePreferenceChange): Promise<UpdatePreference> {
  const write = pendingWrite.then(
    () => replaceUpdatePreference(path, change),
    () => replaceUpdatePreference(path, change),
  );
  pendingWrite = write.catch(() => undefined);
  return write;
}

/** The read runs inside the chain, so a change to one field never writes back a stale other one. */
function replaceUpdatePreference(path: string, change: UpdatePreferenceChange): Promise<UpdatePreference> {
  return runPreference(
    Effect.gen(function* () {
      const preference = { ...(yield* readUpdatePreferenceEffect(path)), ...change };
      yield* writePreferenceFile(path, { version: 1, ...preference });
      return preference;
    }),
  );
}
