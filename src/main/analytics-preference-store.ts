import type { AnalyticsPreference } from "@openbot/contracts/ipc";
import { isBoolean, isDynamicRecord } from "@openbot/contracts/runtime-values";
import { Effect } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import { readPreferenceFile, runPreference, writePreferenceFile } from "./preference-file";

const DEFAULT_PREFERENCE: AnalyticsPreference = { enabled: true };

export function readAnalyticsPreference(path: string): Promise<AnalyticsPreference> {
  return runPreference(readAnalyticsPreferenceEffect(path));
}
const readAnalyticsPreferenceEffect = Effect.fn("readAnalyticsPreference")((path: string) =>
  readPreferenceFile(path, (parsed): AnalyticsPreference => {
    if (!isDynamicRecord(parsed) || parsed.version !== 1 || !isBoolean(parsed.enabled)) {
      return { enabled: false };
    }
    return { enabled: parsed.enabled };
  }).pipe(
    Effect.catch((failure) => {
      const error = failure.cause;
      if (isMissingFileError(error)) return Effect.succeed({ ...DEFAULT_PREFERENCE });
      if (error instanceof SyntaxError) return Effect.succeed({ enabled: false });
      return Effect.fail(failure);
    }),
  ),
);

export function writeAnalyticsPreference(path: string, enabled: boolean): Promise<AnalyticsPreference> {
  return runPreference(
    Effect.gen(function* () {
      const preference = { enabled };
      yield* writePreferenceFile(path, { version: 1, enabled });
      return preference;
    }),
  );
}
