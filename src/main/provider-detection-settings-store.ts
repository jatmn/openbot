// Where the local provider scan looks, and the rows the user hid. Main reads it at each scan, so it
// is held in memory after `load`.
//
// The file holds addresses and folder paths, never a key. A file that this build cannot read is not
// replaced: a newer build may have written it, so writes are refused until it is readable again.

import {
  DEFAULT_PROVIDER_DETECTION_SETTINGS,
  isProviderDetectionSettings,
  type ProviderDetectionSettings,
} from "@openbot/contracts/ipc";
import { isDynamicRecord } from "@openbot/contracts/runtime-values";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Result } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import { readPreferenceFile, runPreference, writePreferenceFile } from "./preference-file";

export const PROVIDER_DETECTION_SETTINGS_FILE = "openbot-provider-detection-v1.json";

function copy(settings: ProviderDetectionSettings): ProviderDetectionSettings {
  return {
    enabled: settings.enabled,
    addresses: [...settings.addresses],
    folders: [...settings.folders],
    hiddenIds: [...settings.hiddenIds],
  };
}

export class ProviderDetectionSettingsStore {
  readonly #path: string;
  #settings: ProviderDetectionSettings = copy(DEFAULT_PROVIDER_DETECTION_SETTINGS);
  #readOnly = false;
  #pendingWrite: Promise<unknown> = Promise.resolve();

  constructor(path: string) {
    this.#path = path;
  }

  /** A missing file is a first run. Bad JSON or another version keeps the defaults, read only. */
  load(): Promise<void> {
    return runPreference(
      Effect.gen({ self: this }, function* () {
        const loaded = yield* Effect.result(
          readPreferenceFile(this.#path, (parsed) => {
            if (isDynamicRecord(parsed) && parsed.version === 1 && isProviderDetectionSettings(parsed.settings))
              return copy(parsed.settings);
            return null;
          }),
        );
        if (Result.isFailure(loaded)) {
          if (isMissingFileError(loaded.failure.cause)) return;
          if (!(loaded.failure.cause instanceof SyntaxError)) return yield* loaded.failure;
        } else if (loaded.success) {
          this.#settings = loaded.success;
          return;
        }
        this.#readOnly = true;
      }),
    );
  }

  get(): ProviderDetectionSettings {
    return copy(this.#settings);
  }

  /** Writes are chained, so a queued write never lands before the one that came before it. */
  set(settings: ProviderDetectionSettings): Promise<ProviderDetectionSettings> {
    if (this.#readOnly) return Promise.reject(new Error(sourceText("error.provider.detectionSettingsReadOnly")));
    const next = copy(settings);
    const write = this.#pendingWrite.then(() =>
      runPreference(
        Effect.gen({ self: this }, function* () {
          yield* writePreferenceFile(this.#path, { version: 1, settings: next }, { createDirectory: true });
          this.#settings = next;
          return this.get();
        }).pipe(Effect.uninterruptible),
      ),
    );
    this.#pendingWrite = write.catch(() => undefined);
    return write;
  }
}
