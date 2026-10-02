import { randomUUID } from "node:crypto";
import { type FileHandle, mkdir, open, rm } from "node:fs/promises";
import { join } from "node:path";
import type { BrowserRecordingArtifact } from "@openbot/contracts/ipc";
import { Effect, Result, Schema } from "effect";
import { BrowserWindow, type WebContents } from "electron";
import {
  type BrowserOperationError,
  browserCall,
  browserFailure,
  browserSync,
  runBrowserEffect,
} from "./browser-effects";

const MAX_RECORDING_MS = 5 * 60 * 1_000;
const MAX_RECORDING_BYTES = 100 * 1024 * 1024;
const RECORDING_STOP_BYTES = MAX_RECORDING_BYTES - 10 * 1024 * 1024;
const MAX_CONCURRENT_RECORDINGS = 2;
const MAX_AGGREGATE_RECORDING_BYTES = 200 * 1024 * 1024;
const stoppedReasonSchema = Schema.Literals(["requested", "duration-limit", "size-limit", "tab-closed", "error"]);
const recorderResultSchema = Schema.Struct({
  durationMs: Schema.Number,
  reason: stoppedReasonSchema,
  error: Schema.optional(Schema.NullOr(Schema.String)),
});
const recorderStartErrorSchema = Schema.Struct({
  __openbotRecorderError: Schema.Literal(true),
  name: Schema.String,
  message: Schema.String,
});

interface RecorderSession {
  tabId: string;
  window: BrowserWindow;
  startedAt: number;
  path: string;
  file: FileHandle;
  bytes: number;
  writeQueue: Promise<void>;
  writeError: Error | null;
  stoppedReason: BrowserRecordingArtifact["stoppedReason"] | null;
  finalizing: Promise<BrowserRecordingArtifact> | null;
  discarding: boolean;
}

export class BrowserRecorder {
  readonly #downloadsRoot: string;
  readonly #sessions = new Map<string, RecorderSession>();
  readonly #artifacts = new Map<string, BrowserRecordingArtifact>();
  readonly #errors = new Map<string, Error>();
  readonly #starting = new Set<string>();
  readonly #onStateChanged: (tabId: string, recording: boolean) => void;
  readonly #maxRecordingMs: number;
  readonly #maxConcurrentRecordings: number;
  readonly #maxAggregateBytes: number;

  constructor(
    downloadsRoot: string,
    onStateChanged: (tabId: string, recording: boolean) => void,
    options: { maxRecordingMs?: number; maxConcurrentRecordings?: number; maxAggregateBytes?: number } = {},
  ) {
    this.#downloadsRoot = downloadsRoot;
    this.#onStateChanged = onStateChanged;
    this.#maxRecordingMs = options.maxRecordingMs ?? MAX_RECORDING_MS;
    this.#maxConcurrentRecordings = options.maxConcurrentRecordings ?? MAX_CONCURRENT_RECORDINGS;
    this.#maxAggregateBytes = options.maxAggregateBytes ?? MAX_AGGREGATE_RECORDING_BYTES;
  }

  isRecording(tabId: string): boolean {
    return this.#sessions.get(tabId)?.stoppedReason === null;
  }

  start(tabId: string, contents: WebContents): Promise<void> {
    return runBrowserEffect(this.#startEffect(tabId, contents));
  }

  readonly #startEffect = Effect.fn("BrowserRecorder.start")(function* (
    this: BrowserRecorder,
    tabId: string,
    contents: WebContents,
  ): Effect.fn.Return<void, BrowserOperationError> {
    const existing = this.#sessions.get(tabId);
    if (existing) {
      const finalization = existing.finalizing;
      if (!finalization) return yield* browserFailure(new Error("This browser tab already has a recording."));
      yield* browserCall(() => finalization.catch(() => undefined));
    }
    if (this.#artifacts.has(tabId)) {
      return yield* browserFailure(
        new Error("Retrieve the completed recording with recording_stop before starting another recording."),
      );
    }
    let startReserved = false;
    return yield* Effect.gen({ self: this }, function* () {
      yield* browserSync(() => this.#reserveStart(tabId));
      startReserved = true;
      if (contents.isDestroyed()) return yield* browserFailure(new Error("Browser tab was closed."));
      this.#errors.delete(tabId);
      yield* browserCall(() => mkdir(this.#downloadsRoot, { recursive: true }));
      const startedAt = Date.now();
      const path = join(
        this.#downloadsRoot,
        `openbot-browser-${new Date(startedAt).toISOString().replace(/[:.]/g, "-")}-${randomUUID().slice(0, 8)}.webm`,
      );
      let retained = false;
      let openedWindow: BrowserWindow | undefined;
      yield* Effect.acquireUseRelease(
        browserCall(() => open(path, "wx", 0o600)),
        (file) =>
          Effect.gen({ self: this }, function* () {
            const recorderPartition = `openbot-recorder-${randomUUID()}`;
            const recorderWindow = yield* Effect.gen({ self: this }, function* () {
              return yield* browserSync(
                () =>
                  new BrowserWindow({
                    show: false,
                    width: 1,
                    height: 1,
                    webPreferences: {
                      sandbox: true,
                      contextIsolation: true,
                      nodeIntegration: false,
                      backgroundThrottling: false,
                      webSecurity: true,
                      partition: recorderPartition,
                    },
                  }),
              );
            }).pipe(
              Effect.catch((operationFailure) =>
                Effect.gen({ self: this }, function* () {
                  const error = operationFailure.cause;
                  return yield* browserFailure(error);
                }),
              ),
            );
            openedWindow = recorderWindow;
            recorderWindow.webContents.session.setPermissionCheckHandler(
              (_webContents, permission) => permission === "media",
            );
            recorderWindow.webContents.session.setPermissionRequestHandler((_webContents, permission, callback) =>
              callback(permission === "media" || permission === "display-capture"),
            );
            recorderWindow.webContents.session.setDisplayMediaRequestHandler(
              (_request, callback) => callback({ video: contents.mainFrame }),
              { useSystemPicker: false },
            );
            const session: RecorderSession = {
              tabId,
              window: recorderWindow,
              startedAt,
              path,
              file,
              bytes: 0,
              writeQueue: Promise.resolve(),
              writeError: null,
              stoppedReason: null,
              finalizing: null,
              discarding: false,
            };
            this.#sessions.set(tabId, session);
            recorderWindow.on("closed", () => {
              if (this.#sessions.get(tabId) !== session) return;
              void this.#discardSession(session, false);
            });
            recorderWindow.webContents.on("page-title-updated", (_event, title) => {
              if (!title.startsWith("openbot-recorder:stopped:")) return;
              const reason = title.slice("openbot-recorder:stopped:".length);
              session.stoppedReason = parseStoppedReason(reason);
              this.#onStateChanged(tabId, false);
              if (session.discarding || session.finalizing) return;
              session.finalizing = this.#finalizeSession(session, session.stoppedReason);
              void session.finalizing.catch(() => undefined);
            });
            yield* Effect.gen({ self: this }, function* () {
              recorderWindow.webContents.session.protocol.handle("https", async (request) => {
                const url = new URL(request.url);
                if (request.method === "POST" && url.pathname === "/chunk") {
                  const chunk = Buffer.from(await request.arrayBuffer());
                  const writing = session.writeQueue.then(() => this.#writeChunk(session, chunk));
                  session.writeQueue = writing.catch(() => undefined);
                  try {
                    await writing;
                    return new Response(null, { status: 204 });
                  } catch (error) {
                    session.writeError = error instanceof Error ? error : new Error(String(error));
                    return new Response("Unable to save recording chunk.", { status: 500 });
                  }
                }
                return new Response("<!doctype html><title>openbot-recorder:ready</title>", {
                  headers: {
                    "content-type": "text/html; charset=utf-8",
                    "content-security-policy": "default-src 'none'; connect-src 'self'",
                  },
                });
              });
              yield* browserCall(() => recorderWindow.loadURL("https://recorder.openbot.invalid/"));
              const sourceId = contents.getMediaSourceId(recorderWindow.webContents);
              const startResult = yield* browserCall(() =>
                recorderWindow.webContents.executeJavaScript(startScript(sourceId, this.#maxRecordingMs), true),
              );
              const started = Schema.decodeUnknownResult(recorderStartErrorSchema)(startResult);
              if (Result.isSuccess(started))
                return yield* browserFailure(new Error(`${started.success.name}: ${started.success.message}`));
              this.#onStateChanged(tabId, true);
            }).pipe(
              Effect.catch((operationFailure) =>
                Effect.gen({ self: this }, function* () {
                  const error = operationFailure.cause;
                  return yield* browserFailure(new Error(`Unable to start browser recording: ${String(error)}`));
                }),
              ),
            );
            retained = true;
          }),
        (file) =>
          Effect.gen({ self: this }, function* () {
            if (retained) return;
            this.#sessions.delete(tabId);
            const window = openedWindow;
            if (window && !window.isDestroyed()) window.destroy();
            yield* browserCall(() => file.close()).pipe(Effect.catch(() => Effect.void));
            yield* browserCall(() => rm(path, { force: true })).pipe(Effect.catch(() => Effect.void));
          }),
      );
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (startReserved) this.#starting.delete(tabId);
        }),
      ),
    );
  });

  stop(
    tabId: string,
    requestedReason: BrowserRecordingArtifact["stoppedReason"] = "requested",
  ): Promise<BrowserRecordingArtifact> {
    return runBrowserEffect(this.#stopEffect(tabId, requestedReason));
  }

  readonly #stopEffect = Effect.fn("BrowserRecorder.stop")(function* (
    this: BrowserRecorder,
    tabId: string,
    requestedReason: BrowserRecordingArtifact["stoppedReason"] = "requested",
  ): Effect.fn.Return<BrowserRecordingArtifact, BrowserOperationError> {
    const artifact = this.#artifacts.get(tabId);
    if (artifact) {
      this.#artifacts.delete(tabId);
      return artifact;
    }
    const finalizationError = this.#errors.get(tabId);
    if (finalizationError) {
      this.#errors.delete(tabId);
      return yield* browserFailure(finalizationError);
    }
    const session = this.#sessions.get(tabId);
    if (!session) return yield* browserFailure(new Error("This browser tab is not being recorded."));
    session.finalizing ??= this.#finalizeSession(session, requestedReason);
    const finalization = session.finalizing;
    const result = yield* browserCall(() => finalization);
    this.#artifacts.delete(tabId);
    return result;
  });

  discard(tabId: string, reason: BrowserRecordingArtifact["stoppedReason"] = "tab-closed"): Promise<void> {
    return runBrowserEffect(this.#discardEffect(tabId, reason));
  }

  readonly #discardEffect = Effect.fn("BrowserRecorder.discard")(function* (
    this: BrowserRecorder,
    tabId: string,
    reason: BrowserRecordingArtifact["stoppedReason"] = "tab-closed",
  ): Effect.fn.Return<void, BrowserOperationError> {
    const session = this.#sessions.get(tabId);
    if (session) {
      const finalization = session.finalizing;
      if (finalization) {
        yield* browserCall(() => finalization.catch(() => undefined));
      } else {
        session.discarding = true;
        yield* Effect.gen({ self: this }, function* () {
          yield* browserCall(() => session.window.webContents.executeJavaScript(stopScript(reason), true));
        }).pipe(Effect.catch(() => Effect.void));
        yield* this.#discardSessionEffect(session, true);
      }
    }
    this.#artifacts.delete(tabId);
    this.#errors.delete(tabId);
  });

  destroy(): Promise<void> {
    return runBrowserEffect(this.#destroyEffect());
  }

  readonly #destroyEffect = Effect.fn("BrowserRecorder.destroy")(function* (
    this: BrowserRecorder,
  ): Effect.fn.Return<void, BrowserOperationError> {
    const tabIds = new Set([...this.#sessions.keys(), ...this.#artifacts.keys(), ...this.#errors.keys()]);
    yield* browserCall(() => Promise.allSettled([...tabIds].map((tabId) => this.discard(tabId, "tab-closed"))));
    this.#artifacts.clear();
    this.#errors.clear();
  });

  #reserveStart(tabId: string): void {
    if (this.#starting.has(tabId) || this.#sessions.has(tabId)) {
      throw new Error("This browser tab already has a recording.");
    }
    const activeRecordings = new Set([...this.#sessions.keys(), ...this.#starting]).size;
    if (activeRecordings >= this.#maxConcurrentRecordings) {
      throw new Error(`At most ${this.#maxConcurrentRecordings} browser recordings can run at the same time.`);
    }
    const artifactBytes = [...this.#artifacts.values()].reduce((total, artifact) => total + artifact.bytes, 0);
    const reservedBytes = activeRecordings * MAX_RECORDING_BYTES;
    if (artifactBytes + reservedBytes + MAX_RECORDING_BYTES > this.#maxAggregateBytes) {
      throw new Error(`Browser recordings can use up to ${this.#maxAggregateBytes} bytes in total.`);
    }
    this.#starting.add(tabId);
  }

  #writeChunk(session: RecorderSession, chunk: Buffer): Promise<void> {
    return runBrowserEffect(this.#writeChunkEffect(session, chunk));
  }

  readonly #writeChunkEffect = Effect.fn("BrowserRecorder.writeChunk")(function* (
    this: BrowserRecorder,
    session: RecorderSession,
    chunk: Buffer,
  ): Effect.fn.Return<void, BrowserOperationError> {
    if (session.writeError) return yield* browserFailure(session.writeError);
    if (chunk.length === 0) return;
    if (session.bytes + chunk.length > MAX_RECORDING_BYTES)
      return yield* browserFailure(new Error("Recorder output exceeds 100 MB."));
    let offset = 0;
    while (offset < chunk.length) {
      const { bytesWritten } = yield* browserCall(() => session.file.write(chunk, offset, chunk.length - offset, null));
      if (bytesWritten <= 0)
        return yield* browserFailure(new Error("Recorder could not write the complete video chunk."));
      offset += bytesWritten;
    }
    session.bytes += chunk.length;
  });

  #finalizeSession(
    session: RecorderSession,
    requestedReason: BrowserRecordingArtifact["stoppedReason"],
  ): Promise<BrowserRecordingArtifact> {
    return runBrowserEffect(this.#finalizeSessionEffect(session, requestedReason));
  }

  readonly #finalizeSessionEffect = Effect.fn("BrowserRecorder.finalizeSession")(function* (
    this: BrowserRecorder,
    session: RecorderSession,
    requestedReason: BrowserRecordingArtifact["stoppedReason"],
  ): Effect.fn.Return<BrowserRecordingArtifact, BrowserOperationError> {
    return yield* Effect.gen({ self: this }, function* () {
      const metadata = yield* browserCall(() =>
        session.window.webContents.executeJavaScript(stopScript(requestedReason), true),
      );
      const result = Schema.decodeUnknownResult(recorderResultSchema)(metadata);
      if (Result.isFailure(result))
        return yield* browserFailure(new Error("Recorder returned invalid video metadata."));
      yield* browserCall(() => session.writeQueue);
      if (session.writeError) return yield* browserFailure(session.writeError);
      if (result.success.error) return yield* browserFailure(new Error(`Recorder failed: ${result.success.error}`));
      if (session.bytes === 0) return yield* browserFailure(new Error("Recorder produced an empty video."));
      yield* browserCall(() => session.file.sync());
      yield* browserCall(() => session.file.close());
      const artifact: BrowserRecordingArtifact = {
        path: session.path,
        mimeType: "video/webm",
        bytes: session.bytes,
        durationMs: Math.max(0, result.success.durationMs),
        stoppedReason: session.stoppedReason ?? result.success.reason,
      };
      this.#artifacts.set(session.tabId, artifact);
      return artifact;
    })
      .pipe(
        Effect.catch((operationFailure) =>
          Effect.gen({ self: this }, function* () {
            const error = operationFailure.cause;
            const failure = error instanceof Error ? error : new Error(String(error));
            this.#errors.set(session.tabId, failure);
            yield* browserCall(() => session.file.close().catch(() => undefined));
            yield* browserCall(() => rm(session.path, { force: true }).catch(() => undefined));
            return yield* browserFailure(failure);
          }),
        ),
      )
      .pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (this.#sessions.get(session.tabId) === session) this.#sessions.delete(session.tabId);
            if (!session.window.isDestroyed()) session.window.destroy();
            this.#onStateChanged(session.tabId, false);
          }),
        ),
      );
  });

  #discardSession(session: RecorderSession, destroyWindow: boolean): Promise<void> {
    return runBrowserEffect(this.#discardSessionEffect(session, destroyWindow));
  }

  readonly #discardSessionEffect = Effect.fn("BrowserRecorder.discardSession")(function* (
    this: BrowserRecorder,
    session: RecorderSession,
    destroyWindow: boolean,
  ): Effect.fn.Return<void, BrowserOperationError> {
    if (this.#sessions.get(session.tabId) === session) this.#sessions.delete(session.tabId);
    yield* browserCall(() => session.writeQueue.catch(() => undefined));
    yield* browserCall(() => session.file.close().catch(() => undefined));
    yield* browserCall(() => rm(session.path, { force: true }).catch(() => undefined));
    if (destroyWindow && !session.window.isDestroyed()) session.window.destroy();
    this.#onStateChanged(session.tabId, false);
  });
}

function startScript(sourceId: string, maxRecordingMs: number): string {
  return `(async () => {
    let stage = 'initialization';
    try {
    const STOP_BYTES = ${RECORDING_STOP_BYTES};
    const MAX_MS = ${maxRecordingMs};
    const sourceId = ${JSON.stringify(sourceId)};
    stage = 'getUserMedia';
    let stream;
    try {
      stream = await navigator.mediaDevices.getUserMedia({
        audio: false,
        video: { mandatory: { chromeMediaSource: 'tab', chromeMediaSourceId: sourceId } },
      });
    } catch {
      stage = 'getDisplayMedia';
      stream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: false });
    }
    const mimeType = MediaRecorder.isTypeSupported('video/webm;codecs=vp9')
      ? 'video/webm;codecs=vp9'
      : MediaRecorder.isTypeSupported('video/webm;codecs=vp8')
        ? 'video/webm;codecs=vp8'
        : 'video/webm';
    stage = 'MediaRecorder';
    const recorder = new MediaRecorder(stream, { mimeType, videoBitsPerSecond: 2500000 });
    const state = {
      bytes: 0, startedAt: performance.now(), reason: null, stopped: null, pending: Promise.resolve(), error: null,
      stop(reason) {
        if (this.reason === null) this.reason = reason;
        if (recorder.state !== 'inactive') recorder.stop();
      },
    };
    state.stopped = new Promise(resolve => recorder.addEventListener('stop', async () => {
      await state.pending;
      stream.getTracks().forEach(track => track.stop());
      const reason = state.error ? 'error' : (state.reason || 'requested');
      const result = { durationMs: Math.round(performance.now() - state.startedAt), reason, error: state.error };
      document.title = 'openbot-recorder:stopped:' + reason;
      resolve(result);
    }, { once: true }));
    recorder.addEventListener('dataavailable', event => {
      if (!event.data || event.data.size === 0) return;
      state.bytes += event.data.size;
      state.pending = state.pending.then(async () => {
        const response = await fetch('/chunk', { method: 'POST', body: event.data });
        if (!response.ok) throw new Error(await response.text() || 'Unable to save recording chunk.');
      }).catch(error => {
        state.error = String(error?.message || error);
        state.stop('error');
      });
      if (state.bytes >= STOP_BYTES) state.stop('size-limit');
    });
    globalThis.__openbotRecorder = state;
    stage = 'start'; recorder.start(1000);
    setTimeout(() => state.stop('duration-limit'), MAX_MS);
    return true;
    } catch (error) {
      return { __openbotRecorderError: true, name: String(error?.name || 'Error'), message: stage + ': ' + String(error?.message || error) };
    }
  })()`;
}

function stopScript(reason: BrowserRecordingArtifact["stoppedReason"]): string {
  return `(async () => {
    const state = globalThis.__openbotRecorder;
    if (!state) throw new Error('Recorder is not initialized.');
    state.stop(${JSON.stringify(reason)});
    return state.stopped;
  })()`;
}

function parseStoppedReason(value: string): BrowserRecordingArtifact["stoppedReason"] {
  return value === "duration-limit" || value === "size-limit" || value === "tab-closed" || value === "error"
    ? value
    : "requested";
}
