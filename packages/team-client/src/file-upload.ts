import { sha256 } from "@noble/hashes/sha2.js";
import {
  decodeTeamProtocolV2FileControlFrame,
  encodeTeamProtocolV2FileChunk,
  encodeTeamProtocolV2Frame,
  TEAM_PROTOCOL_V2_MAX_FILE_BYTES,
} from "@openbot/contracts/team-protocol/v2";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Schema } from "effect";
import { runTeamEffect } from "./effect-boundary";

export class FileTransferError extends Schema.TaggedError<FileTransferError>()("FileTransferError", {
  message: Schema.String,
}) {}

export function fileTransferError(error: unknown): FileTransferError {
  return error instanceof FileTransferError
    ? error
    : new FileTransferError({
        message: error instanceof Error ? error.message : String(error),
      });
}

// Native/DOM bridge copies Base64 strings. Keep its working set below the host's larger file limit.
export const MOBILE_ATTACHMENT_BYTES = 10 * 1024 * 1024;
export interface RemoteFileUpload {
  name: string;
  mimeType: string;
  base64: string;
  /** A larger cap for one kind of upload, such as an agent export. The host's file limit still applies. */
  maxBytes?: number;
}

/** The slowest upload rate a transfer waits for, in bytes per millisecond (256 KB/s). */
const MINIMUM_UPLOAD_RATE = 256;

export function createRemoteFileSender(send: (data: string | ArrayBuffer) => Promise<void>, createId: () => string) {
  const pending = new Map<string, { opened: () => void; reject: (error: Error) => void; error: Error | null }>();
  const sendFrame = (data: string | ArrayBuffer) =>
    Effect.tryPromise({
      try: () => send(data),
      catch: fileTransferError,
    });
  const upload = Effect.fn("RemoteFileSender.upload")(function* (
    input: RemoteFileUpload,
    onProgress?: (sent: number, total: number) => void,
  ) {
    const maxBytes = Math.min(input.maxBytes ?? MOBILE_ATTACHMENT_BYTES, TEAM_PROTOCOL_V2_MAX_FILE_BYTES);
    if (input.base64.length > Math.ceil(maxBytes / 3) * 4)
      return yield* new FileTransferError({ message: sourceText("error.remote.attachmentTooLarge") });
    const decoded = yield* Effect.try({ try: () => atob(input.base64), catch: fileTransferError });
    const bytes = Uint8Array.from(decoded, (character) => character.charCodeAt(0));
    if (bytes.length > maxBytes)
      return yield* new FileTransferError({ message: sourceText("error.remote.attachmentTooLarge") });
    if (pending.size !== 0) return yield* new FileTransferError({ message: sourceText("error.remote.attachmentBusy") });
    const transferId = createId();
    let opened = () => {};
    let reject = (_error: Error) => {};
    const acknowledged = new Promise<void>((resolve, fail) => {
      opened = resolve;
      reject = fail;
    });
    const transfer: { opened: () => void; reject: (error: Error) => void; error: Error | null } = {
      opened,
      reject,
      error: null,
    };
    pending.set(transferId, transfer);
    // A file of up to about 15 MB has one minute; a larger one has the time 256 KB/s needs.
    const timer = setTimeout(
      () => {
        transfer.error = new Error(sourceText("error.remote.uploadTimeout"));
        reject(transfer.error);
      },
      Math.max(60_000, bytes.length / MINIMUM_UPLOAD_RATE),
    );
    return yield* Effect.gen(function* () {
      // Observe rejection before starting I/O, including a synchronous native disconnect.
      yield* Effect.all(
        [
          Effect.tryPromise({ try: () => acknowledged, catch: fileTransferError }),
          sendFrame(
            encodeTeamProtocolV2Frame({
              version: 2,
              type: "file-open",
              transferId,
              name: input.name,
              mimeType: input.mimeType,
              size: bytes.length,
              sha256: Array.from(sha256(bytes), (byte) => byte.toString(16).padStart(2, "0")).join(""),
            }),
          ),
        ],
        { concurrency: "unbounded" },
      );
      for (let offset = 0; offset < bytes.length; offset += 60 * 1024) {
        if (transfer.error) return yield* fileTransferError(transfer.error);
        const chunk = encodeTeamProtocolV2FileChunk({
          transferId,
          offset,
          bytes: bytes.slice(offset, offset + 60 * 1024),
        });
        yield* sendFrame(new Uint8Array(chunk).buffer);
        onProgress?.(Math.min(offset + 60 * 1024, bytes.length), bytes.length);
      }
      if (transfer.error) return yield* fileTransferError(transfer.error);
      yield* sendFrame(encodeTeamProtocolV2Frame({ version: 2, type: "file-complete", transferId }));
      return transferId;
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          clearTimeout(timer);
          pending.delete(transferId);
        }),
      ),
    );
  });
  const cancelUpload = Effect.fn("RemoteFileSender.cancel")(function* () {
    for (const [transferId, transfer] of pending) {
      transfer.error = new FileTransferError({ message: sourceText("error.remote.uploadCancelled") });
      transfer.reject(transfer.error);
      yield* sendFrame(encodeTeamProtocolV2Frame({ version: 2, type: "file-cancel", transferId, reason: "Cancelled" }));
    }
  });
  return {
    uploadEffect: upload,
    cancelUploadEffect: cancelUpload,
    cancelUpload: () => runTeamEffect(cancelUpload()),
    receive(data: string) {
      const frame = decodeTeamProtocolV2FileControlFrame(data);
      const transfer = pending.get(frame.transferId);
      if (!transfer) return;
      if (frame.type === "file-ack") transfer.opened();
      if (frame.type === "file-cancel") {
        transfer.error = new Error(sourceText("error.remote.attachmentRejected"));
        transfer.reject(transfer.error);
      }
    },
    cancel() {
      for (const transfer of pending.values()) {
        transfer.error = new Error(sourceText("error.remote.attachmentConnectionClosed"));
        transfer.reject(transfer.error);
      }
      pending.clear();
    },
    upload: (input: RemoteFileUpload, onProgress?: (sent: number, total: number) => void) =>
      runTeamEffect(upload(input, onProgress)),
  };
}
