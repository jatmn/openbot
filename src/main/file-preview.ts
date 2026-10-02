import { readFile } from "node:fs/promises";
import { attachmentMimeTypeForName } from "@openbot/contracts/attachment-files";
import { ATTACHMENT_LIMITS } from "@openbot/contracts/input-limits";
import { type FilePreview, filePreviewKindForFile } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Effect } from "effect";
import { attachmentCall, attachmentFailure, runAttachmentEffect } from "../backend/attachment-effects";

export function mimeTypeForName(name: string) {
  return attachmentMimeTypeForName(name);
}

export function filePreviewFromBytes(name: string, bytes: Uint8Array): FilePreview {
  if (bytes.byteLength > ATTACHMENT_LIMITS.fileBytes) throw new Error(sourceText("error.attachment.previewTooLarge"));
  const mimeType = mimeTypeForName(name);
  const kind = filePreviewKindForFile(name, mimeType);
  return { name, size: bytes.byteLength, mimeType, previewKind: kind, bytes: kind === "none" ? null : bytes };
}

export function localFilePreview(path: string, name: string, size: number): Promise<FilePreview> {
  return runAttachmentEffect(localFilePreviewEffect(path, name, size));
}

export const localFilePreviewEffect = Effect.fn("FilePreview.local")(function* (
  path: string,
  name: string,
  size: number,
) {
  if (size > ATTACHMENT_LIMITS.fileBytes)
    return yield* Effect.fail(attachmentFailure(new Error(sourceText("error.attachment.previewTooLarge"))));
  const mimeType = mimeTypeForName(name);
  const kind = filePreviewKindForFile(name, mimeType);
  return {
    name,
    size,
    mimeType,
    previewKind: kind,
    bytes: kind === "none" ? null : new Uint8Array(yield* attachmentCall(() => readFile(path))),
  };
});
