// The 1Password service account token of this computer, encrypted at rest by the operating system.

import { readFile, rm } from "node:fs/promises";
import { sourceText } from "@openbot/i18n/source";
import { z } from "zod";
import { writeJsonFileAtomically } from "../backend/atomic-json-file";
import type { SecretCipher } from "./provider-credential-store";

const onePasswordConnectorRecordSchema = z.object({
  /** The service account token. It reads only the vaults that the user shared with OpenBot. */
  token: z.string().min(1),
  /** The account that the CLI created the service account in, or null for a pasted token. */
  accountId: z.string().min(1).nullable(),
  connectedAt: z.number(),
});

export type OnePasswordConnectorRecord = z.infer<typeof onePasswordConnectorRecordSchema>;

/**
 * `version` is checked, not tolerated: an envelope that this build cannot read is reported as
 * unreadable. The file stays on disk until a new connection replaces it.
 */
const envelopeSchema = z.object({ version: z.literal(1), record: z.string() });

/** One token is small; a larger file is not one that this app wrote. */
const MAX_ENVELOPE_BYTES = 64 * 1024;

/**
 * Reads and writes the 1Password connection. The decrypted record is kept in memory only.
 *
 * `electron` is not imported: the cipher arrives as an argument, so a test can use a fake one.
 * Writes run one at a time, because a connect and a disconnect can arrive together.
 */
export class OnePasswordConnectorStore {
  readonly #path: string;
  readonly #cipher: SecretCipher;
  #record: OnePasswordConnectorRecord | null = null;
  #queue: Promise<void> = Promise.resolve();

  constructor(path: string, cipher: SecretCipher) {
    this.#path = path;
    this.#cipher = cipher;
  }

  /** Reads the file. Returns the error when the file is there but cannot be read. */
  async load(): Promise<Error | null> {
    this.#record = null;
    try {
      this.#record = await this.#read();
      return null;
    } catch (error) {
      return error instanceof Error ? error : new Error(sourceText("error.connector.onePasswordFileUnreadable"));
    }
  }

  read(): OnePasswordConnectorRecord | null {
    return this.#record;
  }

  write(record: OnePasswordConnectorRecord): Promise<void> {
    return this.#enqueue(async () => {
      const encrypted = this.#cipher.encrypt(JSON.stringify(record)).toString("base64");
      // Write then rename, so a crash leaves the previous connection readable.
      await writeJsonFileAtomically(this.#path, { version: 1, record: encrypted }, { createDirectory: true });
      this.#record = record;
    });
  }

  clear(): Promise<void> {
    return this.#enqueue(async () => {
      await rm(this.#path, { force: true });
      this.#record = null;
    });
  }

  #enqueue(change: () => Promise<void>): Promise<void> {
    const result = this.#queue.then(change, change);
    this.#queue = result.catch(() => undefined);
    return result;
  }

  async #read(): Promise<OnePasswordConnectorRecord | null> {
    let source: string;
    try {
      source = await readFile(this.#path, "utf8");
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") return null;
      throw error;
    }
    if (source.length > MAX_ENVELOPE_BYTES) throw new Error(sourceText("error.connector.onePasswordFileTooLarge"));
    try {
      const envelope = envelopeSchema.parse(JSON.parse(source));
      return onePasswordConnectorRecordSchema.parse(
        JSON.parse(this.#cipher.decrypt(Buffer.from(envelope.record, "base64"))),
      );
    } catch {
      throw new Error(sourceText("error.connector.onePasswordFileUnreadable"));
    }
  }
}
