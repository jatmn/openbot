// The 1Password connection of this computer: a service account that reads the vault the user shares
// with OpenBot. The embedded browser fills logins from it; agents see titles and usernames only.

import { execFile } from "node:child_process";
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { delimiter, join } from "node:path";
import {
  DISCONNECTED_ONEPASSWORD_CONNECTOR,
  type OnePasswordAccount,
  type OnePasswordConnectInput,
  type OnePasswordConnectorStatus,
} from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { createOpenBotLogger, redactText, registerSecretValue, toLogValue } from "@openbot/logging";
import { z } from "zod";
import {
  type PasswordVault,
  type VaultAutofill,
  type VaultLogin,
  type VaultWebsite,
  websiteMatchesOrigin,
} from "../backend/password-vault";
import type { OnePasswordConnectorRecord, OnePasswordConnectorStore } from "./onepassword-connector-store";

const logger = createOpenBotLogger("onepassword-connector");

/** The vault that Connect creates. The user moves the logins that OpenBot may use into it. */
export const ONEPASSWORD_SHARED_VAULT = "Shared with OpenBot";
/** `op service-account create` needs this version. */
const MIN_CLI_VERSION = [2, 18] as const;
/** 1Password can wait for the user to approve the CLI in the desktop app. */
const CLI_TIMEOUT_MS = 2 * 60_000;
/** Service accounts have hourly read limits, so the list of logins is read again only this often. */
const INDEX_MAX_AGE_MS = 5 * 60_000;
/** Each listed login costs one read. A page with more saved logins than this lists the first ones. */
const MAX_LISTED_LOGINS = 10;

const accountListSchema = z.array(
  z.object({ account_uuid: z.string().min(1), email: z.string().optional(), url: z.string().optional() }),
);
const vaultListSchema = z.array(z.object({ id: z.string().min(1), name: z.string() }));
const vaultSchema = z.object({ id: z.string().min(1) });

/** Runs the 1Password CLI and returns its standard output. */
export type OnePasswordCli = (args: string[], signal: AbortSignal) => Promise<string>;

/** The part of the 1Password SDK client this service uses, so a test can pass a fake. */
export interface OnePasswordClient {
  vaults: { list(): Promise<Array<{ id: string; title: string }>> };
  items: {
    list(
      vaultId: string,
    ): Promise<
      Array<{ id: string; title: string; category: string; websites: Array<{ url: string; autofillBehavior: string }> }>
    >;
    get(
      vaultId: string,
      itemId: string,
    ): Promise<{
      fields: Array<{ id: string; fieldType: string; value: string; details?: { type: string; content?: unknown } }>;
    }>;
  };
}

export interface OnePasswordConnectorServiceOptions {
  store: OnePasswordConnectorStore;
  /** Names the service account, so the user can find it on 1Password.com. */
  hostName: string;
  appVersion: string;
  cli?: OnePasswordCli;
  createClient?: (token: string) => Promise<OnePasswordClient>;
  now?: () => number;
}

interface IndexedLogin {
  id: string;
  vaultId: string;
  title: string;
  websites: VaultWebsite[];
}

interface LoginIndex {
  readAt: number;
  vaultNames: string[];
  logins: IndexedLogin[];
}

function autofill(value: string): VaultAutofill {
  if (value === "ExactDomain") return "exact";
  if (value === "Never") return "never";
  return "anywhere";
}

function cliVersionSupported(output: string): boolean {
  const [major = 0, minor = 0] = output.trim().split(".").map(Number);
  return major > MIN_CLI_VERSION[0] || (major === MIN_CLI_VERSION[0] && minor >= MIN_CLI_VERSION[1]);
}

function accountLabel(account: z.infer<typeof accountListSchema>[number]): string {
  if (account.email && account.url) return `${account.email} (${account.url})`;
  return account.email ?? account.url ?? account.account_uuid;
}

/** A failure whose message is already a `sourceText` sentence for the user. */
class OnePasswordConnectError extends Error {}

/** The 1Password CLI, found where installers put it: a Finder launch has no shell `PATH`. */
async function findCli(): Promise<string | null> {
  const name = process.platform === "win32" ? "op.exe" : "op";
  const directories = [...(process.env.PATH ?? "").split(delimiter), "/opt/homebrew/bin", "/usr/local/bin"];
  for (const directory of directories.filter(Boolean)) {
    const candidate = join(directory, name);
    const executable = await access(candidate, constants.X_OK).then(
      () => true,
      () => false,
    );
    if (executable) return candidate;
  }
  return null;
}

/** Variables that would make `op` act as another identity than the user's own session. */
const CLI_IDENTITY_VARIABLES = new Set(["OP_SERVICE_ACCOUNT_TOKEN", "OP_CONNECT_TOKEN", "OP_CONNECT_HOST"]);

/** Runs `op` with no shell, as the user's own CLI session. */
export const runOnePasswordCli: OnePasswordCli = async (args, signal) => {
  const executable = await findCli();
  if (!executable) throw new OnePasswordConnectError(sourceText("error.connector.onePasswordCliMissing"));
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !CLI_IDENTITY_VARIABLES.has(name)));
  return new Promise((resolve, reject) => {
    execFile(
      executable,
      args,
      { env, signal, timeout: CLI_TIMEOUT_MS, maxBuffer: 4 * 1024 * 1024, windowsHide: true },
      (error, stdout, stderr) => {
        if (!error) {
          resolve(stdout);
          return;
        }
        if (error.name === "AbortError") {
          reject(error);
          return;
        }
        const detail = redactText(String(stderr).trim().split("\n").at(-1) || error.message);
        reject(new OnePasswordConnectError(sourceText("error.connector.onePasswordCliFailed", { detail })));
      },
    );
  });
};

async function createSdkClient(token: string, appVersion: string): Promise<OnePasswordClient> {
  const sdk = await import("@1password/sdk");
  const client = await sdk.createClient({ auth: token, integrationName: "OpenBot", integrationVersion: appVersion });
  return {
    vaults: { list: () => client.vaults.list() },
    items: {
      list: (vaultId) => client.items.list(vaultId),
      get: (vaultId, itemId) => client.items.get(vaultId, itemId),
    },
  };
}

/**
 * Owns the 1Password connection: Connect through the CLI, a pasted token, the stored token, and the
 * list of logins it reads. It never imports the agent service, which reads it as a `PasswordVault`.
 *
 * Connect uses the CLI only once, to create the vault "Shared with OpenBot" and a service account
 * that can only read it. The CLI session is the user's own; OpenBot keeps only the service account
 * token. The token never leaves the main process except to 1Password, and it is registered for
 * redaction before it is used. Passwords and codes go to the browser only.
 */
export class OnePasswordConnectorService implements PasswordVault {
  readonly #store: OnePasswordConnectorStore;
  readonly #hostName: string;
  readonly #cli: OnePasswordCli;
  readonly #createClient: (token: string) => Promise<OnePasswordClient>;
  readonly #now: () => number;
  readonly #listeners = new Set<(status: OnePasswordConnectorStatus) => void>();
  #connecting: AbortController | null = null;
  #accounts: OnePasswordAccount[] = [];
  #error: string | null = null;
  #client: { token: string; client: Promise<OnePasswordClient> } | null = null;
  #index: LoginIndex | null = null;
  #indexing: Promise<LoginIndex> | null = null;
  /** Changes each time the token is replaced or removed. A read that started before does not keep its answer. */
  #generation = 0;

  constructor(options: OnePasswordConnectorServiceOptions) {
    this.#store = options.store;
    this.#hostName = options.hostName;
    this.#cli = options.cli ?? runOnePasswordCli;
    this.#createClient = options.createClient ?? ((token) => createSdkClient(token, options.appVersion));
    this.#now = options.now ?? Date.now;
  }

  /** Reads the stored token. A file that cannot be read is logged and treated as no connection. */
  async load(): Promise<void> {
    const error = await this.#store.load();
    if (error) logger.warn("The 1Password connection file could not be read.", { cause: toLogValue(error) });
    const record = this.#store.read();
    if (!record) return;
    registerSecretValue(record.token);
    // The vault names and the login count come from 1Password; the app does not wait for them.
    void this.#readIndex().then(
      () => this.#emitStatus(),
      (cause: unknown) => logger.warn("The 1Password vault could not be read.", { cause: toLogValue(cause) }),
    );
  }

  status(): OnePasswordConnectorStatus {
    const connected = this.#store.read() !== null && !this.#connecting;
    return {
      ...DISCONNECTED_ONEPASSWORD_CONNECTOR,
      state: this.#connecting
        ? "connecting"
        : connected
          ? "connected"
          : this.#accounts.length > 0
            ? "choose-account"
            : "disconnected",
      accounts: this.#connecting || connected ? [] : [...this.#accounts],
      vaultNames: connected ? [...(this.#index?.vaultNames ?? [])] : [],
      loginCount: connected ? (this.#index?.logins.length ?? null) : null,
      error: this.#error,
    };
  }

  onChanged(listener: (status: OnePasswordConnectorStatus) => void): () => void {
    this.#listeners.add(listener);
    return () => this.#listeners.delete(listener);
  }

  /**
   * Creates the shared vault and a service account that reads it, with the user's CLI session. With
   * several accounts and none named, it answers `choose-account` and creates nothing.
   */
  async connect({ accountId }: OnePasswordConnectInput): Promise<OnePasswordConnectorStatus> {
    this.#connecting?.abort();
    const controller = new AbortController();
    this.#connecting = controller;
    this.#accounts = [];
    this.#error = null;
    this.#emitStatus();
    try {
      const token = await this.#createServiceAccount(accountId, controller.signal);
      if (token !== null) await this.#save({ token, accountId, connectedAt: this.#now() }, controller.signal);
    } catch (error) {
      if (this.#connecting === controller && !controller.signal.aborted) {
        logger.warn("Unable to connect 1Password", { cause: toLogValue(error) });
        this.#error =
          error instanceof OnePasswordConnectError
            ? error.message
            : sourceText("error.connector.onePasswordUnexpected", {
                detail: redactText(error instanceof Error ? error.message : String(error)),
              });
      }
    } finally {
      if (this.#connecting === controller) this.#connecting = null;
      this.#emitStatus();
    }
    return this.status();
  }

  /** Connects with a service account token the user created on 1Password.com. */
  async connectWithToken(token: string): Promise<OnePasswordConnectorStatus> {
    this.#connecting?.abort();
    this.#connecting = null;
    this.#accounts = [];
    this.#error = null;
    const trimmed = token.trim();
    registerSecretValue(trimmed);
    try {
      await this.#save({ token: trimmed, accountId: null, connectedAt: this.#now() }, new AbortController().signal);
    } catch (error) {
      logger.warn("Unable to connect 1Password with a token", { cause: toLogValue(error) });
      this.#error =
        error instanceof OnePasswordConnectError
          ? error.message
          : sourceText("error.connector.onePasswordTokenRejected");
    }
    this.#emitStatus();
    return this.status();
  }

  cancel(): OnePasswordConnectorStatus {
    this.#connecting?.abort();
    this.#connecting = null;
    this.#accounts = [];
    this.#error = null;
    this.#emitStatus();
    return this.status();
  }

  /**
   * Forgets the token. The vault and the service account stay in 1Password: only the user can remove
   * them there, and the panel says so.
   */
  async disconnect(): Promise<OnePasswordConnectorStatus> {
    this.#connecting?.abort();
    this.#connecting = null;
    this.#accounts = [];
    this.#error = null;
    this.#generation += 1;
    this.#client = null;
    this.#index = null;
    this.#indexing = null;
    await this.#store.clear();
    this.#emitStatus();
    return this.status();
  }

  async loginsFor(origin: string): Promise<VaultLogin[] | null> {
    if (!this.#store.read()) return null;
    const index = await this.#readIndex();
    const client = await this.#clientForToken();
    const matches = index.logins.filter((login) => login.websites.some((site) => websiteMatchesOrigin(site, origin)));
    return Promise.all(
      matches.slice(0, MAX_LISTED_LOGINS).map(async (login) => {
        const item = await client.items.get(login.vaultId, login.id);
        const username = item.fields.find((field) => field.id === "username")?.value || null;
        return {
          id: login.id,
          title: login.title,
          username,
          hasOneTimePassword: item.fields.some((field) => field.fieldType === "Totp"),
        };
      }),
    );
  }

  async secretFor(loginId: string, origin: string, kind: "password" | "totp"): Promise<string | null> {
    if (!this.#store.read()) return null;
    const login = (await this.#readIndex()).logins.find((candidate) => candidate.id === loginId);
    if (!login?.websites.some((site) => websiteMatchesOrigin(site, origin))) return null;
    const item = await (await this.#clientForToken()).items.get(login.vaultId, login.id);
    const value =
      kind === "password"
        ? item.fields.find((field) => field.id === "password")?.value
        : otpCode(item.fields.find((field) => field.fieldType === "Totp")?.details);
    if (!value) return null;
    registerSecretValue(value);
    return value;
  }

  dispose(): void {
    this.#connecting?.abort();
    this.#connecting = null;
    this.#listeners.clear();
  }

  /** The new token, or null when the user must first choose an account. */
  async #createServiceAccount(accountId: string | null, signal: AbortSignal): Promise<string | null> {
    if (!cliVersionSupported(await this.#cli(["--version"], signal)))
      throw new OnePasswordConnectError(sourceText("error.connector.onePasswordCliTooOld"));
    const accounts = accountListSchema.parse(
      JSON.parse(await this.#cli(["account", "list", "--format", "json"], signal)),
    );
    if (accounts.length === 0) throw new OnePasswordConnectError(sourceText("error.connector.onePasswordCliSignedOut"));
    const account = accountId
      ? accounts.find((candidate) => candidate.account_uuid === accountId)
      : accounts.length === 1
        ? accounts[0]
        : undefined;
    if (!account) {
      if (accountId) throw new OnePasswordConnectError(sourceText("error.connector.onePasswordCliSignedOut"));
      this.#accounts = accounts.map((candidate) => ({ id: candidate.account_uuid, label: accountLabel(candidate) }));
      return null;
    }
    const scope = ["--account", account.account_uuid];
    const vaults = vaultListSchema.parse(
      JSON.parse(await this.#cli(["vault", "list", ...scope, "--format", "json"], signal)),
    );
    const vaultId =
      vaults.find((vault) => vault.name === ONEPASSWORD_SHARED_VAULT)?.id ??
      vaultSchema.parse(
        JSON.parse(
          await this.#cli(
            [
              "vault",
              "create",
              ONEPASSWORD_SHARED_VAULT,
              "--description",
              "Logins that OpenBot agents may use to sign in to sites.",
              ...scope,
              "--format",
              "json",
            ],
            signal,
          ),
        ),
      ).id;
    const token = (
      await this.#cli(
        [
          "service-account",
          "create",
          `OpenBot on ${this.#hostName}`,
          "--vault",
          `${vaultId}:read_items`,
          ...scope,
          "--raw",
        ],
        signal,
      )
    ).trim();
    if (!token) throw new OnePasswordConnectError(sourceText("error.connector.onePasswordTokenRejected"));
    registerSecretValue(token);
    return token;
  }

  /** Checks the token with 1Password, then stores it. A token that reads no vault is refused. */
  async #save(record: OnePasswordConnectorRecord, signal: AbortSignal): Promise<void> {
    const client = await this.#createClient(record.token);
    const index = await this.#buildIndex(client);
    if (index.vaultNames.length === 0)
      throw new OnePasswordConnectError(sourceText("error.connector.onePasswordNoVault"));
    if (signal.aborted) return;
    await this.#store.write(record);
    this.#generation += 1;
    this.#client = { token: record.token, client: Promise.resolve(client) };
    this.#index = index;
    this.#indexing = null;
  }

  #clientForToken(): Promise<OnePasswordClient> {
    const record = this.#store.read();
    if (!record) return Promise.reject(new Error(sourceText("error.connector.onePasswordNoVault")));
    if (this.#client?.token !== record.token) {
      const client = this.#createClient(record.token);
      // A failed start is tried again at the next read.
      client.catch(() => {
        if (this.#client?.client === client) this.#client = null;
      });
      this.#client = { token: record.token, client };
    }
    return this.#client.client;
  }

  #readIndex(): Promise<LoginIndex> {
    if (this.#index && this.#now() - this.#index.readAt < INDEX_MAX_AGE_MS) return Promise.resolve(this.#index);
    if (this.#indexing) return this.#indexing;
    const generation = this.#generation;
    const indexing = this.#clientForToken()
      .then((client) => this.#buildIndex(client))
      .then((index) => {
        if (generation === this.#generation) this.#index = index;
        return index;
      })
      .finally(() => {
        if (this.#indexing === indexing) this.#indexing = null;
      });
    this.#indexing = indexing;
    return indexing;
  }

  async #buildIndex(client: OnePasswordClient): Promise<LoginIndex> {
    const vaults = await client.vaults.list();
    const logins: IndexedLogin[] = [];
    for (const vault of vaults) {
      for (const item of await client.items.list(vault.id)) {
        if (item.category !== "Login") continue;
        logins.push({
          id: item.id,
          vaultId: vault.id,
          title: item.title,
          websites: item.websites.map((site) => ({ url: site.url, autofill: autofill(site.autofillBehavior) })),
        });
      }
    }
    return { readAt: this.#now(), vaultNames: vaults.map((vault) => vault.title), logins };
  }

  #emitStatus(): void {
    const status = this.status();
    for (const listener of this.#listeners) listener(status);
  }
}

function otpCode(details: { type: string; content?: unknown } | undefined): string | undefined {
  if (details?.type !== "Otp" || typeof details.content !== "object" || details.content === null) return undefined;
  const code = "code" in details.content ? details.content.code : undefined;
  return typeof code === "string" ? code : undefined;
}
