import type { ProviderCodeLoginStart } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Result, Schema } from "effect";
import type { AgentClient } from "./../agent-client";
import { type CodexCliInfo, resolveCodexCli } from "./../cli";
import {
  type AccountLoginCompletedResult,
  type AccountReadResult,
  decodeAccountDeviceCodeLoginStartResult,
  decodeAccountLoginStartResult,
  decodeAccountReadResult,
  decodeRecordResponse,
} from "./../protocol";
import { recordRestartActivity } from "../restart-activity";

const CODEX_LOGIN_TIMEOUT_MS = 10 * 60_000;
const UNVERIFIED_MESSAGE = "OpenBot could not verify the ChatGPT connection. Try again.";

interface PendingCodexLogin {
  client: AgentClient;
  cli: CodexCliInfo;
  loginId: string;
  timer: NodeJS.Timeout;
  completing: boolean;
}

/** What the ChatGPT sign-in needs from the provider runtime that owns the Codex status and client. */
export interface CodexLoginHost {
  bundledExecutable(): string | null | undefined;
  /** A new Codex client, bound to the runtime's events but not started. */
  createClient(cli: CodexCliInfo): AgentClient;
  /** True when this client, or any client when none is given, is the active Codex client. */
  hasActiveClient(client?: AgentClient): boolean;
  activate(
    client: AgentClient,
    cli: CodexCliInfo,
    account: NonNullable<AccountReadResult["account"]>,
    options?: { isCurrent?: () => boolean },
  ): Promise<void>;
  setConnecting(): void;
  isConnecting(): boolean;
  clearConnectionState(): void;
  setFailure(error: unknown, version?: string | null): void;
}

/**
 * The ChatGPT sign-in of the Codex provider, in its browser and its code shapes. At most one is
 * pending. Its client stays running until the provider reports the end, so the token it issues
 * stays with that client.
 */
export class CodexLoginFlow {
  readonly #host: CodexLoginHost;
  #pending: PendingCodexLogin | null = null;

  constructor(host: CodexLoginHost) {
    this.#host = host;
  }

  get pending(): boolean {
    return this.#pending !== null;
  }

  /** Stops the timer and returns the pending login's client, which the caller stops. */
  dispose(): AgentClient | null {
    const pending = this.#pending;
    this.#pending = null;
    if (!pending) return null;
    clearTimeout(pending.timer);
    return pending.client;
  }

  startBrowser(openExternal: (url: string) => Promise<void>): Promise<void> {
    return runLogin(this.startBrowserEffect(openExternal));
  }

  readonly startBrowserEffect = Effect.fn("CodexLogin.startBrowser")(function* (
    this: CodexLoginFlow,
    openExternal: (url: string) => Promise<void>,
  ) {
    yield* this.#withLoginClientEffect((client, cli) =>
      Effect.gen({ self: this }, function* () {
        const login = yield* loginIo(() =>
          client.request(
            "account/login/start",
            {
              type: "chatgpt",
              appBrand: "chatgpt",
              codexStreamlinedLogin: true,
              useHostedLoginSuccessPage: true,
            },
            decodeAccountLoginStartResult,
          ),
        );
        this.#track(client, cli, login.loginId);
        const opened = yield* Effect.result(loginIo(() => openExternal(login.authUrl)));
        if (Result.isFailure(opened)) {
          yield* this.cancelEffect("OpenBot could not open the ChatGPT connection page.");
          return yield* new CodexLoginFailed({ cause: new Error(sourceText("error.provider.chatgptPageFailed")) });
        }
      }),
    );
  }, Effect.uninterruptible);

  /**
   * Starts the sign-in the user finishes on another device, and reports the code to show.
   *
   * Only the code and the page it is typed on cross back: the token the provider issues for that
   * code stays with the Codex client this method leaves running, exactly as it does for the browser
   * sign-in. How this one ends reaches the renderer the same way too, through the provider's status.
   */
  startDevice(): Promise<ProviderCodeLoginStart> {
    return runLogin(this.startDeviceEffect());
  }

  readonly startDeviceEffect = Effect.fn("CodexLogin.startDevice")(function* (
    this: CodexLoginFlow,
  ): Effect.fn.Return<ProviderCodeLoginStart, CodexLoginFailed> {
    const started = yield* this.#withLoginClientEffect((client, cli) =>
      Effect.gen({ self: this }, function* () {
        const login = yield* loginIo(() =>
          client.request("account/login/start", { type: "chatgptDeviceCode" }, decodeAccountDeviceCodeLoginStartResult),
        );
        this.#track(client, cli, login.loginId);
        const result: ProviderCodeLoginStart = {
          kind: "code",
          userCode: login.userCode,
          verificationUrl: login.verificationUrl,
          expiresAt: Date.now() + CODEX_LOGIN_TIMEOUT_MS,
        };
        return result;
      }),
    );
    return started ?? { kind: "connected" };
  }, Effect.uninterruptible);

  complete(completion: AccountLoginCompletedResult, source: AgentClient): Promise<void> {
    return runLogin(this.completeEffect(completion, source));
  }

  readonly completeEffect = Effect.fn("CodexLogin.complete")(function* (
    this: CodexLoginFlow,
    completion: AccountLoginCompletedResult,
    source: AgentClient,
  ) {
    const pending = this.#pending;
    if (!pending || pending.completing) return;
    if (pending.client !== source) return;
    if (completion.loginId !== null && completion.loginId !== pending.loginId) return;
    pending.completing = true;
    clearTimeout(pending.timer);

    if (!completion.success) {
      yield* this.#failEffect(pending, "ChatGPT connection was not completed. Try again.");
      return;
    }

    yield* Effect.gen({ self: this }, function* () {
      const account = yield* loginIo(() =>
        pending.client.request("account/read", { refreshToken: true }, decodeAccountReadResult),
      );
      if (account.account?.type !== "chatgpt") {
        return yield* new CodexLoginFailed({
          cause: new Error(sourceText("error.provider.noAuthenticatedAccount", { provider: "ChatGPT" })),
        });
      }
      if (this.#pending !== pending) return;
      const value = account.account;
      yield* loginIo(() =>
        this.#host.activate(pending.client, pending.cli, value, {
          isCurrent: () => this.#pending === pending,
        }),
      );
      if (this.#pending === pending) this.#pending = null;
    }).pipe(Effect.catch(() => this.#failEffect(pending, UNVERIFIED_MESSAGE)));
  }, Effect.uninterruptible);

  /** Ends the pending login after a completion the runtime could not decode. */
  async failUnverified(): Promise<void> {
    const pending = this.#pending;
    if (pending) await this.#fail(pending, UNVERIFIED_MESSAGE);
  }

  cancel(message: string | null, expected?: PendingCodexLogin): Promise<void> {
    return runLogin(this.cancelEffect(message, expected));
  }

  readonly cancelEffect = Effect.fn("CodexLogin.cancel")(function* (
    this: CodexLoginFlow,
    message: string | null,
    expected?: PendingCodexLogin,
  ) {
    const pending = this.#pending;
    if (!pending || (expected && pending !== expected)) return;
    this.#pending = null;
    clearTimeout(pending.timer);
    yield* loginIo(() =>
      pending.client.request("account/login/cancel", { loginId: pending.loginId }, decodeRecordResponse),
    ).pipe(Effect.ignore);
    yield* loginIo(() => pending.client.stop()).pipe(Effect.ignore);
    if (message) this.#host.setFailure(new Error(message), pending.cli.version);
    else this.#host.clearConnectionState();
  }, Effect.uninterruptible);

  /** Keeps a login the user already finished, and cancels one they did not, before a provider refresh. */
  settleForRefresh(): Promise<void> {
    return runLogin(this.settleForRefreshEffect());
  }

  readonly settleForRefreshEffect = Effect.fn("CodexLogin.settleForRefresh")(function* (this: CodexLoginFlow) {
    const pending = this.#pending;
    if (!pending) {
      this.#host.clearConnectionState();
      return;
    }
    this.#pending = null;
    clearTimeout(pending.timer);
    const activated = yield* Effect.result(
      Effect.gen({ self: this }, function* () {
        const result = yield* loginIo(() =>
          pending.client.request("account/read", { refreshToken: true }, decodeAccountReadResult),
        );
        const account = result.account;
        if (account?.type !== "chatgpt") return false;
        yield* loginIo(() => this.#host.activate(pending.client, pending.cli, account));
        return true;
      }),
    );
    if (Result.isSuccess(activated) && activated.success) return;
    yield* loginIo(() =>
      pending.client.request("account/login/cancel", { loginId: pending.loginId }, decodeRecordResponse),
    ).pipe(Effect.ignore);
    yield* loginIo(() => pending.client.stop()).pipe(Effect.ignore);
    this.#host.clearConnectionState();
  }, Effect.uninterruptible);

  /**
   * Brings a Codex client up to the point where a sign-in can start, and hands it to `run`.
   *
   * Returns null when the client turned out to be signed in already: the account was activated and
   * there is no login to start. Both sign-in shapes share this because everything before the
   * `account/login/start` call - the CLI, the handshake, the account already on this computer - and
   * everything the failure path has to undo is the same for a browser hand-off and for a code.
   */
  readonly #withLoginClientEffect = Effect.fn("CodexLogin.withClient")(function* <T>(
    this: CodexLoginFlow,
    run: (client: AgentClient, cli: CodexCliInfo) => Effect.Effect<T, CodexLoginFailed>,
  ) {
    let cli: CodexCliInfo | null = null;
    this.#host.setConnecting();
    return yield* Effect.gen({ self: this }, function* () {
      const bundledExecutable = this.#host.bundledExecutable();
      const resolved = yield* loginIo(() =>
        resolveCodexCli(bundledExecutable === undefined ? {} : { bundledExecutable }),
      );
      cli = resolved;
      return yield* Effect.acquireUseRelease(
        loginStep(() => this.#host.createClient(resolved)),
        (client) =>
          Effect.gen({ self: this }, function* () {
            yield* loginStep(() => client.start());
            yield* loginIo(() =>
              client.request(
                "initialize",
                {
                  clientInfo: { name: "openbot", title: "OpenBot", version: "0.1.0" },
                  capabilities: { experimentalApi: true, mcpServerOpenaiFormElicitation: true },
                },
                decodeRecordResponse,
              ),
            );
            yield* loginStep(() => client.notify("initialized"));
            if (!this.#host.hasActiveClient()) {
              const existing = yield* loginIo(() =>
                client.request("account/read", { refreshToken: false }, decodeAccountReadResult),
              );
              const account = existing.account;
              if (account?.type === "chatgpt") {
                yield* loginIo(() => this.#host.activate(client, resolved, account));
                return null;
              }
            }
            return yield* run(client, resolved);
          }),
        (client) =>
          this.#pending?.client !== client && !this.#host.hasActiveClient(client)
            ? loginIo(() => client.stop()).pipe(Effect.ignore)
            : Effect.void,
      );
    }).pipe(
      Effect.tapError((failure) =>
        Effect.sync(() => {
          if (!this.#pending && this.#host.isConnecting()) this.#host.setFailure(failure.cause, cli?.version);
        }),
      ),
    );
  }, Effect.uninterruptible);

  /**
   * Holds a started login open until the provider reports it finished, or until it times out.
   *
   * The deadline is OpenBot's, not the provider's. The code flow counts down to the same moment on
   * screen, so the number the user reads is the one this timer acts on.
   */
  #track(client: AgentClient, cli: CodexCliInfo, loginId: string): PendingCodexLogin {
    let pending: PendingCodexLogin;
    const timer = setTimeout(() => {
      void this.cancel("ChatGPT connection timed out. Try again.", pending);
    }, CODEX_LOGIN_TIMEOUT_MS);
    timer.unref?.();
    pending = { client, cli, loginId, timer, completing: false };
    this.#pending = pending;
    recordRestartActivity();
    client.once("exit", () => {
      if (this.#pending?.client === client) {
        void this.#fail(this.#pending, "ChatGPT connection stopped. Try again.");
      }
    });
    return pending;
  }

  #fail(pending: PendingCodexLogin, message: string): Promise<void> {
    return runLogin(this.#failEffect(pending, message));
  }

  readonly #failEffect = Effect.fn("CodexLogin.fail")(function* (
    this: CodexLoginFlow,
    pending: PendingCodexLogin,
    message: string,
  ) {
    if (this.#pending !== pending) return;
    clearTimeout(pending.timer);
    this.#pending = null;
    yield* loginIo(() => pending.client.stop()).pipe(Effect.ignore);
    this.#host.setFailure(new Error(message), pending.cli.version);
  }, Effect.uninterruptible);
}

export class CodexLoginFailed extends Schema.TaggedError<CodexLoginFailed>()("CodexLoginFailed", {
  cause: Schema.Defect(),
}) {}
function loginIo<A>(run: () => Promise<A>): Effect.Effect<A, CodexLoginFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new CodexLoginFailed({ cause }) });
}
function loginStep<A>(run: () => A): Effect.Effect<A, CodexLoginFailed> {
  return Effect.try({ try: run, catch: (cause) => new CodexLoginFailed({ cause }) });
}
async function runLogin<A>(effect: Effect.Effect<A, CodexLoginFailed>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(effect));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
