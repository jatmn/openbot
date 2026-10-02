import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";
import { chmod, mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { parseHostedServerClaim } from "@openbot/contracts/hosted-servers";
import type {
  AvatarImageInput,
  CentralAuthIssue,
  CentralAuthState,
  CentralAuthUser,
  MobileConnectedDevice,
  MobileConnectTicket,
} from "@openbot/contracts/ipc";
import { decodeRecord, requiredString } from "@openbot/contracts/ipc-decoding";
import type { LiveActivityRelayPush } from "@openbot/contracts/live-activity-relay";
import { createMobileConnectUrl, type MobileConnectHostBinding } from "@openbot/contracts/mobile-connect";
import {
  decodeRemoteSession,
  decodeRemoteSessionTicket,
  type RemoteSession,
  type RemoteSessionTicket,
} from "@openbot/contracts/remote-control-plane";
import { isDynamicRecord, isNumber, isString } from "@openbot/contracts/runtime-values";
import {
  REMOTE_TICKET_AUDIENCE,
  type RemoteMemberRole,
  type RemoteTicketClaims,
} from "@openbot/contracts/signal-protocol/ticket";
import { sourceText } from "@openbot/i18n/source";
import { Effect, ManagedRuntime, Result, Schema } from "effect";
import { createLocalJWKSet, jwtVerify } from "jose";
import { z } from "zod";
import { isMissingFileError } from "../backend/file-errors";
import {
  authCall,
  authDecode,
  CentralAuthOperationError,
  CentralAuthTransport,
  runCentralAuthEffect,
} from "./central-auth-effects";
import {
  decodeAcceptedRemoteInvite,
  decodeCentralAuthUser,
  decodeCreatedRemoteInvite,
  decodeEmailChallenge,
  decodeMobileConnectedDevices,
  decodeRecordHealth,
  decodeRegisteredRemoteHost,
  decodeRemoteHosts,
  decodeRemoteInvitePreview,
  decodeRemoteInvites,
  decodeRemoteMembers,
  decodeSessionResponse,
  decodeTicketResponse,
  decodeVoid,
  type RegisteredRemoteHost,
  type RemoteHostSummary,
  type RemoteInvitePreview,
  type RemoteInviteRecord,
  type RemoteMemberRecord,
} from "./central-auth-records";

interface CentralAuthEvents {
  changed: [state: CentralAuthState];
}

type AuthFetcher = (input: string | URL | Request, init?: RequestInit) => Promise<Response>;

interface CentralAuthManagerOptions {
  apiUrl: string;
  mobileConnectApiUrl?: string;
  storagePath: string;
  encrypt: (value: string) => Buffer;
  decrypt: (value: Buffer) => string;
  canPersist?: () => boolean;
  fetch?: AuthFetcher;
  startupRetryWindowMs?: number;
  startupRequestTimeoutMs?: number;
  startupRetryDelaysMs?: readonly number[];
  emailCodeRequestTimeoutMs?: number;
}

interface EmailCodeRequest {
  email: string;
  idempotencyKey: string;
  promise: Promise<CentralAuthState> | null;
}

const STARTUP_RETRY_WINDOW_MS = 30_000;
const STARTUP_REQUEST_TIMEOUT_MS = 3_000;
const STARTUP_RETRY_DELAYS_MS = [250, 500, 1_000, 2_000] as const;
const EMAIL_CODE_REQUEST_TIMEOUT_MS = 35_000;
const RESEND_FALLBACK_DELAY_MS = 60_000;
const DEFINITIVE_EMAIL_CODE_REQUEST_FAILURES = new Set([
  "email_delivery_failed",
  "email_delivery_rate_limited",
  "idempotency_conflict",
  "idempotency_key_completed",
  "invalid_email",
  "invalid_idempotency_key",
  "sign_in_code_expired",
]);
const UNCERTAIN_EMAIL_CODE_REQUEST_FAILURES = new Set([
  "email_delivery_pending",
  "email_delivery_timeout",
  "email_delivery_unknown",
]);
const remoteTicketJwksSchema = z.object({
  keys: z.array(z.object({ kty: z.string() }).loose()).min(1),
});

// The account API answers the same shape for a host credential and for a member session, so both
// paths below decode it with the one function in `@openbot/contracts/remote-control-plane`.
export type RemoteConnectionBootstrap = RemoteSessionTicket;

// The claims this host reads off a client's ticket, derived from the contract the account API mints
// against. `clientPublicKey` is optional there because a host ticket carries none; a client that
// reached this check without one is rejected below, so it is required here.
export type VerifiedRemoteSessionTicket = Pick<
  RemoteTicketClaims,
  "sessionId" | "hostId" | "userId" | "membershipId" | "authEpoch" | "sessionExpiresAt"
> & {
  role: RemoteMemberRole;
  clientPublicKey: string;
};

export class CentralAuthManager extends EventEmitter<CentralAuthEvents> {
  readonly #options: Required<CentralAuthManagerOptions>;
  readonly #runtime: ManagedRuntime.ManagedRuntime<CentralAuthTransport, never>;
  #state: CentralAuthState = { status: "loading" };
  #sessionToken: string | null = null;
  readonly #teamHostTokens = new Map<string, string>();
  #sessionWriteChain: Promise<void> = Promise.resolve();
  /** The account the stored host credentials were issued to, or none while signed out. */
  #sessionAccountId: string | null = null;
  #remoteTicketJwks: Promise<z.infer<typeof remoteTicketJwksSchema>> | null = null;
  #initializationPromise: Promise<CentralAuthState> | null = null;
  #emailCodeRequest: EmailCodeRequest | null = null;
  #profileRefreshPromise: Promise<CentralAuthState> | null = null;
  #profileRefreshGeneration = 0;

  constructor(options: CentralAuthManagerOptions) {
    super();
    this.#runtime = ManagedRuntime.make(CentralAuthTransport.layer(options.fetch ?? fetch));
    this.#options = {
      ...options,
      mobileConnectApiUrl: options.mobileConnectApiUrl ?? options.apiUrl,
      canPersist: options.canPersist ?? (() => true),
      fetch: options.fetch ?? fetch,
      startupRetryWindowMs: options.startupRetryWindowMs ?? STARTUP_RETRY_WINDOW_MS,
      startupRequestTimeoutMs: options.startupRequestTimeoutMs ?? STARTUP_REQUEST_TIMEOUT_MS,
      startupRetryDelaysMs: options.startupRetryDelaysMs ?? STARTUP_RETRY_DELAYS_MS,
      emailCodeRequestTimeoutMs: options.emailCodeRequestTimeoutMs ?? EMAIL_CODE_REQUEST_TIMEOUT_MS,
    };
  }

  readonly #pending = new Set<Promise<unknown>>();
  async dispose(): Promise<void> {
    this.stopProfileRefresh();
    while (this.#pending.size) await Promise.allSettled([...this.#pending]);
    await this.#sessionWriteChain.catch(() => undefined);
    await this.#runtime.dispose();
  }
  #run<A>(operation: Effect.Effect<A, CentralAuthOperationError, CentralAuthTransport>): Promise<A> {
    const pending = runCentralAuthEffect(this.#runtime, operation);
    this.#pending.add(pending);
    void pending.then(
      () => this.#pending.delete(pending),
      () => this.#pending.delete(pending),
    );
    return pending;
  }

  getState(): CentralAuthState {
    return structuredClone(this.#state);
  }

  stopProfileRefresh(): void {
    this.#profileRefreshGeneration += 1;
  }

  refreshProfile(): Promise<CentralAuthState> {
    if (this.#profileRefreshPromise) return this.#profileRefreshPromise;
    const state = this.#state;
    const token = this.#sessionToken;
    const generation = this.#profileRefreshGeneration;
    if (state.status !== "signed_in" || !token) return Promise.resolve(this.getState());
    const pending = this.#run(this.#refreshProfileEffect(state, token, generation));
    this.#profileRefreshPromise = pending;
    return pending;
  }

  readonly #refreshProfileEffect = Effect.fn("CentralAuth.refreshProfile")(function* (
    this: CentralAuthManager,
    state: Extract<CentralAuthState, { status: "signed_in" }>,
    token: string,
    generation: number,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    return yield* Effect.gen({ self: this }, function* () {
      const user = yield* this.#authorizedRequestEffect("/v1/me", { method: "GET" }, decodeCentralAuthUser);
      if (this.#state !== state || this.#sessionToken !== token || generation !== this.#profileRefreshGeneration)
        return this.getState();
      if (user.id !== state.user.id)
        return yield* new CentralAuthOperationError({
          cause: new Error("The account service returned an invalid user."),
        });
      const resolved = yield* authDecode(() => this.#resolveUserAvatar(user));
      if (
        resolved.name === state.user.name &&
        resolved.email === state.user.email &&
        resolved.avatarUrl === state.user.avatarUrl
      )
        return this.getState();
      return this.#setState({ status: "signed_in", user: resolved });
    }).pipe(
      Effect.catch(() => Effect.sync(() => this.getState())),
      Effect.ensuring(
        Effect.sync(() => {
          this.#profileRefreshPromise = null;
        }),
      ),
    );
  });

  getSignedInUser(): CentralAuthUser {
    if (this.#state.status !== "signed_in") {
      throw new AuthApiError(401, "unauthorized", sourceText("error.auth.signInFirst"));
    }
    return structuredClone(this.#state.user);
  }

  resolveApiUrl(path: string): string {
    return new URL(path, this.#options.apiUrl).toString();
  }

  requestAuthorized<T>(
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    timeoutMs?: number,
  ): Promise<T> {
    return this.#run(this.#requestAuthorizedEffect(path, init, decoder, timeoutMs));
  }
  readonly #requestAuthorizedEffect = Effect.fn("CentralAuth.requestAuthorized")(function* <T>(
    this: CentralAuthManager,
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    timeoutMs?: number,
  ): Effect.fn.Return<T, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(path, init, decoder, timeoutMs);
  });

  downloadAuthorized(path: string, timeoutMs = 30_000): Promise<Uint8Array> {
    return this.#run(this.#downloadAuthorizedEffect(path, timeoutMs));
  }
  readonly #downloadAuthorizedEffect = Effect.fn("CentralAuth.downloadAuthorized")(function* (
    this: CentralAuthManager,
    path: string,
    timeoutMs = 30_000,
  ): Effect.fn.Return<Uint8Array, CentralAuthOperationError, CentralAuthTransport> {
    if (!this.#sessionToken)
      return yield* new CentralAuthOperationError({
        cause: new AuthApiError(401, "unauthorized", sourceText("error.auth.signInRequired")),
      });
    const response = yield* CentralAuthTransport.use((transport) =>
      transport.fetch(new URL(path, this.#options.apiUrl), {
        headers: { Authorization: `Bearer ${this.#sessionToken}` },
        signal: AbortSignal.timeout(timeoutMs),
      }),
    );
    if (!response.ok)
      return yield* new CentralAuthOperationError({
        cause: yield* AuthApiError.fromResponseEffect(response),
      });
    return new Uint8Array(yield* authCall(() => response.arrayBuffer()));
  });

  createTeamAuthTicket(serverId: string): Promise<string> {
    return this.#run(this.#createTeamAuthTicketEffect(serverId));
  }
  readonly #createTeamAuthTicketEffect = Effect.fn("CentralAuth.createTeamAuthTicket")(function* (
    this: CentralAuthManager,
    serverId: string,
  ): Effect.fn.Return<string, CentralAuthOperationError, CentralAuthTransport> {
    const result = yield* this.#authorizedRequestEffect(
      "/v1/team-auth/ticket",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ serverId }),
      },
      decodeTicketResponse,
    );
    if (!result.ticket || !Number.isFinite(result.expiresAt)) {
      return yield* new CentralAuthOperationError({
        cause: new Error("The account service returned an invalid team ticket."),
      });
    }
    return result.ticket;
  });

  createMobileConnect(host: MobileConnectHostBinding): Promise<MobileConnectTicket> {
    return this.#run(this.#createMobileConnectEffect(host));
  }
  readonly #createMobileConnectEffect = Effect.fn("CentralAuth.createMobileConnect")(function* (
    this: CentralAuthManager,
    host: MobileConnectHostBinding,
  ): Effect.fn.Return<MobileConnectTicket, CentralAuthOperationError, CentralAuthTransport> {
    const result = yield* this.#authorizedRequestEffect(
      "/v1/mobile-auth/ticket",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ host }),
      },
      decodeTicketResponse,
    );
    if (!result.ticket || !Number.isFinite(result.expiresAt) || result.expiresAt <= Date.now()) {
      return yield* new CentralAuthOperationError({
        cause: new Error("The account service returned an invalid Mobile Connect ticket."),
      });
    }
    return {
      qrData: createMobileConnectUrl({ apiUrl: this.#options.mobileConnectApiUrl, ticket: result.ticket, host }),
      expiresAt: result.expiresAt,
    };
  });

  listMobileConnectedDevices(): Promise<MobileConnectedDevice[]> {
    return this.#run(this.#listMobileConnectedDevicesEffect());
  }
  readonly #listMobileConnectedDevicesEffect = Effect.fn("CentralAuth.listMobileConnectedDevices")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<MobileConnectedDevice[], CentralAuthOperationError, CentralAuthTransport> {
    const result = yield* this.#authorizedRequestEffect(
      "/v1/mobile-auth/devices",
      { method: "GET" },
      decodeMobileConnectedDevices,
    );
    return result.devices;
  });

  listAccountSessions() {
    return this.#run(this.#listAccountSessionsEffect());
  }
  readonly #listAccountSessionsEffect = Effect.fn("CentralAuth.listAccountSessions")(function* (
    this: CentralAuthManager,
  ) {
    const result = yield* this.#authorizedRequestEffect(
      "/v1/mobile-auth/devices?includeDesktop=true",
      { method: "GET" },
      (value) =>
        z
          .object({
            sessions: z.array(
              z.object({
                sessionId: z.string().uuid(),
                name: z.string(),
                kind: z.enum(["desktop", "mobile"]),
                current: z.boolean(),
                connectedAt: z.number().finite(),
                lastActiveAt: z.number().finite(),
              }),
            ),
          })
          .parse(value),
    );
    return result.sessions;
  });

  revokeAccountSession(sessionId: string): Promise<void> {
    return this.#run(this.#revokeAccountSessionEffect(sessionId));
  }
  readonly #revokeAccountSessionEffect = Effect.fn("CentralAuth.revokeAccountSession")(function* (
    this: CentralAuthManager,
    sessionId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    yield* this.#authorizedRequestEffect(
      `/v1/mobile-auth/devices/${encodeURIComponent(sessionId)}?includeDesktop=true`,
      { method: "DELETE" },
      () => undefined,
    );
  });

  revokeMobileConnectedDevice(sessionId: string): Promise<void> {
    return this.#run(this.#revokeMobileConnectedDeviceEffect(sessionId));
  }
  readonly #revokeMobileConnectedDeviceEffect = Effect.fn("CentralAuth.revokeMobileConnectedDevice")(function* (
    this: CentralAuthManager,
    sessionId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    yield* this.#authorizedRequestEffect(
      `/v1/mobile-auth/devices/${encodeURIComponent(sessionId)}`,
      { method: "DELETE" },
      () => undefined,
    );
  });

  registerRemoteHost(input: {
    hostId: string;
    name: string;
    ownerMembershipId: string;
    devicePublicKey?: string | null;
  }): Promise<RegisteredRemoteHost> {
    return this.#run(this.#registerRemoteHostEffect(input));
  }
  readonly #registerRemoteHostEffect = Effect.fn("CentralAuth.registerRemoteHost")(function* (
    this: CentralAuthManager,
    input: {
      hostId: string;
      name: string;
      ownerMembershipId: string;
      devicePublicKey?: string | null;
    },
  ): Effect.fn.Return<RegisteredRemoteHost, CentralAuthOperationError, CentralAuthTransport> {
    const sessionToken = this.#sessionToken;
    const storedMachineToken = this.#teamHostTokens.get(input.hostId.toLowerCase());
    const result = yield* this.#authorizedRequestEffect(
      "/v2/remote/hosts/register",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...input,
          rotateCredential: !storedMachineToken,
          ...(storedMachineToken ? { machineToken: storedMachineToken } : {}),
        }),
      },
      decodeRegisteredRemoteHost,
    );
    if (this.#sessionToken !== sessionToken) {
      // The credential belongs to the account that asked for it. Writing it now would file
      // it under whichever session is stored next, so the caller is told the registration
      // no longer applies instead.
      return yield* new CentralAuthOperationError({
        cause: new Error(sourceText("error.auth.accountChangedDuringRegister")),
      });
    }
    if (result.machineToken) this.#teamHostTokens.set(input.hostId.toLowerCase(), result.machineToken);
    yield* authCall(() => this.#writeStoredSession());
    return result;
  });

  /** The machine token of a registered host, so a site request can prove the server. Never log it. */
  hostSiteCredential(hostId: string): { hostId: string; machineToken: string } | null {
    const machineToken = this.#teamHostTokens.get(hostId.toLowerCase());
    return machineToken ? { hostId, machineToken } : null;
  }

  issueRemoteHostTicket(hostId: string): Promise<RemoteConnectionBootstrap> {
    return this.#run(this.#issueRemoteHostTicketEffect(hostId));
  }
  readonly #issueRemoteHostTicketEffect = Effect.fn("CentralAuth.issueRemoteHostTicket")(function* (
    this: CentralAuthManager,
    hostId: string,
  ): Effect.fn.Return<RemoteConnectionBootstrap, CentralAuthOperationError, CentralAuthTransport> {
    const machineToken = this.#teamHostTokens.get(hostId.toLowerCase());
    if (!machineToken)
      return yield* new CentralAuthOperationError({
        cause: new Error(sourceText("error.auth.hostCredentialUnavailable")),
      });
    return yield* this.#requestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/ticket`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ machineToken }) },
      decodeRemoteSessionTicket,
    );
  });

  /**
   * The Slack route ticket of this host: the workspaces that the account service links to it, which
   * Signal routes to its `ingress` socket.
   */
  issueSlackRoute(hostId: string): Promise<string> {
    return this.#run(this.#issueSlackRouteEffect(hostId));
  }
  readonly #issueSlackRouteEffect = Effect.fn("CentralAuth.issueSlackRoute")(function* (
    this: CentralAuthManager,
    hostId: string,
  ): Effect.fn.Return<string, CentralAuthOperationError, CentralAuthTransport> {
    const machineToken = this.#teamHostTokens.get(hostId.toLowerCase());
    if (!machineToken)
      return yield* new CentralAuthOperationError({
        cause: new Error(sourceText("error.auth.hostCredentialUnavailable")),
      });
    return yield* this.#requestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/slack-route`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ machineToken }) },
      (value) => requiredString(decodeRecord(value, "Slack route"), "ticket"),
    );
  });

  /** Unlinks a Slack workspace from this host, so Signal stops routing its events here. */
  unlinkSlackWorkspace(hostId: string, teamId: string): Promise<void> {
    return this.#run(this.#unlinkSlackWorkspaceEffect(hostId, teamId));
  }
  readonly #unlinkSlackWorkspaceEffect = Effect.fn("CentralAuth.unlinkSlackWorkspace")(function* (
    this: CentralAuthManager,
    hostId: string,
    teamId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    const machineToken = this.#teamHostTokens.get(hostId.toLowerCase());
    if (!machineToken)
      return yield* new CentralAuthOperationError({
        cause: new Error(sourceText("error.auth.hostCredentialUnavailable")),
      });
    yield* this.#requestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/slack-disconnect`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ machineToken, teamId }),
      },
      () => undefined,
    );
  });

  /**
   * Sends one Live Activity update through the account service to Apple. The host sealed the
   * content with keys that only the phone has, so the service forwards bytes it cannot read.
   * Returns `gone` when Apple refused the token.
   */
  sendLiveActivityPush(hostId: string, push: LiveActivityRelayPush): Promise<"sent" | "gone"> {
    return this.#run(this.#sendLiveActivityPushEffect(hostId, push));
  }
  readonly #sendLiveActivityPushEffect = Effect.fn("CentralAuth.sendLiveActivityPush")(function* (
    this: CentralAuthManager,
    hostId: string,
    push: LiveActivityRelayPush,
  ): Effect.fn.Return<"sent" | "gone", CentralAuthOperationError, CentralAuthTransport> {
    const machineToken = this.#teamHostTokens.get(hostId.toLowerCase());
    if (!machineToken)
      return yield* new CentralAuthOperationError({
        cause: new Error(sourceText("error.auth.hostCredentialUnavailable")),
      });
    return yield* Effect.gen({ self: this }, function* (): Effect.fn.Return<
      "sent" | "gone",
      CentralAuthOperationError,
      CentralAuthTransport
    > {
      yield* this.#requestEffect(
        `/v2/remote/hosts/${encodeURIComponent(hostId)}/live-activity`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ machineToken, ...push }),
        },
        () => undefined,
      );
      return "sent";
    }).pipe(
      Effect.catch(({ cause: error }) =>
        Effect.gen({ self: this }, function* (): Effect.fn.Return<
          "sent" | "gone",
          CentralAuthOperationError,
          CentralAuthTransport
        > {
          if (error instanceof AuthApiError && error.status === 410) return "gone";
          return yield* new CentralAuthOperationError({ cause: error });
        }),
      ),
    );
  });

  startRemoteSession(hostId: string): Promise<RemoteSession> {
    return this.#run(this.#startRemoteSessionEffect(hostId));
  }
  readonly #startRemoteSessionEffect = Effect.fn("CentralAuth.startRemoteSession")(function* (
    this: CentralAuthManager,
    hostId: string,
  ): Effect.fn.Return<RemoteSession, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      "/v2/remote/sessions/",
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ hostId }) },
      decodeRemoteSession,
    );
  });

  listRemoteHosts(): Promise<RemoteHostSummary[]> {
    return this.#run(this.#listRemoteHostsEffect());
  }
  readonly #listRemoteHostsEffect = Effect.fn("CentralAuth.listRemoteHosts")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<RemoteHostSummary[], CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect("/v2/remote/hosts/", { method: "GET" }, decodeRemoteHosts);
  });

  issueRemoteSessionTicket(sessionId: string, clientPublicKey: string): Promise<RemoteConnectionBootstrap> {
    return this.#run(this.#issueRemoteSessionTicketEffect(sessionId, clientPublicKey));
  }
  readonly #issueRemoteSessionTicketEffect = Effect.fn("CentralAuth.issueRemoteSessionTicket")(function* (
    this: CentralAuthManager,
    sessionId: string,
    clientPublicKey: string,
  ): Effect.fn.Return<RemoteConnectionBootstrap, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/sessions/${encodeURIComponent(sessionId)}/ticket`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ clientPublicKey }),
      },
      decodeRemoteSessionTicket,
    );
  });

  verifyRemoteSessionTicket(ticket: string): Promise<VerifiedRemoteSessionTicket> {
    return this.#run(this.#verifyRemoteSessionTicketEffect(ticket));
  }
  readonly #verifyRemoteSessionTicketEffect = Effect.fn("CentralAuth.verifyRemoteSessionTicket")(function* (
    this: CentralAuthManager,
    ticket: string,
  ): Effect.fn.Return<VerifiedRemoteSessionTicket, CentralAuthOperationError, CentralAuthTransport> {
    const verify = Effect.gen({ self: this }, function* () {
      if (!this.#remoteTicketJwks) this.#remoteTicketJwks = this.#fetchRemoteTicketJwks();
      const jwksPromise = this.#remoteTicketJwks;
      const jwks = yield* authCall(() => jwksPromise);
      const keySet = yield* authDecode(() => createLocalJWKSet(jwks));
      return yield* authCall(() =>
        jwtVerify(ticket, keySet, { audience: REMOTE_TICKET_AUDIENCE, algorithms: ["ES256"] }),
      );
    });
    const { payload } = yield* verify.pipe(
      Effect.catch((failure) => {
        const error = failure.cause;
        if (!isDynamicRecord(error) || error.code !== "ERR_JWKS_NO_MATCHING_KEY") return Effect.fail(failure);
        this.#remoteTicketJwks = null;
        return verify;
      }),
    );
    if (
      !isString(payload.sessionId) ||
      !isString(payload.hostId) ||
      !isString(payload.userId) ||
      !isString(payload.membershipId) ||
      (payload.role !== "owner" && payload.role !== "admin" && payload.role !== "member") ||
      !isNumber(payload.authEpoch) ||
      !Number.isInteger(payload.authEpoch) ||
      !isNumber(payload.sessionExpiresAt) ||
      !Number.isInteger(payload.sessionExpiresAt) ||
      !isString(payload.clientPublicKey)
    ) {
      return yield* new CentralAuthOperationError({
        cause: new Error("The remote session ticket has invalid claims."),
      });
    }
    return {
      sessionId: payload.sessionId,
      hostId: payload.hostId,
      userId: payload.userId,
      membershipId: payload.membershipId,
      role: payload.role,
      authEpoch: payload.authEpoch,
      sessionExpiresAt: payload.sessionExpiresAt,
      clientPublicKey: payload.clientPublicKey,
    };
  });

  #fetchRemoteTicketJwks(): Promise<z.infer<typeof remoteTicketJwksSchema>> {
    return this.#run(this.#fetchRemoteTicketJwksEffect());
  }
  readonly #fetchRemoteTicketJwksEffect = Effect.fn("CentralAuth.fetchRemoteTicketJwks")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<z.infer<typeof remoteTicketJwksSchema>, CentralAuthOperationError, CentralAuthTransport> {
    const response = yield* CentralAuthTransport.use((transport) =>
      transport.fetch(new URL("/.well-known/jwks.json", this.#options.apiUrl), {
        signal: AbortSignal.timeout(10_000),
      }),
    );
    if (!response.ok)
      return yield* new CentralAuthOperationError({
        cause: yield* AuthApiError.fromResponseEffect(response),
      });
    const value = yield* authCall(() => response.json());
    return yield* authDecode(() => remoteTicketJwksSchema.parse(value));
  });

  endRemoteSession(sessionId: string): Promise<void> {
    return this.#run(this.#endRemoteSessionEffect(sessionId));
  }
  readonly #endRemoteSessionEffect = Effect.fn("CentralAuth.endRemoteSession")(function* (
    this: CentralAuthManager,
    sessionId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/sessions/${encodeURIComponent(sessionId)}/end`,
      { method: "POST" },
      decodeVoid,
    );
  });

  createRemoteInvite(
    hostId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ): Promise<{ inviteId: string; token: string; expiresAt: number; permanent: boolean; useCount: number }> {
    return this.#run(this.#createRemoteInviteEffect(hostId, input));
  }
  readonly #createRemoteInviteEffect = Effect.fn("CentralAuth.createRemoteInvite")(function* (
    this: CentralAuthManager,
    hostId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ): Effect.fn.Return<
    { inviteId: string; token: string; expiresAt: number; permanent: boolean; useCount: number },
    CentralAuthOperationError,
    CentralAuthTransport
  > {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/invites`,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(input) },
      decodeCreatedRemoteInvite,
    );
  });

  listRemoteInvites(hostId: string): Promise<RemoteInviteRecord[]> {
    return this.#run(this.#listRemoteInvitesEffect(hostId));
  }
  readonly #listRemoteInvitesEffect = Effect.fn("CentralAuth.listRemoteInvites")(function* (
    this: CentralAuthManager,
    hostId: string,
  ): Effect.fn.Return<RemoteInviteRecord[], CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/invites`,
      { method: "GET" },
      decodeRemoteInvites,
    );
  });

  previewRemoteInvite(token: string): Promise<RemoteInvitePreview> {
    return this.#run(this.#previewRemoteInviteEffect(token));
  }
  readonly #previewRemoteInviteEffect = Effect.fn("CentralAuth.previewRemoteInvite")(function* (
    this: CentralAuthManager,
    token: string,
  ): Effect.fn.Return<RemoteInvitePreview, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#requestEffect(
      "/v2/remote/invites/preview",
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }) },
      decodeRemoteInvitePreview,
    );
  });

  acceptRemoteInvite(token: string): Promise<{ hostId: string; membershipId: string; role: "admin" | "member" }> {
    return this.#run(this.#acceptRemoteInviteEffect(token));
  }
  readonly #acceptRemoteInviteEffect = Effect.fn("CentralAuth.acceptRemoteInvite")(function* (
    this: CentralAuthManager,
    token: string,
  ): Effect.fn.Return<
    { hostId: string; membershipId: string; role: "admin" | "member" },
    CentralAuthOperationError,
    CentralAuthTransport
  > {
    return yield* this.#authorizedRequestEffect(
      "/v2/remote/invites/accept",
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ token }) },
      decodeAcceptedRemoteInvite,
    );
  });

  revokeRemoteInvite(inviteId: string): Promise<void> {
    return this.#run(this.#revokeRemoteInviteEffect(inviteId));
  }
  readonly #revokeRemoteInviteEffect = Effect.fn("CentralAuth.revokeRemoteInvite")(function* (
    this: CentralAuthManager,
    inviteId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/invites/${encodeURIComponent(inviteId)}`,
      { method: "DELETE" },
      decodeVoid,
    );
  });

  listRemoteMembers(hostId: string): Promise<RemoteMemberRecord[]> {
    return this.#run(this.#listRemoteMembersEffect(hostId));
  }
  readonly #listRemoteMembersEffect = Effect.fn("CentralAuth.listRemoteMembers")(function* (
    this: CentralAuthManager,
    hostId: string,
  ): Effect.fn.Return<RemoteMemberRecord[], CentralAuthOperationError, CentralAuthTransport> {
    const members = yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/members/`,
      { method: "GET" },
      decodeRemoteMembers,
    );
    return members.map((member) => ({
      ...member,
      avatarUrl: member.avatarUrl ? this.resolveApiUrl(member.avatarUrl) : null,
    }));
  });

  updateRemoteMember(
    hostId: string,
    membershipId: string,
    role: "admin" | "member",
    reactivate = false,
  ): Promise<void> {
    return this.#run(this.#updateRemoteMemberEffect(hostId, membershipId, role, reactivate));
  }
  readonly #updateRemoteMemberEffect = Effect.fn("CentralAuth.updateRemoteMember")(function* (
    this: CentralAuthManager,
    hostId: string,
    membershipId: string,
    role: "admin" | "member",
    reactivate = false,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/members/${encodeURIComponent(membershipId)}`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ role, ...(reactivate ? { reactivate: true } : {}) }),
      },
      decodeVoid,
    );
  });

  removeRemoteMember(hostId: string, membershipId: string): Promise<void> {
    return this.#run(this.#removeRemoteMemberEffect(hostId, membershipId));
  }
  readonly #removeRemoteMemberEffect = Effect.fn("CentralAuth.removeRemoteMember")(function* (
    this: CentralAuthManager,
    hostId: string,
    membershipId: string,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/members/${encodeURIComponent(membershipId)}`,
      { method: "DELETE" },
      decodeVoid,
    );
  });

  updateRemoteHostLogo(
    hostId: string,
    image: AvatarImageInput | null,
    version?: string | null,
  ): Promise<string | null> {
    return this.#run(this.#updateRemoteHostLogoEffect(hostId, image, version));
  }
  readonly #updateRemoteHostLogoEffect = Effect.fn("CentralAuth.updateRemoteHostLogo")(function* (
    this: CentralAuthManager,
    hostId: string,
    image: AvatarImageInput | null,
    version?: string | null,
  ): Effect.fn.Return<string | null, CentralAuthOperationError, CentralAuthTransport> {
    if (image === null) {
      yield* this.#authorizedRequestEffect(
        `/v2/remote/hosts/${encodeURIComponent(hostId)}/logo`,
        { method: "DELETE" },
        decodeVoid,
      );
      return null;
    }
    return yield* this.#authorizedRequestEffect(
      `/v2/remote/hosts/${encodeURIComponent(hostId)}/logo`,
      {
        method: "PUT",
        headers: { "Content-Type": image.mimeType, ...(version ? { "OpenBot-Logo-Version": version } : {}) },
        body: Buffer.from(image.bytes),
      },
      (value) => requiredString(decodeRecord(value, "remote host logo"), "logoKey"),
    );
  });

  downloadRemoteHostLogo(hostId: string, version: string): Promise<{ bytes: Uint8Array; mimeType: string }> {
    return this.#run(this.#downloadRemoteHostLogoEffect(hostId, version));
  }
  readonly #downloadRemoteHostLogoEffect = Effect.fn("CentralAuth.downloadRemoteHostLogo")(function* (
    this: CentralAuthManager,
    hostId: string,
    version: string,
  ): Effect.fn.Return<{ bytes: Uint8Array; mimeType: string }, CentralAuthOperationError, CentralAuthTransport> {
    if (!this.#sessionToken)
      return yield* new CentralAuthOperationError({
        cause: new AuthApiError(401, "unauthorized", sourceText("error.auth.signInRequired")),
      });
    const url = new URL(`/v2/remote/hosts/${encodeURIComponent(hostId)}/logo`, this.#options.apiUrl);
    url.searchParams.set("v", version);
    const response = yield* CentralAuthTransport.use((transport) =>
      transport.fetch(url, {
        headers: { Authorization: `Bearer ${this.#sessionToken}` },
        signal: AbortSignal.timeout(30_000),
      }),
    );
    if (!response.ok)
      return yield* new CentralAuthOperationError({
        cause: yield* AuthApiError.fromResponseEffect(response),
      });
    return {
      bytes: new Uint8Array(yield* authCall(() => response.arrayBuffer())),
      mimeType: response.headers.get("content-type")?.split(";", 1)[0]?.trim() || "application/octet-stream",
    };
  });

  redeemTeamAuthTicket(ticket: string, serverId: string): Promise<CentralAuthUser | null> {
    return this.#run(this.#redeemTeamAuthTicketEffect(ticket, serverId));
  }
  readonly #redeemTeamAuthTicketEffect = Effect.fn("CentralAuth.redeemTeamAuthTicket")(function* (
    this: CentralAuthManager,
    ticket: string,
    serverId: string,
  ): Effect.fn.Return<CentralAuthUser | null, CentralAuthOperationError, CentralAuthTransport> {
    if (!ticket) return null;
    return yield* Effect.gen({ self: this }, function* () {
      const user = yield* this.#requestEffect(
        "/v1/team-auth/redeem",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ ticket, serverId }),
        },
        decodeCentralAuthUser,
      );
      return this.#resolveUserAvatar(user);
    }).pipe(
      Effect.catch(({ cause: error }) =>
        Effect.gen({ self: this }, function* () {
          if (error instanceof AuthApiError && error.status === 401) return null;
          return yield* new CentralAuthOperationError({ cause: error });
        }),
      ),
    );
  });

  sendTeamInviteEmail(input: {
    email: string;
    serverName: string;
    inviteUrl: string;
    role: "admin" | "member";
  }): Promise<void> {
    return this.#run(this.#sendTeamInviteEmailEffect(input));
  }
  readonly #sendTeamInviteEmailEffect = Effect.fn("CentralAuth.sendTeamInviteEmail")(function* (
    this: CentralAuthManager,
    input: {
      email: string;
      serverName: string;
      inviteUrl: string;
      role: "admin" | "member";
    },
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    return yield* this.#authorizedRequestEffect(
      "/v1/team-invitations/email",
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(input),
      },
      decodeVoid,
    );
  });

  initialize(): Promise<CentralAuthState> {
    if (this.#initializationPromise) return this.#initializationPromise;
    const pending = this.#initialize().catch((error) => this.#setInitializationError(error));
    this.#initializationPromise = pending;
    void pending.then(() => {
      if (this.#initializationPromise === pending) this.#initializationPromise = null;
    });
    return pending;
  }

  retry(): Promise<CentralAuthState> {
    return this.initialize();
  }

  #initialize(): Promise<CentralAuthState> {
    return this.#run(this.#initializeEffect());
  }
  readonly #initializeEffect = Effect.fn("CentralAuth.initialize")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    this.#setState({ status: "loading" });
    if (this.#options.canPersist()) {
      const attempt4 = yield* Effect.gen({ self: this }, function* () {
        const encrypted = Buffer.from(yield* authCall(() => readFile(this.#options.storagePath, "utf8")), "base64");
        yield* authDecode(() => this.#restoreStoredSession(this.#options.decrypt(encrypted)));
      }).pipe(Effect.result);
      if (Result.isFailure(attempt4)) {
        const error = attempt4.failure.cause;
        if (!isMissingFileError(error)) {
          yield* this.#clearStoredSessionEffect();
        }
      }
    } else {
      yield* authCall(() => rm(this.#options.storagePath, { force: true }));
    }
    if (!this.#sessionToken) {
      yield* this.#startupRequestEffect("/health/live", { method: "GET" }, decodeRecordHealth);
      return this.#setState({ status: "signed_out" });
    }
    const sessionToken = this.#sessionToken;
    return yield* Effect.gen({ self: this }, function* () {
      const user = yield* this.#startupRequestEffect("/v1/me", { method: "GET" }, decodeCentralAuthUser, sessionToken);
      return this.#setState({ status: "signed_in", user: this.#resolveUserAvatar(user) });
    }).pipe(
      Effect.catch(({ cause: error }) =>
        Effect.gen({ self: this }, function* () {
          if (error instanceof AuthApiError && error.status === 401) {
            yield* this.#clearStoredSessionEffect();
            return this.#setState({ status: "signed_out" });
          }
          return yield* new CentralAuthOperationError({ cause: error });
        }),
      ),
    );
  });

  requestEmailCode(email: string): Promise<CentralAuthState> {
    const normalizedEmail = email.trim().toLowerCase();
    const existingRequest = this.#emailCodeRequest;
    if (existingRequest?.email === normalizedEmail && existingRequest.promise) return existingRequest.promise;

    const request: EmailCodeRequest =
      existingRequest?.email === normalizedEmail
        ? existingRequest
        : { email: normalizedEmail, idempotencyKey: randomUUID(), promise: null };
    this.#emailCodeRequest = request;
    const pending = this.#performEmailCodeRequest(request);
    request.promise = pending;
    return pending;
  }

  #performEmailCodeRequest(request: EmailCodeRequest): Promise<CentralAuthState> {
    return this.#run(this.#performEmailCodeRequestEffect(request));
  }
  readonly #performEmailCodeRequestEffect = Effect.fn("CentralAuth.performEmailCodeRequest")(function* (
    this: CentralAuthManager,
    request: EmailCodeRequest,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    const existingChallenge = this.#state.status === "code_sent" ? this.#state : null;
    if (existingChallenge) {
      this.#setState({ ...existingChallenge, issue: undefined });
    } else {
      this.#setState({ status: "signing_in" });
    }
    return yield* Effect.gen({ self: this }, function* () {
      const result = yield* this.#requestEffect(
        "/v1/auth/email/start",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Idempotency-Key": request.idempotencyKey,
          },
          body: JSON.stringify({ email: request.email }),
        },
        decodeEmailChallenge,
        this.#options.emailCodeRequestTimeoutMs,
      );
      if (!result.challengeId || !Number.isFinite(result.expiresAt)) {
        return yield* new CentralAuthOperationError({
          cause: new Error("The account service returned an invalid sign-in challenge."),
        });
      }
      if (this.#emailCodeRequest === request) this.#emailCodeRequest = null;
      return this.#setState({
        status: "code_sent",
        challengeId: result.challengeId,
        email: request.email,
        expiresAt: result.expiresAt,
        resendAvailableAt: result.resendAt ?? Math.min(result.expiresAt, Date.now() + RESEND_FALLBACK_DELAY_MS),
        ...(result.developmentCode ? { developmentCode: result.developmentCode } : {}),
      });
    })
      .pipe(
        Effect.catch(({ cause: error }) =>
          Effect.sync(() => {
            if (isDefinitiveEmailCodeRequestFailure(error) && this.#emailCodeRequest === request) {
              this.#emailCodeRequest = null;
            }
            const issue = emailCodeRequestIssue(error);
            if (existingChallenge && !UNCERTAIN_EMAIL_CODE_REQUEST_FAILURES.has(issue.code)) {
              return this.#setState({ ...existingChallenge, issue });
            }
            return this.#setState({
              status: "error",
              issue,
            });
          }),
        ),
      )
      .pipe(
        Effect.ensuring(
          Effect.sync(() => {
            if (this.#emailCodeRequest === request) request.promise = null;
          }).pipe(Effect.orDie),
        ),
      );
  });

  verifyEmailCode(challengeId: string, code: string): Promise<CentralAuthState> {
    return this.#run(this.#verifyEmailCodeEffect(challengeId, code));
  }
  readonly #verifyEmailCodeEffect = Effect.fn("CentralAuth.verifyEmailCode")(function* (
    this: CentralAuthManager,
    challengeId: string,
    code: string,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    const challenge = this.#state.status === "code_sent" ? this.#state : null;
    if (challenge) this.#setState({ ...challenge, issue: undefined });
    let sessionApplied = false;
    return yield* Effect.gen({ self: this }, function* () {
      const session = yield* this.#requestEffect(
        "/v1/auth/email/verify",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ challengeId, code }),
        },
        decodeSessionResponse,
      );
      // Signing in as somebody else without signing out first. The host credentials belong
      // to the account that was issued them, and must not be filed under this session.
      if (this.#sessionAccountId !== null && this.#sessionAccountId !== session.user.id) {
        this.#teamHostTokens.clear();
      }
      this.#sessionToken = session.sessionToken;
      sessionApplied = true;
      yield* authCall(() => this.#writeStoredSession());
      return this.#setState({
        status: "signed_in",
        user: this.#resolveUserAvatar(session.user),
      });
    }).pipe(
      Effect.catch(({ cause: error }) =>
        Effect.gen({ self: this }, function* () {
          // A wrong code or a failed request for a challenge leaves the stored session as it was: the
          // user can still be signed in to another account, or have a session that only a startup
          // check failed on.
          if (sessionApplied || !challenge) yield* this.#clearStoredSessionEffect();
          if (challenge) {
            return this.#setState({
              ...challenge,
              issue: centralAuthIssue(error, "email_sign_in_failed", sourceText("error.auth.codeNotVerified")),
            });
          }
          return this.#setState({
            status: "error",
            issue: centralAuthIssue(error, "email_sign_in_failed", sourceText("error.auth.codeNotVerified")),
          });
        }),
      ),
    );
  });

  /** False when the session can live only in memory, so it would be lost at the next start. */
  canPersistSession(): boolean {
    return this.#options.canPersist();
  }

  /**
   * Signs a new hosted server in with the claim that the account server put in its VM.
   * The result names the host ID that the account server reserved for this account.
   */
  redeemHostedServerClaim(claim: string): Promise<{ hostId: string; name: string; user: CentralAuthUser }> {
    return this.#run(this.#redeemHostedServerClaimEffect(claim));
  }
  readonly #redeemHostedServerClaimEffect = Effect.fn("CentralAuth.redeemHostedServerClaim")(function* (
    this: CentralAuthManager,
    claim: string,
  ): Effect.fn.Return<
    { hostId: string; name: string; user: CentralAuthUser },
    CentralAuthOperationError,
    CentralAuthTransport
  > {
    const redeemed = yield* this.#requestEffect(
      "/v2/hosting/claims/redeem",
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ claim }) },
      (value) => {
        const parsed = parseHostedServerClaim(value);
        if (!parsed) throw new Error("Invalid hosted server claim.");
        return parsed;
      },
    );
    if (this.#sessionAccountId !== null && this.#sessionAccountId !== redeemed.user.id) {
      this.#teamHostTokens.clear();
    }
    const previousToken = this.#sessionToken;
    this.#sessionToken = redeemed.sessionToken;
    // The claim is spent. A session that is not stored ends at the next start, so a failed write fails
    // the redeem, and the session is not kept in memory. The start retry redeems the claim again in its
    // retry window.
    const attempt7 = yield* Effect.gen({ self: this }, function* () {
      yield* authCall(() => this.#writeStoredSession({ required: true }));
    }).pipe(Effect.result);
    if (Result.isFailure(attempt7)) {
      const error = attempt7.failure.cause;
      this.#sessionToken = previousToken;
      return yield* new CentralAuthOperationError({ cause: error });
    }
    const user = this.#resolveUserAvatar(redeemed.user);
    this.#setState({ status: "signed_in", user });
    return { hostId: redeemed.hostId, name: redeemed.name, user };
  });

  logout(): Promise<CentralAuthState> {
    return this.#run(this.#logoutEffect());
  }
  readonly #logoutEffect = Effect.fn("CentralAuth.logout")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    this.#emailCodeRequest = null;
    if (this.#sessionToken) {
      const attempt8 = yield* Effect.gen({ self: this }, function* () {
        yield* this.#authorizedRequestEffect("/v1/auth/logout", { method: "POST" }, decodeVoid);
      }).pipe(Effect.result);
      if (Result.isFailure(attempt8)) {
        // Local logout must still remove the session from this device.
      }
    }
    yield* this.#clearStoredSessionEffect();
    return this.#setState({ status: "signed_out" });
  });

  updateAvatar(image: AvatarImageInput | null): Promise<CentralAuthState> {
    return this.#run(this.#updateAvatarEffect(image));
  }
  readonly #updateAvatarEffect = Effect.fn("CentralAuth.updateAvatar")(function* (
    this: CentralAuthManager,
    image: AvatarImageInput | null,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    const sessionToken = this.#sessionToken;
    if (!sessionToken)
      return yield* new CentralAuthOperationError({
        cause: new AuthApiError(401, "unauthorized", sourceText("error.auth.signInRequired")),
      });
    const user = image
      ? yield* this.#authorizedRequestEffect(
          "/v1/me/avatar",
          {
            method: "PUT",
            headers: { "Content-Type": image.mimeType },
            body: Buffer.from(image.bytes),
          },
          decodeCentralAuthUser,
        )
      : yield* this.#authorizedRequestEffect(
          "/v1/me/avatar",
          {
            method: "DELETE",
          },
          decodeCentralAuthUser,
        );
    if (this.#sessionToken !== sessionToken || this.#state.status !== "signed_in") return this.getState();
    const resolvedUser = this.#resolveUserAvatar(user);
    return this.#setState({
      status: "signed_in",
      user: { ...this.#state.user, avatarUrl: resolvedUser.avatarUrl },
    });
  });

  updateName(name: string): Promise<CentralAuthState> {
    return this.#run(this.#updateNameEffect(name));
  }
  readonly #updateNameEffect = Effect.fn("CentralAuth.updateName")(function* (
    this: CentralAuthManager,
    name: string,
  ): Effect.fn.Return<CentralAuthState, CentralAuthOperationError, CentralAuthTransport> {
    const sessionToken = this.#sessionToken;
    if (!sessionToken)
      return yield* new CentralAuthOperationError({
        cause: new AuthApiError(401, "unauthorized", sourceText("error.auth.signInRequired")),
      });
    const user = yield* this.#authorizedRequestEffect(
      "/v1/me/profile",
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      },
      decodeCentralAuthUser,
    );
    if (this.#sessionToken !== sessionToken || this.#state.status !== "signed_in") return this.getState();
    return this.#setState({
      status: "signed_in",
      user: { ...this.#state.user, name: user.name },
    });
  });

  readonly #requestEffect = Effect.fn("CentralAuth.request")(function* <T>(
    this: CentralAuthManager,
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    timeoutMs = 10_000,
  ): Effect.fn.Return<T, CentralAuthOperationError, CentralAuthTransport> {
    const response = yield* CentralAuthTransport.use((transport) =>
      transport.fetch(new URL(path, this.#options.apiUrl), {
        ...init,
        signal: AbortSignal.timeout(timeoutMs),
      }),
    );
    if (!response.ok)
      return yield* new CentralAuthOperationError({
        cause: yield* AuthApiError.fromResponseEffect(response),
      });
    const value = response.status === 204 ? undefined : yield* authCall(() => response.json());
    return yield* authDecode(() => decoder(value));
  });

  readonly #startupRequestEffect = Effect.fn("CentralAuth.startupRequest")(function* <T>(
    this: CentralAuthManager,
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    sessionToken?: string,
  ): Effect.fn.Return<T, CentralAuthOperationError, CentralAuthTransport> {
    const deadline = Date.now() + this.#options.startupRetryWindowMs;
    let retryIndex = 0;
    while (true) {
      const remainingMs = deadline - Date.now();
      if (remainingMs <= 0)
        return yield* new CentralAuthOperationError({ cause: new Error(sourceText("error.auth.serviceUnavailable")) });
      const result = yield* this.#requestEffect(
        path,
        {
          ...init,
          headers: sessionToken ? { ...init.headers, Authorization: `Bearer ${sessionToken}` } : init.headers,
        },
        decoder,
        Math.max(1, Math.min(this.#options.startupRequestTimeoutMs, remainingMs)),
      ).pipe(Effect.result);
      if (Result.isSuccess(result)) return result.success;
      const error = result.failure.cause;
      if (!isTransientStartupError(error)) return yield* result.failure;
      const delayMs = Math.min(
        this.#options.startupRetryDelaysMs[Math.min(retryIndex, this.#options.startupRetryDelaysMs.length - 1)] ?? 0,
        Math.max(0, deadline - Date.now()),
      );
      if (delayMs <= 0) return yield* result.failure;
      yield* Effect.sleep(delayMs);
      retryIndex += 1;
    }
  });

  readonly #authorizedRequestEffect = Effect.fn("CentralAuth.authorizedRequest")(function* <T>(
    this: CentralAuthManager,
    path: string,
    init: RequestInit,
    decoder: (value: unknown) => T,
    timeoutMs?: number,
  ): Effect.fn.Return<T, CentralAuthOperationError, CentralAuthTransport> {
    if (!this.#sessionToken)
      return yield* new CentralAuthOperationError({
        cause: new AuthApiError(401, "unauthorized", sourceText("error.auth.signInRequired")),
      });
    // A spread drops the entries of a `Headers` object, such as the hosting developer key.
    const headers = new Headers(init.headers);
    headers.set("Authorization", `Bearer ${this.#sessionToken}`);
    return yield* this.#requestEffect(path, { ...init, headers }, decoder, timeoutMs);
  });

  #resolveUserAvatar(user: CentralAuthUser): CentralAuthUser {
    return {
      ...user,
      avatarUrl: user.avatarUrl ? new URL(user.avatarUrl, this.#options.apiUrl).toString() : null,
    };
  }

  #writeStoredSession(options: { required?: boolean } = {}): Promise<void> {
    // Serialized: two writes racing inside their filesystem awaits would let the earlier
    // one rename its snapshot over the later one, restoring a session the user has left.
    this.#sessionWriteChain = this.#sessionWriteChain.then(
      () => this.#writeStoredSessionNow(options.required === true),
      () => this.#writeStoredSessionNow(options.required === true),
    );
    return this.#sessionWriteChain;
  }

  #writeStoredSessionNow(required: boolean): Promise<void> {
    return this.#run(this.#writeStoredSessionNowEffect(required));
  }
  readonly #writeStoredSessionNowEffect = Effect.fn("CentralAuth.writeStoredSessionNow")(function* (
    this: CentralAuthManager,
    required: boolean,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    if (!this.#sessionToken) return;
    if (!this.#options.canPersist()) {
      yield* authCall(() => rm(this.#options.storagePath, { force: true }));
      if (required)
        return yield* new CentralAuthOperationError({ cause: new Error("The session could not be stored.") });
      return;
    }
    const temporaryPath = `${this.#options.storagePath}.${randomUUID()}.tmp`;
    yield* Effect.gen({ self: this }, function* () {
      const value = JSON.stringify({
        version: 2,
        sessionToken: this.#sessionToken,
        teamHostTokens: Object.fromEntries(this.#teamHostTokens),
      });
      const encrypted = yield* authDecode(() => this.#options.encrypt(value).toString("base64"));
      yield* authCall(() => mkdir(dirname(this.#options.storagePath), { recursive: true }));
      yield* authCall(() => writeFile(temporaryPath, encrypted, { mode: 0o600 }));
      yield* authCall(() => chmod(temporaryPath, 0o600));
      yield* authCall(() => rename(temporaryPath, this.#options.storagePath));
    }).pipe(
      Effect.catch((failure) =>
        Effect.gen({ self: this }, function* () {
          yield* Effect.all(
            [this.#options.storagePath, temporaryPath].map((path) =>
              authCall(() => rm(path, { force: true })).pipe(Effect.catch(() => Effect.void)),
            ),
            { concurrency: "unbounded" },
          );
          if (required) return yield* failure;
        }),
      ),
      Effect.ensuring(authCall(() => rm(temporaryPath, { force: true })).pipe(Effect.catch(() => Effect.void))),
    );
  });

  readonly #clearStoredSessionEffect = Effect.fn("CentralAuth.clearStoredSession")(function* (
    this: CentralAuthManager,
  ): Effect.fn.Return<void, CentralAuthOperationError, CentralAuthTransport> {
    this.#sessionToken = null;
    this.#sessionAccountId = null;
    this.#teamHostTokens.clear();
    // Through the same chain as the writes, so a write already in flight cannot put the
    // file back after it is removed.
    this.#sessionWriteChain = this.#sessionWriteChain.then(
      () => rm(this.#options.storagePath, { force: true }),
      () => rm(this.#options.storagePath, { force: true }),
    );
    yield* authCall(() => this.#sessionWriteChain);
  });

  #restoreStoredSession(value: string): void {
    if (!value.trimStart().startsWith("{")) {
      this.#sessionToken = value;
      this.#teamHostTokens.clear();
      return;
    }
    const stored = JSON.parse(value);
    if (!isDynamicRecord(stored) || stored.version !== 2 || !isString(stored.sessionToken)) {
      throw new Error("Invalid protected account session.");
    }
    this.#sessionToken = stored.sessionToken;
    this.#teamHostTokens.clear();
    if (isDynamicRecord(stored.teamHostTokens)) {
      for (const [serverId, token] of Object.entries(stored.teamHostTokens)) {
        if (/^[0-9a-f-]{36}$/iu.test(serverId) && isString(token) && /^[A-Za-z0-9_-]{32,128}$/u.test(token)) {
          this.#teamHostTokens.set(serverId.toLowerCase(), token);
        }
      }
    }
  }

  #setState(state: CentralAuthState): CentralAuthState {
    // Held apart from the state, which passes through `code_sent` on the way to another
    // account: this is whose credentials the store is holding, until they are cleared.
    if (state.status === "signed_in") this.#sessionAccountId = state.user.id;
    this.#state = state;
    const copy = this.getState();
    this.emit("changed", copy);
    return copy;
  }

  #setInitializationError(error: unknown): CentralAuthState {
    const apiError = error instanceof AuthApiError ? error : null;
    const unavailable = !apiError || apiError.status >= 500;
    return this.#setState({
      status: "error",
      issue: {
        code: unavailable ? "auth_api_unavailable" : apiError.code,
        message: unavailable ? sourceText("error.auth.serviceUnavailable") : apiError.message,
        ...(apiError?.retryAfterSeconds === undefined ? {} : { retryAfterSeconds: apiError.retryAfterSeconds }),
      },
    });
  }
}

export function readCentralAuthApiUrl(value: string | undefined, fallback = "http://127.0.0.1:3100"): string {
  const url = new URL(value ?? fallback);
  const loopback = url.hostname === "127.0.0.1" || url.hostname === "localhost";
  if ((url.protocol !== "https:" && !(url.protocol === "http:" && loopback)) || url.pathname !== "/") {
    throw new Error("OPENBOT_AUTH_API_URL must be HTTPS or an HTTP loopback origin.");
  }
  return url.origin;
}

export function readMobileConnectApiUrl(value: string | undefined, fallback: string): string {
  const apiUrl = value ?? fallback;
  createMobileConnectUrl({ apiUrl, ticket: "x".repeat(32) });
  return new URL(apiUrl).origin;
}

class AuthApiError extends Schema.TaggedError<AuthApiError>()("AuthApiError", {
  status: Schema.Number,
  code: Schema.String,
  message: Schema.String,
  retryAfterSeconds: Schema.optional(Schema.Number),
}) {
  constructor(status: number, code: string, message: string, retryAfterSeconds?: number) {
    super({ status, code, message, retryAfterSeconds });
  }

  static readonly fromResponseEffect = Effect.fn("CentralAuth.decodeError")(function* (response: Response) {
    const retryAfterSeconds = parseRetryAfterSeconds(response.headers.get("Retry-After"));
    const value = yield* authCall(() => response.json()).pipe(Effect.catch(() => Effect.succeed(null)));
    if (
      isDynamicRecord(value) &&
      isDynamicRecord(value.error) &&
      isString(value.error.code) &&
      isString(value.error.message)
    ) {
      return new AuthApiError(response.status, value.error.code, value.error.message, retryAfterSeconds);
    }
    return new AuthApiError(
      response.status,
      "auth_api_error",
      sourceText("error.auth.serviceError"),
      retryAfterSeconds,
    );
  });
}

function centralAuthIssue(error: unknown, fallbackCode: string, fallbackMessage: string): CentralAuthIssue {
  if (error instanceof AuthApiError) {
    return {
      code: error.code,
      message: error.message,
      ...(error.retryAfterSeconds === undefined ? {} : { retryAfterSeconds: error.retryAfterSeconds }),
    };
  }
  return { code: fallbackCode, message: errorMessage(error, fallbackMessage) };
}

function emailCodeRequestIssue(error: unknown): CentralAuthIssue {
  if (error instanceof AuthApiError) {
    return centralAuthIssue(error, "email_sign_in_start_failed", sourceText("error.auth.codeNotSent"));
  }
  if (error instanceof DOMException && error.name === "TimeoutError") {
    return {
      code: "email_delivery_timeout",
      message: sourceText("error.auth.deliveryTimeout"),
    };
  }
  if (error instanceof TypeError || (error instanceof DOMException && error.name === "AbortError")) {
    return {
      code: "email_delivery_unknown",
      message: sourceText("error.auth.deliveryInterrupted"),
    };
  }
  return {
    code: "email_delivery_unknown",
    message: sourceText("error.auth.deliveryUnknown"),
  };
}

function isDefinitiveEmailCodeRequestFailure(error: unknown): boolean {
  if (!(error instanceof AuthApiError)) return false;
  return DEFINITIVE_EMAIL_CODE_REQUEST_FAILURES.has(error.code);
}

function parseRetryAfterSeconds(value: string | null): number | undefined {
  if (value === null) return undefined;
  const trimmed = value.trim();
  if (/^\d+$/u.test(trimmed)) {
    const seconds = Number.parseInt(trimmed, 10);
    return seconds > 0 ? seconds : undefined;
  }
  const retryAt = Date.parse(trimmed);
  if (!Number.isFinite(retryAt)) return undefined;
  const seconds = Math.ceil((retryAt - Date.now()) / 1_000);
  return seconds > 0 ? seconds : undefined;
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function isTransientStartupError(error: unknown): boolean {
  return !(error instanceof AuthApiError) || error.status >= 500;
}
