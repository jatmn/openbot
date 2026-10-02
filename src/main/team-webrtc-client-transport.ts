import { generateKeyPairSync, randomBytes, sign, verify } from "node:crypto";
import { EventEmitter } from "node:events";
import type { AgentEvent, TeamRealtimeEvent } from "@openbot/contracts/ipc";
import { isDynamicRecord, isNumber, isString } from "@openbot/contracts/runtime-values";
import { TEAM_CURRENT_CAPABILITIES } from "@openbot/contracts/team-protocol/current";
import { optionalTeamEvent } from "@openbot/contracts/team-protocol/optional-events";
import { teamSideRouteCodec } from "@openbot/contracts/team-protocol/side-routes";
import {
  type TeamProtocolV1CurrentEventControl,
  toWireTeamProtocolV1ClientEvent,
} from "@openbot/contracts/team-protocol/v1-adapter";
import {
  decodeTeamProtocolV2AuthFrame,
  decodeTeamProtocolV2EventFrame,
  decodeTeamProtocolV2RpcFrame,
  encodeTeamProtocolV2Frame,
  type TeamProtocolV2AuthFrame,
  type TeamProtocolV2Json,
  type TeamProtocolV2RpcFrame,
  teamProtocolV2AuthenticationTranscript,
} from "@openbot/contracts/team-protocol/v2";
import {
  decodeTeamProtocolV5CurrentEvent,
  decodeTeamProtocolV5WebRtcHttpResponse,
  encodeTeamProtocolV5WebRtcHttpRequest,
} from "@openbot/contracts/team-protocol/v5-webrtc-adapter";
import { sourceText } from "@openbot/i18n/source";
import { Context, Effect, Layer, ManagedRuntime, Result, Schema } from "effect";
import type { RemoteConnectionBootstrap } from "./central-auth-manager";
import type {
  RemoteHostSummary,
  RemoteInvitePreview,
  RemoteInviteRecord,
  RemoteMemberRecord,
} from "./central-auth-records";
import { RemoteWorkflowError, remoteCall, remoteDecode } from "./remote-service-effects";
import type { TeamWebRtcBridge } from "./team-webrtc-bridge";
import { TeamWebRtcFileTransfer } from "./team-webrtc-file-transfer";

export const TEAM_WEBRTC_REMOTE_REQUEST_TIMEOUT_MILLISECONDS = 10 * 60_000 + 30_000;

/** Node clamps a longer `setTimeout` to one millisecond, and says so on stderr. */
const MAXIMUM_TIMER_DELAY_MILLISECONDS = 2_147_483_647;

interface TeamWebRtcClientTransportEvents {
  connected: [hostId: string];
  disconnected: [hostId: string];
  event: [hostId: string, event: AgentEvent | TeamRealtimeEvent];
  path: [hostId: string, path: "p2p" | "relay"];
  error: [hostId: string, code: string, message: string];
  desktopData: [hostId: string, data: string | ArrayBuffer];
}

interface TeamWebRtcClientTransportOptions {
  bridge: TeamWebRtcBridge;
  listHosts: () => Promise<RemoteHostSummary[]>;
  startSession: (hostId: string) => Promise<{ sessionId: string; hostId: string; expiresAt: number }>;
  issueTicket: (sessionId: string, clientPublicKey: string) => Promise<RemoteConnectionBootstrap>;
  endSession: (sessionId: string) => Promise<void>;
  createInvite: (
    hostId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ) => Promise<{ inviteId: string; token: string; expiresAt: number; permanent: boolean; useCount: number }>;
  listInvites: (hostId: string) => Promise<RemoteInviteRecord[]>;
  previewInvite: (token: string) => Promise<RemoteInvitePreview>;
  acceptInvite: (token: string) => Promise<{ hostId: string; membershipId: string; role: "admin" | "member" }>;
  revokeInvite: (inviteId: string) => Promise<void>;
  listMembers: (hostId: string) => Promise<RemoteMemberRecord[]>;
  updateMember: (hostId: string, membershipId: string, role: "admin" | "member", reactivate?: boolean) => Promise<void>;
  removeMember: (hostId: string, membershipId: string) => Promise<void>;
  getPrincipalId: () => string;
  controlPlaneUrl: string;
  downloadHostLogo: (hostId: string, version: string) => Promise<{ bytes: Uint8Array; mimeType: string }>;
  transferDirectory: string;
}

interface ActiveHost {
  sessionId: string;
  expiresAt: number;
  principalId: string;
  connected: boolean;
  connecting: Promise<void> | null;
  cancelled: boolean;
  cancelConnectionWait: (() => void) | null;
  expirationTimer: ReturnType<typeof setTimeout> | null;
  authentication: {
    ticket: string;
    clientPublicKey: string;
    clientPrivateKey: string;
    clientNonce: string;
    hostPublicKey: string;
    binding: { localFingerprint: string; remoteFingerprint: string } | null;
    started: boolean;
    completed: boolean;
    hostNonce: string | null;
  } | null;
}

/**
 * A session kept after a connect attempt failed. The control plane keeps a session until the client
 * ends it, so the next attempt only needs a ticket for it.
 */
interface RetainedSession {
  sessionId: string;
  expiresAt: number;
  principalId: string;
  connected: false;
  connecting: null;
}

class TeamClientBridge extends Context.Service<
  TeamClientBridge,
  {
    send(...args: Parameters<TeamWebRtcBridge["send"]>): Effect.Effect<void, RemoteWorkflowError>;
    connect(...args: Parameters<TeamWebRtcBridge["connect"]>): Effect.Effect<void, RemoteWorkflowError>;
    disconnect(hostId: string): Effect.Effect<void, RemoteWorkflowError>;
  }
>()("openbot/main/TeamClientBridge") {}

export class TeamWebRtcClientTransport extends EventEmitter<TeamWebRtcClientTransportEvents> {
  readonly #options: TeamWebRtcClientTransportOptions;
  readonly #runtime: ManagedRuntime.ManagedRuntime<TeamClientBridge, never>;
  readonly #operations = new Set<Promise<unknown>>();
  #stopping: Promise<void> | null = null;
  readonly #active = new Map<string, ActiveHost>();
  // A failed attempt used to end its session, so each retry against an offline host was a create, a
  // ticket and an end: three Worker requests and a Signal webhook. Only `disconnect` ends it now.
  readonly #retainedSessions = new Map<string, RetainedSession>();
  readonly #files: TeamWebRtcFileTransfer;
  readonly #pending = new Map<
    string,
    {
      hostId: string;
      resolve: (value: TeamProtocolV2Json) => void;
      reject: (error: Error) => void;
      timer: ReturnType<typeof setTimeout>;
    }
  >();
  readonly #lastEventSequence = new Map<string, number>();
  readonly #hostPublicKeys = new Map<string, string>();

  constructor(options: TeamWebRtcClientTransportOptions) {
    super();
    this.#options = options;
    this.#runtime = ManagedRuntime.make(
      Layer.succeed(
        TeamClientBridge,
        TeamClientBridge.of({
          send: (...args) => remoteCall(() => options.bridge.send(...args)),
          connect: (...args) => remoteCall(() => options.bridge.connect(...args)),
          disconnect: (hostId) => remoteCall(() => options.bridge.disconnect(hostId)),
        }),
      ),
    );
    this.#files = new TeamWebRtcFileTransfer(
      options.bridge,
      options.transferDirectory,
      undefined,
      (peerId) => this.#active.get(peerId)?.connected === true,
    );
    options.bridge.on("connected", this.#onConnected);
    options.bridge.on("disconnected", this.#onDisconnected);
    options.bridge.on("data", this.#onData);
    options.bridge.on("path", this.#onPath);
    options.bridge.on("error", this.#onError);
  }

  listHosts(): Promise<RemoteHostSummary[]> {
    return this.#run(this.listHostsEffect());
  }
  readonly listHostsEffect = Effect.fn("TeamWebRtcClient.listHosts")(function* (
    this: TeamWebRtcClientTransport,
  ): Effect.fn.Return<RemoteHostSummary[], RemoteWorkflowError, TeamClientBridge> {
    return yield* remoteCall(() => this.#options.listHosts());
  });

  pinHostKey(hostId: string, publicKey: string): void {
    this.#hostPublicKeys.set(hostId, publicKey);
  }

  get controlPlaneUrl(): string {
    return this.#options.controlPlaneUrl;
  }

  downloadHostLogo(hostId: string, version: string) {
    return this.#run(remoteCall(() => this.#options.downloadHostLogo(hostId, version)));
  }

  createInvite(hostId: string, input: { role: "admin" | "member"; email?: string }) {
    return this.#run(remoteCall(() => this.#options.createInvite(hostId, input)));
  }

  listInvites(hostId: string) {
    return this.#run(remoteCall(() => this.#options.listInvites(hostId)));
  }

  previewInvite(token: string) {
    return this.#run(remoteCall(() => this.#options.previewInvite(token)));
  }

  acceptInvite(token: string) {
    return this.#run(remoteCall(() => this.#options.acceptInvite(token)));
  }

  revokeInvite(inviteId: string) {
    return this.#run(remoteCall(() => this.#options.revokeInvite(inviteId)));
  }

  listMembers(hostId: string) {
    return this.#run(remoteCall(() => this.#options.listMembers(hostId)));
  }

  updateMember(hostId: string, membershipId: string, role: "admin" | "member", reactivate = false) {
    return this.#run(remoteCall(() => this.#options.updateMember(hostId, membershipId, role, reactivate)));
  }

  removeMember(hostId: string, membershipId: string) {
    return this.#run(remoteCall(() => this.#options.removeMember(hostId, membershipId)));
  }

  leaveHost(hostId: string): Promise<void> {
    return this.#run(this.leaveHostEffect(hostId));
  }
  readonly leaveHostEffect = Effect.fn("TeamWebRtcClient.leaveHost")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    const host = (yield* remoteCall(() => this.#options.listHosts())).find((candidate) => candidate.hostId === hostId);
    if (!host) return;
    if (host.role === "owner")
      return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.remote.ownerCannotLeave")) });
    yield* remoteCall(() => this.#options.removeMember(hostId, host.membershipId));
  });

  sendDesktop(hostId: string, data: string | ArrayBuffer): Promise<void> {
    return this.#run(this.sendDesktopEffect(hostId, data));
  }
  readonly sendDesktopEffect = Effect.fn("TeamWebRtcClient.sendDesktop")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    data: string | ArrayBuffer,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    yield* this.#ensureConnectedEffect(hostId);
    yield* TeamClientBridge.use((bridge) => bridge.send(hostId, "desktop", data));
  });

  requestRuntimeSnapshot(hostId: string): Promise<void> {
    return this.#run(this.requestRuntimeSnapshotEffect(hostId));
  }
  readonly requestRuntimeSnapshotEffect = Effect.fn("TeamWebRtcClient.requestRuntimeSnapshot")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    yield* this.#sendEventControlEffect(hostId, { type: "runtime-snapshot-request" });
  });

  setTyping(hostId: string, agentId: string | null, typing: boolean): Promise<void> {
    return this.#run(this.setTypingEffect(hostId, agentId, typing));
  }
  readonly setTypingEffect = Effect.fn("TeamWebRtcClient.setTyping")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    agentId: string | null,
    typing: boolean,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    yield* this.#sendEventControlEffect(hostId, { type: "team-typing", agentId, typing });
  });

  setDirectTyping(hostId: string, recipientMemberId: string, typing: boolean): Promise<void> {
    return this.#run(this.setDirectTypingEffect(hostId, recipientMemberId, typing));
  }
  readonly setDirectTypingEffect = Effect.fn("TeamWebRtcClient.setDirectTyping")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    recipientMemberId: string,
    typing: boolean,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    yield* this.#sendEventControlEffect(hostId, { type: "team-direct-typing", recipientMemberId, typing });
  });

  connect(hostId: string): Promise<void> {
    return this.#ensureConnected(hostId);
  }

  /**
   * Whether the data channel to this host is up and authenticated. `connect` resolves either way,
   * and only the first of the two announces itself with a `connected` event, so a caller that has
   * to reconcile its own state with the transport's needs to be able to ask.
   */
  isConnected(hostId: string): boolean {
    return this.#active.get(hostId)?.connected === true;
  }

  request(
    hostId: string,
    path: string,
    init: { method?: string; body?: unknown; preserveSemanticTags?: boolean; agentCreateModel?: boolean } = {},
  ): Promise<TeamProtocolV2Json | undefined> {
    return this.#run(this.requestEffect(hostId, path, init));
  }
  readonly requestEffect = Effect.fn("TeamWebRtcClient.request")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    path: string,
    init: { method?: string; body?: unknown; preserveSemanticTags?: boolean; agentCreateModel?: boolean } = {},
  ): Effect.fn.Return<TeamProtocolV2Json | undefined, RemoteWorkflowError, TeamClientBridge> {
    const response = yield* this.requestResponseEffect(hostId, path, init);
    return response.status === 204 ? undefined : response.body;
  });

  requestResponse(
    hostId: string,
    path: string,
    init: {
      method?: string;
      body?: unknown;
      contentType?: string;
      preserveSemanticTags?: boolean;
      agentCreateModel?: boolean;
    } = {},
  ): Promise<{
    status: number;
    body: TeamProtocolV2Json;
    file?: { bytes: Uint8Array; name: string; mimeType: string };
  }> {
    return this.#run(this.requestResponseEffect(hostId, path, init));
  }
  readonly requestResponseEffect = Effect.fn("TeamWebRtcClient.requestResponse")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    path: string,
    init: {
      method?: string;
      body?: unknown;
      contentType?: string;
      preserveSemanticTags?: boolean;
      agentCreateModel?: boolean;
    } = {},
  ): Effect.fn.Return<
    {
      status: number;
      body: TeamProtocolV2Json;
      file?: { bytes: Uint8Array; name: string; mimeType: string };
    },
    RemoteWorkflowError,
    TeamClientBridge
  > {
    yield* this.#ensureConnectedEffect(hostId);
    const method = (init.method ?? "GET").toUpperCase();
    const binary = binaryBody(init.body);
    const sideRoute = teamSideRouteCodec(path);
    const bodyTransferId = binary
      ? yield* this.#files.sendEffect(hostId, {
          name: "upload",
          mimeType: init.contentType ?? "application/octet-stream",
          bytes: binary,
        })
      : null;
    const requestId = crypto.randomUUID();
    const frame = yield* remoteDecode(() =>
      encodeTeamProtocolV2Frame({
        version: 2,
        type: "request",
        requestId,
        operation: "http.request",
        payload: {
          method,
          path,
          body: binary
            ? null
            : sideRoute
              ? sideRoute.request(path, init.body)
              : encodeTeamProtocolV5WebRtcHttpRequest(method, path, init.body, {
                  preserveSemanticTags: init.preserveSemanticTags,
                  agentCreateModel: init.agentCreateModel,
                }),
          capabilities: [...TEAM_CURRENT_CAPABILITIES],
          ...(bodyTransferId ? { bodyTransferId } : {}),
          ...(init.contentType ? { contentType: init.contentType } : {}),
        },
      }),
    );
    const result = new Promise<TeamProtocolV2Json>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.#pending.delete(requestId);
        reject(new TeamWebRtcRequestError(504, "remote_timeout", sourceText("error.remote.requestTimeout")));
      }, TEAM_WEBRTC_REMOTE_REQUEST_TIMEOUT_MILLISECONDS);
      this.#pending.set(requestId, { hostId, resolve, reject, timer });
    });
    void result.catch(() => undefined);
    const attempt1 = yield* Effect.gen({ self: this }, function* () {
      yield* TeamClientBridge.use((bridge) => bridge.send(hostId, "rpc", frame));
    }).pipe(Effect.result);
    if (Result.isFailure(attempt1)) {
      const error = attempt1.failure.cause;
      const pending = this.#pending.get(requestId);
      if (pending) {
        clearTimeout(pending.timer);
        this.#pending.delete(requestId);
        pending.reject(error instanceof Error ? error : new Error(sourceText("error.remote.requestFailed")));
      }
    }
    const envelope = yield* remoteCall(() => result).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          const pending = this.#pending.get(requestId);
          if (pending) {
            clearTimeout(pending.timer);
            this.#pending.delete(requestId);
          }
        }),
      ),
    );
    if (!isDynamicRecord(envelope) || !isNumber(envelope.status) || !Object.hasOwn(envelope, "body")) {
      return yield* new RemoteWorkflowError({
        cause: new TeamWebRtcRequestError(502, "protocol_error", "The host returned an invalid response."),
      });
    }
    const fileRecord = isDynamicRecord(envelope.file) ? envelope.file : null;
    const transferId = fileRecord && isString(fileRecord.transferId) ? fileRecord.transferId : null;
    const file = transferId ? yield* this.#files.consumeEffect(hostId, transferId) : undefined;
    // The envelope check above catches a frame that is not shaped like a response. This catches a
    // well-formed frame whose *body* the released V3 adapter refuses, which is the same kind of
    // failure and has to carry the same code: a plain error here reads to the caller as an ordinary
    // request failure, so the host stays healthy and reconnectable while talking nonsense.
    const status = envelope.status;
    const body = file
      ? null
      : yield* remoteDecode(() =>
          sideRoute
            ? sideRoute.response(path, status, envelope.body)
            : decodeTeamProtocolV5WebRtcHttpResponse(method, path, status, envelope.body),
        ).pipe(
          Effect.mapError(
            () =>
              new RemoteWorkflowError({
                cause: new TeamWebRtcRequestError(502, "protocol_error", "The host returned an invalid response body."),
              }),
          ),
        );
    return { status: envelope.status, body, ...(file ? { file } : {}) };
  });

  disconnect(hostId: string): Promise<void> {
    return this.#run(this.disconnectEffect(hostId));
  }
  readonly disconnectEffect = Effect.fn("TeamWebRtcClient.disconnect")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    const active = this.#active.get(hostId);
    const sessionId = active?.sessionId || this.#retainedSessions.get(hostId)?.sessionId;
    if (active) {
      active.cancelled = true;
      active.cancelConnectionWait?.();
    }
    // A request sent before disconnect may have committed. Fail it without replaying the request.
    for (const [requestId, pending] of this.#pending) {
      if (pending.hostId !== hostId) continue;
      clearTimeout(pending.timer);
      this.#pending.delete(requestId);
      pending.reject(
        new TeamWebRtcRequestError(503, "remote_disconnected", sourceText("error.remote.hostDisconnected")),
      );
    }
    if (active?.expirationTimer) clearTimeout(active.expirationTimer);
    this.#active.delete(hostId);
    this.#retainedSessions.delete(hostId);
    this.#files.setPeerAuthenticated(hostId, false);
    let disconnectError: unknown;
    const attempt2 = yield* Effect.gen({ self: this }, function* () {
      yield* TeamClientBridge.use((bridge) => bridge.disconnect(hostId));
    }).pipe(Effect.result);
    if (Result.isFailure(attempt2)) {
      const error = attempt2.failure.cause;
      disconnectError = error;
    }
    if (sessionId) yield* remoteCall(() => this.#options.endSession(sessionId)).pipe(Effect.catch(() => Effect.void));
    if (disconnectError) return yield* new RemoteWorkflowError({ cause: disconnectError });
  });

  #run<A>(operation: Effect.Effect<A, RemoteWorkflowError, TeamClientBridge>): Promise<A> {
    const promise = this.#runtime.runPromise(Effect.result(operation)).then((result) => {
      if (Result.isFailure(result)) throw result.failure.cause;
      return result.success;
    });
    this.#operations.add(promise);
    void promise.then(
      () => this.#operations.delete(promise),
      () => this.#operations.delete(promise),
    );
    return promise;
  }

  stop(): Promise<void> {
    if (!this.#stopping) this.#stopping = this.#stop();
    return this.#stopping;
  }
  async #stop(): Promise<void> {
    const hostIds = new Set([...this.#active.keys(), ...this.#retainedSessions.keys()]);
    await Promise.allSettled([...hostIds].map((hostId) => this.disconnect(hostId)));
    this.#options.bridge.off("connected", this.#onConnected);
    this.#options.bridge.off("disconnected", this.#onDisconnected);
    this.#options.bridge.off("data", this.#onData);
    this.#options.bridge.off("path", this.#onPath);
    this.#options.bridge.off("error", this.#onError);
    await this.#files.stop();
    await Promise.allSettled([...this.#operations]);
    await this.#runtime.dispose();
  }

  /** Whether a transfer is moving right now, either direction. */
  hasActiveTransfers(): boolean {
    return this.#files.hasActiveTransfers();
  }

  #ensureConnected(hostId: string): Promise<void> {
    return this.#run(this.#ensureConnectedEffect(hostId));
  }
  readonly #ensureConnectedEffect = Effect.fn("TeamWebRtcClient.ensureConnected")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    const principalId = this.#options.getPrincipalId();
    let current: ActiveHost | RetainedSession | undefined =
      this.#active.get(hostId) ?? this.#retainedSessions.get(hostId);
    if (current?.expiresAt && current.expiresAt <= Date.now() + 30_000) {
      yield* this.disconnectEffect(hostId);
      current = undefined;
    }
    if (current && current.principalId !== principalId) {
      yield* this.disconnectEffect(hostId);
      current = undefined;
    }
    if (current?.connected) return;
    const connecting = current?.connecting;
    if (connecting) return yield* remoteCall(() => connecting);
    const active: ActiveHost = {
      sessionId: current?.sessionId ?? "",
      expiresAt: current?.expiresAt ?? 0,
      principalId,
      connected: false,
      connecting: null,
      cancelled: false,
      cancelConnectionWait: null,
      expirationTimer: null,
      authentication: null,
    };
    const operation = this.#connect(hostId, active, current?.sessionId || null).catch((error) => {
      if (this.#active.get(hostId) === active) this.#active.delete(hostId);
      throw error;
    });
    active.connecting = operation;
    this.#retainedSessions.delete(hostId);
    this.#active.set(hostId, active);
    return yield* remoteCall(() => operation);
  });

  #connect(hostId: string, active: ActiveHost, existingSessionId: string | null): Promise<void> {
    return this.#run(this.#connectEffect(hostId, active, existingSessionId));
  }
  readonly #connectEffect = Effect.fn("TeamWebRtcClient.connect")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    active: ActiveHost,
    existingSessionId: string | null,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    const hostPublicKey = this.#hostPublicKeys.get(hostId);
    if (!hostPublicKey)
      return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.remote.pinnedKeyMissing")) });
    const clientKeys = yield* remoteDecode(() =>
      generateKeyPairSync("ed25519", {
        publicKeyEncoding: { type: "spki", format: "pem" },
        privateKeyEncoding: { type: "pkcs8", format: "pem" },
      }),
    );
    const clientPublicKey = clientKeys.publicKey.trim();
    let sessionId = existingSessionId;
    let startedNewSession = false;
    const bootstrap = yield* Effect.gen({ self: this }, function* () {
      if (!sessionId) {
        const session = yield* remoteCall(() => this.#options.startSession(hostId));
        sessionId = session.sessionId;
        active.sessionId = sessionId;
        active.expiresAt = session.expiresAt;
        startedNewSession = true;
        yield* this.#assertCurrentEffect(hostId, active, sessionId);
      }
      const ticketSessionId = sessionId;
      return yield* Effect.gen({ self: this }, function* () {
        const ticket = yield* remoteCall(() => this.#options.issueTicket(ticketSessionId, clientPublicKey));
        yield* this.#assertCurrentEffect(hostId, active, ticketSessionId);
        return ticket;
      }).pipe(
        Effect.catch((failure) =>
          Effect.gen({ self: this }, function* () {
            // Only an ended session is replaced. Other failures keep it for the next ticket request.
            if (!existingSessionId || !isEndedSessionError(failure.cause)) return yield* failure;
            yield* remoteCall(() => this.#options.endSession(existingSessionId)).pipe(Effect.catch(() => Effect.void));
            const session = yield* remoteCall(() => this.#options.startSession(hostId));
            sessionId = session.sessionId;
            active.sessionId = sessionId;
            active.expiresAt = session.expiresAt;
            startedNewSession = true;
            yield* this.#assertCurrentEffect(hostId, active, session.sessionId);
            const ticket = yield* remoteCall(() => this.#options.issueTicket(session.sessionId, clientPublicKey));
            yield* this.#assertCurrentEffect(hostId, active, session.sessionId);
            return ticket;
          }),
        ),
      );
    }).pipe(
      Effect.catch((failure) =>
        Effect.gen({ self: this }, function* () {
          const failedSessionId = sessionId;
          if (failedSessionId && !this.#retainSession(hostId, active, failedSessionId)) {
            yield* remoteCall(() => this.#options.endSession(failedSessionId)).pipe(Effect.catch(() => Effect.void));
          }
          return yield* failure;
        }),
      ),
    );
    if (startedNewSession) this.#lastEventSequence.delete(hostId);
    const connectedSessionId = active.sessionId;
    let cleanupConnectionWait: () => void = () => undefined;
    // Subscribe before bridge.connect: a bridge can deliver authentication events before it resolves.
    const connected = new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timer);
        this.off("connected", onConnected);
        this.off("error", onError);
      };
      cleanupConnectionWait = cleanup;
      active.cancelConnectionWait = () => {
        cleanup();
        reject(new Error(sourceText("error.remote.connectionCancelled")));
      };
      const timer = setTimeout(() => {
        cleanup();
        reject(new Error(sourceText("error.remote.hostDidNotConnect")));
      }, 30_000);
      const onConnected = (connectedHostId: string) => {
        if (connectedHostId !== hostId) return;
        cleanup();
        resolve();
      };
      const onError = (failedHostId: string, _code: string, message: string) => {
        if (failedHostId !== hostId) return;
        cleanup();
        reject(new Error(message));
      };
      this.on("connected", onConnected);
      this.on("error", onError);
    });
    // The wait can fail while the bridge is still starting; observe that rejection immediately.
    void connected.catch(() => undefined);
    active.authentication = {
      ticket: bootstrap.ticket,
      clientPublicKey,
      clientPrivateKey: clientKeys.privateKey,
      clientNonce: yield* remoteDecode(() => randomBytes(32).toString("base64url")),
      hostPublicKey,
      binding: null,
      started: false,
      completed: false,
      hostNonce: null,
    };
    yield* Effect.gen({ self: this }, function* () {
      yield* TeamClientBridge.use((bridge) =>
        bridge.connect({
          peerId: hostId,
          signalUrl: bootstrap.signalUrl,
          token: bootstrap.ticket,
          peer: "client",
        }),
      );
      yield* this.#assertCurrentEffect(hostId, active, connectedSessionId);
      yield* remoteCall(() => connected);
      this.#scheduleExpiration(hostId, active);
    }).pipe(
      Effect.catch((failure) =>
        Effect.gen({ self: this }, function* () {
          const retained = this.#retainSession(hostId, active, connectedSessionId);
          if (this.#active.get(hostId) === active) this.#active.delete(hostId);
          yield* TeamClientBridge.use((bridge) => bridge.disconnect(hostId)).pipe(Effect.catch(() => Effect.void));
          if (!retained)
            yield* remoteCall(() => this.#options.endSession(connectedSessionId)).pipe(Effect.catch(() => Effect.void));
          return yield* failure;
        }),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          cleanupConnectionWait();
          active.cancelConnectionWait = null;
        }),
      ),
    );
  });

  /** Keeps the session of an attempt that failed on its own. A cancelled attempt ends its session. */
  #retainSession(hostId: string, active: ActiveHost, sessionId: string): boolean {
    if (active.cancelled || this.#active.get(hostId) !== active) return false;
    this.#retainedSessions.set(hostId, {
      sessionId,
      expiresAt: active.expiresAt,
      principalId: active.principalId,
      connected: false,
      connecting: null,
    });
    return true;
  }

  readonly #assertCurrentEffect = Effect.fn("TeamWebRtcClient.assertCurrent")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    active: ActiveHost,
    sessionId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    if (!active.cancelled && this.#active.get(hostId) === active) return;
    yield* TeamClientBridge.use((bridge) => bridge.disconnect(hostId)).pipe(Effect.catch(() => Effect.void));
    yield* remoteCall(() => this.#options.endSession(sessionId)).pipe(Effect.catch(() => Effect.void));
    return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.remote.connectionCancelled")) });
  });

  readonly #sendEventControlEffect = Effect.fn("TeamWebRtcClient.sendEventControl")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    control: TeamProtocolV1CurrentEventControl,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    yield* this.#ensureConnectedEffect(hostId);
    yield* TeamClientBridge.use((bridge) =>
      bridge.send(
        hostId,
        "events",
        encodeTeamProtocolV2Frame({
          version: 2,
          type: "event-control",
          control: toWireTeamProtocolV1ClientEvent(control),
        }),
      ),
    );
  });

  #scheduleExpiration(hostId: string, active: ActiveHost): void {
    if (active.expirationTimer) clearTimeout(active.expirationTimer);
    if (!active.expiresAt) return;
    const remaining = Math.max(0, active.expiresAt - Date.now() - 30_000);
    // Wait in bounded steps, exactly as the host schedules its half of the same session in
    // `#scheduleSessionExpiration`. An account session is persistent -- the control plane answers
    // `startSession` with `PERSISTENT_SESSION_EXPIRES_AT`, the largest date JavaScript has -- so the
    // delay is a quarter of a million years and overflows Node's signed 32-bit timer range. Node
    // resolves that by firing in one millisecond, which disconnected the client roughly as fast as
    // it finished authenticating: the channel closed under the first request, and the caller waited
    // out the full ten-minute request timeout for a frame that had nowhere to go.
    active.expirationTimer = setTimeout(
      () => {
        active.expirationTimer = null;
        if (this.#active.get(hostId) !== active) return;
        if (remaining > MAXIMUM_TIMER_DELAY_MILLISECONDS) this.#scheduleExpiration(hostId, active);
        else void this.disconnect(hostId).catch(() => undefined);
      },
      Math.min(remaining, MAXIMUM_TIMER_DELAY_MILLISECONDS),
    );
    active.expirationTimer.unref?.();
  }

  readonly #onConnected = (hostId: string, binding?: { localFingerprint: string; remoteFingerprint: string }): void => {
    void this.#run(this.#beginAuthenticationEffect(hostId, binding));
  };
  readonly #beginAuthenticationEffect = Effect.fn("TeamWebRtcClient.beginAuthentication")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    binding?: { localFingerprint: string; remoteFingerprint: string },
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    const active = this.#active.get(hostId);
    if (!active) return;
    if (active.cancelled) {
      yield* TeamClientBridge.use((bridge) => bridge.disconnect(hostId)).pipe(Effect.catch(() => Effect.void));
      return;
    }
    const authentication = active.authentication;
    if (!binding) {
      this.#failProtocol(hostId, "The WebRTC channel binding is unavailable.");
      return;
    }
    if (!authentication || authentication.started) return;
    authentication.started = true;
    authentication.binding = binding;
    const transcript = teamProtocolV2AuthenticationTranscript({
      hostId,
      sessionId: active.sessionId,
      ticket: authentication.ticket,
      clientPublicKey: authentication.clientPublicKey,
      clientNonce: authentication.clientNonce,
      clientFingerprint: binding.localFingerprint,
      hostFingerprint: binding.remoteFingerprint,
    });
    yield* remoteDecode(() =>
      encodeTeamProtocolV2Frame({
        version: 2,
        type: "auth-init",
        ticket: authentication.ticket,
        clientPublicKey: authentication.clientPublicKey,
        clientNonce: authentication.clientNonce,
        signature: sign(null, Buffer.from(transcript), authentication.clientPrivateKey).toString("base64url"),
      }),
    ).pipe(
      Effect.flatMap((frame) => TeamClientBridge.use((bridge) => bridge.send(hostId, "rpc", frame))),
      Effect.catch(() => Effect.sync(() => this.#failProtocol(hostId, "The client authentication handshake failed."))),
    );
  });

  #finishConnected(hostId: string, active: ActiveHost): void {
    this.#files.setPeerAuthenticated(hostId, true);
    active.connected = true;
    active.connecting = null;
    this.#sendRecoverable(
      hostId,
      "events",
      encodeTeamProtocolV2Frame({
        version: 2,
        type: "event-ack",
        throughSequence: this.#lastEventSequence.get(hostId) ?? 0,
      }),
    );
    this.emit("connected", hostId);
  }

  readonly #onDisconnected = (hostId: string): void => {
    const active = this.#active.get(hostId);
    if (!active) return;
    this.#files.setPeerAuthenticated(hostId, false);
    active.connected = false;
    this.#lastEventSequence.delete(hostId);
    for (const [requestId, pending] of this.#pending) {
      if (pending.hostId !== hostId) continue;
      clearTimeout(pending.timer);
      this.#pending.delete(requestId);
      pending.reject(
        new TeamWebRtcRequestError(503, "remote_disconnected", sourceText("error.remote.hostDisconnected")),
      );
    }
    this.emit("disconnected", hostId);
  };

  readonly #onData = (
    hostId: string,
    channel: "rpc" | "events" | "files" | "desktop",
    data: string | ArrayBuffer,
  ): void => {
    const active = this.#active.get(hostId);
    if (!active) return;
    const authFrame = isString(data) && channel === "rpc" ? authenticationFrame(data) : null;
    if (!active?.connected && authFrame?.type !== "auth-ready" && authFrame?.type !== "auth-confirmed") {
      this.#failProtocol(hostId, "The host sent data before end-to-end authentication.");
      return;
    }
    if (channel === "desktop") {
      this.emit("desktopData", hostId, data);
      return;
    }
    if (!isString(data)) {
      if (channel === "rpc" || channel === "events") {
        this.#failProtocol(hostId, `The host returned binary data on the ${channel} channel.`);
      }
      return;
    }
    if (authFrame?.type === "auth-ready") void this.#handleAuthentication(hostId, authFrame);
    else if (authFrame?.type === "auth-confirmed") this.#handleAuthenticationConfirmation(hostId, authFrame);
    else if (channel === "rpc") this.#handleRpc(hostId, data);
    else if (channel === "events") this.#handleEvent(hostId, data);
  };

  #handleAuthentication(
    hostId: string,
    frame: Extract<TeamProtocolV2AuthFrame, { type: "auth-ready" }>,
  ): Promise<void> {
    return this.#run(this.#handleAuthenticationEffect(hostId, frame));
  }
  readonly #handleAuthenticationEffect = Effect.fn("TeamWebRtcClient.handleAuthentication")(function* (
    this: TeamWebRtcClientTransport,
    hostId: string,
    frame: Extract<TeamProtocolV2AuthFrame, { type: "auth-ready" }>,
  ): Effect.fn.Return<void, RemoteWorkflowError, TeamClientBridge> {
    return yield* Effect.gen({ self: this }, function* () {
      const active = this.#active.get(hostId);
      const authentication = active?.authentication;
      if (!active || !authentication?.binding || active.connected || authentication.completed) {
        return yield* new RemoteWorkflowError({ cause: new Error("Authentication is not pending.") });
      }
      if (frame.clientNonce !== authentication.clientNonce) {
        return yield* new RemoteWorkflowError({
          cause: new Error("The host authentication response does not match the request."),
        });
      }
      const transcript = teamProtocolV2AuthenticationTranscript({
        hostId,
        sessionId: active.sessionId,
        ticket: authentication.ticket,
        clientPublicKey: authentication.clientPublicKey,
        clientNonce: authentication.clientNonce,
        hostNonce: frame.hostNonce,
        clientFingerprint: authentication.binding.localFingerprint,
        hostFingerprint: authentication.binding.remoteFingerprint,
      });
      if (
        !(yield* remoteDecode(() =>
          verify(
            null,
            Buffer.from(transcript),
            authentication.hostPublicKey,
            Buffer.from(frame.signature, "base64url"),
          ),
        ))
      ) {
        return yield* new RemoteWorkflowError({ cause: new Error("The host device signature is invalid.") });
      }
      authentication.completed = true;
      authentication.hostNonce = frame.hostNonce;
      yield* TeamClientBridge.use((bridge) =>
        bridge.send(
          hostId,
          "rpc",
          encodeTeamProtocolV2Frame({
            version: 2,
            type: "auth-complete",
            clientNonce: authentication.clientNonce,
            hostNonce: frame.hostNonce,
          }),
        ),
      );
    }).pipe(
      Effect.catch(() =>
        Effect.sync(() => {
          this.#failProtocol(hostId, "The host failed end-to-end authentication.");
        }),
      ),
    );
  });

  #handleAuthenticationConfirmation(
    hostId: string,
    frame: Extract<TeamProtocolV2AuthFrame, { type: "auth-confirmed" }>,
  ): void {
    const active = this.#active.get(hostId);
    const authentication = active?.authentication;
    if (
      !active ||
      !authentication?.completed ||
      active.connected ||
      frame.clientNonce !== authentication.clientNonce ||
      frame.hostNonce !== authentication.hostNonce
    ) {
      this.#failProtocol(hostId, "The host returned an invalid authentication confirmation.");
      return;
    }
    this.#finishConnected(hostId, active);
  }

  #handleRpc(hostId: string, data: string): void {
    let frame: TeamProtocolV2RpcFrame;
    try {
      frame = decodeTeamProtocolV2RpcFrame(data);
    } catch {
      this.#failProtocol(hostId, "The host returned an invalid RPC frame.");
      return;
    }
    if (frame.type !== "response") {
      this.#failProtocol(hostId, "The host returned a client RPC frame on the RPC channel.");
      return;
    }
    const pending = this.#pending.get(frame.requestId);
    if (!pending || pending.hostId !== hostId) return;
    clearTimeout(pending.timer);
    this.#pending.delete(frame.requestId);
    if ("error" in frame) {
      pending.reject(new TeamWebRtcRequestError(frame.error.status ?? 500, frame.error.code, frame.error.message));
    } else pending.resolve(frame.result);
  }

  #handleEvent(hostId: string, data: string): void {
    try {
      const frame = decodeTeamProtocolV2EventFrame(data);
      if (frame.type === "event-reset") {
        this.#lastEventSequence.set(hostId, frame.nextSequence - 1);
        this.#sendRecoverable(
          hostId,
          "events",
          encodeTeamProtocolV2Frame({ version: 2, type: "event-ack", throughSequence: frame.nextSequence - 1 }),
        );
        this.#sendRecoverable(
          hostId,
          "events",
          encodeTeamProtocolV2Frame({
            version: 2,
            type: "event-control",
            control: { type: "runtime-snapshot-request" },
          }),
        );
        return;
      }
      if (frame.type !== "event") {
        this.#failProtocol(hostId, "The host returned a client event frame on the event channel.");
        return;
      }
      const lastSequence = this.#lastEventSequence.get(hostId) ?? 0;
      if (frame.sequence <= lastSequence) {
        this.#sendRecoverable(
          hostId,
          "events",
          encodeTeamProtocolV2Frame({ version: 2, type: "event-ack", throughSequence: lastSequence }),
        );
        return;
      }
      if (frame.sequence !== lastSequence + 1) {
        this.#failProtocol(hostId, sourceText("error.remote.eventGap"));
        return;
      }
      const optional = frame.type === "event" ? optionalTeamEvent(frame.payload) : null;
      const decoded = optional
        ? { status: "known" as const, event: optional }
        : decodeTeamProtocolV5CurrentEvent(frame);
      if (decoded.status === "invalid") {
        this.#failProtocol(hostId, sourceText("error.remote.malformedKnownEvent"));
        return;
      }
      if (decoded.status === "known") this.emit("event", hostId, decoded.event);
      this.#lastEventSequence.set(hostId, frame.sequence);
      this.#sendRecoverable(
        hostId,
        "events",
        encodeTeamProtocolV2Frame({ version: 2, type: "event-ack", throughSequence: frame.sequence }),
      );
    } catch {
      this.#failProtocol(hostId, "The host returned an invalid event frame.");
    }
  }

  #failProtocol(hostId: string, message: string): void {
    this.#files.setPeerAuthenticated(hostId, false);
    const error = new TeamWebRtcRequestError(502, "protocol_error", message);
    for (const [requestId, pending] of this.#pending) {
      if (pending.hostId !== hostId) continue;
      clearTimeout(pending.timer);
      this.#pending.delete(requestId);
      pending.reject(error);
    }
    this.emit("error", hostId, error.code, error.message);
    void this.disconnect(hostId).catch(() => undefined);
  }

  readonly #onPath = (hostId: string, path: "p2p" | "relay"): void => {
    if (!this.#active.has(hostId)) return;
    this.emit("path", hostId, path);
  };
  readonly #onError = (hostId: string, code: string, message: string): void => {
    if (!this.#active.has(hostId)) return;
    this.emit("error", hostId, code, message);
  };

  #sendRecoverable(hostId: string, channel: "events", data: string): void {
    void this.#run(
      TeamClientBridge.use((bridge) => bridge.send(hostId, channel, data)).pipe(Effect.catch(() => Effect.void)),
    );
  }
}

export class TeamWebRtcRequestError extends Schema.TaggedError<TeamWebRtcRequestError>()("TeamWebRtcRequestError", {
  status: Schema.Number,
  code: Schema.String,
  message: Schema.String,
}) {
  constructor(status: number, code: string, message: string) {
    super({ status, code, message });
  }
}

function authenticationFrame(data: string): TeamProtocolV2AuthFrame | null {
  try {
    return decodeTeamProtocolV2AuthFrame(data);
  } catch {
    return null;
  }
}

function binaryBody(value: unknown): Uint8Array | null {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  return null;
}

/** The account API answers 403 or 404 for a session that ended, expired, or does not exist. */
function isEndedSessionError(error: unknown): boolean {
  return error instanceof Error && "status" in error && (error.status === 403 || error.status === 404);
}
