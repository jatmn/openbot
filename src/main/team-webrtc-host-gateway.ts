import { sourceText } from "@openbot/i18n/source";
import { Context, Effect, Layer, ManagedRuntime, Result } from "effect";
import { RemoteWorkflowError, remoteCall } from "./remote-service-effects";
import type { TeamWebRtcBridge } from "./team-webrtc-bridge";
import { type IncomingConnection, TeamWebRtcHostPeer, type TeamWebRtcHostPeerOptions } from "./team-webrtc-host-peer";

interface TeamWebRtcHostGatewayOptions extends TeamWebRtcHostPeerOptions {
  renewSignal?: (hostId: string) => Promise<{ signalUrl: string; ticket: string }>;
  onSignalRecoveryFailure?: (error: Error) => void;
}

class HostSignal extends Context.Service<
  HostSignal,
  {
    connect(peerId: string, signalUrl: string, token: string): Effect.Effect<void, RemoteWorkflowError>;
    disconnect(peerId: string): Effect.Effect<void, RemoteWorkflowError>;
  }
>()("openbot/main/HostSignal") {
  static layer(bridge: TeamWebRtcBridge, pendingConnections: Set<Promise<void>>) {
    return Layer.succeed(
      HostSignal,
      HostSignal.of({
        disconnect: (peerId) => remoteCall(() => bridge.disconnect(peerId)),
        connect: Effect.fn("HostSignal.connect")((peerId: string, signalUrl: string, token: string) =>
          Effect.callback<void, RemoteWorkflowError>((resume) => {
            const cleanup = () => {
              clearTimeout(timer);
              bridge.off("signalReady", onReady);
              bridge.off("error", onError);
            };
            const onReady = (id: string) => {
              if (id !== peerId) return;
              cleanup();
              resume(Effect.void);
            };
            const onError = (id: string, _code: string, message: string) => {
              if (id !== peerId) return;
              cleanup();
              resume(Effect.fail(new RemoteWorkflowError({ cause: new Error(message) })));
            };
            const timer = setTimeout(() => {
              cleanup();
              resume(
                Effect.fail(new RemoteWorkflowError({ cause: new Error(sourceText("error.remote.signalTimeout")) })),
              );
            }, 30_000);
            // Subscribe before connect: local Signal can answer before the bridge command resolves.
            bridge.on("signalReady", onReady);
            bridge.on("error", onError);
            const operation = bridge.connect({ peerId, signalUrl, token, peer: "host" });
            pendingConnections.add(operation);
            void operation.then(
              () => pendingConnections.delete(operation),
              () => pendingConnections.delete(operation),
            );
            void operation.catch((cause) => {
              cleanup();
              resume(Effect.fail(new RemoteWorkflowError({ cause })));
            });
            return Effect.sync(cleanup);
          }),
        ),
      }),
    );
  }
}

/** One Signal registration, with independently authenticated device connections. */
export class TeamWebRtcHostGateway {
  readonly #options: TeamWebRtcHostGatewayOptions;
  readonly #bridge: TeamWebRtcBridge;
  readonly #runtime: ManagedRuntime.ManagedRuntime<HostSignal, never>;
  readonly #operations = new Set<Promise<unknown>>();
  readonly #pendingConnections = new Set<Promise<void>>();
  readonly #retiring = new Set<Promise<void>>();
  readonly #peers = new Map<string, TeamWebRtcHostPeer>();
  #hostId: string | null = null;
  #localApiPort: number | null = null;
  #signalRecovery: Promise<void> | null = null;
  #connectionAbort = new AbortController();
  #disposal: Promise<void> | null = null;

  constructor(options: TeamWebRtcHostGatewayOptions) {
    this.#options = options;
    this.#bridge = options.bridge;
    this.#runtime = ManagedRuntime.make(HostSignal.layer(options.bridge, this.#pendingConnections));
    this.#bridge.on("incoming", this.#onIncoming);
    this.#bridge.on("disconnected", this.#onDisconnected);
    this.#bridge.on("error", this.#onError);
  }

  #run<A>(operation: Effect.Effect<A, RemoteWorkflowError, HostSignal>, signal?: AbortSignal): Promise<A> {
    const promise = this.#runtime.runPromise(Effect.result(operation), { signal }).then((result) => {
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

  start(input: { hostId: string; signalUrl: string; ticket: string; localApiPort: number }): Promise<void> {
    this.#hostId = input.hostId;
    this.#localApiPort = input.localApiPort;
    this.#connectionAbort = new AbortController();
    return this.#run(
      HostSignal.use((signal) => signal.connect(input.hostId, input.signalUrl, input.ticket)),
      this.#connectionAbort.signal,
    ).catch(async (error) => {
      await this.stop();
      throw error;
    });
  }

  async stop(): Promise<void> {
    const hostId = this.#hostId;
    this.#hostId = null;
    this.#localApiPort = null;
    this.#connectionAbort.abort();
    await this.#clearPeers();
    await this.#signalRecovery?.catch(() => undefined);
    await Promise.allSettled([...this.#pendingConnections]);
    if (hostId) await this.#run(HostSignal.use((signal) => signal.disconnect(hostId)));
  }

  revokeSession(sessionId: string): Promise<void> {
    return this.#run(
      Effect.forEach([...this.#peers.values()], (peer) => remoteCall(() => peer.revokeSession(sessionId)), {
        concurrency: "unbounded",
        discard: true,
      }),
    );
  }

  /** Whether any connected device has a file transfer moving right now, either direction. */
  hasActiveTransfers(): boolean {
    return [...this.#peers.values()].some((peer) => peer.hasActiveTransfers());
  }

  dispose(): Promise<void> {
    if (!this.#disposal) this.#disposal = this.#disposeRuntime();
    return this.#disposal;
  }
  async #disposeRuntime(): Promise<void> {
    try {
      await this.stop();
    } finally {
      this.#bridge.off("incoming", this.#onIncoming);
      this.#bridge.off("disconnected", this.#onDisconnected);
      this.#bridge.off("error", this.#onError);
      await Promise.allSettled([...this.#operations]);
      await this.#runtime.dispose();
    }
  }

  #retire(peer: TeamWebRtcHostPeer): void {
    const operation = peer.dispose();
    this.#retiring.add(operation);
    void operation.then(
      () => this.#retiring.delete(operation),
      () => this.#retiring.delete(operation),
    );
  }
  async #clearPeers(): Promise<void> {
    for (const peer of this.#peers.values()) this.#retire(peer);
    this.#peers.clear();
    await Promise.allSettled([...this.#retiring]);
  }

  readonly #onIncoming = (peerId: string, connection: IncomingConnection): void => {
    if (connection.hostId !== this.#hostId || this.#localApiPort === null || peerId === this.#hostId) return;
    let peer = this.#peers.get(peerId);
    if (!peer) {
      peer = new TeamWebRtcHostPeer(this.#options, {
        peerId,
        hostId: connection.hostId,
        localApiPort: this.#localApiPort,
      });
      this.#peers.set(peerId, peer);
    }
    peer.incoming(connection);
  };

  readonly #onDisconnected = (peerId: string): void => {
    if (peerId === this.#hostId) void this.#clearPeers();
    else {
      const peer = this.#peers.get(peerId);
      if (peer) this.#retire(peer);
      this.#peers.delete(peerId);
    }
  };

  readonly #onError = (peerId: string, code: string): void => {
    if (
      peerId !== this.#hostId ||
      !this.#options.renewSignal ||
      this.#signalRecovery ||
      (code !== "authentication_required" && code !== "session_revoked")
    )
      return;
    this.#signalRecovery = this.#run(this.#recoverSignalEffect(peerId), this.#connectionAbort.signal)
      .catch((error) => {
        if (this.#hostId === peerId)
          this.#options.onSignalRecoveryFailure?.(
            error instanceof Error ? error : new Error(sourceText("error.remote.signalRecoveryFailed")),
          );
      })
      .finally(() => {
        this.#signalRecovery = null;
      });
  };

  readonly #recoverSignalEffect = Effect.fn("HostGateway.recoverSignal")(function* (
    this: TeamWebRtcHostGateway,
    hostId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, HostSignal> {
    const renew = this.#options.renewSignal;
    if (!renew) return;
    const bootstrap = yield* remoteCall(() => renew(hostId));
    if (this.#hostId !== hostId) return;
    yield* remoteCall(() => this.#clearPeers());
    yield* HostSignal.use((signal) => signal.disconnect(hostId)).pipe(Effect.catch(() => Effect.void));
    if (this.#hostId !== hostId) return;
    yield* HostSignal.use((signal) => signal.connect(hostId, bootstrap.signalUrl, bootstrap.ticket));
    if (this.#hostId !== hostId)
      yield* HostSignal.use((signal) => signal.disconnect(hostId)).pipe(Effect.catch(() => Effect.void));
  });
}
