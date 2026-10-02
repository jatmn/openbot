import { EventEmitter } from "node:events";
import {
  REMOTE_DESKTOP_ERROR_CODES,
  type RemoteDesktopConnectInput,
  type RemoteDesktopConnectResult,
  type RemoteDesktopSession,
} from "@openbot/contracts/ipc";
import { Effect, Result } from "effect";
import { RemoteRequestError } from "./remote-server-errors";
import type { RemoteServerManager } from "./remote-server-manager";
import { remoteCall, runRemoteWorkflow } from "./remote-service-effects";

interface RemoteDesktopEvents {
  changed: [sessions: RemoteDesktopSession[]];
}

export class RemoteDesktopManager extends EventEmitter<RemoteDesktopEvents> {
  readonly #servers: Pick<
    RemoteServerManager,
    "createRemoteDesktopSession" | "closeRemoteDesktopSession" | "selectRemoteDesktopDisplay"
  >;
  readonly #sessions = new Map<string, RemoteDesktopSession>();

  constructor(
    servers: Pick<
      RemoteServerManager,
      "createRemoteDesktopSession" | "closeRemoteDesktopSession" | "selectRemoteDesktopDisplay"
    >,
  ) {
    super();
    this.#servers = servers;
  }

  list(): RemoteDesktopSession[] {
    return [...this.#sessions.values()].map((session) => structuredClone(session));
  }

  connect(input: RemoteDesktopConnectInput): Promise<RemoteDesktopConnectResult> {
    return runRemoteWorkflow(this.connectEffect(input));
  }
  readonly connectEffect = Effect.fn("RemoteDesktop.connect")(function* (
    this: RemoteDesktopManager,
    input: RemoteDesktopConnectInput,
  ) {
    const existing = [...this.#sessions.values()].find((session) => session.serverId === input.serverId);
    if (existing) return { status: "connected" as const, session: structuredClone(existing) };
    const attempt = yield* remoteCall(() => this.#servers.createRemoteDesktopSession(input.serverId)).pipe(
      Effect.result,
    );
    if (Result.isFailure(attempt)) {
      const refusal = hostRefusal(attempt.failure.cause);
      if (!refusal) return yield* attempt.failure;
      return refusal;
    }
    const session = attempt.success;
    this.#sessions.set(session.id, session);
    this.#emitChanged();
    return { status: "connected" as const, session: structuredClone(session) };
  });

  disconnect(sessionId: string): Promise<void> {
    return runRemoteWorkflow(this.disconnectEffect(sessionId));
  }
  readonly disconnectEffect = Effect.fn("RemoteDesktop.disconnect")(function* (
    this: RemoteDesktopManager,
    sessionId: string,
  ) {
    const session = this.#sessions.get(sessionId);
    if (!session) return;
    this.#sessions.delete(sessionId);
    this.#emitChanged();
    yield* remoteCall(() => this.#servers.closeRemoteDesktopSession(session.serverId, session.id)).pipe(
      Effect.catch(() => Effect.void),
    );
  });

  selectDisplay(serverId: string, displayId: string): Promise<void> {
    return runRemoteWorkflow(this.selectDisplayEffect(serverId, displayId));
  }
  readonly selectDisplayEffect = Effect.fn("RemoteDesktop.selectDisplay")(function* (
    this: RemoteDesktopManager,
    serverId: string,
    displayId: string,
  ) {
    yield* remoteCall(() => this.#servers.selectRemoteDesktopDisplay(serverId, displayId));
    for (const [id, session] of this.#sessions) {
      if (session.serverId !== serverId) continue;
      this.#sessions.set(id, { ...session, selectedDisplayId: displayId, phase: "connecting" });
    }
    this.#emitChanged();
  });

  stop(): Promise<void> {
    return runRemoteWorkflow(this.stopEffect());
  }
  readonly stopEffect = Effect.fn("RemoteDesktop.stop")(function* (this: RemoteDesktopManager) {
    yield* Effect.forEach([...this.#sessions.keys()], (sessionId) => this.disconnectEffect(sessionId), {
      concurrency: "unbounded",
      discard: true,
    });
  });

  #emitChanged(): void {
    this.emit("changed", this.list());
  }
}

function hostRefusal(error: unknown): Extract<RemoteDesktopConnectResult, { status: "refused" }> | null {
  if (!(error instanceof RemoteRequestError)) return null;
  const errorCode = REMOTE_DESKTOP_ERROR_CODES.find((candidate) => candidate === error.code);
  return errorCode ? { status: "refused", errorCode, message: error.message } : null;
}
