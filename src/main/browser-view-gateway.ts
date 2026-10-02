import { sourceText } from "@openbot/i18n/source";
import { Context, Effect, Layer, ManagedRuntime, Result } from "effect";
import { recordRestartActivity } from "../backend/restart-activity";
import { type RemoteWorkflowError, remoteCall, remoteDecode } from "./remote-service-effects";
// The host's side of the live browser view: a session a member asks for, a socket that carries the
// frames, and the pointer and key input that comes back on it.
//
// It is a gateway rather than a route because a route answers once and this does not: the frames go
// on for as long as the member watches. The shape follows `remote-screen-gateway.ts`, including how
// the socket is authorized -- a tunneled socket arrives with the WebRTC session header and no token,
// because the host itself opened it on the member's behalf.

import { randomUUID } from "node:crypto";
import type { IncomingMessage } from "node:http";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import type { Duplex } from "node:stream";
import {
  type BrowserViewSessionResponse,
  browserViewClientAcksFrames,
  browserViewStreamPath,
  browserViewStreamSessionId,
  decodeBrowserViewInput,
  encodeBrowserViewFrame,
} from "@openbot/contracts/team-protocol/browser-view-v1";
import type * as Ws from "ws";
import type { BrowserHost } from "../backend/browser-host";
import { rawDataSize, rawDataText } from "./ws-raw-data";

const requireModule = createRequire(import.meta.url);
const webSockets: typeof Ws = requireModule(join(dirname(requireModule.resolve("ws/package.json")), "index.js"));

/**
 * How many frames may wait in the socket before the next one is dropped. A live view that falls
 * behind must show the page as it is now, not replay the seconds the link was slow.
 */
const MAX_BUFFERED_FRAME_BYTES = 4 * 1024 * 1024;
/**
 * Frames kept after the one the client has drawn. A client that asked to name frames and then
 * stops saying which one is on screen would otherwise keep one entry per frame for the whole
 * view. Past this the view is closed. The drawn frame is not dropped in place: that is what made
 * a click land on the wrong page.
 */
const MAX_UNACKNOWLEDGED_FRAMES = 120;

/** Frames older than the one on the member's screen. The named frame itself stays, so a click on it still expands. */
function forgetFramesBefore(sizes: Map<number, { width: number; height: number }>, drawn: number): void {
  for (const sequence of sizes.keys()) {
    if (sequence < drawn) sizes.delete(sequence);
  }
}
const MAX_SESSIONS = 4;
const MAX_INPUT_MESSAGE_BYTES = 4 * 1024;

export interface BrowserViewGatewayOptions {
  browser: Pick<BrowserHost, "startView" | "dispatchViewInput">;
  /** Answers the member a direct socket's token belongs to, for a client that is not tunneled. */
  authenticate: (token: string) => { id: string } | null;
  maxSessions?: number;
}

class BrowserViewPort extends Context.Service<
  BrowserViewPort,
  {
    start(
      ...args: Parameters<BrowserHost["startView"]>
    ): Effect.Effect<Awaited<ReturnType<BrowserHost["startView"]>>, RemoteWorkflowError>;
    input(...args: Parameters<BrowserHost["dispatchViewInput"]>): Effect.Effect<void, RemoteWorkflowError>;
  }
>()("openbot/main/BrowserViewPort") {
  static layer(browser: BrowserViewGatewayOptions["browser"]) {
    return Layer.succeed(
      BrowserViewPort,
      BrowserViewPort.of({
        start: (...args) => remoteCall(() => browser.startView(...args)),
        input: (...args) => remoteCall(() => browser.dispatchViewInput(...args)),
      }),
    );
  }
}

interface ManagedViewSession {
  id: string;
  tabId: string;
  memberId: string;
  teamSessionId: string;
  socket: Ws.WebSocket | null;
  stopView: (() => Promise<void>) | null;
  /** The size of the last frame sent, which is what a fractional input coordinate refers to. */
  frameWidth: number;
  frameHeight: number;
  /**
   * The shape of each frame this session sent and the client may still be drawing, by sequence.
   * Only a client that acknowledged frames at connect fills this. An older client names no frame,
   * so there is nothing to look up, and keeping one entry per frame would grow for the whole view.
   */
  frameSizes: Map<number, { width: number; height: number }>;
  rememberFrames: boolean;
}

export class BrowserViewGateway {
  readonly #options: BrowserViewGatewayOptions;
  #runtime: ManagedRuntime.ManagedRuntime<BrowserViewPort, never> | null = null;
  readonly #operations = new Set<Promise<unknown>>();
  #stopping: Promise<void> | null = null;
  readonly #webSockets = new webSockets.WebSocketServer({ noServer: true });
  readonly #sessions = new Map<string, ManagedViewSession>();

  constructor(options: BrowserViewGatewayOptions) {
    this.#options = options;
  }

  createSession(input: { memberId: string; teamSessionId: string; tabId: string }): BrowserViewSessionResponse {
    if (this.#sessions.size >= (this.#options.maxSessions ?? MAX_SESSIONS)) {
      throw new Error(sourceText("error.backend.browserViewLimit"));
    }
    const id = randomUUID().replaceAll("-", "");
    this.#sessions.set(id, {
      id,
      tabId: input.tabId,
      memberId: input.memberId,
      teamSessionId: input.teamSessionId,
      socket: null,
      stopView: null,
      frameWidth: 0,
      frameHeight: 0,
      frameSizes: new Map(),
      rememberFrames: false,
    });
    return { id, tabId: input.tabId, streamPath: browserViewStreamPath(id) };
  }

  closeMemberSession(id: string, memberId: string): Promise<boolean> {
    return this.#run(this.closeMemberSessionEffect(id, memberId));
  }
  readonly closeMemberSessionEffect = Effect.fn("BrowserViewGateway.closeMemberSession")(function* (
    this: BrowserViewGateway,
    id: string,
    memberId: string,
  ): Effect.fn.Return<boolean, RemoteWorkflowError, BrowserViewPort> {
    const session = this.#sessions.get(id);
    if (!session || session.memberId !== memberId) return false;
    yield* this.#closeSessionEffect(session, "The browser view was closed.");
    return true;
  });

  /** Views with a live socket. Created but never opened views do not hold anything. */
  activeViewCount(): number {
    let count = 0;
    for (const session of this.#sessions.values()) {
      if (session.socket) count += 1;
    }
    return count;
  }

  revokeTeamSession(teamSessionId: string): Promise<void> {
    return this.#run(this.revokeTeamSessionEffect(teamSessionId));
  }
  readonly revokeTeamSessionEffect = Effect.fn("BrowserViewGateway.revokeTeamSession")(function* (
    this: BrowserViewGateway,
    teamSessionId: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    for (const session of [...this.#sessions.values()]) {
      if (session.teamSessionId === teamSessionId) yield* this.#closeSessionEffect(session, "Team access ended.");
    }
  });

  stop(): Promise<void> {
    this.#stopping ??= (async () => {
      try {
        await this.#run(this.stopEffect());
        await Promise.allSettled([...this.#operations]);
      } finally {
        await this.#runtime?.dispose();
        this.#runtime = null;
        this.#stopping = null;
      }
    })();
    return this.#stopping;
  }
  readonly stopEffect = Effect.fn("BrowserViewGateway.stop")(function* (
    this: BrowserViewGateway,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    for (const session of [...this.#sessions.values()]) yield* this.#closeSessionEffect(session, "The host stopped.");
    this.#webSockets.close();
  });

  handlesUpgrade(url: URL): boolean {
    return browserViewStreamSessionId(url.pathname) !== null;
  }

  handleUpgrade(request: IncomingMessage, socket: Duplex, head: Buffer, url: URL): void {
    const sessionId = browserViewStreamSessionId(url.pathname);
    const session = sessionId ? this.#sessions.get(sessionId) : undefined;
    if (this.#stopping || !session || !this.#authorized(request, session)) {
      socket.write("HTTP/1.1 401 Unauthorized\r\nConnection: close\r\n\r\n");
      socket.destroy();
      return;
    }
    // A new socket is a new stream. Sizes from the previous one name frames this client never saw.
    session.frameWidth = 0;
    session.frameHeight = 0;
    session.frameSizes.clear();
    session.rememberFrames = browserViewClientAcksFrames(url);
    this.#webSockets.handleUpgrade(request, socket, head, (client) => void this.#connect(session, client));
  }

  /**
   * A tunneled socket carries the WebRTC session this host opened it for and no token: the member
   * never reaches this port themselves. A direct socket carries the member's token, and it has to be
   * the member the session was made for -- another member's token opens their own view, not this one.
   */
  #authorized(request: IncomingMessage, session: ManagedViewSession): boolean {
    const remoteSession = request.headers["x-openbot-webrtc-session"];
    if (remoteSession === session.teamSessionId) return true;
    const protocols = (request.headers["sec-websocket-protocol"] ?? "").split(",").map((value) => value.trim());
    const encodedToken = protocols.find((value) => value.startsWith("openbot-token."));
    const token = encodedToken?.slice("openbot-token.".length) ?? "";
    if (!token || token.length > 512) return false;
    return this.#options.authenticate(token)?.id === session.memberId;
  }

  #connect(session: ManagedViewSession, client: Ws.WebSocket): Promise<void> {
    return this.#run(this.#connectEffect(session, client));
  }
  readonly #connectEffect = Effect.fn("BrowserViewGateway.connect")(function* (
    this: BrowserViewGateway,
    session: ManagedViewSession,
    client: Ws.WebSocket,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    if (session.socket) session.socket.close(1000, "The browser view moved to a new connection.");
    session.socket = client;
    recordRestartActivity();
    client.on("message", (data, binary) => {
      if (binary || session.socket !== client) return;
      void this.#handleInput(session, data);
    });
    client.once("close", () => void this.#detach(session, client));
    client.once("error", () => void this.#detach(session, client));
    return yield* Effect.gen({ self: this }, function* () {
      const browser = yield* BrowserViewPort;
      const stopView = yield* browser.start(
        session.tabId,
        (frame) => {
          if (session.socket !== client || client.readyState !== webSockets.WebSocket.OPEN) return;
          // A dropped frame is one the client never sees, so it cannot be the frame a fraction is a
          // fraction of. Recording its shape here would expand the client's next point with a size
          // only this side knows about, and the click would land somewhere the user never pointed.
          if (client.bufferedAmount > MAX_BUFFERED_FRAME_BYTES) return;
          client.send(encodeBrowserViewFrame(frame), { binary: true });
          session.frameWidth = frame.width;
          session.frameHeight = frame.height;
          if (session.rememberFrames) {
            session.frameSizes.set(frame.sequence, { width: frame.width, height: frame.height });
            if (session.frameSizes.size > MAX_UNACKNOWLEDGED_FRAMES) {
              void this.#closeSession(session, "The live view fell too far behind.");
            }
          }
        },
        (reason) => {
          void this.#closeSession(session, reason);
        },
      );
      if (session.socket === client) session.stopView = stopView;
      else yield* remoteCall(() => stopView());
    }).pipe(
      Effect.catch(({ cause: error }) =>
        Effect.gen({ self: this }, function* () {
          client.close(1011, String(error instanceof Error ? error.message : error).slice(0, 120));
          yield* this.#detachEffect(session, client);
        }),
      ),
    );
  });

  #handleInput(session: ManagedViewSession, data: Ws.RawData): Promise<void> {
    if (!session.socket) return Promise.resolve();
    return this.#run(this.#handleInputEffect(session, data));
  }
  readonly #handleInputEffect = Effect.fn("BrowserViewGateway.handleInput")(function* (
    this: BrowserViewGateway,
    session: ManagedViewSession,
    data: Ws.RawData,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    if (rawDataSize(data) > MAX_INPUT_MESSAGE_BYTES) return;

    const attempt1 = yield* remoteDecode(() => decodeBrowserViewInput(rawDataText(data))).pipe(Effect.result);
    if (Result.isFailure(attempt1)) {
      session.socket?.close(1008, "Invalid browser view input.");
      return;
    }
    const input = attempt1.success;
    // The client has drawn this frame. Older ones are no longer on screen, including when the
    // member never moves the pointer. A frame this session did not send is not a frame to trust.
    if (input.type === "ack") {
      if (!session.frameSizes.has(input.sequence)) return;
      forgetFramesBefore(session.frameSizes, input.sequence);
      return;
    }
    // Input that arrives before the first frame has no frame to be a fraction of.
    let frame = { width: session.frameWidth, height: session.frameHeight };
    if (input.type === "pointer") {
      if (frame.width === 0 || frame.height === 0) return;
      // A client from before the sequence field names no frame and gets the newest one: that is
      // what every client got before a point could name its own. A named frame this session did
      // not send, or one the client has already moved past, is not that client. Expanding it with
      // a newer size clicks a page the user was not looking at, so the point is dropped.
      if (input.sequence !== undefined) {
        const named = session.frameSizes.get(input.sequence);
        if (!named) return;
        frame = named;
        forgetFramesBefore(session.frameSizes, input.sequence);
      }
    }
    // The sequence names a frame on this socket. The page is dispatched pixels, and knows nothing
    // about how they were carried here.
    const dispatched =
      input.type === "pointer"
        ? { ...input, sequence: undefined, x: input.x * frame.width, y: input.y * frame.height }
        : input;
    const browser = yield* BrowserViewPort;
    yield* browser.input(session.tabId, dispatched).pipe(Effect.catch(() => Effect.void));
  });

  #detach(session: ManagedViewSession, client: Ws.WebSocket): Promise<void> {
    if (session.socket !== client) return Promise.resolve();
    return this.#run(this.#detachEffect(session, client));
  }
  readonly #detachEffect = Effect.fn("BrowserViewGateway.detach")(function* (
    this: BrowserViewGateway,
    session: ManagedViewSession,
    client: Ws.WebSocket,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    if (session.socket !== client) return;
    this.#sessions.delete(session.id);
    session.socket = null;
    const stopView = session.stopView;
    session.stopView = null;
    if (stopView) yield* remoteCall(stopView).pipe(Effect.catch(() => Effect.void));
  });

  #closeSession(session: ManagedViewSession, reason: string): Promise<void> {
    if (!this.#sessions.has(session.id) && !session.socket) return Promise.resolve();
    return this.#run(this.#closeSessionEffect(session, reason));
  }
  readonly #closeSessionEffect = Effect.fn("BrowserViewGateway.closeSession")(function* (
    this: BrowserViewGateway,
    session: ManagedViewSession,
    reason: string,
  ): Effect.fn.Return<void, RemoteWorkflowError, BrowserViewPort> {
    this.#sessions.delete(session.id);
    const client = session.socket;
    session.socket = null;
    const stopView = session.stopView;
    session.stopView = null;
    client?.close(1000, reason.slice(0, 120));
    if (stopView) yield* remoteCall(stopView).pipe(Effect.catch(() => Effect.void));
  });
  #run<A>(operation: Effect.Effect<A, RemoteWorkflowError, BrowserViewPort>): Promise<A> {
    this.#runtime ??= ManagedRuntime.make(BrowserViewPort.layer(this.#options.browser));
    const pending = this.#runtime.runPromise(Effect.result(operation)).then((result) => {
      if (Result.isFailure(result)) throw result.failure.cause;
      return result.success;
    });
    this.#operations.add(pending);
    const clear = () => {
      this.#operations.delete(pending);
    };
    void pending.then(clear, clear);
    return pending;
  }
}
