import { randomBytes, randomUUID } from "node:crypto";
import { mkdir, rm } from "node:fs/promises";
import { basename, join } from "node:path";
import { ATTACHMENT_LIMITS, INPUT_LIMITS } from "@openbot/contracts/input-limits";
import type {
  AddSlackOrchestratorInput,
  AddSlackOrchestratorResult,
  AgentApproval,
  AgentEvent,
  AgentSummary,
  MessagingConnection,
  MessagingConnectionState,
  MessagingCredentialState,
  MessagingPlatform,
  RespondToApprovalInput,
  SlackOverview,
} from "@openbot/contracts/ipc";
import { MESSAGING_CONNECTION_STATES } from "@openbot/contracts/ipc";
import { isDynamicRecord, isOneOf, isString } from "@openbot/contracts/runtime-values";
import { SLACK_ORCHESTRATOR_AVATAR } from "@openbot/contracts/slack-app";
import { sourceText } from "@openbot/i18n/source";
import { createOpenBotLogger, redactText } from "@openbot/logging";
import { Effect, Result, Schema } from "effect";
import type { MessagingOrigin } from "../mailbox-store";
import type { SidebarLayoutStore } from "../sidebar-layout-store";
import type { MessagingConnectionRecord, MessagingLink } from "./messaging-store";
import type { MessagingActivity, MessagingThreads } from "./messaging-threads";
import {
  type ConnectionIdentity,
  type InboundAction,
  type InboundMessage,
  type IngressAnswer,
  type IngressDelivery,
  type MessageTarget,
  type MessagingAdapter,
  MessagingConnectionError,
  type MessagingDriver,
  type MessagingIngress,
  type MessagingTransport,
} from "./messaging-types";
import { type SlackAppPort, SlackConnect } from "./slack/slack-connect";
import { slackOrchestratorMemories, slackOrchestratorProfile } from "./slack/slack-orchestrator";
import { SlackWebApi } from "./slack/slack-web-api";

const logger = createOpenBotLogger("messaging");

/** The tokens of each connection, kept by the main process in encrypted storage, by connection id. */
export interface MessagingCredentials {
  keys(): string[];
  status(connectionId: string): MessagingCredentialState;
  get(connectionId: string): Record<string, string> | null;
  set(connectionId: string, values: Record<string, string>): Promise<void>;
  clear(connectionId: string): Promise<void>;
  /** Removes the tokens of every key that is not listed. */
  retain(connectionIds: ReadonlySet<string>): Promise<void>;
}

export interface MessagingAgents {
  listAgents(): AgentSummary[];
  respondToApproval(input: RespondToApprovalInput): Promise<void>;
  onEvent(listener: (event: AgentEvent) => void): () => void;
  /** Creates an agent with no first message, on the named model or a new agent's default. */
  createAgentProfile(
    input: Pick<AddSlackOrchestratorInput, "provider" | "model" | "reasoningEffort"> & {
      name: string;
      title: string;
      description: string;
      avatarSeed: string;
      avatarHue: AgentSummary["avatarHue"];
    },
  ): Promise<AgentSummary>;
  createMemory(input: { agentId: string; text: string }): unknown;
}

export interface MessagingServiceOptions {
  threads: MessagingThreads;
  agents: MessagingAgents;
  credentials: MessagingCredentials;
  drivers: readonly MessagingDriver[];
  /** A private folder for files that arrive, until the mailbox has copied them. */
  downloadsRoot: string;
  /** The Signal relay of the OpenBot Slack app. Without it and `slackApp`, no workspace can connect. */
  ingress?: MessagingIngress;
  slackApp?: SlackAppPort;
  /** Only tests change this. */
  slackOrigin?: string;
  /** Where the Slack Orchestrator goes in the sidebar: an Integrations section. */
  sidebar?: Pick<SidebarLayoutStore, "getSnapshot" | "mutate">;
}

interface LiveConnection {
  record: MessagingConnectionRecord;
  adapter: MessagingAdapter;
  transport: MessagingTransport | null;
  identity: ConnectionIdentity | null;
  state: MessagingConnectionState;
  retryAt: string | null;
}

/** The status post of one external message, which the answer replaces. */
interface StatusPost {
  connectionId: string;
  target: MessageTarget;
  messageId: string | null;
  stopToken: string | null;
}

interface PendingApproval {
  requestId: string | number;
  connectionId: string;
  target: MessageTarget;
  /** Null until Slack answers the post. A button press can arrive first, and it names its message. */
  messageId: string | null;
  /** The Slack user whose message started the turn. Null when it is not known, then only the host answers. */
  allowedUserId: string | null;
  text: string;
  answered: boolean;
}

const FATAL_STATES = new Set<MessagingConnectionState>(["invalid_token"]);
const LIVE_STATES = new Set<MessagingConnectionState>(["connecting", "connected", "reconnecting", "rate_limited"]);
const APPROVAL_TEXT_LIMIT = 2_500;
const CANCEL_TEXT = /^(cancel|stop)$/i;
const RECENT_MESSAGES = 2_000;
/** How long a connection waits before it tries Slack again after Slack was unreachable. */
const IDENTIFY_RETRY_MS = 30_000;
/** The marker `mention` uses. Text that did not come from the host has it taken out. */
const MENTION_MARKERS = /[\uE000-\uE001]/g;

/**
 * Owns the live connections to chat platform workspaces: it starts and stops each one's transport,
 * picks the agent of each new conversation with the router agent, turns inbound messages into agent
 * work through `MessagingThreads`, posts status and answers back as OpenBot, and relays approvals
 * and stop requests. It never stores a token: `MessagingCredentials` does. It is platform-agnostic;
 * everything a platform does goes through its `MessagingDriver`.
 */
export class MessagingService {
  readonly #threads: MessagingThreads;
  readonly #agents: MessagingAgents;
  readonly #credentials: MessagingCredentials;
  readonly #drivers: ReadonlyMap<MessagingPlatform, MessagingDriver>;
  readonly #downloadsRoot: string;
  readonly #live = new Map<string, LiveConnection>();
  /** Status posts by `linkId:platformMessageId`, until the turn that answers the message ends. */
  readonly #posts = new Map<string, StatusPost>();
  /** The reply target of each external message, by `linkId:platformMessageId`. */
  readonly #targets = new Map<string, MessageTarget>();
  readonly #approvals = new Map<string, PendingApproval>();
  readonly #stops = new Map<string, { linkId: string; authorId: string; connectionId: string }>();
  readonly #chains = new Map<string, Promise<void>>();
  readonly #recent = new Set<string>();
  readonly #unsubscribe: Array<() => void> = [];
  readonly #ingress: MessagingIngress | null;
  readonly #connect: SlackConnect | null;
  readonly #slackOrigin: string | undefined;
  readonly #sidebar: Pick<SidebarLayoutStore, "getSnapshot" | "mutate"> | null;
  #started = false;

  constructor(options: MessagingServiceOptions) {
    this.#threads = options.threads;
    this.#agents = options.agents;
    this.#credentials = options.credentials;
    this.#drivers = new Map(options.drivers.map((driver) => [driver.platform, driver]));
    this.#downloadsRoot = options.downloadsRoot;
    this.#ingress = options.ingress ?? null;
    this.#connect = options.ingress && options.slackApp ? new SlackConnect(options.slackApp) : null;
    this.#slackOrigin = options.slackOrigin;
    this.#sidebar = options.sidebar ?? null;
  }

  start(): Promise<void> {
    return runMessaging(this.startEffect());
  }

  readonly startEffect = Effect.fn("MessagingService.start")(function* (
    this: MessagingService,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    if (this.#started) return;
    this.#started = true;
    this.#unsubscribe.push(
      this.#threads.onActivity((activity) => this.#serial(activity.link.linkId, () => this.#activity(activity))),
      this.#agents.onEvent((event) => this.#agentEvent(event)),
    );
    this.#threads.setContextSource((link, origin) => this.#promptContext(link, origin));
    this.#ingress?.handle((workspaceId, delivery) => this.deliverSlack(workspaceId, delivery));
    const records = this.#threads.store.connections();
    yield* messagingIo(() => this.#credentials.retain(new Set(records.map((record) => record.connectionId))));
    yield* Effect.forEach(
      records.filter((record) => record.enabled),
      (record) => this.#startConnectionEffect(record),
      { concurrency: "unbounded", discard: true },
    );
  });

  stop(): Promise<void> {
    return runMessaging(this.stopEffect());
  }

  readonly stopEffect = Effect.fn("MessagingService.stop")(function* (
    this: MessagingService,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    this.#started = false;
    for (const unsubscribe of this.#unsubscribe.splice(0)) unsubscribe();
    this.#threads.setContextSource(null);
    this.#ingress?.handle(null);
    yield* Effect.forEach(
      [...this.#live.values()],
      (live) => {
        const transport = live.transport;
        return transport ? messagingIo(() => transport.stop()) : Effect.void;
      },
      { concurrency: "unbounded", discard: true },
    );
    this.#live.clear();
  });

  /** After the computer wakes, every socket may be dead without knowing it. */
  resume(): void {
    this.#ingress?.reconnect();
    for (const live of this.#live.values()) live.transport?.reconnect();
  }

  hasLiveConnection(): boolean {
    return [...this.#live.values()].some((live) => LIVE_STATES.has(live.state));
  }

  /** The Slack workspaces of this computer, for Server settings > Connectors. A disconnected one is left out. */
  slackOverview(): SlackOverview {
    return {
      connections: this.#threads.store
        .connections()
        .filter((record) => record.platform === "slack" && this.#credentials.status(record.connectionId) !== "missing")
        .map((record) => this.#summary(record)),
    };
  }

  reconnect(workspaceId: string): Promise<void> {
    return runMessaging(this.reconnectEffect(workspaceId));
  }

  readonly reconnectEffect = Effect.fn("MessagingService.reconnect")(function* (
    this: MessagingService,
    workspaceId: string,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    const record = this.#requireConnection(workspaceId);
    yield* this.#stopConnectionEffect(record.connectionId);
    this.#threads.store.updateConnection(record.connectionId, { enabled: true, lastErrorCode: null });
    const updated = this.#threads.store.connection(record.connectionId);
    if (updated) yield* this.#startConnectionEffect(updated);
    this.#ingress?.reconnect();
  }, Effect.uninterruptible);

  setEnabled(workspaceId: string, enabled: boolean): Promise<void> {
    return runMessaging(this.setEnabledEffect(workspaceId, enabled));
  }

  readonly setEnabledEffect = Effect.fn("MessagingService.setEnabled")(function* (
    this: MessagingService,
    workspaceId: string,
    enabled: boolean,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    const record = this.#requireConnection(workspaceId);
    yield* this.#stopConnectionEffect(record.connectionId);
    this.#threads.store.updateConnection(record.connectionId, { enabled, lastErrorCode: null });
    const updated = this.#threads.store.connection(record.connectionId);
    if (enabled && updated) yield* this.#startConnectionEffect(updated);
  }, Effect.uninterruptible);

  /**
   * Adds the workspace's Slack Orchestrator: a new agent with its standing remit and the facts it
   * starts with, on the model the user picked. It receives every new conversation of the workspace.
   * A workspace that already has one keeps it.
   */
  addOrchestrator(input: AddSlackOrchestratorInput): Promise<AddSlackOrchestratorResult> {
    return runMessaging(this.addOrchestratorEffect(input));
  }

  readonly addOrchestratorEffect = Effect.fn("MessagingService.addOrchestrator")(function* (
    this: MessagingService,
    input: AddSlackOrchestratorInput,
  ) {
    const record = yield* messagingStep(() => this.#requireConnection(input.workspaceId));
    const current = this.#agents.listAgents().find((agent) => agent.id === record.orchestratorAgentId);
    if (current) return { agentId: current.id, sectionId: null };
    const agent = yield* messagingIo(() =>
      this.#agents.createAgentProfile({
        ...slackOrchestratorProfile(),
        ...SLACK_ORCHESTRATOR_AVATAR,
        ...(input.provider ? { provider: input.provider } : {}),
        ...(input.model ? { model: input.model } : {}),
        ...(input.reasoningEffort ? { reasoningEffort: input.reasoningEffort } : {}),
      }),
    );
    for (const text of slackOrchestratorMemories(record.workspaceName))
      this.#agents.createMemory({ agentId: agent.id, text });
    this.#threads.store.updateConnection(record.connectionId, { orchestratorAgentId: agent.id });
    // A sidebar it cannot reach leaves the agent where new agents go; the orchestrator still answers.
    const sectionId = yield* this.#placeInIntegrationsEffect(agent.id).pipe(
      Effect.catch((failure) => {
        this.#warn(failure.cause);
        return Effect.succeed(null);
      }),
    );
    return { agentId: agent.id, sectionId };
  }, Effect.uninterruptible);

  /**
   * Puts the orchestrator in the sidebar's Integrations section, which it creates the first time, so
   * the agents that serve an integration stay apart from the user's own. Returns the section id.
   */
  readonly #placeInIntegrationsEffect = Effect.fn("MessagingService.placeInIntegrations")(function* (
    this: MessagingService,
    agentId: string,
  ) {
    const sidebar = this.#sidebar;
    if (!sidebar) return null;
    const name = sourceText("status.messaging.integrationsSection");
    const agentIds = new Set(this.#agents.listAgents().map((agent) => agent.id));
    const existing = sidebar.getSnapshot().sections.find((section) => section.name === name);
    const layout = yield* messagingIo(() =>
      sidebar.mutate(
        existing ? { type: "assign", agentId, sectionId: existing.id } : { type: "create", name, agentId },
        agentIds,
      ),
    );
    return layout.agentAssignments[agentId] ?? null;
  });

  /** Opens the OpenBot Slack app's install in the browser. A deep link to `completeSlackWorkspace` ends it. */
  connectSlackWorkspace(): Promise<void> {
    return runMessaging(
      Effect.gen({ self: this }, function* () {
        const connect = yield* messagingStep(() => this.#requireConnect());
        yield* connect
          .startEffect()
          .pipe(Effect.mapError((failure) => new MessagingOperationFailed({ cause: failure.cause })));
      }),
    );
  }

  /**
   * Saves the bot token of a workspace that installed the OpenBot app, and starts its connection. A
   * workspace connected before keeps its conversations. False for a link that this run did not start.
   */
  completeSlackWorkspace(nonce: string, grant: string): Promise<boolean> {
    return runMessaging(this.completeSlackWorkspaceEffect(nonce, grant));
  }

  readonly completeSlackWorkspaceEffect = Effect.fn("MessagingService.completeSlackWorkspace")(function* (
    this: MessagingService,
    nonce: string,
    grant: string,
  ): Effect.fn.Return<boolean, MessagingOperationFailed> {
    const connect = yield* messagingStep(() => this.#requireConnect());
    const opened = yield* connect
      .completeEffect(nonce, grant)
      .pipe(Effect.mapError((failure) => new MessagingOperationFailed({ cause: failure.cause })));
    if (!opened) return false;
    const record = this.#threads.store.ensureConnection("slack", opened.workspaceId, opened.workspaceName);
    yield* this.#stopConnectionEffect(record.connectionId);
    yield* messagingIo(() =>
      this.#credentials.set(record.connectionId, {
        botToken: opened.botToken,
        botUserId: opened.botUserId,
        appId: opened.appId,
        workspaceId: opened.workspaceId,
      }),
    );
    this.#threads.store.updateConnection(record.connectionId, {
      enabled: true,
      workspaceName: opened.workspaceName,
      botUserId: opened.botUserId,
      appId: opened.appId,
      lastErrorCode: null,
    });
    const updated = this.#threads.store.connection(record.connectionId);
    if (updated) yield* this.#startConnectionEffect(updated);
    // Signal learns the workspaces of this host when the socket connects.
    this.#ingress?.reconnect();
    return true;
  }, Effect.uninterruptible);

  /**
   * Stops the workspace's connection, asks Slack to revoke the bot token, removes it, and unlinks the
   * workspace from this host. The conversations stay, for a later connect.
   */
  disconnectSlackWorkspace(workspaceId: string): Promise<void> {
    return runMessaging(this.disconnectSlackWorkspaceEffect(workspaceId));
  }

  readonly disconnectSlackWorkspaceEffect = Effect.fn("MessagingService.disconnectSlackWorkspace")(function* (
    this: MessagingService,
    workspaceId: string,
  ) {
    const record = yield* messagingStep(() => this.#requireConnection(workspaceId));
    yield* this.#stopConnectionEffect(record.connectionId);
    const botToken = this.#credentials.get(record.connectionId)?.botToken;
    if (botToken)
      yield* new SlackWebApi({ token: botToken, origin: this.#slackOrigin })
        .callEffect("auth.revoke")
        .pipe(Effect.catch((failure) => Effect.sync(() => this.#warn(failure.cause))));
    yield* messagingIo(() => this.#credentials.clear(record.connectionId));
    yield* messagingStep(() =>
      this.#threads.store.updateConnection(record.connectionId, { enabled: false, lastErrorCode: null }),
    );
    if (this.#connect)
      yield* this.#connect
        .unlinkEffect(workspaceId)
        .pipe(Effect.catch((failure) => Effect.sync(() => this.#warn(failure.cause))));
    this.#ingress?.reconnect();
  }, Effect.uninterruptible);

  /**
   * One request that Slack sent for a workspace, which Signal has checked. A paused connection answers
   * 200, so Slack does not send it again, and does nothing. An unknown workspace answers 404.
   */
  async deliverSlack(workspaceId: string, delivery: IngressDelivery): Promise<IngressAnswer> {
    const record = this.#threads.store.connectionForWorkspace("slack", workspaceId);
    if (!record) return { status: 404 };
    if (!record.enabled) return { status: 200 };
    const transport = this.#live.get(record.connectionId)?.transport;
    if (!transport?.deliver) return { status: 503 };
    return transport.deliver(delivery);
  }

  // Connection lifecycle.

  readonly #startConnectionEffect = Effect.fn("MessagingService.startConnection")(function* (
    this: MessagingService,
    record: MessagingConnectionRecord,
  ) {
    const credentials = this.#credentials.get(record.connectionId);
    const driver = this.#drivers.get(record.platform);
    if (!credentials?.botToken || !driver) return;
    const live: LiveConnection = {
      record,
      adapter: driver.createAdapter(credentials, {
        rateLimited: (retryAt) => {
          live.retryAt = retryAt;
          this.#setState(live, "rate_limited");
        },
      }),
      transport: null,
      identity: null,
      state: "connecting",
      retryAt: null,
    };
    this.#live.set(record.connectionId, live);
    const identified = yield* Effect.result(messagingIo(() => live.adapter.identify()));
    if (Result.isFailure(identified)) {
      const error = identified.failure.cause;
      if (this.#live.get(record.connectionId) !== live) return;
      if (error instanceof MessagingConnectionError) return this.#setState(live, error.state);
      // Slack is unreachable now. Try again later; Reconnect and a wake from sleep do it at once.
      const retry = setTimeout(() => {
        if (this.#live.get(record.connectionId) === live) void this.#restart(record.connectionId);
      }, IDENTIFY_RETRY_MS);
      retry.unref?.();
      live.transport = {
        start: () => undefined,
        reconnect: () => void this.#restart(record.connectionId),
        stop: async () => clearTimeout(retry),
      };
      return this.#setState(live, "reconnecting");
    }
    live.identity = identified.success;
    if (this.#live.get(record.connectionId) !== live) return;
    const identity = live.identity;
    this.#threads.store.updateConnection(record.connectionId, {
      workspaceName: identity.workspaceName,
      botUserId: identity.botUserId,
      appId: identity.appId,
    });
    live.record = this.#threads.store.connection(record.connectionId) ?? record;
    const transport = driver.createTransport(credentials, identity);
    live.transport = transport;
    transport.start({
      state: (state) => {
        if (state === "connected") live.retryAt = null;
        this.#setState(live, state);
      },
      message: (message) => void this.#receive(live, message).catch((error) => this.#warn(error)),
      action: (action) => void this.#action(live, action).catch((error) => this.#warn(error)),
      placeCreated: (platformChannelId) =>
        void live.adapter.joinPlace?.(platformChannelId).catch((error) => this.#warn(error)),
    });
    // So people can mention OpenBot in any public channel without inviting it first. Channels made
    // while the host was off are joined here too.
    void live.adapter.joinPublicPlaces?.().catch((error) => this.#warn(error));
  });

  #restart(connectionId: string): Promise<void> {
    return runMessaging(this.#restartEffect(connectionId));
  }

  readonly #restartEffect = Effect.fn("MessagingService.restart")(function* (
    this: MessagingService,
    connectionId: string,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    yield* this.#stopConnectionEffect(connectionId);
    const record = this.#threads.store.connection(connectionId);
    if (record?.enabled) yield* this.#startConnectionEffect(record);
  });

  readonly #stopConnectionEffect = Effect.fn("MessagingService.stopConnection")(function* (
    this: MessagingService,
    connectionId: string,
  ): Effect.fn.Return<void, MessagingOperationFailed> {
    const live = this.#live.get(connectionId);
    this.#live.delete(connectionId);
    const transport = live?.transport;
    if (transport) yield* messagingIo(() => transport.stop());
  });

  #setState(live: LiveConnection, state: MessagingConnectionState): void {
    live.state = state;
    if (FATAL_STATES.has(state)) {
      this.#threads.store.updateConnection(live.record.connectionId, { lastErrorCode: state });
      logger.warn("A messaging connection stopped.", { platform: live.record.platform, state });
      // The workspace uninstalled OpenBot or revoked its token: Signal stops routing it here.
      void this.#connect?.unlink(live.record.workspaceId).catch((error) => this.#warn(error));
    }
  }

  #summary(record: MessagingConnectionRecord): MessagingConnection {
    const live = this.#live.get(record.connectionId);
    const credentials = this.#credentials.status(record.connectionId);
    const values = credentials === "saved" ? this.#credentials.get(record.connectionId) : null;
    const stored = isOneOf(MESSAGING_CONNECTION_STATES, record.lastErrorCode) ? record.lastErrorCode : null;
    const state: MessagingConnectionState = !record.enabled
      ? "paused"
      : credentials === "unreadable" || (values && !values.botToken)
        ? "secret_storage_unavailable"
        : (live?.state ?? stored ?? "connecting");
    return {
      workspaceId: record.workspaceId,
      platform: record.platform,
      enabled: record.enabled,
      state,
      workspaceName: record.workspaceName,
      botUserId: record.botUserId,
      missingScopes: live?.identity?.missingScopes ?? [],
      retryAt: state === "rate_limited" ? (live?.retryAt ?? null) : null,
      credentials,
      orchestratorAgentId: record.orchestratorAgentId,
    };
  }

  // Inbound.

  #receive(live: LiveConnection, message: InboundMessage): Promise<void> {
    return runMessaging(this.#receiveEffect(live, message));
  }

  readonly #receiveEffect = Effect.fn("MessagingService.receive")(function* (
    this: MessagingService,
    live: LiveConnection,
    message: InboundMessage,
  ) {
    if (!live.identity) return;
    // Before any await: a redelivered event can arrive while the first copy is still downloading its
    // files, before the mailbox holds its idempotency key. The mailbox key covers a restart.
    const store = this.#threads.store;
    const existing = store.linkByKey(live.record.connectionId, message.platformChannelId, message.threadKey);
    // Before the dedup key: a reply with a mention also arrives as `app_mention`, which must still run.
    if (message.requiresLink && !existing) return;
    const dedupKey = `${live.record.connectionId}:${message.dedupKey}`;
    if (this.#recent.has(dedupKey)) return;
    this.#recent.add(dedupKey);
    if (this.#recent.size > RECENT_MESSAGES) {
      const oldest = this.#recent.values().next().value;
      if (oldest !== undefined) this.#recent.delete(oldest);
    }
    if (existing && CANCEL_TEXT.test(message.text) && message.files.length === 0) {
      if (
        yield* this.#threads
          .stopEffect(existing.linkId, message.authorId)
          .pipe(Effect.mapError((failure) => new MessagingOperationFailed({ cause: failure.cause })))
      ) {
        yield* messagingIo(() => live.adapter.react(message.target, message.platformMessageId, "stopped", true)).pipe(
          Effect.catch(() => Effect.void),
        );
        return;
      }
    }
    const { adapter } = live;
    const authorName = yield* messagingIo(() => adapter.authorName(message.authorId));
    const place = message.isDirect ? null : yield* messagingIo(() => adapter.placeName(message.platformChannelId));
    // A conversation keeps its agent. A new one goes to the workspace's orchestrator, which asks its
    // teammates; without one, nothing answers.
    const agentId = existing?.agentId ?? this.#orchestrator(live.record);
    if (!agentId) {
      yield* messagingIo(() => adapter.post(message.target, { text: sourceText("status.messaging.noAgent") }));
      return;
    }
    const staging = join(this.#downloadsRoot, randomUUID());
    yield* Effect.acquireUseRelease(
      Effect.succeed(staging),
      () =>
        Effect.gen({ self: this }, function* () {
          const { paths, skipped } = yield* this.#downloadEffect(adapter, message, staging);
          const text = skipped.length
            ? `${message.text}\n\n(Files that did not arrive: ${skipped.join(", ")})`
            : message.text;
          const result = yield* messagingIo(() =>
            this.#threads.receive({
              connectionId: live.record.connectionId,
              agentId,
              platformChannelId: message.platformChannelId,
              threadKey: message.threadKey,
              isDirect: message.isDirect,
              title: message.isDirect ? authorName : `${place} · ${message.text.slice(0, 80) || authorName}`,
              text,
              sourcePaths: paths,
              origin: { authorId: message.authorId, authorName, platformMessageId: message.platformMessageId },
              idempotencyKey: `messaging:${live.record.connectionId}:${message.dedupKey}`,
            }),
          );
          if (result.status === "duplicate") return;
          const key = `${result.link.linkId}:${message.platformMessageId}`;
          if (result.status === "busy") {
            yield* messagingIo(() => adapter.post(message.target, { text: sourceText("status.messaging.busy") }));
            yield* messagingIo(() => adapter.react(message.target, message.platformMessageId, "failed", true)).pipe(
              Effect.catch(() => Effect.void),
            );
            return;
          }
          this.#targets.set(key, message.target);
          yield* messagingIo(() => adapter.react(message.target, message.platformMessageId, "received", true)).pipe(
            Effect.catch(() => Effect.void),
          );
          // In the same order as the turn's own posts: a turn that already started has its status post.
          if (result.waiting)
            this.#serial(result.link.linkId, async () => {
              if (this.#posts.has(key) || !this.#targets.has(key)) return;
              const messageId = await adapter.post(message.target, { text: sourceText("status.messaging.queued") });
              this.#posts.set(key, {
                connectionId: live.record.connectionId,
                target: message.target,
                messageId,
                stopToken: null,
              });
            });
        }),
      () => messagingIo(() => rm(staging, { recursive: true, force: true })).pipe(Effect.orDie),
    );
  }, Effect.uninterruptible);

  #orchestrator(record: MessagingConnectionRecord): string | null {
    const id = this.#threads.store.connection(record.connectionId)?.orchestratorAgentId ?? null;
    return id && this.#agents.listAgents().some((agent) => agent.id === id) ? id : null;
  }

  /** Downloads the files of a message into `staging`, within the attachment limits. */
  readonly #downloadEffect = Effect.fn("MessagingService.download")(function* (
    this: MessagingService,
    adapter: MessagingAdapter,
    message: InboundMessage,
    staging: string,
  ) {
    const paths: string[] = [];
    const skipped: string[] = [];
    let total = 0;
    for (const [index, file] of message.files.entries()) {
      if (
        paths.length >= INPUT_LIMITS.attachments ||
        file.size > ATTACHMENT_LIMITS.fileBytes ||
        total + file.size > ATTACHMENT_LIMITS.totalBytes
      ) {
        skipped.push(file.name);
        continue;
      }
      const downloaded = yield* Effect.result(
        Effect.gen(function* () {
          yield* messagingIo(() => mkdir(join(staging, String(index)), { recursive: true, mode: 0o700 }));
          // Only the final path segment can come from the external file name.
          const destination = join(staging, String(index), basename(file.name) || "file");
          yield* messagingIo(() =>
            adapter.download(
              file,
              destination,
              Math.min(ATTACHMENT_LIMITS.fileBytes, ATTACHMENT_LIMITS.totalBytes - total),
            ),
          );
          return destination;
        }),
      );
      if (Result.isFailure(downloaded)) skipped.push(file.name);
      else {
        total += file.size;
        paths.push(downloaded.success);
      }
    }
    return { paths, skipped };
  });

  #promptContext(link: MessagingLink, origin: MessagingOrigin) {
    return runMessaging(this.#promptContextEffect(link, origin));
  }

  readonly #promptContextEffect = Effect.fn("MessagingService.promptContext")(function* (
    this: MessagingService,
    link: MessagingLink,
    origin: MessagingOrigin,
  ) {
    const live = this.#live.get(link.connectionId);
    const workspaceName = live?.record.workspaceName ?? null;
    if (!live) return { workspaceName, place: link.title, messages: [], cursor: null, skippedFiles: [] };
    const place = link.isDirect
      ? "direct message"
      : yield* messagingIo(() => live.adapter.placeName(link.platformChannelId));
    const messages = yield* messagingIo(() =>
      live.adapter.history(link.platformChannelId, link.threadKey, link.historyCursor, origin.platformMessageId),
    ).pipe(Effect.catch(() => Effect.succeed([])));
    return { workspaceName, place, messages, cursor: origin.platformMessageId, skippedFiles: [] };
  });

  // Outbound.

  #activity(activity: MessagingActivity): Promise<void> {
    return runMessaging(this.#activityEffect(activity));
  }

  readonly #activityEffect = Effect.fn("MessagingService.activity")(function* (
    this: MessagingService,
    activity: MessagingActivity,
  ) {
    const live = this.#live.get(activity.link.connectionId);
    if (!live || !activity.origin) return;
    const { adapter } = live;
    const origin = activity.origin;
    const key = `${activity.link.linkId}:${origin.platformMessageId}`;
    const target = this.#targets.get(key) ?? linkTarget(activity.link);
    const post = this.#posts.get(key);
    if (activity.type === "started") {
      const stopToken = token();
      this.#stops.set(stopToken, {
        linkId: activity.link.linkId,
        authorId: origin.authorId,
        connectionId: live.record.connectionId,
      });
      const body = {
        text: sourceText("status.messaging.working"),
        buttons: [{ action: "stop" as const, label: sourceText("status.messaging.stop"), token: stopToken }],
      };
      const existingMessageId = post?.messageId;
      const messageId = existingMessageId
        ? yield* messagingIo(() => adapter.edit(target, existingMessageId, body)).pipe(Effect.as(existingMessageId))
        : yield* messagingIo(() => adapter.post(target, body));
      this.#posts.set(key, { connectionId: live.record.connectionId, target, messageId, stopToken });
      return;
    }
    this.#posts.delete(key);
    this.#targets.delete(key);
    if (post?.stopToken) this.#stops.delete(post.stopToken);
    const placeholder = post?.messageId ?? null;
    const reaction =
      activity.type === "cancelled"
        ? "stopped"
        : activity.status === "completed"
          ? "done"
          : activity.status === "interrupted"
            ? "stopped"
            : "failed";
    // A follow-up turn answers a teammate's reply: the person's message already shows how its own turn ended.
    if (activity.type === "cancelled" || !activity.followUp) {
      yield* messagingIo(() => adapter.react(target, origin.platformMessageId, "received", false)).pipe(
        Effect.catch(() => Effect.void),
      );
      yield* messagingIo(() => adapter.react(target, origin.platformMessageId, reaction, true)).pipe(
        Effect.catch(() => Effect.void),
      );
    }
    if (activity.type === "cancelled" || activity.status === "interrupted") {
      yield* this.#sayEffect(adapter, target, placeholder, sourceText("status.messaging.stopped"));
      return;
    }
    if (activity.status === "failed") {
      // The provider's error can hold local paths or MCP values, so Slack gets a fixed sentence.
      yield* this.#sayEffect(adapter, target, placeholder, sourceText("status.messaging.failed"));
      return;
    }
    // Slack is outside this computer, so a secret in the answer must not reach it.
    if (activity.answer) {
      const answer = redactText(activity.answer);
      yield* messagingIo(() => adapter.postAnswer(target, answer, placeholder));
    }
    // A turn that only asked a teammate has nothing to say yet: the answer comes back to this thread.
    else if (!activity.followUp && this.#threads.awaitsTeammate(activity.link.linkId))
      yield* this.#sayEffect(adapter, target, placeholder, sourceText("status.messaging.delegated"));
    else yield* this.#sayEffect(adapter, target, placeholder, sourceText("status.messaging.noAnswer"));
    if (activity.files.length) {
      const skipped = yield* messagingIo(() => adapter.upload(target, activity.files));
      if (skipped.length)
        yield* messagingIo(() =>
          adapter.post(target, {
            text: sourceText("status.messaging.filesSkipped", { names: skipped.join(", ") }),
          }),
        );
    }
  });

  readonly #sayEffect = Effect.fn("MessagingService.say")(function* (
    this: MessagingService,
    adapter: MessagingAdapter,
    target: MessageTarget,
    placeholder: string | null,
    text: string,
  ) {
    if (placeholder) yield* messagingIo(() => adapter.edit(target, placeholder, { text }));
    else yield* messagingIo(() => adapter.post(target, { text }));
  });

  #agentEvent(event: AgentEvent): void {
    if (event.type === "approval") void this.#approval(event.approval).catch((error) => this.#warn(error));
    else if (event.type === "agent-input-resolved" && event.kind === "approval")
      void this.#approvalResolved(event.requestId).catch((error) => this.#warn(error));
  }

  #approval(approval: AgentApproval): Promise<void> {
    return runMessaging(this.#approvalEffect(approval));
  }

  readonly #approvalEffect = Effect.fn("MessagingService.approval")(function* (
    this: MessagingService,
    approval: AgentApproval,
  ) {
    const link = this.#threads.store.linkForThread(approval.threadId);
    const live = link ? this.#live.get(link.connectionId) : undefined;
    if (!link || !live) return;
    const running = this.#threads.runningOrigin(link.linkId);
    const key = running ? `${link.linkId}:${running.origin.platformMessageId}` : null;
    const target = (key && this.#targets.get(key)) || (key && this.#posts.get(key)?.target) || linkTarget(link);
    const kind =
      approval.kind === "command"
        ? sourceText("status.messaging.approvalCommand")
        : approval.kind === "file-change"
          ? sourceText("status.messaging.approvalFileChange")
          : sourceText("status.messaging.approvalPermissions");
    const detail = redactText([approval.command, approval.cwd, approval.reason].filter(Boolean).join("\n"))
      .replace(MENTION_MARKERS, "")
      .slice(0, APPROVAL_TEXT_LIMIT);
    const text = [
      sourceText("status.messaging.approvalTitle"),
      `**${kind}**`,
      detail ? `\`\`\`\n${detail}\n\`\`\`` : "",
    ]
      .filter(Boolean)
      .join("\n");
    const accept = token();
    const decline = token();
    // Known before the post: a person can press a button before Slack's answer to the post arrives.
    const pending: PendingApproval = {
      requestId: approval.requestId,
      connectionId: live.record.connectionId,
      target,
      messageId: null,
      allowedUserId: running?.origin.authorId ?? null,
      text,
      answered: false,
    };
    this.#approvals.set(accept, pending);
    this.#approvals.set(decline, pending);
    pending.messageId = yield* messagingIo(() =>
      live.adapter.post(target, {
        text,
        buttons: [
          { action: "accept", label: sourceText("status.messaging.approve"), token: accept, style: "primary" },
          { action: "decline", label: sourceText("status.messaging.deny"), token: decline, style: "danger" },
        ],
      }),
    ).pipe(
      Effect.onError(() =>
        Effect.sync(() => {
          this.#approvals.delete(accept);
          this.#approvals.delete(decline);
        }),
      ),
    );
  });

  #approvalResolved(requestId: string | number): Promise<void> {
    return runMessaging(this.#approvalResolvedEffect(requestId));
  }

  readonly #approvalResolvedEffect = Effect.fn("MessagingService.approvalResolved")(function* (
    this: MessagingService,
    requestId: string | number,
  ) {
    const entries = [...this.#approvals.entries()].filter(([, pending]) => pending.requestId === requestId);
    const pending = entries[0]?.[1];
    for (const [key] of entries) this.#approvals.delete(key);
    if (!pending || pending.answered) return;
    const live = this.#live.get(pending.connectionId);
    const messageId = pending.messageId;
    if (!messageId || !live) return;
    yield* messagingIo(() =>
      live.adapter.edit(pending.target, messageId, {
        text: `${pending.text}\n${sourceText("status.messaging.answeredOnHost")}`,
      }),
    );
  });

  #action(live: LiveConnection, action: InboundAction): Promise<void> {
    return runMessaging(this.#actionEffect(live, action));
  }

  readonly #actionEffect = Effect.fn("MessagingService.action")(function* (
    this: MessagingService,
    live: LiveConnection,
    action: InboundAction,
  ) {
    const { adapter } = live;
    if (action.type === "stop") {
      const stop = this.#stops.get(action.token);
      if (!stop || stop.connectionId !== live.record.connectionId) return;
      if (action.actorId !== stop.authorId) {
        yield* messagingIo(() =>
          adapter.postPrivate(
            action.target,
            action.actorId,
            sourceText("status.messaging.onlyRequester", { user: adapter.mention(stop.authorId) }),
          ),
        );
        return;
      }
      yield* this.#threads
        .stopEffect(stop.linkId, stop.authorId)
        .pipe(Effect.mapError((failure) => new MessagingOperationFailed({ cause: failure.cause })));
      return;
    }
    const pending = this.#approvals.get(action.token);
    if (!pending || pending.connectionId !== live.record.connectionId) {
      yield* messagingIo(() =>
        adapter.edit(action.target, action.platformMessageId, { text: sourceText("status.messaging.requestInactive") }),
      ).pipe(Effect.catch(() => Effect.void));
      return;
    }
    if (!pending.allowedUserId || action.actorId !== pending.allowedUserId) {
      yield* messagingIo(() =>
        adapter.postPrivate(
          action.target,
          action.actorId,
          pending.allowedUserId
            ? sourceText("status.messaging.onlyRequester", { user: adapter.mention(pending.allowedUserId) })
            : sourceText("status.messaging.hostOnly"),
        ),
      );
      return;
    }
    pending.answered = true;
    const answered = yield* Effect.result(
      messagingIo(() => this.#agents.respondToApproval({ requestId: pending.requestId, decision: action.decision })),
    );
    const outcome = Result.isFailure(answered)
      ? sourceText("status.messaging.requestInactive")
      : sourceText(action.decision === "accept" ? "status.messaging.approvedBy" : "status.messaging.deniedBy", {
          user: adapter.mention(action.actorId),
        });
    for (const [key, candidate] of this.#approvals) if (candidate === pending) this.#approvals.delete(key);
    yield* messagingIo(() =>
      adapter.edit(pending.target, pending.messageId ?? action.platformMessageId, {
        text: `${pending.text}\n${outcome}`,
      }),
    );
  }, Effect.uninterruptible);

  // Helpers.

  /**
   * Runs the posts of one conversation in order. A turn can start and end while the post that
   * announced it is still on its way, and the answer must replace that post, not race it.
   */
  #serial(linkId: string, task: () => Promise<void>): void {
    const next = (this.#chains.get(linkId) ?? Promise.resolve())
      .then(task)
      .catch((error) => this.#warn(error))
      .finally(() => {
        if (this.#chains.get(linkId) === next) this.#chains.delete(linkId);
      });
    this.#chains.set(linkId, next);
  }

  #requireConnection(workspaceId: string): MessagingConnectionRecord {
    const record = this.#threads.store.connectionForWorkspace("slack", workspaceId);
    if (!record) throw new Error(sourceText("error.messaging.notConnected"));
    return record;
  }

  #requireConnect(): SlackConnect {
    if (!this.#connect) throw new Error(sourceText("error.messaging.unsupported"));
    return this.#connect;
  }

  #warn(error: unknown): void {
    // Only the kind of failure and the platform's error code: a Slack error message can quote
    // message text, and its code cannot.
    const code = isDynamicRecord(error) && isString(error.code) ? error.code : undefined;
    logger.warn("A messaging action failed.", {
      error: error instanceof Error ? error.name : "unknown",
      ...(code ? { code } : {}),
    });
  }
}

/** Where to answer when the message that started the work is not known: the conversation's thread. */
function linkTarget(link: MessagingLink): MessageTarget {
  return { platformChannelId: link.platformChannelId, replyThreadId: link.threadKey };
}

function token(): string {
  return randomBytes(16).toString("hex");
}

export class MessagingOperationFailed extends Schema.TaggedError<MessagingOperationFailed>()(
  "MessagingOperationFailed",
  {
    cause: Schema.Defect(),
  },
) {}

function messagingIo<A>(run: () => Promise<A>): Effect.Effect<A, MessagingOperationFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new MessagingOperationFailed({ cause }) });
}

function messagingStep<A>(run: () => A): Effect.Effect<A, MessagingOperationFailed> {
  return Effect.try({ try: run, catch: (cause) => new MessagingOperationFailed({ cause }) });
}

async function runMessaging<A>(operation: Effect.Effect<A, MessagingOperationFailed>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(operation));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
