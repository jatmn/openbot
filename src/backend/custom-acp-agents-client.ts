// The one client of the provider `acp`: a router over one ACP process for each custom agent.
//
// The rest of the backend sees one provider with one catalogue. A model id names the custom agent
// (`<customAgentId>/<agentModel>`), and a session id that leaves here names it too
// (`<customAgentId>:<agentSessionId>`), so a turn, a resume or an answer finds the process that owns
// it. Two agents can give the same session id; the prefix keeps them apart.
//
// A process starts when its agent is first needed. Its model list is kept, so a list that fails
// later answers with the last one, and an agent that never listed stays out of the catalogue: an
// empty answer would move the agents on it to another model.

import { EventEmitter } from "node:events";
import { CUSTOM_AGENT_DEFAULT_MODEL, customAgentIdOfModel } from "@openbot/contracts/agent-providers";
import { isAgentModel } from "@openbot/contracts/ipc";
import type { DynamicRecord } from "@openbot/contracts/runtime-values";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Result, Schema } from "effect";
import { assertAgentArgs, assertWindowsScriptArgs, resolveAgentCommand } from "./acp-agent-command";
import type { AgentClient, DiagnosticOrigin } from "./agent-client";
import {
  type AppServerNotification,
  type AppServerRequest,
  decodeModelListResponse,
  decodeRecordResponse,
  getRecord,
  getString,
  isRecord,
  type ModelListResponse,
  type RequestId,
  type ResponseDecoder,
  type RpcError,
} from "./protocol";

export interface CustomAgentConfig {
  readonly id: string;
  readonly name: string;
  readonly command: string;
  readonly args: readonly string[];
  readonly env: readonly { readonly name: string; readonly value: string }[];
}

/** The saved custom agents, read when a process starts. */
export type CustomAgentSource = () => readonly CustomAgentConfig[];

/** Builds the ACP client of one agent, on the file its command resolved to. */
export type CustomAgentChildFactory = (config: CustomAgentConfig, executable: string) => AgentClient;

const MODEL_LIST_TIMEOUT_MS = 5_000;

interface ClientEvents {
  notification: [notification: AppServerNotification];
  request: [request: AppServerRequest];
  exit: [error: Error];
  diagnostic: [message: string, origin?: DiagnosticOrigin];
}

type ModelEntry = ModelListResponse["data"][number];

/** `<agentId>:<sessionId>`, or null for an id that no custom agent gave. */
function splitCustomAgentSessionId(value: string): { agentId: string; sessionId: string } | null {
  const colon = value.indexOf(":");
  if (colon <= 0 || colon === value.length - 1) return null;
  const agentId = value.slice(0, colon);
  return customAgentIdOfModel(`${agentId}/x`) === agentId ? { agentId, sessionId: value.slice(colon + 1) } : null;
}

/** Read as a missing session by `isMissingProviderSessionError`, so the caller opens a new one. */
function unknownSession(threadId: string): Error {
  return new Error(`Unknown ACP session: ${threadId}`);
}

export class CustomAcpAgentsClient extends EventEmitter<ClientEvents> implements AgentClient {
  readonly provider = "acp" as const;
  readonly #source: CustomAgentSource;
  readonly #createChild: CustomAgentChildFactory;
  readonly #resolve: typeof resolveAgentCommand;
  readonly #children = new Map<string, Promise<AgentClient>>();
  /**
   * Which process sent each request that waits for an answer, and its own id. Two processes can use
   * the same id, so the router gives each request an id of its own.
   */
  readonly #requests = new Map<RequestId, { child: AgentClient; id: RequestId }>();
  #nextRequestId = 0;
  readonly #lastModels = new Map<string, ModelEntry[]>();
  #running = false;

  constructor(source: CustomAgentSource, createChild: CustomAgentChildFactory, resolve = resolveAgentCommand) {
    super();
    this.#source = source;
    this.#createChild = createChild;
    this.#resolve = resolve;
  }

  get running(): boolean {
    return this.#running;
  }

  start(): void {
    this.#running = true;
  }

  async stop(): Promise<void> {
    await Effect.runPromise(this.stopEffect());
  }

  readonly stopEffect = Effect.fn("CustomAcp.stop")(function* (this: CustomAcpAgentsClient) {
    this.#running = false;
    const children = [...this.#children.values()];
    this.#children.clear();
    this.#requests.clear();
    yield* Effect.forEach(
      children,
      (pending) =>
        customIo(() => pending).pipe(
          Effect.flatMap((client) => customIo(() => client.stop())),
          Effect.catch(() => Effect.void),
        ),
      { concurrency: "unbounded", discard: true },
    );
  });

  async releaseThread(externalThreadId: string): Promise<void> {
    const result = await Effect.runPromise(Effect.result(this.releaseThreadEffect(externalThreadId)));
    if (Result.isFailure(result)) throw result.failure.cause;
  }

  readonly releaseThreadEffect = Effect.fn("CustomAcp.releaseThread")(function* (
    this: CustomAcpAgentsClient,
    externalThreadId: string,
  ) {
    const routed = splitCustomAgentSessionId(externalThreadId);
    if (!routed) return;
    const pending = this.#children.get(routed.agentId);
    if (!pending) return;
    const child = yield* customIo(() => pending).pipe(Effect.catch(() => Effect.succeed(null)));
    const release = child?.releaseThread?.bind(child);
    if (release) yield* customIo(() => release(routed.sessionId));
  });

  async request<T>(method: string, params: unknown, decoder: ResponseDecoder<T>, timeoutMs?: number): Promise<T> {
    const result = await Effect.runPromise(Effect.result(this.requestEffect(method, params, decoder, timeoutMs)));
    if (Result.isFailure(result)) throw this.#redactError(result.failure.cause);
    return result.success;
  }

  readonly requestEffect = Effect.fn("CustomAcp.request")(
    function* <T>(
      this: CustomAcpAgentsClient,
      method: string,
      params: unknown,
      decoder: ResponseDecoder<T>,
      timeoutMs?: number,
    ): Effect.fn.Return<T, CustomAcpOperationFailed> {
      if (!this.#running)
        return yield* new CustomAcpOperationFailed({ cause: new Error("ACP client is not running.") });
      switch (method) {
        case "initialize":
        case "thread/compact/start":
          return yield* customStep(() => decoder({}));
        case "account/read":
          return yield* customStep(() =>
            decoder(
              this.#source().length > 0
                ? { account: { type: "acp", email: null, planType: null }, requiresOpenaiAuth: false }
                : { account: null, requiresOpenaiAuth: false },
            ),
          );
        case "account/rateLimits/read":
          return yield* customStep(() => decoder({ rateLimits: null, rateLimitsByLimitId: null }));
        case "plugin/list":
          return yield* customStep(() => decoder({ marketplaces: [] }));
        case "model/list": {
          const data = yield* this.#listModelsEffect(params, timeoutMs ?? MODEL_LIST_TIMEOUT_MS);
          return yield* customStep(() => decoder({ data }));
        }
        case "thread/start": {
          const agentId = yield* customStep(() => this.#modelAgent(params));
          const child = yield* customIo(() => this.#child(agentId));
          const response = yield* customIo(() =>
            child.request(method, forChild(params, null), decodeRecordResponse, timeoutMs),
          );
          return yield* customStep(() => decoder(withThreadId(response, agentId)));
        }
        case "thread/resume": {
          const routed = yield* customStep(() => this.#routed(params));
          const child = yield* customIo(() => this.#child(routed.agentId));
          const response = yield* customIo(() =>
            child.request(method, forChild(params, routed.sessionId), decodeRecordResponse, timeoutMs),
          );
          return yield* customStep(() => decoder(withThreadId(response, routed.agentId)));
        }
        case "thread/read": {
          const threadId = getString(params, "threadId") ?? "";
          const routed = splitCustomAgentSessionId(threadId);
          if (!routed || !this.#source().some((config) => config.id === routed.agentId)) {
            return yield* customStep(() => decoder({ thread: { id: threadId, turns: [] } }));
          }
          const child = yield* customIo(() => this.#child(routed.agentId));
          const response = yield* customIo(() =>
            child.request(method, forChild(params, routed.sessionId), decodeRecordResponse, timeoutMs),
          );
          return yield* customStep(() => decoder(withThreadId(response, routed.agentId)));
        }
        case "turn/start":
        case "turn/steer": {
          const routed = yield* customStep(() => this.#routed(params));
          const child = yield* customIo(() => this.#child(routed.agentId));
          return yield* customIo(() => child.request(method, forChild(params, routed.sessionId), decoder, timeoutMs));
        }
        case "turn/interrupt": {
          const routed = splitCustomAgentSessionId(getString(params, "threadId") ?? "");
          const pending = routed ? this.#children.get(routed.agentId) : undefined;
          const child = pending ? yield* customIo(() => pending).pipe(Effect.catch(() => Effect.succeed(null))) : null;
          if (!routed || !child) return yield* customStep(() => decoder({}));
          return yield* customIo(() => child.request(method, forChild(params, routed.sessionId), decoder, timeoutMs));
        }
        default:
          return yield* new CustomAcpOperationFailed({ cause: new Error(`ACP adapter does not implement ${method}.`) });
      }
    },
    Effect.mapError((error) => new CustomAcpOperationFailed({ cause: this.#redactError(error.cause) })),
  );

  notify(): void {
    // The processes are initialized when they start. There is nothing to send to all of them.
  }

  respond(id: RequestId, result: unknown): void {
    const pending = this.#requests.get(id);
    this.#requests.delete(id);
    pending?.child.respond(pending.id, result);
  }

  respondError(id: RequestId, error: RpcError): void {
    const pending = this.#requests.get(id);
    this.#requests.delete(id);
    pending?.child.respondError(pending.id, error);
  }

  /** The custom agent that the model in `params` names. */
  #modelAgent(params: unknown): string {
    const agentId = customAgentIdOfModel(getString(params, "model") ?? "");
    if (!agentId) throw new Error(sourceText("error.provider.customAgentMissing"));
    return agentId;
  }

  /**
   * The agent and its own session id. A session of another agent than the model names is missing
   * for this turn: the user switched agents, and the caller opens a session on the new one, with the
   * conversation handed over.
   */
  #routed(params: unknown): { agentId: string; sessionId: string } {
    const threadId = getString(params, "threadId") ?? "";
    const routed = splitCustomAgentSessionId(threadId);
    if (!routed) throw unknownSession(threadId);
    const model = getString(params, "model");
    if (model && customAgentIdOfModel(model) !== routed.agentId) throw unknownSession(threadId);
    return routed;
  }

  /** One started, initialized process for each agent, however many callers ask for it at once. */
  #child(agentId: string): Promise<AgentClient> {
    const existing = this.#children.get(agentId);
    if (existing) return existing;
    const starting = this.#startChild(agentId);
    this.#children.set(agentId, starting);
    starting.catch(() => {
      if (this.#children.get(agentId) === starting) this.#children.delete(agentId);
    });
    return starting;
  }

  async #startChild(agentId: string): Promise<AgentClient> {
    const result = await Effect.runPromise(Effect.result(this.#startChildEffect(agentId)));
    if (Result.isFailure(result)) throw result.failure.cause;
    return result.success;
  }

  readonly #startChildEffect = Effect.fn("CustomAcp.startChild")(
    function* (this: CustomAcpAgentsClient, agentId: string) {
      const config = this.#source().find((candidate) => candidate.id === agentId);
      if (!config)
        return yield* new CustomAcpOperationFailed({
          cause: new Error(sourceText("error.provider.customAgentMissing")),
        });
      yield* customStep(() => assertAgentArgs(config.args));
      const executable = yield* customIo(() => this.#resolve(config.command));
      if (!executable)
        return yield* new CustomAcpOperationFailed({
          cause: new Error(sourceText("error.provider.customAgentNotFound", { command: config.command })),
        });
      yield* customStep(() => assertWindowsScriptArgs(executable, config.args));
      const child = yield* customStep(() => this.#createChild(config, executable));
      child.on("notification", (notification) => {
        this.emit("notification", withRoutedThreadId(notification, agentId));
      });
      child.on("request", (request) => {
        this.#nextRequestId += 1;
        const id = `${agentId}:${this.#nextRequestId}`;
        this.#requests.set(id, { child, id: request.id });
        this.emit("request", withRoutedThreadId({ ...request, id }, agentId));
      });
      child.on("diagnostic", (message, origin) => this.emit("diagnostic", this.#redact(message), origin));
      child.once("exit", (error) => this.#childExited(agentId, child, this.#redactError(error)));
      yield* customStep(() => child.start());
      yield* customIo(() => child.request("initialize", {}, decodeRecordResponse)).pipe(
        Effect.onError(() => customIo(() => child.stop()).pipe(Effect.catch(() => Effect.void))),
      );
      return child;
    },
    Effect.mapError((error) => new CustomAcpOperationFailed({ cause: this.#redactError(error.cause) })),
  );

  /**
   * One agent's process ended on its own. The runtime replaces the whole router, which is how the
   * other providers recover as well: the threads of every custom agent are loaded again.
   */
  #childExited(agentId: string, child: AgentClient, error: Error): void {
    void this.#children
      .get(agentId)
      ?.then((current) => {
        if (current !== child || !this.#running) return;
        this.#running = false;
        this.emit("exit", error);
      })
      .catch(() => undefined);
  }

  /**
   * The saved environment values of every custom agent, masked. An agent can quote one in an RPC
   * error, and the router's errors and diagnostics go to the log and to the renderer.
   */
  #redact(text: string): string {
    let result = text;
    for (const config of this.#source()) {
      for (const { value } of config.env) {
        if (value.length >= 4) result = result.split(value).join("[redacted]");
      }
    }
    return result;
  }

  /** The same error when it holds no value, so its type and fields stay for the callers that read them. */
  #redactError<E>(error: E): E | Error {
    if (!(error instanceof Error)) return error;
    const message = this.#redact(error.message);
    return message === error.message ? error : new Error(message);
  }

  readonly #listModelsEffect = Effect.fn("CustomAcp.listModels")(function* (
    this: CustomAcpAgentsClient,
    params: unknown,
    timeoutMs: number,
  ) {
    const configs = this.#source();
    const lists = yield* Effect.forEach(
      configs,
      (config) =>
        customIo(() => this.#child(config.id)).pipe(
          Effect.flatMap((child) =>
            customIo(() => child.request("model/list", params, decodeModelListResponse, timeoutMs)),
          ),
          Effect.timeoutOrElse({
            duration: timeoutMs,
            orElse: () =>
              Effect.fail(
                new CustomAcpOperationFailed({
                  cause: new Error(`${config.name} request timed out: model/list`),
                }),
              ),
          }),
          Effect.flatMap((response) =>
            customStep(() => {
              const models = routedModels(config, response.data);
              this.#lastModels.set(config.id, models);
              return models;
            }),
          ),
          Effect.catch((error) =>
            Effect.sync(() => {
              this.emit(
                "diagnostic",
                this.#redact(`Custom agent ${config.id} did not list its models: ${String(error.cause)}`),
              );
              return this.#lastModels.get(config.id) ?? [];
            }),
          ),
        ),
      { concurrency: "unbounded" },
    );
    return lists.flat();
  });
}

/**
 * The agent's models under its id. An id that the contract refuses is dropped alone: the model list
 * decoders fail closed on the whole array. An agent with no list gets the one model `default`.
 */
function routedModels(config: CustomAgentConfig, data: readonly ModelEntry[]): ModelEntry[] {
  if (data.length === 0) {
    return [{ model: `${config.id}/${CUSTOM_AGENT_DEFAULT_MODEL}`, displayName: config.name }];
  }
  return data.flatMap((item) => {
    const model = item.model?.trim();
    if (!model) return [];
    const id = `${config.id}/${model}`;
    // `<agent name>/<model name>`, as a custom endpoint's models are named: the picker groups by it.
    return isAgentModel(id) ? [{ ...item, model: id, displayName: `${config.name}/${item.displayName ?? model}` }] : [];
  });
}

/** The params as the agent's own process takes them: its session id, and its model with no prefix. */
function forChild(params: unknown, sessionId: string | null): DynamicRecord {
  const record: DynamicRecord = isRecord(params) ? params : {};
  const { model, ...rest } = record;
  const agentModel = typeof model === "string" ? model.slice(model.indexOf("/") + 1) : "";
  return {
    ...rest,
    ...(sessionId === null ? {} : { threadId: sessionId }),
    ...(agentModel && agentModel !== CUSTOM_AGENT_DEFAULT_MODEL ? { model: agentModel } : {}),
  };
}

function withRoutedThreadId<T extends { params: unknown }>(message: T, agentId: string): T {
  const { params } = message;
  if (!isRecord(params)) return message;
  const threadId = getString(params, "threadId");
  return threadId === null ? message : { ...message, params: { ...params, threadId: `${agentId}:${threadId}` } };
}

function withThreadId(response: DynamicRecord, agentId: string): DynamicRecord {
  const thread = getRecord(response, "thread");
  const id = getString(thread, "id");
  if (!thread || id === null) return response;
  return { ...response, thread: { ...thread, id: `${agentId}:${id}` } };
}

export class CustomAcpOperationFailed extends Schema.TaggedError<CustomAcpOperationFailed>()(
  "CustomAcpOperationFailed",
  {
    cause: Schema.Defect(),
  },
) {}

function customIo<A>(run: () => Promise<A>): Effect.Effect<A, CustomAcpOperationFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new CustomAcpOperationFailed({ cause }) });
}

function customStep<A>(run: () => A): Effect.Effect<A, CustomAcpOperationFailed> {
  return Effect.try({ try: run, catch: (cause) => new CustomAcpOperationFailed({ cause }) });
}
