import { randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import { createServer, type Server as HttpServer, type IncomingMessage, type ServerResponse } from "node:http";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  CallToolRequestSchema,
  type CallToolResult,
  CancelledNotificationSchema,
  type JSONRPCMessage,
  JSONRPCMessageSchema,
  ListToolsRequestSchema,
  type RequestId,
  type Tool,
} from "@modelcontextprotocol/sdk/types.js";
import { isString } from "@openbot/contracts/runtime-values";
import { Effect } from "effect";
import { type McpOperationError, mcpCall, mcpFailure, mcpResult, mcpSync, runMcpEffect } from "./mcp-effects";
import type { DynamicToolResult } from "./protocol";

interface DynamicToolDefinition {
  type: "function";
  name: string;
  description?: string;
  inputSchema: Tool["inputSchema"];
}

export interface DynamicToolNamespace {
  type: "namespace";
  name: string;
  description?: string;
  tools: DynamicToolDefinition[];
}

export interface LocalMcpSession {
  readonly servers: Array<{
    type: "http";
    name: string;
    url: string;
    headers: Array<{ name: string; value: string }>;
  }>;
  setThreadId(threadId: string): void;
  close(): void;
}

/**
 * How often a tool call that is still running tells the client it is alive. An MCP client times out
 * a call that sends nothing: opencode after 60 seconds, unless a progress notification resets the
 * deadline. `ask_user` waits for the user, which can take much longer.
 */
export const LOCAL_MCP_PROGRESS_INTERVAL_MS = 20_000;

interface BridgeRoute {
  namespace: DynamicToolNamespace;
  threadId: string;
  /**
   * `signal` aborts when the client abandons the call: it sent `notifications/cancelled` (an MCP
   * client does this when its own timeout ends) or closed the response stream. Nothing reads the
   * result after that, so the owner must stop waiting for the user.
   */
  call: (
    params: {
      threadId: string;
      turnId: string;
      callId: string;
      namespace: string;
      tool: string;
      arguments: unknown;
    },
    signal: AbortSignal,
  ) => Promise<DynamicToolResult>;
  activeTurnId: () => string | null;
  /**
   * The calls still running, by JSON-RPC id. Each POST gets a new MCP server, so the POST that
   * carries `notifications/cancelled` cannot reach the call through the SDK and finds it here.
   */
  running: Map<RequestId, AbortController>;
}

export class LocalMcpBridge {
  #server: HttpServer | null = null;
  #port: number | null = null;
  #listening: Promise<void> | null = null;
  /** Counts the closes, so a session asked for before a close does not register after it. */
  #closes = 0;
  #closing: Promise<void> | null = null;
  readonly #routes = new Map<string, BridgeRoute>();

  createSession(
    threadId: string,
    namespaces: DynamicToolNamespace[],
    activeTurnId: () => string | null,
    call: BridgeRoute["call"],
  ): Promise<LocalMcpSession> {
    return runMcpEffect(this.#createSessionOperation(threadId, namespaces, activeTurnId, call));
  }

  readonly #createSessionOperation = Effect.fn("LocalMcpBridge.createSession")(function* (
    this: LocalMcpBridge,
    threadId: string,
    namespaces: DynamicToolNamespace[],
    activeTurnId: () => string | null,
    call: BridgeRoute["call"],
  ): Effect.fn.Return<LocalMcpSession, McpOperationError> {
    const closes = this.#closes;
    if (this.#closing) return yield* mcpFailure(new Error("The local OpenBot MCP bridge closed."));
    yield* mcpCall(() => this.#listen());
    if (closes !== this.#closes) return yield* mcpFailure(new Error("The local OpenBot MCP bridge closed."));
    const tokens: string[] = [];
    const servers = namespaces.map((namespace) => {
      const token = randomBytes(32).toString("base64url");
      tokens.push(token);
      this.#routes.set(token, { namespace, threadId, activeTurnId, call, running: new Map() });
      return {
        type: "http" as const,
        name: namespace.name,
        url: `http://127.0.0.1:${this.#port}/mcp`,
        headers: [{ name: "Authorization", value: `Bearer ${token}` }],
      };
    });
    return {
      servers,
      setThreadId: (nextThreadId) => {
        for (const token of tokens) {
          const route = this.#routes.get(token);
          if (route) route.threadId = nextThreadId;
        }
      },
      close: () => {
        for (const token of tokens) this.#routes.delete(token);
      },
    };
  });

  close(): Promise<void> {
    return runMcpEffect(this.#closeOperation().pipe(Effect.uninterruptible));
  }

  readonly #closeOperation = Effect.fn("LocalMcpBridge.close")(function* (
    this: LocalMcpBridge,
  ): Effect.fn.Return<void, McpOperationError> {
    this.#closes += 1;
    this.#routes.clear();
    const closing = this.#shutDown();
    this.#closing = closing;
    yield* Effect.gen({ self: this }, function* () {
      yield* mcpCall(() => closing);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (this.#closing === closing) this.#closing = null;
        }),
      ),
    );
  });

  /** Until the server is closed, no session may start a second bind: that server would stay open. */
  #shutDown(): Promise<void> {
    return runMcpEffect(this.#shutDownEffect());
  }

  readonly #shutDownEffect = Effect.fn("LocalMcpBridge.shutDown")(function* (
    this: LocalMcpBridge,
  ): Effect.fn.Return<void, McpOperationError> {
    // A bind still in flight sets the server when it ends, so it is awaited before the close.
    yield* mcpCall(() => this.#listening?.catch(() => undefined));
    const server = this.#server;
    this.#server = null;
    this.#port = null;
    if (server) yield* mcpCall(() => new Promise<void>((resolve) => server.close(() => resolve())));
    this.#listening = null;
  });

  /** One bind, however many sessions ask for it at once: a second would leave a server listening. */
  #listen(): Promise<void> {
    if (!this.#listening) {
      const listening = this.#bind();
      this.#listening = listening;
      listening.catch(() => {
        if (this.#listening === listening) this.#listening = null;
      });
    }
    return this.#listening;
  }

  #bind(): Promise<void> {
    return runMcpEffect(this.#bindEffect());
  }

  readonly #bindEffect = Effect.fn("LocalMcpBridge.bind")(function* (
    this: LocalMcpBridge,
  ): Effect.fn.Return<void, McpOperationError> {
    let retained = false;
    yield* Effect.acquireUseRelease(
      mcpSync(() => createServer((request, response) => void this.#handle(request, response))),
      (server) =>
        Effect.gen({ self: this }, function* () {
          yield* mcpCall(
            () =>
              new Promise<void>((resolve, reject) => {
                server.once("error", reject);
                server.listen(0, "127.0.0.1", () => {
                  server.off("error", reject);
                  resolve();
                });
              }),
          ).pipe(Effect.uninterruptible);
          const address = server.address();
          if (!address || isString(address))
            return yield* mcpFailure(new Error("Unable to bind the local OpenBot MCP bridge."));
          this.#server = server;
          this.#port = address.port;
          retained = true;
        }),
      (server) =>
        retained
          ? Effect.void
          : mcpCall(() => new Promise<void>((resolve) => server.close(() => resolve()))).pipe(Effect.orDie),
    );
  });

  #handle(request: IncomingMessage, response: ServerResponse): Promise<void> {
    return runMcpEffect(this.#handleEffect(request, response));
  }

  readonly #handleEffect = Effect.fn("LocalMcpBridge.handle")(function* (
    this: LocalMcpBridge,
    request: IncomingMessage,
    response: ServerResponse,
  ): Effect.fn.Return<void, McpOperationError> {
    const route = this.#authorize(request);
    if (!route) {
      response.writeHead(401, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: "unauthorized" }));
      return;
    }
    if (request.method !== "POST") {
      response.writeHead(405, { allow: "POST" });
      response.end();
      return;
    }

    const mcp = new Server(
      { name: route.namespace.name, version: "0.1.0" },
      { capabilities: { tools: {} }, instructions: route.namespace.description },
    );
    mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
      tools: route.namespace.tools.map((tool) => ({
        name: tool.name,
        description: tool.description,
        inputSchema: tool.inputSchema,
      })),
    }));
    mcp.setRequestHandler(CallToolRequestSchema, ({ params }, extra) =>
      runMcpEffect(
        Effect.gen({ self: this }, function* (): Effect.fn.Return<CallToolResult, McpOperationError> {
          const tool = route.namespace.tools.find((candidate) => candidate.name === params.name);
          if (!tool) return yield* mcpFailure(new Error(`Unknown ${route.namespace.name} tool: ${params.name}`));
          // The SDK aborts `extra.signal` when this POST's transport closes, which follows a closed
          // response stream. A cancel on another POST aborts `abandoned` through `route.running`.
          const abandoned = new AbortController();
          const abandon = () => abandoned.abort();
          extra.signal.addEventListener("abort", abandon, { once: true });
          route.running.set(extra.requestId, abandoned);
          const progressToken = extra._meta?.progressToken;
          let progress = 0;
          const keepAlive =
            progressToken === undefined
              ? undefined
              : setInterval(() => {
                  progress += 1;
                  extra
                    .sendNotification({ method: "notifications/progress", params: { progressToken, progress } })
                    .catch(() => undefined);
                }, LOCAL_MCP_PROGRESS_INTERVAL_MS);
          const result = yield* Effect.gen({ self: this }, function* () {
            return yield* mcpCall(() =>
              route.call(
                {
                  threadId: route.threadId,
                  turnId: route.activeTurnId() ?? randomUUID(),
                  callId: randomUUID(),
                  namespace: route.namespace.name,
                  tool: tool.name,
                  arguments: params.arguments ?? {},
                },
                abandoned.signal,
              ),
            );
          }).pipe(
            Effect.onInterrupt(() => Effect.sync(abandon)),
            Effect.ensuring(
              Effect.sync(() => {
                clearInterval(keepAlive);
                extra.signal.removeEventListener("abort", abandon);
                if (route.running.get(extra.requestId) === abandoned) route.running.delete(extra.requestId);
              }),
            ),
          );
          const content: CallToolResult["content"] = [];
          for (const item of result.contentItems) {
            if (item.type === "inputText") {
              content.push({ type: "text", text: item.text });
              continue;
            }
            const [, mimeType, data] = item.imageUrl.match(/^data:([^;]+);base64,(.+)$/s) ?? [];
            if (mimeType !== undefined && data !== undefined) content.push({ type: "image", mimeType, data });
          }
          return {
            isError: !result.success,
            content,
          };
        }),
      ),
    );

    const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    yield* Effect.gen({ self: this }, function* () {
      try {
        mcpResult(yield* Effect.result(mcpCall(() => mcp.connect(transport))));
        const body = mcpResult(yield* Effect.result(readJsonBodyEffect(request)));
        for (const message of Array.isArray(body) ? body : body ? [body] : []) {
          const cancelled = CancelledNotificationSchema.safeParse(message);
          const requestId = cancelled.success ? cancelled.data.params.requestId : undefined;
          if (requestId !== undefined) route.running.get(requestId)?.abort();
        }
        mcpResult(yield* Effect.result(mcpCall(() => transport.handleRequest(request, response, body))));
      } catch {
        if (!response.headersSent) {
          response.writeHead(500, { "content-type": "application/json" });
          response.end(
            JSON.stringify({
              jsonrpc: "2.0",
              id: null,
              error: { code: -32603, message: "Internal MCP bridge error." },
            }),
          );
        }
      }
    }).pipe(
      Effect.ensuring(
        Effect.gen(function* () {
          yield* mcpCall(() => transport.close()).pipe(Effect.catch(() => Effect.void));
          yield* mcpCall(() => mcp.close()).pipe(Effect.catch(() => Effect.void));
        }),
      ),
    );
  });

  #authorize(request: IncomingMessage): BridgeRoute | null {
    const header = request.headers.authorization;
    if (!header?.startsWith("Bearer ")) return null;
    const candidate = header.slice(7);
    for (const [token, route] of this.#routes) {
      const left = Buffer.from(candidate);
      const right = Buffer.from(token);
      if (left.length === right.length && timingSafeEqual(left, right)) return route;
    }
    return null;
  }
}

/** The JSON-RPC messages of one MCP POST. It rejects a body larger than `maxBytes`. */
export function readJsonBody(
  request: IncomingMessage,
  maxBytes = 1_000_000,
): Promise<JSONRPCMessage | JSONRPCMessage[] | undefined> {
  return runMcpEffect(readJsonBodyEffect(request, maxBytes));
}
export const readJsonBodyEffect = Effect.fn("LocalMcpBridge.readJsonBody")(function* (
  request: IncomingMessage,
  maxBytes = 1_000_000,
): Effect.fn.Return<JSONRPCMessage | JSONRPCMessage[] | undefined, McpOperationError> {
  const chunks: Buffer[] = [];
  let size = 0;
  const iterator = request[Symbol.asyncIterator]();
  let finished = false;
  yield* Effect.gen(function* () {
    while (true) {
      const next = yield* mcpCall(() => iterator.next());
      if (next.done) {
        finished = true;
        break;
      }
      const buffer = yield* mcpSync(() => (Buffer.isBuffer(next.value) ? next.value : Buffer.from(next.value)));
      size += buffer.length;
      if (size > maxBytes) return yield* mcpFailure(new Error("MCP request body is too large."));
      chunks.push(buffer);
    }
  }).pipe(
    Effect.ensuring(
      Effect.gen(function* () {
        if (!finished) {
          request.destroy();
          yield* mcpCall(() => iterator.return?.()).pipe(Effect.catch(() => Effect.void));
        }
      }),
    ),
  );
  if (chunks.length === 0) return undefined;
  return yield* mcpSync(() => {
    const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
    const decode = (item: unknown): JSONRPCMessage => {
      const parsed = JSONRPCMessageSchema.safeParse(item);
      if (!parsed.success) throw new Error("Invalid MCP JSON-RPC message.");
      return parsed.data;
    };
    return Array.isArray(value) ? value.map(decode) : decode(value);
  });
});
