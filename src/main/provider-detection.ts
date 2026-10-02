// The local model server scan and the model list of one endpoint. Both run in main: see
// `model-server-probe.ts`.
//
// A scan sends no key to any address. A model list for a saved endpoint uses the stored key and
// headers only when the user leaves them blank and the address keeps the saved origin, so an edited
// address never receives the old credentials.

import { INPUT_LIMITS } from "@openbot/contracts/input-limits";
import type {
  DetectedAcpAgent,
  DetectedModelServer,
  DiscoverModelsInput,
  DiscoverModelsResult,
  ProviderDetectionSettings,
} from "@openbot/contracts/ipc";
import {
  customProviderEndpointKey,
  DEFAULT_MODEL_SERVERS,
  isNewCustomProviderId,
  sameCustomProviderOrigin,
} from "@openbot/contracts/ipc";
import { Context, Effect, Layer, ManagedRuntime, Result } from "effect";
import { scanAcpAgentsEffect } from "../backend/acp-agent-scan";
import type { CustomProviderConfig } from "../backend/opencode-config";
import { ModelServerProbe } from "./model-server-probe";

/** A server on this computer answers at once, so a slow address is not held for long. */
const SCAN_TIMEOUT_MS = 1_500;
/** The user asked for this list, and a remote server can be slow. */
const DISCOVER_TIMEOUT_MS = 8_000;

interface KnownServer {
  id: string;
  name: string;
  baseUrl: string;
}

export interface ProviderDetectionDependencies {
  settings: { get(): ProviderDetectionSettings };
  customProviders: { configs(): readonly CustomProviderConfig[] };
  customAgents: { configs(): readonly { id: string }[] };
  probe?: Context.Service.Shape<typeof ModelServerProbe>["probe"];
  scanAgents?: typeof scanAcpAgentsEffect;
}

export interface ProviderDetection {
  scanModelServers(): Promise<DetectedModelServer[]>;
  /** The known ACP agent commands on this computer. None is started. */
  scanAgents(): Promise<DetectedAcpAgent[]>;
  discoverModels(input: DiscoverModelsInput): Promise<DiscoverModelsResult>;
}

/**
 * A free endpoint id for a server: its known id, or `server-<host>-<port>`. A taken id gets `-2`,
 * `-3` and so on.
 */
function suggestServerId(base: string, taken: ReadonlySet<string>): string {
  const limit = INPUT_LIMITS.identifier - 4;
  const stem =
    base
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, limit)
      .replace(/-+$/, "") || "server";
  if (!taken.has(stem) && isNewCustomProviderId(stem)) return stem;
  for (let suffix = 2; ; suffix += 1) {
    const candidate = `${stem}-${suffix}`;
    if (!taken.has(candidate) && isNewCustomProviderId(candidate)) return candidate;
  }
}

/** The servers to probe: the defaults, then the user's addresses, one row for each endpoint key. */
function scanTargets(addresses: readonly string[]): KnownServer[] {
  const targets: KnownServer[] = [];
  const keys = new Set<string>();
  const add = (server: KnownServer) => {
    const key = customProviderEndpointKey(server.baseUrl);
    if (!key || keys.has(key)) return;
    keys.add(key);
    targets.push(server);
  };
  for (const [id, server] of Object.entries(DEFAULT_MODEL_SERVERS)) add({ id, ...server });
  for (const address of addresses) {
    const baseUrl = address.trim();
    if (!baseUrl) continue;
    let url: URL;
    try {
      url = new URL(baseUrl);
    } catch {
      continue;
    }
    // The settings parser refuses these already. A file edited by hand is checked again here.
    if ((url.protocol !== "http:" && url.protocol !== "https:") || url.username || url.password) continue;
    add({ id: `server-${url.hostname}-${url.port || url.protocol.slice(0, -1)}`, name: url.host, baseUrl });
  }
  return targets;
}

export class ProviderDiscovery extends Context.Service<
  ProviderDiscovery,
  {
    scanModelServers(): Effect.Effect<DetectedModelServer[]>;
    scanAgents(): Effect.Effect<DetectedAcpAgent[]>;
    discoverModels(input: DiscoverModelsInput): ReturnType<typeof discoverModelsEffect>;
  }
>()("openbot/main/ProviderDiscovery") {
  static layer(dependencies: ProviderDetectionDependencies) {
    return Layer.effect(
      ProviderDiscovery,
      Effect.gen(function* () {
        const modelProbe = yield* ModelServerProbe;
        const injectedProbe = dependencies.probe;
        const injectedScanAgents = dependencies.scanAgents;
        const probe = injectedProbe ?? modelProbe.probe;
        const scan = Effect.fn("ProviderDiscovery.scanModelServers")(function* () {
          const current = dependencies.settings.get();
          if (!current.enabled) return [];
          const targets = scanTargets(current.addresses);
          const results = yield* Effect.forEach(
            targets,
            (target) =>
              probe({ baseUrl: target.baseUrl, apiKey: null, headers: [] }, SCAN_TIMEOUT_MS).pipe(
                Effect.map((models) => ({ target, models })),
                Effect.catch(() => Effect.succeed(null)),
              ),
            { concurrency: "unbounded" },
          );
          const taken = new Set(dependencies.customProviders.configs().map((config) => config.id));
          const found: DetectedModelServer[] = [];
          for (const result of results) {
            if (!result) continue;
            const id = suggestServerId(result.target.id, taken);
            taken.add(id);
            found.push({ id, name: result.target.name, baseUrl: result.target.baseUrl, models: result.models });
          }
          return found;
        });
        const scanAgents = Effect.fn("ProviderDiscovery.scanAgents")(function* () {
          const current = dependencies.settings.get();
          if (!current.enabled) return [];
          const input = {
            folders: current.folders,
            takenIds: new Set(dependencies.customAgents.configs().map((config) => config.id)),
          };
          return yield* (injectedScanAgents ?? scanAcpAgentsEffect)(input);
        });
        return ProviderDiscovery.of({
          scanModelServers: scan,
          scanAgents,
          discoverModels: (input) => discoverModelsEffect(input, dependencies.customProviders, probe),
        });
      }),
    ).pipe(Layer.provide(ModelServerProbe.layer));
  }
}

const discoverModelsEffect = Effect.fn("ProviderDiscovery.discoverModels")(function* (
  input: DiscoverModelsInput,
  customProviders: ProviderDetectionDependencies["customProviders"],
  probe: Context.Service.Shape<typeof ModelServerProbe>["probe"],
) {
  let apiKey = input.apiKey;
  let headers = input.headers;
  const saved = input.savedProviderId
    ? customProviders.configs().find((config) => config.id === input.savedProviderId)
    : undefined;
  // Blank fields retain credentials only when the destination keeps the stored origin.
  if (saved && sameCustomProviderOrigin(saved.baseUrl, input.baseUrl)) {
    if (!apiKey) apiKey = saved.apiKey;
    if (headers.length === 0) headers = [...saved.headers];
  }
  return { models: yield* probe({ baseUrl: input.baseUrl, apiKey, headers }, DISCOVER_TIMEOUT_MS) };
});

/** Owns one runtime and shares overlapping scans across desktop windows. */
export function createProviderDetection(
  dependencies: ProviderDetectionDependencies,
): ProviderDetection & { close(): Promise<void> } {
  const runtime = ManagedRuntime.make(ProviderDiscovery.layer(dependencies));
  let inFlight: Promise<DetectedModelServer[]> | null = null;
  let agentsInFlight: Promise<DetectedAcpAgent[]> | null = null;
  return {
    scanModelServers() {
      if (!inFlight) {
        inFlight = runtime.runPromise(ProviderDiscovery.use((service) => service.scanModelServers())).finally(() => {
          inFlight = null;
        });
      }
      return inFlight;
    },
    scanAgents() {
      if (!agentsInFlight) {
        agentsInFlight = runtime.runPromise(ProviderDiscovery.use((service) => service.scanAgents())).finally(() => {
          agentsInFlight = null;
        });
      }
      return agentsInFlight;
    },
    async discoverModels(input) {
      const result = await runtime.runPromise(
        Effect.result(ProviderDiscovery.use((service) => service.discoverModels(input))),
      );
      if (Result.isFailure(result)) throw result.failure;
      return result.success;
    },
    close: () => runtime.dispose(),
  };
}
