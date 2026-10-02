import {
  type AgentSummary,
  COMPUTER_USE_MCP_SERVER_NAME,
  type InstalledSkill,
  type McpServerConfig,
  RESERVED_MCP_SERVER_NAMES,
  type Routine,
} from "@openbot/contracts/ipc";
import { isDynamicRecord, isString } from "@openbot/contracts/runtime-values";
import { Effect } from "effect";
import { isMissingFileError } from "../backend/file-errors";
import type { AnalyticsInventory, AnalyticsInventoryDayStore } from "./analytics";
import { analyticsIO, runAnalytics } from "./analytics-effects";
import { readPreferenceFile, runPreference, writePreferenceFile } from "./preference-file";

const DAY_PATTERN = /^\d{4}-\d{2}-\d{2}$/u;

/** Stores the local day of the last inventory event. A malformed file counts as sent today. */
export function analyticsInventoryDayStore(path: string): AnalyticsInventoryDayStore {
  return {
    read: () =>
      runPreference(
        readPreferenceFile(path, (parsed) => {
          if (
            !isDynamicRecord(parsed) ||
            parsed.version !== 1 ||
            !isString(parsed.day) ||
            !DAY_PATTERN.test(parsed.day)
          )
            return "malformed";
          return parsed.day;
        }).pipe(
          Effect.catch((failure) => {
            if (isMissingFileError(failure.cause)) return Effect.succeed("missing");
            if (failure.cause instanceof SyntaxError) return Effect.succeed("malformed");
            return Effect.fail(failure);
          }),
        ),
      ),
    write: (day) => runPreference(writePreferenceFile(path, { version: 1, day })),
  };
}

const CURATED_SKILL_PREFIX = "openbot-curated-skill-";
const CURATED_AGENT_PREFIX = "openbot-curated-agent-";
const BUILTIN_SERVER_NAMES = new Set<string>([...RESERVED_MCP_SERVER_NAMES, COMPUTER_USE_MCP_SERVER_NAME]);

export interface AnalyticsInventorySources {
  agents: () => AgentSummary[];
  routines: (agentId: string) => Routine[];
  skills: (agentId: string) => Promise<InstalledSkill[]>;
  mcpServers: () => McpServerConfig[];
  pluginSlug: (config: McpServerConfig) => string | null;
  computerUseEnabled: () => boolean;
}

/**
 * Counts what this computer has set up. Only OpenBot's own catalog names leave this function:
 * a custom server, a community listing or a local skill is a count, never a name.
 */
export function collectAnalyticsInventory(sources: AnalyticsInventorySources): Promise<AnalyticsInventory> {
  return runAnalytics(collectAnalyticsInventoryEffect(sources));
}
export const collectAnalyticsInventoryEffect = Effect.fn("Analytics.collectInventory")(function* (
  sources: AnalyticsInventorySources,
) {
  const agents = sources.agents();
  const plugins = new Set<string>();
  let customMcpServerCount = 0;
  for (const server of sources.mcpServers()) {
    if (!server.enabled || BUILTIN_SERVER_NAMES.has(server.name)) continue;
    const slug = sources.pluginSlug(server);
    if (slug) plugins.add(slug);
    else customMcpServerCount += 1;
  }
  const curatedSkills = new Set<string>();
  let localSkillCount = 0;
  let communitySkillCount = 0;
  let enabledRoutineCount = 0;
  for (const agent of agents) {
    enabledRoutineCount += sources.routines(agent.id).filter((routine) => routine.active).length;
    const skills = yield* analyticsIO(() => sources.skills(agent.id)).pipe(Effect.catch(() => Effect.succeed([])));
    for (const skill of skills) {
      if (skill.enabled === false) continue;
      if (skill.skillId.startsWith(CURATED_SKILL_PREFIX))
        curatedSkills.add(skill.skillId.slice(CURATED_SKILL_PREFIX.length));
      else if (skill.origin === "local" || skill.origin === "workspace") localSkillCount += 1;
      else if (skill.origin === undefined || skill.origin === "marketplace") communitySkillCount += 1;
    }
  }
  const curatedAgents = agents.flatMap((agent) => {
    const listingId = agent.marketplaceSource?.listingId;
    return listingId?.startsWith(CURATED_AGENT_PREFIX) ? [listingId.slice(CURATED_AGENT_PREFIX.length)] : [];
  });
  return {
    agentCount: agents.length,
    enabledRoutineCount,
    customMcpServerCount,
    plugins: [...plugins],
    curatedSkills: [...curatedSkills],
    curatedAgents,
    localSkillCount,
    communitySkillCount,
    providers: agents.map((agent) => agent.provider),
    computerUseEnabled: sources.computerUseEnabled(),
  };
});
