import { join } from "node:path";
import { Effect } from "effect";
import type { LocalSkillTools } from "../backend/agent/skill-tools";
import { archiveCall, archiveFailure, runArchiveEffect } from "./archive-effects";
import type { SkillMarketplaceService } from "./skill-marketplace-service";

export function localSkillTools(skills: SkillMarketplaceService): LocalSkillTools {
  const library = skills.requireLocalLibrary();
  return {
    list: () => library.list(),
    get: (input) =>
      runArchiveEffect(
        Effect.gen(function* () {
          const detail = yield* library
            .getEffect(input.skillId, input.revision)
            .pipe(Effect.mapError((error) => archiveFailure(error.cause)));
          return { ...detail, archivePath: join(library.root, detail.id, String(detail.version), "bundle.zip") };
        }),
      ),
    revise: (input) => library.revise(input.agentId, input.skillId, input.expectedRevision, input.sourcePath),
    install: (input) => skills.installLocal(input),
    listInstalled: (agentId) => skills.listInstalled(agentId),
    setEnabled: (input) => skills.setEnabled(input),
    uninstall: (input) => skills.uninstall(input),
    create: (input) =>
      runArchiveEffect(
        Effect.gen(function* () {
          const skill = yield* archiveCall(() => library.create(input.agentId, input.sourcePath));
          yield* archiveCall(() =>
            skills.installLocal({ agentId: input.agentId, skillId: skill.id, revision: skill.version }),
          ).pipe(
            Effect.mapError(() =>
              archiveFailure(
                new Error(
                  `Skill ${skill.id} was saved as revision ${skill.version}, but installation failed. Read it and retry install_local_skill; do not create it again.`,
                ),
              ),
            ),
          );
          return skill;
        }),
      ),
  };
}
