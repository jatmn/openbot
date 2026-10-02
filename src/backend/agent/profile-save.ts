import { rm } from "node:fs/promises";
import type {
  AgentSummary,
  ConversationMessageSender,
  SaveAgentProfileInput,
  SaveAgentProfileResult,
  SidebarLayoutSnapshot,
} from "@openbot/contracts/ipc";
import { decodeSaveAgentProfileResult } from "@openbot/contracts/ipc";
import { sourceText } from "@openbot/i18n/source";
import { Effect, Result, Schema } from "effect";
import type { AgentStore } from "../agent-store";
import type { SidebarLayoutStore } from "../sidebar-layout-store";

interface ProfileSaveHooks {
  create(
    input: SaveAgentProfileInput,
    configure: (agent: AgentSummary) => Promise<AgentSummary>,
    sender: ConversationMessageSender | undefined,
  ): Promise<AgentSummary>;
  changed(agent: AgentSummary): void;
  delete(agent: AgentSummary): Promise<void>;
}

/** Coordinates reviewed profiles with the separately persisted sidebar, and receipts for network retries. */
export class ProfileSave {
  readonly #pendingAgents = new Set<string>();

  mayDrain(agentId: string): boolean {
    return !this.#pendingAgents.has(agentId);
  }

  #queue: Promise<void> = Promise.resolve();
  constructor(
    private readonly store: AgentStore,
    private readonly hooks: ProfileSaveHooks,
  ) {}

  /** `sender` is the person who writes the first message of a new agent. */
  save(
    input: SaveAgentProfileInput,
    sidebar: Pick<SidebarLayoutStore, "getSnapshot" | "withProfileAssignment">,
    sender?: ConversationMessageSender,
  ): Promise<SaveAgentProfileResult> {
    const operation = this.#queue.then(() => this.#save(input, sidebar, sender));
    this.#queue = operation.then(
      () => undefined,
      () => undefined,
    );
    return operation;
  }

  #save(
    input: SaveAgentProfileInput,
    sidebar: Pick<SidebarLayoutStore, "getSnapshot" | "withProfileAssignment">,
    sender: ConversationMessageSender | undefined,
  ): Promise<SaveAgentProfileResult> {
    return runProfileSave(this.#saveEffect(input, sidebar, sender));
  }

  readonly #saveEffect = Effect.fn("ProfileSave.save")(function* (
    this: ProfileSave,
    input: SaveAgentProfileInput,
    sidebar: Pick<SidebarLayoutStore, "getSnapshot" | "withProfileAssignment">,
    sender: ConversationMessageSender | undefined,
  ) {
    const commandId = `agent-profile:${input.operationId}`;
    const receipt = yield* profileStep(() => this.store.database.commandResult(commandId));
    if (receipt !== undefined) {
      return yield* profileStep(() => {
        const saved = decodeSaveAgentProfileResult(receipt);
        if (input.agentId && saved.agent.id !== input.agentId)
          throw new Error(sourceText("error.agent.saveOtherAgent"));
        const agent = this.store.list().find((candidate) => candidate.id === saved.agent.id);
        if (!agent) throw new Error(sourceText("error.agent.savedGone"));
        return { agent, layout: sidebar.getSnapshot() };
      });
    }
    const previous = yield* profileStep(() => {
      const agent = input.agentId ? this.store.list().find((candidate) => candidate.id === input.agentId) : null;
      if (input.agentId && !agent) throw new Error(sourceText("error.agent.gone"));
      return agent;
    });
    const oldAvatar = previous ? this.store.resolveAvatar(previous.id) : null;
    // The sidebar owns its serialization; the callback runs the profile workflow under that lock.
    const result = yield* profileIo(() =>
      sidebar.withProfileAssignment(input.draft.sectionId, (assign) =>
        runProfileSave(this.#assignEffect(input, previous, commandId, sidebar.getSnapshot(), assign, sender)),
      ),
    );
    if (oldAvatar) yield* profileIo(() => rm(oldAvatar.path, { force: true })).pipe(Effect.ignore);
    this.hooks.changed(result.agent);
    return result;
  }, Effect.uninterruptible);

  readonly #assignEffect = Effect.fn("ProfileSave.assign")(function* (
    this: ProfileSave,
    input: SaveAgentProfileInput,
    previous: AgentSummary | null | undefined,
    commandId: string,
    initialLayout: SidebarLayoutSnapshot,
    assign: (agentId: string) => Promise<SidebarLayoutSnapshot>,
    sender: ConversationMessageSender | undefined,
  ) {
    let layout = initialLayout;
    const pending: { created: AgentSummary | null } = { created: null };
    const operation = Effect.gen({ self: this }, function* () {
      let agent: AgentSummary;
      if (previous) {
        layout = yield* profileIo(() => assign(previous.id));
        agent = yield* profileStep(
          () => this.store.commitReviewedProfile(previous.id, input.draft, commandId, layout).agent,
        );
      } else {
        agent = yield* profileIo(() =>
          this.hooks.create(
            input,
            (candidate) => {
              pending.created = candidate;
              this.#pendingAgents.add(candidate.id);
              return runProfileSave(
                Effect.gen({ self: this }, function* () {
                  layout = yield* profileIo(() => assign(candidate.id));
                  return yield* profileStep(() => this.store.saveReviewedProfile(candidate.id, input.draft));
                }),
              );
            },
            sender,
          ),
        );
        agent = yield* profileStep(
          () => this.store.commitReviewedProfile(agent.id, input.draft, commandId, layout).agent,
        );
      }
      return { agent, layout };
    }).pipe(
      Effect.catch((failure) =>
        Effect.gen({ self: this }, function* () {
          const created = pending.created;
          if (created && this.store.list().some((agent) => agent.id === created.id))
            yield* profileIo(() => this.hooks.delete(created));
          return yield* failure;
        }),
      ),
    );
    return yield* operation.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (pending.created) this.#pendingAgents.delete(pending.created.id);
        }),
      ),
    );
  }, Effect.uninterruptible);
}

export class ProfileSaveFailed extends Schema.TaggedError<ProfileSaveFailed>()("ProfileSaveFailed", {
  cause: Schema.Defect(),
}) {}

function profileIo<A>(run: () => Promise<A>): Effect.Effect<A, ProfileSaveFailed> {
  return Effect.tryPromise({ try: run, catch: (cause) => new ProfileSaveFailed({ cause }) });
}

function profileStep<A>(run: () => A): Effect.Effect<A, ProfileSaveFailed> {
  return Effect.try({ try: run, catch: (cause) => new ProfileSaveFailed({ cause }) });
}

async function runProfileSave<A>(effect: Effect.Effect<A, ProfileSaveFailed>): Promise<A> {
  const result = await Effect.runPromise(Effect.result(effect));
  if (Result.isFailure(result)) throw result.failure.cause;
  return result.success;
}
