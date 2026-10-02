import type { SaveAgentProfileInput, SaveAgentProfileResult } from "@openbot/contracts/ipc";
import { Effect, Schema } from "effect";
import { runTeamEffect } from "./effect-boundary";

class AgentProfileSaveFailure extends Schema.TaggedError<AgentProfileSaveFailure>()("AgentProfileSaveFailure", {
  cause: Schema.Defect(),
}) {}

/** Reconcile an ambiguous save before applying later edits, retaining a single created identity. */
export const saveReviewedAgentProfileEffect = Effect.fn("AgentProfile.saveReviewed")(function* (
  send: (input: SaveAgentProfileInput) => Promise<SaveAgentProfileResult>,
  input: SaveAgentProfileInput,
  pending?: SaveAgentProfileInput,
) {
  const submit = (value: SaveAgentProfileInput) =>
    Effect.tryPromise({
      try: () => send(value),
      catch: (cause) => new AgentProfileSaveFailure({ cause }),
    });
  if (!pending) return yield* submit(input);
  // Keep the original operation id when a previous save may have committed.
  const recovered = yield* submit({ ...input, operationId: pending.operationId });
  if (pending.operationId === input.operationId) return recovered;
  return yield* submit({ operationId: input.operationId, agentId: recovered.agent.id, draft: input.draft });
});

export function saveReviewedAgentProfile(
  send: (input: SaveAgentProfileInput) => Promise<SaveAgentProfileResult>,
  input: SaveAgentProfileInput,
  pending?: SaveAgentProfileInput,
): Promise<SaveAgentProfileResult> {
  // Existing callers classify the transport failure; do not expose a wrapper or log its cause.
  return runTeamEffect(
    saveReviewedAgentProfileEffect(send, input, pending).pipe(Effect.mapError((error) => error.cause)),
  );
}
