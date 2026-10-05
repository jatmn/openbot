import { runChannel } from "./channel-test-runtime";
import { Effect } from "effect";
import { channelFailure } from "./channel-effects";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ChannelCommand } from "@openbot/contracts/ipc";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { stores } from "./agent-service-test-harness";
import { AttachmentFiles } from "./attachment-files";
import type { ChannelTextModel } from "./channel-history";
import { ChannelService } from "./channel-service";

let root: string;
let data: ReturnType<typeof stores>;
let service: ChannelService;
let limited = false;
let confirm: ((outcome: "accepted" | "rejected" | "uncertain") => void) | undefined;
const generate = vi.fn<ChannelTextModel>();
const actor = { id: "human", name: "Alex" };
const channelId = "channel";
const members = [{ agentId: "agent-a" }, { agentId: "agent-b" }];
const channelDraft = { name: "Project", title: "", instructions: "", members, leadAgentId: "agent-a" };

beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "openbot-coordination-recovery-"));
  data = stores(root);
  await runChannel(data.store.initialize());
  await runChannel(data.mailbox.initialize());
  await runChannel(data.store.getOrCreate("agent-a"));
  await runChannel(data.store.getOrCreate("agent-b"));
  limited = false;
  confirm = undefined;
  generate.mockReset();
  service = new ChannelService(data.store.database, data.mailbox, {
    agents: () => data.store.list(),
    generate,
    busy: () => limited,
    usageLimited: () => limited,
    schedule: () => {},
    interrupt: () => Effect.void,
    changed: () => {},
    error: (error) => {
      throw error;
    },
  });
  await runChannel(service.command({ type: "save", channelId, operationId: "create", draft: channelDraft }, actor));
});

afterEach(async () => {
  confirm?.("rejected");
  await runChannel(service.stop());
  data.store.database.close();
  await rm(root, { recursive: true, force: true });
});

async function startWork() {
  await runChannel(service.command(
    {
      type: "send",
      channelId,
      operationId: "work",
      text: "Implement the feature",
      recipientAgentId: "agent-a",
      replyToMessageId: null,
      attachmentDraftIds: [],
    },
    actor,
  ));
  await vi.waitFor(() => expect(service.store.assignments(channelId)[0]?.deliveryId).toBeTruthy());
  const assignment = required(service.store.assignments(channelId)[0]);
  const deliveryId = required(assignment.deliveryId);
  await runChannel(service.prepare(required(data.mailbox.getDelivery(deliveryId))));
  await runChannel(data.mailbox.markStarting(deliveryId));
  await runChannel(data.mailbox.markRunning(deliveryId, "work-turn"));
  service.accepted(deliveryId, "work-session", "work-turn");
  return { work: required(service.store.tasks(channelId)[0]), assignment, deliveryId };
}

const coordinate = (
  operationId: string,
  audience: "lead" | "all" = "lead",
): Extract<ChannelCommand, { type: "coordinate" }> => ({
  type: "coordinate",
  audience,
  channelId,
  operationId,
  text: "Report recorded status",
  replyToMessageId: null,
  attachmentDraftIds: [],
});

describe("channel coordination recovery", () => {
  it.each(["resume", "reassign"] as const)(
    "retains an accepted addition in the next work packet when target %s waits for confirmation",
    async (control) => {
      const { work, assignment: originalAssignment, deliveryId } = await startWork();
      const addition = "Preserve extended UNC paths when loading the compatibility configuration.";
      const steering = new Promise<"accepted" | "rejected" | "uncertain">((resolve) => {
        confirm = resolve;
      });
      const steer = vi.fn(() => Effect.tryPromise({ try: async () => steering, catch: channelFailure }));
      service.hooks.steer = steer;
      generate.mockReturnValue(Effect.succeed(
        JSON.stringify({ reply: "", actions: [{ kind: "instruct", taskId: work.id, instruction: addition }] }),
      ));
      await runChannel(service.command({ ...coordinate("instruction"), text: "Pass the agreed compatibility details" }, actor));
      await vi.waitFor(() => expect(steer).toHaveBeenCalledOnce());
      await runChannel(service.command(
        { type: "stop", channelId, operationId: "stop", taskId: work.id, recipientAgentId: null },
        actor,
      ));
      await runChannel(data.mailbox.markTerminal(deliveryId, "interrupted"));
      service.event({
        type: "turn-completed",
        agentId: "agent-a",
        threadId: service.store.context(channelId, "agent-a").threadId,
        turnId: "work-turn",
        status: "interrupted",
      });
      const recovery = runChannel(service.command(
        {
          type: control,
          channelId,
          operationId: "recover",
          taskId: work.id,
          recipientAgentId: control === "reassign" ? "agent-b" : null,
        },
        actor,
      ));
      // Let the public command enter its serialized apply step; its mutation must wait.
      await Promise.resolve();
      const stateBeforeConfirmation = service.store.tasks(channelId).find((task) => task.id === work.id)?.state;
      required(confirm)("accepted");
      await recovery;
      const owner = control === "reassign" ? "agent-b" : "agent-a";
      await vi.waitFor(() =>
        expect(
          service.store.assignments(channelId).some((item) => item.id !== originalAssignment.id && item.deliveryId),
        ).toBe(true),
      );
      const fresh = required(
        service.store.assignments(channelId).find((item) => item.id !== originalAssignment.id && item.deliveryId),
      );
      expect(fresh.agentId).toBe(owner);
      const packet = required(await runChannel(service.prepare(required(data.mailbox.getDelivery(required(fresh.deliveryId))))));
      expect(stateBeforeConfirmation).toBe("paused");
      expect(packet.text).toContain(addition);
      expect(service.store.tasks(channelId).find((task) => task.execution === "instruction")?.state).toBe("completed");
      expect(steer).toHaveBeenCalledOnce();
    },
  );

  it.each(["accepted", "rejected", "uncertain"] as const)(
    "settles a child's %s instruction before recovery of its parent branch",
    async (outcome) => {
      const { work, deliveryId } = await startWork();
      await runChannel(service.tool(channelId, "agent-a", "work-turn", "delegate", "channel_assign", {
        recipientAgentId: "agent-b",
        task: "Implement the compatibility loader",
        expectedResult: "Working loader",
        sourceMessageIds: [work.requestMessageId],
        resources: ["host"],
      }));
      await runChannel(data.mailbox.markTerminal(deliveryId, "completed"));
      service.event({
        type: "turn-completed",
        agentId: "agent-a",
        threadId: service.store.context(channelId, "agent-a").threadId,
        turnId: "work-turn",
        status: "completed",
      });
      const child = required(service.store.tasks(channelId).find((task) => task.parentTaskId === work.id));
      await vi.waitFor(() =>
        expect(service.store.assignments(channelId).find((item) => item.taskId === child.id)?.deliveryId).toBeTruthy(),
      );
      const childAssignment = required(service.store.assignments(channelId).find((item) => item.taskId === child.id));
      const childDeliveryId = required(childAssignment.deliveryId);
      await runChannel(service.prepare(required(data.mailbox.getDelivery(childDeliveryId))));
      await runChannel(data.mailbox.markStarting(childDeliveryId));
      await runChannel(data.mailbox.markRunning(childDeliveryId, "child-turn"));
      service.accepted(childDeliveryId, "child-session", "child-turn");
      const addition = "Keep Windows compatibility";
      const steering = new Promise<"accepted" | "rejected" | "uncertain">((resolve) => {
        confirm = resolve;
      });
      const steer = vi.fn(() => Effect.tryPromise({ try: async () => steering, catch: channelFailure }));
      service.hooks.steer = steer;
      generate.mockReturnValue(Effect.succeed(
        JSON.stringify({ reply: "", actions: [{ kind: "instruct", taskId: child.id, instruction: addition }] }),
      ));
      await runChannel(service.command(coordinate("instruction"), actor));
      await vi.waitFor(() => expect(steer).toHaveBeenCalledOnce());
      await runChannel(service.command(
        { type: "stop", channelId, operationId: "stop-parent", taskId: work.id, recipientAgentId: null },
        actor,
      ));
      await runChannel(data.mailbox.markTerminal(childDeliveryId, "interrupted"));
      service.event({
        type: "turn-completed",
        agentId: "agent-b",
        threadId: service.store.context(channelId, "agent-b").threadId,
        turnId: "child-turn",
        status: "interrupted",
      });
      const recovery = service
        .command(
          { type: "resume", channelId, operationId: "recover-parent", taskId: work.id, recipientAgentId: null },
          actor,
        )
        .then(
          () => null,
          (error: Error) => error,
        );
      await Promise.resolve();
      const stateBeforeConfirmation = service.store.tasks(channelId).find((task) => task.id === work.id)?.state;
      required(confirm)(outcome);
      const error = await recovery;
      expect(stateBeforeConfirmation).toBe("paused");
      expect(steer).toHaveBeenCalledOnce();
      if (outcome === "uncertain") {
        expect(error?.message).toContain("confirmed");
        for (const type of ["resume", "reassign"] as const)
          await expect(
            runChannel(service.command(
              {
                type,
                channelId,
                operationId: `retry-${type}`,
                taskId: work.id,
                recipientAgentId: type === "reassign" ? "agent-b" : null,
              },
              actor,
            )),
          ).rejects.toThrow("confirmed");
        expect(service.store.tasks(channelId).find((task) => task.id === work.id)?.state).toBe("paused");
      } else {
        expect(error).toBeNull();
        const instruction = required(
          service.store.tasks(channelId).find((task) => task.id !== child.id && task.instruction.includes(addition)),
        );
        if (outcome === "accepted")
          expect(service.store.tasks(channelId).find((task) => task.id === child.id)?.instruction).toContain(addition);
        else expect(instruction.execution).toBeUndefined();
      }
    },
  );

  it.each(["lead", "all"] as const)(
    "holds %s generations and resumes queued work after a first refusal and reset",
    async (audience) => {
      limited = true;
      const request = coordinate("request", audience);
      await runChannel(service.command(request, actor));
      expect(generate).not.toHaveBeenCalled();
      generate.mockImplementation(() => Effect.tryPromise({ try: async () => {
        limited = true;
        throw new Error("The profile generation failed.");
      }, catch: channelFailure }));
      limited = false;
      await runChannel(service.wake(channelId));
      await vi.waitFor(() => expect(generate).toHaveBeenCalled());
      await vi.waitFor(() => expect(service.responseCount()).toBe(0));
      expect(service.store.tasks(channelId).every((task) => task.state === "queued" && task.error === null)).toBe(true);
      generate.mockImplementation((agent) => Effect.tryPromise({ try: async () => JSON.stringify({ reply: `Status from ${agent.name}`, ...(audience === "lead" ? { actions: [] } : {}) }), catch: channelFailure }));
      limited = false;
      await runChannel(service.wake(channelId));
      await vi.waitFor(() =>
        expect(service.store.tasks(channelId).every((task) => task.state === "completed")).toBe(true),
      );
      await runChannel(service.command(request, actor));
      expect(service.store.tasks(channelId)).toHaveLength(audience === "lead" ? 1 : 2);
    },
  );

  it.each(["accepted", "uncertain"] as const)(
    "keeps a replacement request independent from an old %s instruction",
    async (outcome) => {
      const { work } = await startWork();
      const pending = new Promise<"accepted" | "rejected" | "uncertain">((resolve) => {
        confirm = resolve;
      });
      let instructionId: string | undefined;
      const steer = vi.fn<NonNullable<typeof service.hooks.steer>>((_agentId, _threadId, _turnId, messageId) => Effect.tryPromise({ try: async () => !instructionId || messageId === instructionId ? pending : "rejected", catch: channelFailure }));
      service.hooks.steer = steer;
      const addition = "Apply the old compatibility setting";
      generate.mockReturnValue(Effect.succeed(
        JSON.stringify({ reply: "", actions: [{ kind: "instruct", taskId: work.id, instruction: addition }] }),
      ));
      await runChannel(service.command(coordinate("old-instruction"), actor));
      await vi.waitFor(() => expect(steer).toHaveBeenCalledOnce());
      instructionId = required(service.store.tasks(channelId).find((task) => task.execution === "instruction")).id;
      await runChannel(service.command(
        {
          type: "send",
          operationId: "replacement",
          channelId,
          text: "Replace the assignment",
          recipientAgentId: "agent-a",
          replyToMessageId: work.requestMessageId,
          attachmentDraftIds: [],
        },
        actor,
      ));
      required(confirm)(outcome);
      await vi.waitFor(() => expect(service.responseCount()).toBe(0));
      expect(service.store.tasks(channelId).find((task) => task.id === work.id)?.instruction).toBe(
        "Replace the assignment",
      );
      await runChannel(service.command(
        { type: "stop", channelId, operationId: "stop-new-request", taskId: work.id, recipientAgentId: null },
        actor,
      ));
      await runChannel(service.command(
        { type: "resume", channelId, operationId: "resume-new-request", taskId: work.id, recipientAgentId: null },
        actor,
      ));
      expect(service.store.tasks(channelId).find((task) => task.id === work.id)?.state).toBe("queued");
    },
  );

  it("keeps unrelated generation failures as failures", async () => {
    generate.mockReturnValue(Effect.fail(channelFailure(new Error("Provider disconnected"))));
    await runChannel(service.command(coordinate("request"), actor));
    await vi.waitFor(() => expect(service.store.tasks(channelId)[0]?.state).toBe("failed"));
  });

  it.each([
    ["lead", "initial"],
    ["all", "initial"],
    ["lead", "during-copy"],
    ["all", "during-copy"],
  ] as const)("preserves file-only %s uploads rejected for missing recipients %s", async (audience, timing) => {
    if (timing === "initial")
      await runChannel(service.command(
        {
          type: "save",
          channelId,
          operationId: "missing-recipients",
          draft: { ...channelDraft, members: audience === "all" ? [] : members, leadAgentId: null },
        },
        actor,
      ));
    const file = join(root, "specification.txt");
    await writeFile(file, "Specification bytes");
    const attachment = required((await runChannel(data.mailbox.prepareAttachments([file])))[0]);
    if (timing === "during-copy") {
      const copy = AttachmentFiles.prototype.commitMessageTransfer;
      vi.spyOn(AttachmentFiles.prototype, "commitMessageTransfer").mockImplementationOnce(function (
        this: AttachmentFiles,
        ...args
      ) {
        return copy.apply(this, args).pipe(Effect.tap(() =>
          Effect.sync(() => service.removeDeletedMembers(new Set())),
        ));
      });
    }
    const request: ChannelCommand = {
      ...coordinate("file-request", audience),
      text: "",
      attachmentDraftIds: [attachment.id],
    };
    await expect(runChannel(service.command(request, actor))).rejects.toThrow("Choose an available channel lead");
    expect(service.committed(actor.id, request.operationId)).toBe(false);
    expect(service.store.messages(channelId)).toHaveLength(0);
    expect(data.store.database.readMailboxState()).toMatchObject({ messages: [], drafts: [{ id: attachment.id }] });
    const retainedDraft = await runChannel(data.mailbox.resolveAttachment(attachment.id));
    expect(retainedDraft).not.toBeNull();
    const retained = required(retainedDraft);
    expect(await readFile(retained.path, "utf8")).toBe("Specification bytes");
    expect(data.mailbox.listStoredFiles()).toHaveLength(0);
    await runChannel(service.command({ type: "save", channelId, operationId: "correct-recipients", draft: channelDraft }, actor));
    await runChannel(service.command(request, actor));
    await runChannel(service.command(request, actor));
    const message = required(service.store.messages(channelId).find((item) => item.author.kind === "member"));
    const committed = required(await runChannel(data.mailbox.resolveAttachment(required(message.message.attachments?.[0]).id)));
    expect(await readFile(committed.path, "utf8")).toBe("Specification bytes");
    expect(service.store.messages(channelId).filter((item) => item.author.kind === "member")).toHaveLength(1);
    expect(service.store.tasks(channelId)).toHaveLength(audience === "lead" ? 1 : 2);
  });

  it("keeps accepted group files and pauses members removed during draft cleanup", async () => {
    const file = join(root, "specification.txt");
    await writeFile(file, "Specification bytes");
    const attachment = required((await runChannel(data.mailbox.prepareAttachments([file])))[0]);
    const remove = AttachmentFiles.prototype.removeAttachmentDirectories;
    vi.spyOn(AttachmentFiles.prototype, "removeAttachmentDirectories").mockImplementationOnce(function (
      this: AttachmentFiles,
      ...args
    ) {
      return Effect.sync(() => service.removeDeletedMembers(new Set())).pipe(
        Effect.andThen(() => remove.apply(this, args)),
      );
    });
    const request: ChannelCommand = {
      ...coordinate("accepted-files", "all"),
      text: "",
      attachmentDraftIds: [attachment.id],
    };
    await runChannel(service.command(request, actor));
    expect(service.committed(actor.id, request.operationId)).toBe(true);
    expect(service.store.get(channelId).members).toEqual([]);
    expect(service.store.tasks(channelId)).toHaveLength(2);
    expect(service.store.tasks(channelId).every((task) => task.state === "paused")).toBe(true);
    const message = required(service.store.messages(channelId).find((item) => item.author.kind === "member"));
    const committed = required(await runChannel(data.mailbox.resolveAttachment(required(message.message.attachments?.[0]).id)));
    expect(await readFile(committed.path, "utf8")).toBe("Specification bytes");
    await runChannel(service.command(request, actor));
    expect(service.store.messages(channelId).filter((item) => item.author.kind === "member")).toHaveLength(1);
  });
});

function required<T>(value: T | null | undefined): T {
  if (value === null || value === undefined) throw new Error("The expected test record is missing.");
  return value;
}
