// Members and invitations -- the one place the two transports answer to different authorities.
//
// Everywhere else in this family a WebRTC host and an HTTPS host are the same server reached two
// ways. Here they are not. An HTTPS server owns its own roster and answers on `TEAM_API_ROUTES.team`
// like every other route. A WebRTC host does not: its memberships and invitations live in the
// account service, which is a *different server* with a different trust story, and this file is the
// only one that talks to it. `revokeInvite(inviteId)` does not even take a host id -- that is the
// tell, and it is why `RemoteControlPlaneTransport` is named for the plane rather than the host.
//
// So the two arms stay visible. Hiding them behind one method would read as tidier and would cost
// the next reader an hour the first time an invitation goes missing from the wrong server.

import { createInviteUrl } from "@openbot/contracts/invite-links";
import type {
  InviteSummary,
  TeamInviteSummary,
  TeamMemberSummary,
  UpdateTeamMemberInput,
} from "@openbot/contracts/ipc";
import { TEAM_API_ROUTES } from "@openbot/contracts/team-api-routes";
import { sourceText } from "@openbot/i18n/source";
import { Effect } from "effect";
import type { RemoteInviteRecord, RemoteMemberRecord } from "./central-auth-records";
import { decodeVoid } from "./remote-host-decoding";
import type { RemoteRequestFn } from "./remote-server-client";
import type { RemoteServerDirectory } from "./remote-server-store";
import {
  RemoteRequest,
  RemoteWorkflowError,
  remoteCall,
  remoteDecode,
  runRemoteWorkflow,
} from "./remote-service-effects";
import { decodeInviteSummary, decodeTeamInvites, decodeTeamMember, decodeTeamMembers } from "./remote-team-decoding";

// The account service, not the host. Every method here crosses to a second authority.
export interface RemoteControlPlaneTransport {
  readonly controlPlaneUrl: string;
  listMembers(hostId: string): Promise<RemoteMemberRecord[]>;
  updateMember(hostId: string, membershipId: string, role: "admin" | "member", reactivate?: boolean): Promise<void>;
  removeMember(hostId: string, membershipId: string): Promise<void>;
  listInvites(hostId: string): Promise<RemoteInviteRecord[]>;
  createInvite(
    hostId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ): Promise<{ inviteId: string; token: string; expiresAt: number; permanent: boolean; useCount: number }>;
  revokeInvite(inviteId: string): Promise<void>;
}

export interface RemoteTeamDirectoryOptions {
  servers: Pick<RemoteServerDirectory, "require">;
  request: RemoteRequestFn;
  transport: RemoteControlPlaneTransport | null;
  // Read through a callback rather than held, so an account that gains email delivery later is not
  // frozen out by whatever was true when this object was built.
  sendInviteEmail: (input: {
    email: string;
    serverName: string;
    inviteUrl: string;
    role: "admin" | "member";
  }) => Promise<void>;
}

export class RemoteTeamDirectory {
  readonly #servers: RemoteTeamDirectoryOptions["servers"];
  readonly #request: RemoteRequestFn;
  readonly #transport: RemoteControlPlaneTransport | null;
  readonly #sendInviteEmail: RemoteTeamDirectoryOptions["sendInviteEmail"];

  constructor(options: RemoteTeamDirectoryOptions) {
    this.#servers = options.servers;
    this.#request = options.request;
    this.#transport = options.transport;
    this.#sendInviteEmail = options.sendInviteEmail;
  }

  #run<A>(operation: Effect.Effect<A, RemoteWorkflowError, RemoteRequest>): Promise<A> {
    return runRemoteWorkflow(operation.pipe(Effect.provide(RemoteRequest.layer(this.#request))));
  }
  listMembers(serverId: string): Promise<TeamMemberSummary[]> {
    return this.#run(this.listMembersEffect(serverId));
  }
  readonly listMembersEffect = Effect.fn("RemoteTeam.listMembers")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
  ): Effect.fn.Return<TeamMemberSummary[], RemoteWorkflowError, RemoteRequest> {
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) {
      const members = yield* remoteCall(() => transport.listMembers(serverId));
      return members.map((member) => ({
        id: member.membershipId,
        username: member.email,
        email: member.email,
        name: member.name,
        avatarUrl: member.avatarUrl,
        role: member.role,
        createdAt: new Date(member.createdAt).toISOString(),
        disabled: member.status !== "active",
      }));
    }
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.members, decodeTeamMembers),
    );
  });

  updateMember(serverId: string, input: UpdateTeamMemberInput): Promise<TeamMemberSummary> {
    return this.#run(this.updateMemberEffect(serverId, input));
  }
  readonly updateMemberEffect = Effect.fn("RemoteTeam.updateMember")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
    input: UpdateTeamMemberInput,
  ): Effect.fn.Return<TeamMemberSummary, RemoteWorkflowError, RemoteRequest> {
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) {
      const members = yield* this.listMembersEffect(serverId);
      const current = members.find((member) => member.id === input.memberId);
      if (!current || current.role === "owner")
        return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.host.memberNotFound")) });
      if (input.disabled) yield* remoteCall(() => transport.removeMember(serverId, input.memberId));
      else {
        const role = input.role ?? current.role;
        yield* remoteCall(() => transport.updateMember(serverId, input.memberId, role, input.disabled === false));
      }
      const updatedMembers = yield* this.listMembersEffect(serverId);
      const updated = updatedMembers.find((member) => member.id === input.memberId);
      if (!updated)
        return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.host.memberNotFound")) });
      return updated;
    }
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.member(input.memberId), decodeTeamMember, {
        method: "PATCH",
        body: { role: input.role, disabled: input.disabled },
      }),
    );
  });

  removeMember(serverId: string, memberId: string): Promise<void> {
    return this.#run(this.removeMemberEffect(serverId, memberId));
  }
  readonly removeMemberEffect = Effect.fn("RemoteTeam.removeMember")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
    memberId: string,
  ) {
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) return yield* remoteCall(() => transport.removeMember(serverId, memberId));
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.member(memberId), decodeVoid, { method: "DELETE" }),
    );
  });

  listInvites(serverId: string): Promise<TeamInviteSummary[]> {
    return this.#run(this.listInvitesEffect(serverId));
  }
  readonly listInvitesEffect = Effect.fn("RemoteTeam.listInvites")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
  ): Effect.fn.Return<TeamInviteSummary[], RemoteWorkflowError, RemoteRequest> {
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) {
      const invites = yield* remoteCall(() => transport.listInvites(serverId));
      return invites
        .filter((invite) => invite.revokedAt === null)
        .map((invite) => ({
          id: invite.inviteId,
          role: invite.role,
          expiresAt: new Date(invite.expiresAt).toISOString(),
          usedAt: invite.usedAt === null ? null : new Date(invite.usedAt).toISOString(),
          email: invite.email,
          permanent: invite.permanent,
          useCount: invite.useCount,
        }));
    }
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.invites, decodeTeamInvites),
    );
  });

  revokeInvite(serverId: string, inviteId: string): Promise<void> {
    return this.#run(this.revokeInviteEffect(serverId, inviteId));
  }
  readonly revokeInviteEffect = Effect.fn("RemoteTeam.revokeInvite")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
    inviteId: string,
  ) {
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) return yield* remoteCall(() => transport.revokeInvite(inviteId));
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.invite(inviteId), decodeVoid, { method: "DELETE" }),
    );
  });

  createInvite(
    serverId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ): Promise<InviteSummary> {
    return this.#run(this.createInviteEffect(serverId, input));
  }
  readonly createInviteEffect = Effect.fn("RemoteTeam.createInvite")(function* (
    this: RemoteTeamDirectory,
    serverId: string,
    input: { role: "admin" | "member"; email?: string; permanent?: boolean },
  ): Effect.fn.Return<InviteSummary, RemoteWorkflowError, RemoteRequest> {
    const server = yield* remoteDecode(() => this.#servers.require(serverId));
    const transport = yield* remoteDecode(() => this.#controlPlaneFor(serverId));
    if (transport) {
      if (!server.fingerprint)
        return yield* new RemoteWorkflowError({ cause: new Error(sourceText("error.remote.inviteNeedsConnection")) });
      const invite = yield* remoteCall(() => transport.createInvite(serverId, input));
      const result: InviteSummary = yield* remoteDecode(() => ({
        id: invite.inviteId,
        role: input.role,
        expiresAt: new Date(invite.expiresAt).toISOString(),
        usedAt: null,
        email: input.email ?? null,
        permanent: invite.permanent,
        useCount: invite.useCount,
        inviteUrl: createInviteUrl({
          apiUrl: transport.controlPlaneUrl,
          serverId,
          fingerprint: server.fingerprint,
          token: invite.token,
        }),
      }));
      const email = input.email;
      if (email) {
        // A mail failure must revoke the undisclosed credential, preserving the original error.
        yield* remoteCall(() =>
          this.#sendInviteEmail({ email, serverName: server.name, inviteUrl: result.inviteUrl, role: input.role }),
        ).pipe(
          Effect.tapError(() =>
            remoteCall(() => transport.revokeInvite(invite.inviteId)).pipe(Effect.catch(() => Effect.void)),
          ),
        );
      }
      return result;
    }
    if (input.permanent)
      return yield* new RemoteWorkflowError({
        cause: new Error(sourceText("error.remote.permanentInviteUnsupported")),
      });
    return yield* RemoteRequest.use((service) =>
      service.request(serverId, TEAM_API_ROUTES.team.invites, decodeInviteSummary, { method: "POST", body: input }),
    );
  });

  #controlPlaneFor(serverId: string): RemoteControlPlaneTransport | null {
    const server = this.#servers.require(serverId);
    return server.transport === "webrtc-v2" ? this.#transport : null;
  }
}
