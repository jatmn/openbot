/**
 * The first start of a hosted server: a VM that the account server created for one account. It runs
 * only in a packaged Linux build that the server template starts with `OPENBOT_HOSTED_SERVER=1`.
 *
 * The VM gets a host ID and a single-use claim. The claim becomes the owner's account session, and
 * the host is configured with the ID that the account server reserved for that owner. It then
 * publishes at launch like any other host. Nothing here goes through IPC: the renderer never sees
 * the claim or the session.
 *
 * `applyHostedServerAccount` must run after `teamStore.initialize()` and before `HostService` is
 * built, at the same position as `applyDevelopmentRemoteAccount`.
 */

import { parseHostedServerList } from "@openbot/contracts/hosted-servers";
import type { CentralAuthState, CentralAuthUser } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import type { CentralAuthManager } from "./central-auth-manager";
import { RemoteWorkflowError, remoteCall, runRemoteWorkflow } from "./remote-service-effects";
import type { TeamStore } from "./team-store";

const HOST_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/u;

export interface HostedServerEnvironment {
  hostId: string;
  /** Null after the first start: the claim is spent, and the stored session signs the server in. */
  claim: string | null;
}

/**
 * Returns null unless this process is a hosted server. It removes the claim from `environment`,
 * because agents and their tools inherit the environment of this process.
 */
export function takeHostedServerEnvironment(
  environment: NodeJS.ProcessEnv,
  isPackaged: boolean,
  platform: NodeJS.Platform,
): HostedServerEnvironment | null {
  const claim = environment.OPENBOT_HOSTED_CLAIM?.trim() || null;
  delete environment.OPENBOT_HOSTED_CLAIM;
  if (!isPackaged || platform !== "linux" || environment.OPENBOT_HOSTED_SERVER !== "1") return null;
  const hostId = environment.OPENBOT_HOSTED_HOST_ID?.trim().toLowerCase() ?? "";
  if (!HOST_ID_PATTERN.test(hostId)) return null;
  return { hostId, claim };
}

export interface HostedServerAccountOptions {
  environment: HostedServerEnvironment;
  centralAuth: Pick<CentralAuthManager, "canPersistSession" | "redeemHostedServerClaim" | "requestAuthorized">;
  centralAuthInitialization: Promise<CentralAuthState>;
  teamStore: Pick<
    TeamStore,
    "activateAccount" | "configured" | "configureWithAccount" | "getIdentity" | "setEnabledOnLaunch"
  >;
}

export function applyHostedServerAccount(input: HostedServerAccountOptions): Promise<void> {
  return runRemoteWorkflow(applyHostedServerAccountEffect(input));
}
export const applyHostedServerAccountEffect = Effect.fn("HostedServer.applyAccount")(function* ({
  environment,
  centralAuth,
  centralAuthInitialization,
  teamStore,
}: HostedServerAccountOptions) {
  const state = yield* remoteCall(() => centralAuthInitialization);
  let user: CentralAuthUser;
  let serverName: string | null = null;
  if (state.status === "signed_in") {
    user = state.user;
  } else {
    // The account server did not answer. A stored session can still exist and the claim can be spent,
    // so the claim waits for an answer. The start retry signs in again.
    if (state.status === "error")
      return yield* new RemoteWorkflowError({ cause: new Error("The account server did not answer at the start.") });
    if (!environment.claim)
      return yield* new RemoteWorkflowError({ cause: new Error("The hosted server is signed out and has no claim.") });
    // The claim works one time. With no secret storage the session would end at the next start, and
    // the server could not sign in again. Keep the claim for a start that has a keyring.
    if (!centralAuth.canPersistSession())
      return yield* new RemoteWorkflowError({
        cause: new Error("The hosted server has no secret storage for its session."),
      });
    const claim = environment.claim;
    const redeemed = yield* remoteCall(() => centralAuth.redeemHostedServerClaim(claim));
    if (redeemed.hostId !== environment.hostId)
      return yield* new RemoteWorkflowError({ cause: new Error("The claim is for a different hosted server.") });
    user = redeemed.user;
    serverName = redeemed.name;
  }
  yield* remoteCall(() => teamStore.activateAccount(user));
  if (!teamStore.configured) {
    // A start that stopped after the claim and before this point has a session and no name.
    serverName ??= yield* hostedServerNameEffect(centralAuth, environment.hostId);
    const name = serverName;
    yield* remoteCall(() => teamStore.configureWithAccount(name, user, undefined, { serverId: environment.hostId }));
  }
  const identity = teamStore.getIdentity();
  if (identity?.serverId !== environment.hostId) {
    return yield* new RemoteWorkflowError({
      cause: new Error("The configured host is not the hosted server that this VM was created for."),
    });
  }
  if (!identity.enabledOnLaunch) yield* remoteCall(() => teamStore.setEnabledOnLaunch(identity.serverId, true));
});

const hostedServerNameEffect = Effect.fn("HostedServer.name")(function* (
  centralAuth: Pick<CentralAuthManager, "requestAuthorized">,
  hostId: string,
) {
  const list = yield* remoteCall(() =>
    centralAuth.requestAuthorized("/v2/hosting/servers/", { method: "GET" }, (value) => {
      const parsed = parseHostedServerList(value);
      if (!parsed) throw new Error("Invalid hosted server list.");
      return parsed;
    }),
  );
  const server = list.servers.find((entry) => entry.serverId === hostId);
  if (!server)
    return yield* new RemoteWorkflowError({
      cause: new Error("The account server has no record of this hosted server."),
    });
  return server.name;
});
