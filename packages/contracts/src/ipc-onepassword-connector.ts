import { isDynamicRecord, isNumber, isString } from "./runtime-values";

/**
 * The 1Password connection of this computer: a service account that reads one vault the user shares
 * with OpenBot. The embedded browser fills logins from it. The token never crosses to the renderer.
 *
 * - `disconnected`: no token is stored.
 * - `connecting`: the 1Password CLI creates the vault and the service account. 1Password can ask the
 *   user to approve it.
 * - `choose-account`: the CLI is signed in to several accounts. The user picks one in `accounts`.
 * - `connected`: the browser fills logins from `vaultNames`.
 */
export type OnePasswordConnectorState = "disconnected" | "connecting" | "choose-account" | "connected";

function isOnePasswordConnectorState(value: unknown): value is OnePasswordConnectorState {
  return value === "disconnected" || value === "connecting" || value === "choose-account" || value === "connected";
}

export interface OnePasswordAccount {
  id: string;
  /** The email and the sign-in address, as the user knows the account. */
  label: string;
}

export interface OnePasswordConnectorStatus {
  state: OnePasswordConnectorState;
  /** Set only in `choose-account`. */
  accounts: OnePasswordAccount[];
  /** The vaults that the service account reads. Empty when not connected. */
  vaultNames: string[];
  /** The logins that the browser can fill, or null when not counted yet. */
  loginCount: number | null;
  /** The last connection failure, as a sentence. Null after a success or a cancel. */
  error: string | null;
}

export const DISCONNECTED_ONEPASSWORD_CONNECTOR: OnePasswordConnectorStatus = {
  state: "disconnected",
  accounts: [],
  vaultNames: [],
  loginCount: null,
  error: null,
};

/** `accountId` is null for the account the CLI uses when it is signed in to only one. */
export interface OnePasswordConnectInput {
  accountId: string | null;
}

function parseOnePasswordAccount(value: unknown): OnePasswordAccount | null {
  if (!isDynamicRecord(value) || !isString(value.id) || !isString(value.label)) return null;
  return { id: value.id, label: value.label };
}

/** Returns null for a value that is not a connector status in the desktop shape. */
export function parseOnePasswordConnectorStatus(value: unknown): OnePasswordConnectorStatus | null {
  if (
    !isDynamicRecord(value) ||
    !isOnePasswordConnectorState(value.state) ||
    !Array.isArray(value.accounts) ||
    !Array.isArray(value.vaultNames) ||
    !value.vaultNames.every(isString) ||
    !(value.loginCount === null || isNumber(value.loginCount)) ||
    !(value.error === null || isString(value.error))
  ) {
    return null;
  }
  const accounts: OnePasswordAccount[] = [];
  for (const item of value.accounts) {
    const account = parseOnePasswordAccount(item);
    if (!account) return null;
    accounts.push(account);
  }
  return {
    state: value.state,
    accounts,
    vaultNames: [...value.vaultNames],
    loginCount: value.loginCount,
    error: value.error,
  };
}
