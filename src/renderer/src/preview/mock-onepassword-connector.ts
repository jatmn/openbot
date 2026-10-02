import {
  DISCONNECTED_ONEPASSWORD_CONNECTOR,
  type OnePasswordConnectorDesktopApi,
  type OnePasswordConnectorStatus,
} from "@openbot/contracts/ipc";

/** How long the mock CLI takes to create the vault and the service account. */
const MOCK_CONNECT_MS = 1_500;

const CONNECTED: OnePasswordConnectorStatus = {
  ...DISCONNECTED_ONEPASSWORD_CONNECTOR,
  state: "connected",
  vaultNames: ["Shared with OpenBot"],
  loginCount: 3,
};

/**
 * Starts disconnected. Connect first asks for one of two accounts, as a CLI signed in to several
 * does, then connects after a moment. A pasted token connects at once.
 */
export function createMockOnePasswordConnector(): OnePasswordConnectorDesktopApi {
  let status = DISCONNECTED_ONEPASSWORD_CONNECTOR;
  /** Each connect, cancel and disconnect replaces the connection that a connect waits for. */
  let attempt = 0;
  const listeners = new Set<(status: OnePasswordConnectorStatus) => void>();
  const set = (next: OnePasswordConnectorStatus): OnePasswordConnectorStatus => {
    status = next;
    for (const listener of listeners) listener({ ...status });
    return { ...status };
  };
  return {
    status: async () => ({ ...status }),
    connect: async ({ accountId }) => {
      attempt += 1;
      const current = attempt;
      if (!accountId)
        return set({
          ...DISCONNECTED_ONEPASSWORD_CONNECTOR,
          state: "choose-account",
          accounts: [
            { id: "personal", label: "ada@example.com (my.1password.com)" },
            { id: "work", label: "ada@example.org (example.1password.com)" },
          ],
        });
      set({ ...DISCONNECTED_ONEPASSWORD_CONNECTOR, state: "connecting" });
      await new Promise<void>((resolve) => setTimeout(resolve, MOCK_CONNECT_MS));
      if (current !== attempt) return { ...status };
      return set(CONNECTED);
    },
    connectWithToken: async () => {
      attempt += 1;
      return set(CONNECTED);
    },
    cancel: async () => {
      attempt += 1;
      return set(DISCONNECTED_ONEPASSWORD_CONNECTOR);
    },
    disconnect: async () => {
      attempt += 1;
      return set(DISCONNECTED_ONEPASSWORD_CONNECTOR);
    },
    onChanged: (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
  };
}
