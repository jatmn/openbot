import { DISCONNECTED_ONEPASSWORD_CONNECTOR, type OnePasswordConnectorStatus } from "@openbot/contracts/ipc";
import { OnePasswordConnectorPanel } from "@openbot/ui/features/settings/OnePasswordConnectorPanel";
import { fn } from "storybook/test";
import type { Meta, StoryObj } from "storybook-solidjs-vite";

const meta = {
  title: "Settings/OnePasswordConnectorPanel",
  component: OnePasswordConnectorPanel,
  parameters: { layout: "padded", a11y: { test: "error" } },
} satisfies Meta<typeof OnePasswordConnectorPanel>;

export default meta;
type Story = StoryObj<typeof meta>;

const args = (status: Partial<OnePasswordConnectorStatus>, busy = false) => ({
  status: { ...DISCONNECTED_ONEPASSWORD_CONNECTOR, ...status },
  busy,
  onConnect: fn(),
  onConnectWithToken: fn(),
  onCancel: fn(),
  onDisconnect: fn(),
});

export const Disconnected: Story = { args: args({}) };

/** The CLI waits for the user to approve it in the 1Password app. */
export const Connecting: Story = { args: args({ state: "connecting" }, true) };

export const ChooseAccount: Story = {
  args: args({
    state: "choose-account",
    accounts: [
      { id: "personal", label: "ada@example.com (my.1password.com)" },
      { id: "work", label: "ada@example.org (example.1password.com)" },
    ],
  }),
};

export const Connected: Story = {
  args: args({ state: "connected", vaultNames: ["Shared with OpenBot"], loginCount: 3 }),
};

/** The login count has not arrived from 1Password. */
export const ConnectedLoading: Story = {
  args: args({ state: "connected", vaultNames: ["Shared with OpenBot"], loginCount: null }),
};

export const Failed: Story = {
  args: args({
    error:
      "OpenBot cannot find the 1Password CLI. Install it and turn on its integration in the 1Password app, or use a service account token.",
  }),
};
