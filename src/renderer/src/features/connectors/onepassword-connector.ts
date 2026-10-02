import { DISCONNECTED_ONEPASSWORD_CONNECTOR, type OnePasswordConnectorStatus } from "@openbot/contracts/ipc";
import { toast } from "@openbot/ui";
import type { OnePasswordConnectorPanelProps } from "@openbot/ui/features/settings/OnePasswordConnectorPanel";
import { currentText } from "@openbot/ui/text";
import { createSignal, onCleanup, onSettled } from "solid-js";
import { type OnePasswordConnectorPort, onePasswordConnectorPort } from "./onepassword-connector-port";

export interface OnePasswordConnectorController {
  status: () => OnePasswordConnectorStatus;
  busy: () => boolean;
  connect: (accountId: string | null) => void;
  connectWithToken: (token: string) => void;
  cancel: () => void;
  disconnect: () => void;
}

/**
 * The 1Password connection of this computer, for the page that shows it. Reads the status once and
 * then follows main's `changed` event: the CLI waits for an approval in the 1Password app, and the
 * login count arrives after the vault is read. Call it inside a component: the subscription ends
 * with that component.
 *
 * Cancel does not wait for another action: Connect waits for the CLI, and Cancel is the way out.
 */
export function createOnePasswordConnector(
  port: () => OnePasswordConnectorPort = onePasswordConnectorPort,
): OnePasswordConnectorController {
  const [status, setStatus] = createSignal<OnePasswordConnectorStatus>(DISCONNECTED_ONEPASSWORD_CONNECTOR);
  const [busy, setBusy] = createSignal(false);
  let disposed = false;

  const unsubscribe = port().onChanged((next) => {
    if (!disposed) setStatus(next);
  });
  onSettled(() => {
    void port()
      .status()
      .then((next) => {
        if (!disposed) setStatus(next);
      })
      .catch(() => undefined);
  });
  onCleanup(() => {
    disposed = true;
    unsubscribe();
  });

  const run = (action: () => Promise<OnePasswordConnectorStatus>, waits = true) => {
    if (waits && busy()) return;
    if (waits) setBusy(true);
    void action()
      .then((next) => {
        if (!disposed) setStatus(next);
      })
      .catch((error: unknown) => {
        const { t, errorMessage } = currentText();
        toast.error(t("connector.onePassword.actionFailed"), {
          description: errorMessage(error, t("connector.onePassword.actionFailed")),
        });
      })
      .finally(() => {
        if (waits && !disposed) setBusy(false);
      });
  };

  return {
    status,
    busy,
    connect: (accountId) => run(() => port().connect({ accountId })),
    connectWithToken: (token) => run(() => port().connectWithToken(token)),
    cancel: () => run(() => port().cancel(), false),
    disconnect: () => run(() => port().disconnect()),
  };
}

/** The panel props of the controller, read live. */
export function onePasswordPanelProps(controller: OnePasswordConnectorController): OnePasswordConnectorPanelProps {
  return {
    get status() {
      return controller.status();
    },
    get busy() {
      return controller.busy();
    },
    onConnect: controller.connect,
    onConnectWithToken: controller.connectWithToken,
    onCancel: controller.cancel,
    onDisconnect: controller.disconnect,
  };
}
