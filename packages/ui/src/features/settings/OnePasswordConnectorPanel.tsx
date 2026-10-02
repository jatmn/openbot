import type { OnePasswordConnectorStatus } from "@openbot/contracts/ipc";
import {
  Alert,
  AlertContent,
  AlertDescription,
  AlertIcon,
  AlertTitle,
  Button,
  Input,
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemGroup,
  ItemTitle,
  OctagonX,
  SettingsSection,
  Spinner,
  Text,
} from "@openbot/ui";
import { createSignal, For, Show } from "solid-js";
import { useText } from "../../text";
import { DangerZone, DetailHeader, type IntegrationStatus, OnePasswordMark } from "./IntegrationLayout";

export interface OnePasswordConnectorPanelProps {
  status: OnePasswordConnectorStatus;
  /** True while an action runs. Every button waits for it. */
  busy: boolean;
  /** `accountId` is null until the user picks one of several accounts. */
  onConnect: (accountId: string | null) => void;
  onConnectWithToken: (token: string) => void;
  onCancel: () => void;
  onDisconnect: () => void;
}

const HEADER_STATUS = {
  disconnected: { status: "idle", label: "connector.onePassword.statusNotSetUp" },
  connecting: { status: "idle", label: "connector.onePassword.statusConnecting" },
  "choose-account": { status: "idle", label: "connector.onePassword.statusConnecting" },
  connected: { status: "connected", label: "connector.onePassword.statusConnected" },
} as const satisfies Record<OnePasswordConnectorStatus["state"], { status: IntegrationStatus; label: string }>;

/**
 * The 1Password page of one OpenBot computer. The token never reaches this component after the user
 * types it: the status holds the vault names and the login count only.
 *
 * Connect asks the 1Password CLI to make a vault "Shared with OpenBot" and a service account that
 * reads only that vault. A user without the CLI pastes a service account token instead.
 */
export function OnePasswordConnectorPanel(props: OnePasswordConnectorPanelProps) {
  const { t, sourceText } = useText();
  const [tokenOpen, setTokenOpen] = createSignal(false);
  const [token, setToken] = createSignal("");
  const header = () => HEADER_STATUS[props.status.state];
  const submitToken = (event: SubmitEvent) => {
    event.preventDefault();
    const value = token().trim();
    if (!value) return;
    props.onConnectWithToken(value);
    setToken("");
  };

  return (
    <div class="onepassword-connector">
      <DetailHeader
        logo={<OnePasswordMark />}
        name={t("connector.onePassword.title")}
        status={header().status}
        statusLabel={t(header().label)}
        subtitle={t("connector.onePassword.description")}
        actions={
          <Show when={props.status.state === "disconnected"}>
            <Button type="button" size="sm" loading={props.busy} onClick={() => props.onConnect(null)}>
              {t("connector.onePassword.connect")}
            </Button>
          </Show>
        }
      />

      <Show when={props.status.error}>
        {(message) => (
          <Alert tone="danger" role="alert">
            <AlertIcon>
              <OctagonX />
            </AlertIcon>
            <AlertContent>
              <AlertTitle>{t("connector.onePassword.actionFailed")}</AlertTitle>
              <AlertDescription>{sourceText(message())}</AlertDescription>
            </AlertContent>
          </Alert>
        )}
      </Show>

      <Show when={props.status.state === "disconnected"}>
        <Text variant="body-sm" tone="muted">
          {t("connector.onePassword.howItWorks")}
        </Text>
        <Show
          when={tokenOpen()}
          fallback={
            <Button
              type="button"
              size="sm"
              variant="link"
              class="onepassword-connector-link"
              onClick={() => setTokenOpen(true)}
            >
              {t("connector.onePassword.useToken")}
            </Button>
          }
        >
          <form class="onepassword-connector-token" onSubmit={submitToken}>
            <Input
              type="password"
              size="sm"
              autocomplete="off"
              spellcheck={false}
              aria-label={t("connector.onePassword.tokenLabel")}
              placeholder={t("connector.onePassword.tokenPlaceholder")}
              value={token()}
              onInput={(event) => setToken(event.currentTarget.value)}
            />
            <Button type="submit" size="sm" loading={props.busy} disabled={!token().trim()}>
              {t("connector.onePassword.connectWithToken")}
            </Button>
          </form>
        </Show>
      </Show>

      <Show when={props.status.state === "connecting"}>
        <div class="onepassword-connector-waiting" aria-live="polite">
          <Spinner size="sm" />
          <Text variant="body-sm" tone="muted">
            {t("connector.onePassword.approveInApp")}
          </Text>
          <Button type="button" size="sm" variant="ghost" onClick={props.onCancel}>
            {t("connector.onePassword.cancel")}
          </Button>
        </div>
      </Show>

      <Show when={props.status.state === "choose-account"}>
        <SettingsSection
          title={t("connector.onePassword.chooseAccountTitle")}
          description={t("connector.onePassword.chooseAccountDescription")}
          actions={
            <Button type="button" size="sm" variant="ghost" onClick={props.onCancel}>
              {t("connector.onePassword.cancel")}
            </Button>
          }
        >
          <ItemGroup class="settings-modal-card">
            <For each={props.status.accounts} keyed={(account) => account.id}>
              {(account) => (
                <Item class="settings-modal-row">
                  <ItemContent>
                    <ItemTitle>{account().label}</ItemTitle>
                  </ItemContent>
                  <ItemActions>
                    <Button
                      type="button"
                      size="sm"
                      variant="outline"
                      disabled={props.busy}
                      onClick={() => props.onConnect(account().id)}
                    >
                      {t("connector.onePassword.useAccount")}
                    </Button>
                  </ItemActions>
                </Item>
              )}
            </For>
          </ItemGroup>
        </SettingsSection>
      </Show>

      <Show when={props.status.state === "connected"}>
        <SettingsSection
          title={t("connector.onePassword.vaultTitle")}
          description={t("connector.onePassword.vaultDescription")}
        >
          <ItemGroup class="settings-modal-card">
            <For each={props.status.vaultNames}>
              {(name) => (
                <Item class="settings-modal-row">
                  <ItemContent>
                    <ItemTitle>{name}</ItemTitle>
                  </ItemContent>
                </Item>
              )}
            </For>
            <Item class="settings-modal-row">
              <ItemContent>
                <ItemDescription>
                  {props.status.loginCount === null
                    ? t("connector.onePassword.loginsLoading")
                    : t("connector.onePassword.loginCount", { count: props.status.loginCount })}
                </ItemDescription>
              </ItemContent>
            </Item>
          </ItemGroup>
        </SettingsSection>
        <DangerZone
          title={t("connector.onePassword.disconnectTitle")}
          description={t("connector.onePassword.disconnectSummary")}
          action={t("connector.onePassword.disconnect")}
          busy={props.busy}
          onAction={props.onDisconnect}
        />
      </Show>
    </div>
  );
}
