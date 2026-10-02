// @vitest-environment node
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type OnePasswordCli,
  type OnePasswordClient,
  OnePasswordConnectorService,
} from "./onepassword-connector-service";
import { OnePasswordConnectorStore } from "./onepassword-connector-store";

const TOKEN = "ops_service-account-token-for-tests";
const PASSWORD = "vault-only-hunter2";
/** Reverses the text, so the file on disk never holds the token as written. */
const cipher = {
  encrypt: (value: string) => Buffer.from([...value].reverse().join("")),
  decrypt: (value: Buffer) => [...value.toString()].reverse().join(""),
};

let directory: string;

beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), "openbot-onepassword-"));
});

afterEach(async () => {
  await rm(directory, { recursive: true, force: true });
});

function fakeClient(): OnePasswordClient {
  return {
    vaults: { list: async () => [{ id: "vault-1", title: "Shared with OpenBot" }] },
    items: {
      list: async () => [
        {
          id: "login-1",
          title: "GitHub",
          category: "Login",
          websites: [{ url: "https://github.com", autofillBehavior: "AnywhereOnWebsite" }],
        },
      ],
      get: async () => ({
        fields: [
          { id: "username", fieldType: "Text", value: "ada" },
          { id: "password", fieldType: "Concealed", value: PASSWORD },
        ],
      }),
    },
  };
}

function fakeCli(accounts: Array<{ account_uuid: string; email: string; url: string }>) {
  return vi.fn<OnePasswordCli>(async (args) => {
    const command = args.slice(0, 2).join(" ");
    if (args[0] === "--version") return "2.30.0\n";
    if (command === "account list") return JSON.stringify(accounts);
    if (command === "vault list") return "[]";
    if (command === "vault create") return JSON.stringify({ id: "vault-1", name: "Shared with OpenBot" });
    if (command === "service-account create") return `${TOKEN}\n`;
    throw new Error(`Unexpected command: ${args.join(" ")}`);
  });
}

function service(
  cli: OnePasswordCli,
  createClient: (token: string) => Promise<OnePasswordClient> = async () => fakeClient(),
) {
  const path = join(directory, "connector.json");
  return {
    path,
    connector: new OnePasswordConnectorService({
      store: new OnePasswordConnectorStore(path, cipher),
      hostName: "test-mac",
      appVersion: "1.0.0",
      cli,
      createClient,
    }),
  };
}

describe("OnePasswordConnectorService", () => {
  it("creates a read-only service account for the shared vault and keeps only its token", async () => {
    const cli = fakeCli([{ account_uuid: "account-1", email: "ada@example.com", url: "my.1password.com" }]);
    const { connector, path } = service(cli);

    const status = await connector.connect({ accountId: null });

    expect(status).toMatchObject({ state: "connected", vaultNames: ["Shared with OpenBot"], loginCount: 1 });
    expect(JSON.stringify(status)).not.toContain(TOKEN);
    const create = cli.mock.calls.find(([args]) => args[0] === "service-account")?.[0];
    expect(create).toEqual(expect.arrayContaining(["--vault", "vault-1:read_items", "--account", "account-1"]));
    expect(await readFile(path, "utf8")).not.toContain(TOKEN);
  });

  it("asks for an account before it creates anything when the CLI has several", async () => {
    const cli = fakeCli([
      { account_uuid: "account-1", email: "ada@example.com", url: "my.1password.com" },
      { account_uuid: "account-2", email: "ada@example.org", url: "example.1password.com" },
    ]);
    const { connector } = service(cli);

    const status = await connector.connect({ accountId: null });

    expect(status.state).toBe("choose-account");
    expect(status.accounts.map((account) => account.id)).toEqual(["account-1", "account-2"]);
    expect(cli.mock.calls.some(([args]) => args[0] === "vault" || args[0] === "service-account")).toBe(false);
  });

  it("gives a password only for a site that the login is saved for", async () => {
    const { connector } = service(fakeCli([]));
    await connector.connectWithToken(TOKEN);

    expect(await connector.secretFor("login-1", "https://evilgithub.com", "password")).toBeNull();
    expect(await connector.secretFor("login-1", "https://github.com", "password")).toBe(PASSWORD);
    expect(await connector.loginsFor("https://github.com")).toEqual([
      { id: "login-1", title: "GitHub", username: "ada", hasOneTimePassword: false },
    ]);
  });

  it("stores nothing when 1Password refuses the token", async () => {
    const { connector, path } = service(fakeCli([]), async () => {
      throw new Error("invalid token");
    });

    const status = await connector.connectWithToken(TOKEN);

    expect(status.state).toBe("disconnected");
    expect(status.error).toBe("1Password did not accept the service account token.");
    expect(await connector.loginsFor("https://github.com")).toBeNull();
    await expect(readFile(path, "utf8")).rejects.toThrow();
  });
});
