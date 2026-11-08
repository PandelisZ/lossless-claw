import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it, vi } from "vitest";

vi.mock("node:fs", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs")>();
  return {
    ...actual,
    existsSync: (path: Parameters<typeof actual.existsSync>[0]) =>
      String(path).endsWith("openclaw.plugin.json") ? false : actual.existsSync(path),
  };
});

import { runCli } from "../src/cli/main.js";

const directory = mkdtempSync(join(tmpdir(), "lcm-cli-startup-"));
const configPath = join(directory, "openclaw.json");

afterAll(() => {
  rmSync(directory, { recursive: true, force: true });
});

async function invoke(args: string[]): Promise<{ exitCode: number; stdout: string; stderr: string }> {
  let stdout = "";
  let stderr = "";
  const exitCode = (await runCli(args, {
    env: { HOME: directory, OPENCLAW_CONFIG_PATH: configPath },
    stdout: (text) => { stdout += text; },
    stderr: (text) => { stderr += text; },
  }));
  return { exitCode, stdout, stderr };
}

describe("CLI startup without a packaged manifest", () => {
  it("keeps commands that do not write config available", async () => {
    const result = (await invoke(["--help"]));

    expect(result).toMatchObject({ exitCode: 0, stderr: "" });
    expect(JSON.parse(result.stdout)).toMatchObject({ ok: true, command: "help" });
  });

  it("reports config schema discovery failures as structured errors", async () => {
    writeFileSync(configPath, "{}\n", { mode: 0o600 });

    const result = (await invoke(["config", "set", "freshTailCount", "20"]));

    expect(result).toMatchObject({ exitCode: 4, stdout: "" });
    expect(JSON.parse(result.stderr)).toEqual({
      ok: false,
      error: {
        code: "CONFIG_SCHEMA_NOT_FOUND",
        message: "Could not locate the packaged Lossless plugin manifest.",
      },
    });
  });
});
