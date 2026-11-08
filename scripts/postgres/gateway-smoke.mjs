// A real local OpenClaw gateway, synthetic model HTTP server and disposable PG only.
import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";
import { build } from "esbuild";
if (process.env.LCM_DISPOSABLE_POSTGRES !== "1")
  throw new Error("Requires an explicitly disposable PostgreSQL server");
const root = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../.."
);
const generated = path.join(root, "experiments/postgres/.generated/native");
await mkdir(generated, { recursive: true });
await build({
  entryPoints: [path.join(root, "index.ts")],
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node22",
  external: ["pg", "openclaw", "openclaw/*", "@earendil-works/*"],
  minifyWhitespace: true,
  outfile: path.join(root, "dist/index.js"),
});
await build({
  stdin: {
    contents: `export {PostgresDatabase,provisionPostgres} from './src/db/postgres/database.ts';export {SummaryStore} from './src/store/summary-store.ts';`,
    resolveDir: root,
  },
  bundle: true,
  platform: "node",
  format: "esm",
  external: ["pg"],
  outfile: path.join(generated, "store.mjs"),
});
const { PostgresDatabase, provisionPostgres, SummaryStore } = await import(
  path.join(generated, "store.mjs")
);
const directory = await mkdtemp(path.join(tmpdir(), "lcm-native-gateway-"));
const schema = `lcm_gateway_${randomBytes(6).toString("hex")}`;
const identity = {
  schema,
  organizationId: "synthetic-gateway-org",
  agentId: "one",
};
await provisionPostgres(identity);
const secondIdentity = { ...identity, schema: schema + "_two", agentId: "two" };
await provisionPostgres(secondIdentity);
let inferenceCalls = 0;
const summaryMarker = "LCM_PG_ONLY_SUMMARY_SEED20261104";
const observedSummary = [];
const model = createServer(async (req, res) => {
  let body = "";
  for await (const chunk of req) body += chunk;
  const input = JSON.parse(body || "{}");
  inferenceCalls++;
  observedSummary.push(
    JSON.stringify(input.messages ?? []).includes(summaryMarker)
  );
  const text = `Deterministic gateway reply ${inferenceCalls}.`;
  if (input.stream) {
    res.writeHead(200, { "content-type": "text/event-stream" });
    const common = {
      id: `synthetic-${inferenceCalls}`,
      object: "chat.completion.chunk",
      created: 1,
      model: "synthetic",
    };
    res.write(
      `data: ${JSON.stringify({
        ...common,
        choices: [
          {
            index: 0,
            delta: { role: "assistant", content: text },
            finish_reason: null,
          },
        ],
      })}\n\n`
    );
    res.write(
      `data: ${JSON.stringify({
        ...common,
        choices: [{ index: 0, delta: {}, finish_reason: "stop" }],
        usage: { prompt_tokens: 10, completion_tokens: 10, total_tokens: 20 },
      })}\n\n`
    );
    res.end("data: [DONE]\n\n");
  } else {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        id: "synthetic",
        object: "chat.completion",
        model: "synthetic",
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: text },
            finish_reason: "stop",
          },
        ],
        usage: { prompt_tokens: 10, completion_tokens: 10, total_tokens: 20 },
      })
    );
  }
});
await new Promise((resolve) => model.listen(0, "127.0.0.1", resolve));
const reserve = createServer();
await new Promise((resolve) => reserve.listen(0, "127.0.0.1", resolve));
const port = reserve.address().port;
await new Promise((resolve) => reserve.close(resolve));
const modelPort = model.address().port;
const workspace = path.join(directory, "workspace");
await mkdir(workspace);
const config = {
  gateway: {
    mode: "local",
    port,
    bind: "loopback",
    auth: { mode: "none" },
    controlUi: { enabled: false },
  },
  browser: { enabled: false },
  agents: {
    defaults: { workspace, model: { primary: "lcmsmoke/synthetic" } },
    list: [
      { id: "one", default: true, workspace },
      { id: "two", workspace },
    ],
  },
  models: {
    providers: {
      lcmsmoke: {
        baseUrl: `http://127.0.0.1:${modelPort}/v1`,
        api: "openai-completions",
        apiKey: "${LCM_SMOKE_MODEL_KEY}",
        models: [
          {
            id: "synthetic",
            name: "Synthetic test only",
            reasoning: false,
            input: ["text"],
            contextWindow: 8192,
            maxTokens: 1024,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
          },
        ],
      },
    },
  },
  plugins: {
    allow: ["lossless-claw"],
    load: { paths: [root] },
    slots: { contextEngine: "lossless-claw" },
    entries: {
      "lossless-claw": {
        enabled: true,
        hooks: { allowConversationAccess: true },
        config: {
          storageBackend: "postgres",
          postgresSchemas: { one: schema, two: secondIdentity.schema },
          postgresOrganizationId: identity.organizationId,
          largeFilesDir: path.join(directory, "lcm-files"),
          summaryProvider: "lcmsmoke",
          summaryModel: "synthetic",
        },
      },
    },
  },
};
const configPath = path.join(directory, "openclaw.json");
await writeFile(configPath, JSON.stringify(config, null, 2), { mode: 0o600 });
const modelKey = randomBytes(24).toString("hex");
const environment = {
  ...process.env,
  OPENCLAW_STATE_DIR: directory,
  OPENCLAW_CONFIG_PATH: configPath,
  LCM_SMOKE_MODEL_KEY: modelKey,
};
const binary = path.join(root, "node_modules/openclaw/openclaw.mjs");
const run = (args, timeout = 45000) =>
  new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [binary, ...args], {
      env: environment,
      cwd: root,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (c) => (output += c));
    child.stderr.on("data", (c) => (output += c));
    const timer = setTimeout(() => {
      child.kill("SIGTERM");
      reject(new Error("Disposable OpenClaw command timeout"));
    }, timeout);
    child.on("error", reject);
    child.on("exit", (code) => {
      clearTimeout(timer);
      code === 0
        ? resolve(output)
        : reject(new Error(output.split(modelKey).join("[synthetic-key]")));
    });
  });
let gateway,
  db,
  secondDb,
  logs = "";
try {
  gateway = spawn(
    process.execPath,
    [
      binary,
      "gateway",
      "run",
      "--auth",
      "none",
      "--bind",
      "loopback",
      "--port",
      String(port),
    ],
    { env: environment, cwd: root, stdio: ["ignore", "pipe", "pipe"] }
  );
  gateway.stdout.on("data", (c) => (logs += c));
  gateway.stderr.on("data", (c) => (logs += c));
  let ready = false;
  for (let i = 0; i < 100; i++) {
    if (gateway.exitCode !== null)
      throw new Error(logs.split(modelKey).join("[synthetic-key]"));
    try {
      const response = await fetch(`http://127.0.0.1:${port}/health`);
      if (response.ok) {
        ready = true;
        break;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  if (!ready)
    throw new Error(
      "Disposable gateway did not become healthy: " +
        logs.split(modelKey).join("[synthetic-key]")
    );
  db = await PostgresDatabase.open(identity);
  for (let turn = 1; turn <= 2; turn++) {
    const result = await run([
      "gateway",
      "call",
      "agent",
      "--port",
      String(port),
      "--params",
      JSON.stringify({
        sessionKey: "agent:one:postgres-smoke",
        message: `SYNTHETIC-PG-GATEWAY-TURN-${turn}: preserve this deterministic canary.`,
        deliver: false,
        idempotencyKey: `synthetic-pg-gateway-turn-${turn}`,
      }),
      "--expect-final",
      "--timeout",
      "30000",
      "--json",
    ]);
    if (!result.includes("Deterministic gateway reply"))
      throw new Error(
        "Gateway did not return synthetic model reply: " + result
      );
    if (turn === 1) {
      const conversation = await db
        .prepare(
          "SELECT conversation_id FROM conversations WHERE session_key=?"
        )
        .get("agent:one:postgres-smoke");
      const source = await db
        .prepare(
          "SELECT message_id FROM messages WHERE conversation_id=? ORDER BY seq"
        )
        .all(conversation.conversation_id);
      const summaries = new SummaryStore(db);
      // Deterministic synthetic compaction output, committed through the real native stores.
      // Native OpenClaw transcript history is left intact. The next model request must use LCM's PG-only projection.
      await summaries.withTransaction(async () => {
        await summaries.insertSummary({
          summaryId: "synthetic-gateway-leaf",
          conversationId: conversation.conversation_id,
          kind: "leaf",
          content:
            summaryMarker +
            ": retained SYNTHETIC-PG-GATEWAY-TURN-1 and its deterministic reply.",
          tokenCount: 24,
          model: "deterministic-inference-fixture",
        });
        await summaries.linkSummaryToMessages(
          "synthetic-gateway-leaf",
          source.map((row) => row.message_id)
        );
        await summaries.replaceContextRangeWithSummary({
          conversationId: conversation.conversation_id,
          startOrdinal: 0,
          endOrdinal: 1,
          summaryId: "synthetic-gateway-leaf",
        });
      });
    }
  }
  if (!observedSummary[1])
    throw new Error(
      "Second gateway model request did not use PostgreSQL-only LCM summary projection"
    );
  const secondResult = await run([
    "gateway",
    "call",
    "agent",
    "--port",
    String(port),
    "--params",
    JSON.stringify({
      sessionKey: "agent:two:postgres-smoke",
      message: "SYNTHETIC-AGENT-TWO-ISOLATION-CANARY",
      deliver: false,
      idempotencyKey: "synthetic-agent-two-turn-1",
    }),
    "--expect-final",
    "--timeout",
    "30000",
    "--json",
  ]);
  if (
    !secondResult.includes("Deterministic gateway reply") ||
    observedSummary[2] !== false
  )
    throw new Error(
      "Second agent model request crossed the PostgreSQL schema boundary"
    );
  secondDb = await PostgresDatabase.open(secondIdentity);
  const secondMessages = await secondDb
    .prepare("SELECT COUNT(*) AS count FROM messages")
    .get();
  const secondReceipts = await secondDb
    .prepare("SELECT COUNT(*) AS count FROM turn_advancements")
    .get();
  const secondCursors = await secondDb
    .prepare("SELECT COUNT(*) AS count FROM conversation_transcript_cursors")
    .get();
  if (
    secondMessages.count !== 2 ||
    secondReceipts.count !== 1 ||
    secondCursors.count !== 1
  )
    throw new Error(
      "Second agent did not persist independent canonical turn state"
    );
  const rows = await db
    .prepare(
      "SELECT m.content,m.role FROM messages m JOIN conversations c USING(conversation_id) WHERE c.session_key=? ORDER BY m.seq"
    )
    .all("agent:one:postgres-smoke");
  const cursors = await db
    .prepare("SELECT COUNT(*) AS count FROM conversation_transcript_cursors")
    .get();
  const receipts = await db
    .prepare("SELECT COUNT(*) AS count FROM turn_advancements")
    .get();
  if (
    rows.filter((r) => r.role === "user").length !== 2 ||
    rows.filter((r) => r.role === "assistant").length !== 2 ||
    cursors.count !== 1 ||
    receipts.count !== 2
  )
    throw new Error(
      `Native gateway did not persist full LCM turn state: messages=${
        rows.length
      } cursors=${cursors.count} receipts=${receipts.count}; logs=${logs
        .split(modelKey)
        .join("[synthetic-key]")}`
    );
  const evidence = {
    environment:
      "real local OpenClaw gateway with synthetic HTTP model and disposable local PostgreSQL",
    openclawVersion: "2026.9.2",
    storeSchema: schema,
    secondAgent: {
      schema: secondIdentity.schema,
      messages: secondMessages.count,
      receipts: secondReceipts.count,
      cursors: secondCursors.count,
      foreignSummaryObserved: observedSummary[2],
    },
    transport:
      "gateway WebSocket agent RPC with expect-final; no embedded fallback",
    pgOnlySummaryObserved: observedSummary[1],
    inferenceCalls,
    messages: rows.length,
    cursors: cursors.count,
    receipts: receipts.count,
    production: false,
  };
  await writeFile(
    path.join(
      generated,
      process.env.LCM_POSTGRES_VIA_POOLER === "1"
        ? "gateway-smoke-pooler.json"
        : "gateway-smoke-direct.json"
    ),
    JSON.stringify(evidence, null, 2) + "\n"
  );
  console.log(JSON.stringify(evidence));
} catch (error) {
  console.error(String(error).split(modelKey).join("[synthetic-key]"));
  process.exitCode = 1;
} finally {
  await db?.close();
  await secondDb?.close();
  if (gateway && gateway.exitCode === null) {
    gateway.kill("SIGTERM");
    await Promise.race([
      new Promise((resolve) => gateway.once("exit", resolve)),
      new Promise((resolve) => setTimeout(resolve, 5000)),
    ]);
    if (gateway.exitCode === null) gateway.kill("SIGKILL");
  }
  await new Promise((resolve) => model.close(resolve));
  await rm(directory, { recursive: true, force: true });
}
