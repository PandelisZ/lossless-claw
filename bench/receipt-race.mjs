// One real-provider regression probe for same-token callers producing different
// local results. Credentials arrive from an audited helper, only in memory.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { parseArgs } from "node:util";
import { writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import path from "node:path";
import { TurbopufferContextStorage } from "../dist/experimental/context-storage.js";

const { values } = parseArgs({
  options: { "key-helper": { type: "string" }, output: { type: "string" } },
});
if (process.env.LCM_TURBOPUFFER_EXPERIMENT !== "1" || !values.output)
  throw new Error("Explicit opt-in and output required");
const key = values["key-helper"]
  ? await (
      await import(pathToFileURL(path.resolve(values["key-helper"])).href)
    ).honchoKey()
  : process.env.TURBOPUFFER_API_KEY;
const prefix = `lcm-fork-exp-${randomUUID()}`;
const store = new TurbopufferContextStorage({
  backend: "turbopuffer",
  namespacePrefix: prefix,
  organizationId: "synthetic-org",
  agentId: "synthetic-agent",
  conversationId: 1,
  apiKey: key,
});
const evidence = {
  timestamp: new Date().toISOString(),
  namespace: store.namespace,
};
try {
  await store.initialize();
  await store.importTables({
    conversations: [
      {
        conversation_id: 1,
        session_id: "synthetic",
        session_key: "agent:synthetic-agent:synthetic",
      },
    ],
  });
  let arrived = 0,
    release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  const start = performance.now();
  const replies = await Promise.all(
    ["first-result", "second-result"].map((nonce) =>
      store.mutate("one-request", { command: "same" }, async () => {
        if (++arrived === 2) release();
        await gate;
        return { nonce, date: new Date(nonce === "first-result" ? 0 : 1000) };
      }),
    ),
  );
  evidence.ms = performance.now() - start;
  assert.deepEqual(replies[0], replies[1]);
  assert.equal((await store.head()).revision, 2);
  const replay = await store.mutate(
    "one-request",
    { command: "same" },
    async () => {
      throw new Error("must not rerun");
    },
  );
  assert.deepEqual(replay, replies[0]);
  evidence.replies = replies;
  evidence.completed = true;
} finally {
  assert.ok(store.namespace.startsWith(prefix + "-"));
  try {
    await store.transport.request("DELETE", "");
    evidence.deleted = true;
  } catch {
    evidence.deleted = false;
    process.exitCode = 1;
  }
  evidence.requests = store.transport.metrics;
  await writeFile(values.output, JSON.stringify(evidence, null, 2) + "\n");
}
