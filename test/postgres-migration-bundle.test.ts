const pathRace = vi.hoisted(() => ({
  beforeOpen: undefined as undefined | ((target: unknown) => Promise<void>),
}));
vi.mock("node:fs/promises", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:fs/promises")>();
  return {
    ...actual,
    open: async (...args: Parameters<typeof actual.open>) => {
      await pathRace.beforeOpen?.(args[0]);
      return actual.open(...args);
    },
  };
});
import { afterEach, describe, expect, it, vi } from "vitest";
import { DatabaseSync } from "node:sqlite";
import {
  mkdtemp,
  mkdir,
  readFile,
  writeFile,
  rm,
  symlink,
  rename,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { runLcmMigrations } from "../src/db/migration.js";
import { ConversationStore } from "../src/store/conversation-store.js";
import { SummaryStore } from "../src/store/summary-store.js";
import {
  exportSqlite,
  exportHash,
  writeSqliteExport,
} from "../src/db/postgres/transfer.js";
import { partitionSqliteExport } from "../src/db/postgres/partition.js";
import {
  createMigrationBundle,
  verifyMigrationBundle,
} from "../src/db/postgres/bundle.js";
import { runPostgresCli } from "../src/cli/postgres.js";

const directories: string[] = [];
afterEach(async () => {
  for (const dir of directories.splice(0))
    await rm(dir, { recursive: true, force: true });
});
async function fixture() {
  const directory = await mkdtemp(path.join(tmpdir(), "lcm-migration-"));
  directories.push(directory);
  const payloadRoot = path.join(directory, "source-files");
  await mkdir(payloadRoot);
  const db = new DatabaseSync(":memory:");
  runLcmMigrations(db);
  const conversations = new ConversationStore(db),
    summaries = new SummaryStore(db);
  for (const agent of ["one", "two"]) {
    const c = await conversations.createConversation({
      sessionId: `${agent}-session`,
      sessionKey: `agent:${agent}:thread`,
    });
    const messages = await conversations.createMessagesBulk(
      Array.from({ length: 12 }, (_, i) => ({
        conversationId: c.conversationId,
        seq: i,
        role: i % 2 ? ("assistant" as const) : ("user" as const),
        content: `SYNTHETIC-ONLY-${agent}-CANARY-${i}`,
        tokenCount: 4,
      }))
    );
    await summaries.appendContextMessages(
      c.conversationId,
      messages.map((m) => m.messageId)
    );
    await summaries.insertSummary({
      summaryId: `leaf-${agent}`,
      conversationId: c.conversationId,
      kind: "leaf",
      content: `SYNTHETIC-${agent} summary`,
      tokenCount: 4,
    });
    await summaries.linkSummaryToMessages(
      `leaf-${agent}`,
      messages.slice(0, 8).map((m) => m.messageId)
    );
    await summaries.replaceContextRangeWithSummary({
      conversationId: c.conversationId,
      startOrdinal: 0,
      endOrdinal: 7,
      summaryId: `leaf-${agent}`,
    });
    const content = Buffer.from(
        `SYNTHETIC-PAYLOAD-${agent}-` + "x".repeat(400000)
      ),
      file = path.join(payloadRoot, `${agent}.txt`);
    await writeFile(file, content);
    await summaries.insertLargeFile({
      fileId: `file_${agent}`,
      conversationId: c.conversationId,
      fileName: `${agent}.txt`,
      mimeType: "text/plain",
      byteSize: content.length,
      lineCount: 1,
      storageUri: file,
      explorationSummary: `${agent} payload`,
    });
  }
  const data = exportSqlite(db, { requireKnownTables: true });
  db.close();
  return { directory, payloadRoot, data };
}
describe("quiesced synthetic migration preparation", () => {
  it("partitions every logical row without mutation and copies/verifies byte-identical payloads", async () => {
    const f = await fixture(),
      before = exportHash(f.data),
      parts = partitionSqliteExport(f.data);
    expect([...parts.keys()]).toEqual(["one", "two"]);
    for (const [agent, data] of parts) {
      expect(JSON.stringify(data)).toContain(`SYNTHETIC-ONLY-${agent}-CANARY`);
      expect(JSON.stringify(data)).not.toContain(
        `SYNTHETIC-ONLY-${agent === "one" ? "two" : "one"}-CANARY`
      );
    }
    const destination = path.join(f.directory, "bundle");
    const prepared = await createMigrationBundle({
      ...f,
      organizationId: "org-one",
      sourcePayloadRoot: f.payloadRoot,
      destination,
    });
    const verified = await verifyMigrationBundle(
      destination,
      prepared.manifestHash,
      "org-one"
    );
    expect(verified.agents.size).toBe(2);
    expect(exportHash(f.data)).toBe(before);
    for (const agent of verified.manifest.agents) {
      expect(agent.files[0].bytes).toBeGreaterThan(256 * 1024);
      expect(
        await readFile(path.join(destination, agent.files[0].relativePath))
      ).toEqual(
        await readFile(path.join(f.payloadRoot, `${agent.agentId}.txt`))
      );
      const sqliteFile = path.join(f.directory, `${agent.agentId}-rollback.db`);
      writeSqliteExport(verified.agents.get(agent.agentId)!, sqliteFile);
      const sqlite = new DatabaseSync(sqliteFile, { readOnly: true });
      try {
        expect(exportHash(exportSqlite(sqlite))).toBe(agent.exportHash);
        expect(sqlite.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
      } finally {
        sqlite.close();
      }
      expect(() =>
        writeSqliteExport(verified.agents.get(agent.agentId)!, sqliteFile)
      ).toThrow();
    }
    await expect(
      verifyMigrationBundle(destination, prepared.manifestHash, "org-two")
    ).rejects.toThrow(/identity/);
    await expect(
      createMigrationBundle({
        ...f,
        organizationId: "org-one",
        sourcePayloadRoot: f.payloadRoot,
        destination,
      })
    ).rejects.toThrow();
  });
  it("rejects cross-agent graph/file edges, legacy authority, unknown tables and NUL before publication", async () => {
    const f = await fixture();
    const invalid = structuredClone(f.data);
    invalid.tables.find(
      (t) => t.name === "summary_messages"
    )!.rows[0].message_id = invalid.tables
      .find((t) => t.name === "messages")!
      .rows.find((r) => r.conversation_id === 2)!.message_id;
    expect(() => partitionSqliteExport(invalid)).toThrow(/cross-agent/);
    const missing = structuredClone(f.data);
    missing.tables.find(
      (t) => t.name === "conversations"
    )!.rows[0].session_key = null;
    expect(() => partitionSqliteExport(missing)).toThrow(/canonical/);
    const nul = structuredClone(f.data);
    nul.tables.find((t) => t.name === "messages")!.rows[0].content =
      "bad\0text";
    expect(() => partitionSqliteExport(nul)).toThrow(/NUL/);
    const future = new DatabaseSync(":memory:");
    runLcmMigrations(future);
    future.exec("CREATE TABLE future_retained_data(value TEXT)");
    try {
      expect(() => exportSqlite(future, { requireKnownTables: true })).toThrow(
        /Unrecognized/
      );
    } finally {
      future.close();
    }
    const fileEdge = structuredClone(f.data);
    fileEdge.tables.find((t) => t.name === "summaries")!.rows[0].file_ids =
      '["file_two"]';
    expect(() => partitionSqliteExport(fileEdge)).toThrow(/file authority/);
  });
  it("rejects escaped/symlink/missing/wrong-size payloads and never marks an incomplete bundle complete", async () => {
    const f = await fixture();
    const file = f.data.tables.find((t) => t.name === "large_files")!.rows[0];
    const outside = path.join(f.directory, "outside.txt");
    await writeFile(outside, "must not be copied");
    const original = file.storage_uri;
    file.storage_uri = outside;
    await expect(
      createMigrationBundle({
        ...f,
        organizationId: "org",
        sourcePayloadRoot: f.payloadRoot,
        destination: path.join(f.directory, "escape"),
      })
    ).rejects.toThrow(/outside/);
    await symlink(outside, path.join(f.payloadRoot, "link"));
    file.storage_uri = path.join(f.payloadRoot, "link");
    await expect(
      createMigrationBundle({
        ...f,
        organizationId: "org",
        sourcePayloadRoot: f.payloadRoot,
        destination: path.join(f.directory, "symlink"),
      })
    ).rejects.toThrow(/Symlink/);
    file.storage_uri = original;
    file.byte_size = 1;
    const target = path.join(f.directory, "wrong-size");
    await expect(
      createMigrationBundle({
        ...f,
        organizationId: "org",
        sourcePayloadRoot: f.payloadRoot,
        destination: target,
      })
    ).rejects.toThrow(/size/);
    await expect(
      readFile(path.join(target, "manifest.json"))
    ).rejects.toMatchObject({ code: "ENOENT" });
  });
  it("detects modified payload/export/manifest and refuses all unguarded operator commands", async () => {
    const f = await fixture(),
      destination = path.join(f.directory, "bundle");
    const prepared = await createMigrationBundle({
      ...f,
      organizationId: "org",
      sourcePayloadRoot: f.payloadRoot,
      destination,
    });
    const verified = await verifyMigrationBundle(
      destination,
      prepared.manifestHash,
      "org"
    );
    await writeFile(
      path.join(destination, verified.manifest.agents[0].files[0].relativePath),
      "changed"
    );
    await expect(
      verifyMigrationBundle(destination, prepared.manifestHash, "org")
    ).rejects.toThrow(/hash/);
    await expect(
      verifyMigrationBundle(destination, "0".repeat(64), "org")
    ).rejects.toThrow(/manifest hash/);
    await expect(runPostgresCli(["init"], {})).rejects.toThrow(/disposable/);
    expect(await runPostgresCli(["--help"], {})).toHaveProperty("usage");
  });
  it("preserves canonical grant authority and stale bindings while rejecting conflicting or ambiguous issuers", async () => {
    const f = await fixture();
    const grants = f.data.tables.find(
        (t) => t.name === "lcm_expansion_grants"
      )!,
      bindings = f.data.tables.find(
        (t) => t.name === "lcm_expansion_grant_bindings"
      )!;
    grants.rows.push({
      grant_id: "synthetic-grant",
      issuer_session_id: "agent:one:thread",
      allowed_conversation_ids: "[1]",
      allowed_summary_ids: '["leaf-one"]',
      max_depth: 2,
      token_cap: 100,
      consumed_tokens: 0,
      expires_at_ms: 100,
      revoked: 0,
      created_at_ms: 1,
    });
    bindings.rows.push({
      session_key: "agent:one:expired-child",
      grant_id: "already-cleaned-up",
    });
    const parts = partitionSqliteExport(f.data);
    expect(
      parts.get("one")!.tables.find((t) => t.name === "lcm_expansion_grants")!
        .rows
    ).toHaveLength(1);
    expect(
      parts
        .get("one")!
        .tables.find((t) => t.name === "lcm_expansion_grant_bindings")!.rows
    ).toEqual(bindings.rows);
    grants.rows[0].issuer_session_id = "agent:two:thread";
    expect(() => partitionSqliteExport(f.data)).toThrow(/cross-agent/);
    grants.rows[0].issuer_session_id = "legacy-overlap";
    for (const row of f.data.tables.find((t) => t.name === "conversations")!
      .rows)
      row.session_id = "legacy-overlap";
    expect(() => partitionSqliteExport(f.data)).toThrow(/ambiguous/);
  });
  it.skipIf(process.platform !== "linux")(
    "rejects an ancestor swapped for an outside symlink between lstat and open",
    async () => {
      const f = await fixture(),
        outside = path.join(f.directory, "outside"),
        moved = path.join(f.directory, "original-files");
      await mkdir(outside);
      await writeFile(
        path.join(outside, "one.txt"),
        "OUTSIDE-CONTENT-MUST-NOT-BE-READ"
      );
      pathRace.beforeOpen = async (target) => {
        if (target === path.join(f.payloadRoot, "one.txt")) {
          pathRace.beforeOpen = undefined;
          await rename(f.payloadRoot, moved);
          await symlink(outside, f.payloadRoot);
        }
      };
      try {
        await expect(
          createMigrationBundle({
            data: f.data,
            organizationId: "org",
            sourcePayloadRoot: f.payloadRoot,
            destination: path.join(f.directory, "raced-bundle"),
          })
        ).rejects.toThrow(/escaped/);
      } finally {
        pathRace.beforeOpen = undefined;
      }
      await expect(
        readFile(path.join(f.directory, "raced-bundle/manifest.json"))
      ).rejects.toHaveProperty("code", "ENOENT");
    }
  );
  it("preserves deleted-highest AUTOINCREMENT allocations across offline rollback and every agent partition", async () => {
    const f = await fixture(),
      sqlite = new DatabaseSync(":memory:");
    runLcmMigrations(sqlite);
    sqlite
      .prepare(
        "INSERT INTO conversations(conversation_id,session_id,session_key) VALUES(100,?,?)"
      )
      .run("gone", "agent:one:gone");
    sqlite.exec("DELETE FROM conversations WHERE conversation_id=100");
    sqlite
      .prepare(
        "INSERT INTO conversations(conversation_id,session_id,session_key) VALUES(1,?,?)"
      )
      .run("kept", "agent:one:kept");
    sqlite
      .prepare(
        "INSERT INTO messages(message_id,conversation_id,seq,role,content,token_count) VALUES(500,1,1,'user','synthetic deleted highest',1)"
      )
      .run();
    sqlite.exec("DELETE FROM messages WHERE message_id=500");
    const data = exportSqlite(sqlite);
    sqlite.close();
    expect(data.sequences).toEqual({ conversations: 100, messages: 500 });
    expect(partitionSqliteExport(data).get("one")!.sequences).toEqual(
      data.sequences
    );
    const target = path.join(f.directory, "highwater.db");
    writeSqliteExport(data, target);
    const restored = new DatabaseSync(target);
    try {
      expect(
        restored
          .prepare(
            "INSERT INTO messages(conversation_id,seq,role,content,token_count) VALUES(1,1,'user','synthetic next',1)"
          )
          .run().lastInsertRowid
      ).toBe(501);
      expect(
        restored
          .prepare(
            "INSERT INTO conversations(session_id,session_key) VALUES('next','agent:one:next')"
          )
          .run().lastInsertRowid
      ).toBe(101);
    } finally {
      restored.close();
    }
  });
  it("materializes missing optional FTS caches from canonical rows without mutating a source built without FTS5", async () => {
    const db = new DatabaseSync(":memory:");
    runLcmMigrations(db, { fts5Available: false });
    const store = new ConversationStore(db, { fts5Available: false }),
      summary = new SummaryStore(db, { fts5Available: false });
    const c = await store.createConversation({
      sessionId: "without-fts",
      sessionKey: "agent:one:without-fts",
    });
    const message = await store.createMessage({
      conversationId: c.conversationId,
      seq: 1,
      role: "user",
      content: "SYNTHETIC optional FTS history",
      tokenCount: 4,
    });
    await summary.insertSummary({
      summaryId: "summary-without-fts",
      conversationId: c.conversationId,
      kind: "leaf",
      content: "SYNTHETIC summary",
      tokenCount: 4,
    });
    try {
      const exported = exportSqlite(db);
      expect(
        exported.tables.find((t) => t.name === "messages_fts")!.rows
      ).toEqual([
        { rowid: message.messageId, content: "SYNTHETIC optional FTS history" },
      ]);
      expect(
        exported.tables.find((t) => t.name === "summaries_fts")!.rows
      ).toHaveLength(1);
      expect(
        db
          .prepare("SELECT name FROM sqlite_master WHERE name='messages_fts'")
          .get()
      ).toBeUndefined();
    } finally {
      db.close();
    }
  });
});
