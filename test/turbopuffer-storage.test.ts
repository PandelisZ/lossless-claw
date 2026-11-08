import { describe, expect, it, vi } from "vitest";
import { DatabaseSync } from "node:sqlite";
import { ContextAssembler } from "../src/assembler.js";
import { runLcmMigrations } from "../src/db/migration.js";
import { ConversationStore } from "../src/store/conversation-store.js";
import { SummaryStore } from "../src/store/summary-store.js";
import {
  createContextStorage,
  StorageConflict,
  TurbopufferContextStorage,
  UnknownCommit,
  type ConversationTables,
} from "../src/storage/context-storage.js";
import {
  TurbopufferTransport,
  type Document,
  type DocumentTransport,
  type Write,
} from "../src/storage/turbopuffer-transport.js";

class MemoryTransport implements DocumentTransport {
  rows = new Map<string, Document>();
  beforePatch?: () => void;
  afterPatch?: () => void;
  matches(row: Document, condition: any): boolean {
    const [name, operator, value] = condition;
    if (name === "And")
      return operator.every((part: unknown) => this.matches(row, part));
    return (row as any)[name] === value;
  }
  reads: string[][] = [];
  writes: Write[] = [];
  async read(ids: string[]) {
    this.reads.push([...ids]);
    return ids.flatMap((id) =>
      this.rows.has(id) ? [structuredClone(this.rows.get(id)!)] : [],
    );
  }
  async write(body: Write) {
    this.writes.push(structuredClone(body));
    if (body.patch_rows?.length) this.beforePatch?.();
    const next = new Map(this.rows);
    let upserted = 0,
      patched = 0;
    for (const row of body.upsert_rows ?? []) {
      if (!next.has(row.id)) {
        next.set(row.id, structuredClone(row));
        upserted++;
      }
    }
    for (const patch of body.patch_rows ?? []) {
      const current = next.get(patch.id!);
      if (current && this.matches(current, body.patch_condition)) {
        next.set(current.id, { ...current, ...patch });
        patched++;
      }
    }
    this.rows = next;
    if (patched) this.afterPatch?.();
    return { upserted, patched };
  }
}
async function fixture() {
  const db = new DatabaseSync(":memory:");
  runLcmMigrations(db, { fts5Available: true });
  const conversations = new ConversationStore(db),
    summaries = new SummaryStore(db);
  const owner = await conversations.createConversation({
    sessionId: "synthetic",
    sessionKey: "agent:agent1:synthetic",
  });
  for (let seq = 0; seq < 16; seq++) {
    const message = await conversations.createMessage({
      conversationId: owner.conversationId,
      seq,
      role: seq % 2 ? "assistant" : "user",
      content: `canary-agent1 quartz ${seq}`,
      tokenCount: 100,
    });
    await summaries.appendContextMessage(
      owner.conversationId,
      message.messageId,
    );
  }
  const data: ConversationTables = {};
  for (const name of ["conversations", "messages", "context_items"])
    data[name] = db.prepare(`SELECT * FROM ${name}`).all() as any;
  return { db, conversations, summaries, data };
}
async function setup() {
  const transport = new MemoryTransport();
  const store = new TurbopufferContextStorage({
    backend: "turbopuffer",
    namespacePrefix: "synthetic-test",
    organizationId: "org1",
    agentId: "agent1",
    conversationId: 1,
    transport,
  });
  const baseline = await fixture();
  await store.initialize();
  await store.importTables(baseline.data);
  return { transport, store, baseline };
}
const assembly = {
  conversationId: 1,
  tokenBudget: 16000,
  freshTailCount: 8,
  freshTailMaxTokens: 65536,
};
const config = {
  contextThreshold: 0.75,
  freshTailCount: 8,
  leafMinFanout: 4,
  condensedMinFanout: 4,
  condensedMinFanoutHard: 2,
  leafChunkTokens: 800,
  leafTargetTokens: 100,
  condensedTargetTokens: 100,
  maxRounds: 1,
  maxSweepIterations: 1,
  summaryMaxOverageFactor: 3,
  timezone: "UTC",
};

describe("experimental remote canonical context storage", () => {
  it("pins an active read while another writer advances the root and a newer read retires its projection", async () => {
    const { store, transport, baseline } = await setup();
    let release!: () => void, entered!: () => void;
    const gate = new Promise<void>((r) => {
        release = r;
      }),
      waiting = new Promise<void>((r) => {
        entered = r;
      });
    const original = SummaryStore.prototype.getContextItems;
    let first = true;
    try {
      const before = await store.assemble(assembly);
      vi.spyOn(SummaryStore.prototype, "getContextItems").mockImplementation(
        async function (this: SummaryStore, id) {
          expect(
            (this as any).db.prepare("PRAGMA query_only").get().query_only,
          ).toBe(1);
          const items = await original.call(this, id);
          if (first) {
            first = false;
            entered();
            await gate;
          }
          return items;
        },
      );
      const active = store.assemble(assembly);
      await waiting;
      const writer = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "synthetic-test",
        organizationId: "org1",
        agentId: "agent1",
        conversationId: 1,
        transport,
      });
      await writer.appendTurn("newer", [
        { role: "user", content: "newer committed canary", tokenCount: 3 },
      ]);
      const after = await store.assemble(assembly);
      expect(JSON.stringify(after.messages)).toContain(
        "newer committed canary",
      );
      release();
      expect(await active).toEqual(before);
      writer.close();
    } finally {
      release();
      vi.restoreAllMocks();
      store.close();
      baseline.db.close();
    }
  });

  it("reuses a query-only committed projection, releases it on close and finishes active reads", async () => {
    const { store, transport, baseline } = await setup();
    try {
      const expected = await store.assemble(assembly);
      const initial = transport.reads.length;
      expect(
        await Promise.all([store.assemble(assembly), store.assemble(assembly)]),
      ).toEqual([expected, expected]);
      expect(
        transport.reads
          .slice(initial)
          .every((ids) => ids.length === 1 && ids[0].startsWith("h:")),
      ).toBe(true);
      const originalRead = transport.read.bind(transport);
      transport.read = async () => {
        throw new Error("strong root unavailable");
      };
      await expect(store.assemble(assembly)).rejects.toThrow(
        "strong root unavailable",
      );
      transport.read = originalRead;
      const active = store.assemble(assembly);
      // Allow the strong head lookup to finish and lease the read projection.
      await Promise.resolve();
      await Promise.resolve();
      store.close();
      expect(await active).toEqual(expected);
      await expect(store.assemble(assembly)).rejects.toThrow("closed");
    } finally {
      store.close();
      baseline.db.close();
    }
  });

  it("uses the provider's root patch count even when losing immutable inserts succeed", async () => {
    const transport = new TurbopufferTransport({
      apiKey: "synthetic-placeholder",
      namespace: "synthetic-count-test",
    });
    transport.request = async () => ({
      rows_affected: 4,
      rows_upserted: 4,
      rows_patched: 0,
    });
    expect(
      await transport.write({ upsert_rows: [], patch_rows: [{ id: "root" }] }),
    ).toEqual({ upserted: 4, patched: 0 });
    transport.request = async () => ({ rows_affected: 4, rows_upserted: 4 });
    expect(await transport.write({ patch_rows: [{ id: "root" }] })).toEqual({
      upserted: 4,
      patched: 0,
    });
    transport.request = async () => ({ rows_affected: 5, rows_upserted: 4 });
    await expect(
      transport.write({ patch_rows: [{ id: "root" }] }),
    ).rejects.toThrow("reconcile outcome");
  });

  it("reuses only verified immutable blocks, checks the strong head every time, and isolates writable projections", async () => {
    const { store, transport, baseline } = await setup();
    try {
      await store.assemble(assembly); // Cold block read.
      const reads = transport.reads.length;
      const first = await store.snapshot(),
        second = await store.snapshot();
      try {
        const nonemptyReads = transport.reads
          .slice(reads)
          .filter((ids) => ids.length);
        expect(nonemptyReads).toHaveLength(2);
        expect(
          nonemptyReads.every(
            (ids) => ids.length === 1 && ids[0].startsWith("h:"),
          ),
        ).toBe(true);
        first.local.db.exec("UPDATE messages SET content='tentative only'");
        expect(await second.local.conversations.getMessageCount(1)).toBe(16);
        expect(
          (await second.assemble(assembly)).messages.some((m) =>
            JSON.stringify(m).includes("tentative only"),
          ),
        ).toBe(false);
      } finally {
        first.close();
        second.close();
      }
      const writer = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "synthetic-test",
        organizationId: "org1",
        agentId: "agent1",
        conversationId: 1,
        transport,
      });
      await writer.appendTurn("new-head", [
        { role: "user", content: "fresh-canary", tokenCount: 3 },
      ]);
      expect(
        (await store.assemble(assembly)).messages.some((m) =>
          JSON.stringify(m).includes("fresh-canary"),
        ),
      ).toBe(true);
      const lastWrite = transport.writes.at(-1)!;
      expect(lastWrite.patch_rows).toHaveLength(1);
      expect(lastWrite.upsert_rows!.some((row) => row.kind === "block")).toBe(
        true,
      );
      expect(lastWrite.upsert_rows!.some((row) => row.kind === "commit")).toBe(
        true,
      );
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("leaves no published state when the atomic batch is rejected before commit", async () => {
    const { store, transport, baseline } = await setup();
    try {
      const before = structuredClone([...transport.rows]);
      transport.beforePatch = () => {
        throw new Error("crash before atomic write");
      };
      await expect(
        store.appendTurn("not-committed", [
          { role: "user", content: "unreachable", tokenCount: 1 },
        ]),
      ).rejects.toBeInstanceOf(UnknownCommit);
      expect([...transport.rows]).toEqual(before);
    } finally {
      store.close();
      baseline.db.close();
    }
  });

  it("returns the committed winner's result to concurrent callers sharing one token", async () => {
    const { store, baseline } = await setup();
    try {
      let arrived = 0;
      let release!: () => void;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const results = await Promise.all(
        ["first-result", "second-result"].map((nonce) =>
          store.mutate("shared-token", { logicalRequest: "same" }, async () => {
            if (++arrived === 2) release();
            await gate;
            return {
              nonce,
              createdAt: new Date(nonce === "first-result" ? 0 : 1000),
            };
          }),
        ),
      );
      expect(results[0]).toEqual(results[1]);
      const repeated = await store.mutate(
        "shared-token",
        { logicalRequest: "same" },
        async () => {
          throw new Error("must not repeat");
        },
      );
      expect(repeated).toEqual(results[0]);
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("rejects foreign auxiliary rows and unsupported owner links before publication", async () => {
    const baseline = await fixture();
    try {
      const cases: ConversationTables[] = [
        { conversation_transcript_cursors: [{ conversation_id: 2 }] },
        { message_parts: [{ message_id: 999999 }] },
        { pending_summary_node_children: [{ node_id: "unproven-owner" }] },
      ];
      for (const extra of cases) {
        const transport = new MemoryTransport();
        const store = new TurbopufferContextStorage({
          backend: "turbopuffer",
          namespacePrefix: "scope-test",
          organizationId: "org1",
          agentId: "agent1",
          conversationId: 1,
          transport,
        });
        await store.initialize();
        await expect(
          store.importTables({
            ...baseline.data,
            ...extra,
          }),
        ).rejects.toThrow(/authority/);
        expect((await store.head()).revision).toBe(0);
        expect(
          [...transport.rows.values()].filter((row) => row.kind === "block"),
        ).toHaveLength(0);
      }
    } finally {
      baseline.db.close();
    }
  });
  it("keeps SQLite the default and assembles the exact same upstream result", async () => {
    const { store, baseline } = await setup();
    try {
      expect(createContextStorage({ db: baseline.db }).backend).toBe("sqlite");
      const expected = await new ContextAssembler(
        baseline.conversations,
        baseline.summaries,
        "UTC",
      ).assemble(assembly);
      expect(await store.assemble(assembly)).toEqual(expected);
      await expect(
        store.assemble({ ...assembly, conversationId: 2 }),
      ).rejects.toThrow("authority");
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("persists upstream leaf compaction graph and context atomically and reopens", async () => {
    const { store, transport, baseline } = await setup();
    try {
      const result = await store.compactLeaf("compact-1", config, {
        conversationId: 1,
        tokenBudget: 2000,
        force: true,
        summarize: async () => "Deterministic summary.",
      });
      expect(result).not.toBeNull();
      const reopened = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "synthetic-test",
        organizationId: "org1",
        agentId: "agent1",
        conversationId: 1,
        transport,
      });
      const snapshot = await reopened.snapshot();
      try {
        const items = await snapshot.local.summaries.getContextItems(1);
        expect(items.some((item) => item.summaryId)).toBe(true);
        expect(
          await snapshot.local.summaries.getSummaryMessages(
            items.find((item) => item.summaryId)!.summaryId!,
          ),
        ).toHaveLength(8);
      } finally {
        snapshot.close();
      }
      expect(
        await reopened.compactLeaf("compact-1", config, {
          conversationId: 1,
          tokenBudget: 2000,
          force: true,
          summarize: async () => {
            throw new Error("must not infer twice");
          },
        }),
      ).toEqual(result);
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("allows one concurrent append, hides losing blocks and preserves Date receipts", async () => {
    const { store, baseline } = await setup();
    try {
      const message = [
        { role: "user" as const, content: "append quartz", tokenCount: 3 },
      ];
      const results = await Promise.allSettled([
        store.appendTurn("a", message),
        store.appendTurn("b", message),
      ]);
      expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
      expect(
        (results.find((r) => r.status === "rejected") as PromiseRejectedResult)
          .reason,
      ).toBeInstanceOf(StorageConflict);
      const winningToken = results[0].status === "fulfilled" ? "a" : "b";
      const first = (
        results.find(
          (r) => r.status === "fulfilled",
        ) as PromiseFulfilledResult<any>
      ).value;
      expect(await store.appendTurn(winningToken, message)).toEqual(first);
      expect(first[0].createdAt).toBeInstanceOf(Date);
      const snapshot = await store.snapshot();
      try {
        expect(await snapshot.local.conversations.getMessageCount(1)).toBe(17);
      } finally {
        snapshot.close();
      }
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("reconciles a lost successful CAS reply and never blindly retries an unchanged root", async () => {
    const { store, transport, baseline } = await setup();
    try {
      transport.afterPatch = () => {
        throw new Error("lost reply after real modeled commit");
      };
      await store.appendTurn("lost", [
        { role: "user", content: "once", tokenCount: 1 },
      ]);
      transport.afterPatch = undefined;
      const head = await store.head();
      transport.beforePatch = () => {
        throw new Error("request might still be in flight");
      };
      await expect(
        store.appendTurn("unknown", [
          { role: "user", content: "unreachable", tokenCount: 1 },
        ]),
      ).rejects.toBeInstanceOf(UnknownCommit);
      expect((await store.head()).commit).toBe(head.commit);
      expect(
        await store.searchMessages({ mode: "full_text", query: "unreachable" }),
      ).toHaveLength(0);
    } finally {
      store.close();
      baseline.db.close();
    }
  });
  it("retains canonical history on inference failure, size overflow, corruption and foreign-agent import", async () => {
    const { store, transport, baseline } = await setup();
    try {
      const before = await store.head();
      await expect(
        store.compactLeaf("fail", config, {
          conversationId: 1,
          tokenBudget: 2000,
          force: true,
          summarize: async () => {
            throw new Error("inference fails");
          },
        }),
      ).rejects.toThrow();
      expect((await store.head()).commit).toBe(before.commit);
      const limited = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "synthetic-test",
        organizationId: "org1",
        agentId: "agent1",
        conversationId: 1,
        transport,
        maxRows: 1,
      });
      await expect(limited.snapshot()).rejects.toThrow("limit");
      transport.rows.get(before.manifest.blocks[0])!.payload += " ";
      // Valid cached immutable bytes remain usable. A fresh client must detect
      // an out-of-protocol overwrite at the provider rather than serving it.
      const cold = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "synthetic-test",
        organizationId: "org1",
        agentId: "agent1",
        conversationId: 1,
        transport,
      });
      await expect(cold.snapshot()).rejects.toThrow("hash");
      const other = new TurbopufferContextStorage({
        backend: "turbopuffer",
        namespacePrefix: "other-test",
        organizationId: "org1",
        agentId: "other",
        conversationId: 1,
        transport: new MemoryTransport(),
      });
      await other.initialize();
      await expect(other.importTables(baseline.data)).rejects.toThrow(
        "agent authority",
      );
    } finally {
      store.close();
      baseline.db.close();
    }
  });
});
