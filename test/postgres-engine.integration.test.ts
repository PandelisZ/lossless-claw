import { createPostgresSharedInit } from "../src/plugin/postgres-init.js";
import { createPostgresRouter } from "../src/plugin/postgres-router.js";
import { RetrievalEngine } from "../src/retrieval.js";
import { runLcmMigrations } from "../src/db/migration.js";
import {
  exportSqlite,
  exportHash,
  importSqliteExport,
  exportPostgresToSqlite,
} from "../src/db/postgres/transfer.js";
import pg from "pg";
import * as snapshots from "../src/db/postgres/context-snapshot.js";
import { POSTGRES_TABLES } from "../src/db/postgres/schema.js";
import { ExpansionAuthManager } from "../src/expansion-auth.js";
import { DatabaseSync } from "node:sqlite";
import lcmPlugin from "../index.js";
import type {
  ContextEngine,
  OpenClawPluginApi,
  AnyAgentTool,
} from "../src/openclaw-bridge.js";
const gatewayTranscript = vi.hoisted(() => ({
  visible: vi.fn(),
  raw: vi.fn(),
  entries: vi.fn(),
}));
vi.mock("openclaw/plugin-sdk/core", () => ({
  buildMemorySystemPromptAddition: () => undefined,
}));
vi.mock("openclaw/plugin-sdk/logging-core", () => ({
  redactSensitiveText: (text: string) => text,
}));
vi.mock("openclaw/plugin-sdk/session-transcript-runtime", () => ({
  readSessionTranscriptVisibleMessageDelta: (p: unknown) =>
    gatewayTranscript.visible(p),
  readSessionTranscriptRawDelta: (p: unknown) => gatewayTranscript.raw(p),
  readVisibleSessionTranscriptMessageEntries: (p: unknown) =>
    gatewayTranscript.entries(p),
}));
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { randomBytes } from "node:crypto";
import {
  mkdtempSync,
  rmSync,
  writeFileSync,
  readFileSync,
  existsSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  PostgresDatabase,
  provisionPostgres,
} from "../src/db/postgres/database.js";
import { ConversationStore } from "../src/store/conversation-store.js";
import { SummaryStore } from "../src/store/summary-store.js";
import { TranscriptCursorStore } from "../src/store/transcript-cursor-store.js";
import { UserReplayKeyStore } from "../src/store/user-replay-key-store.js";
import { TranscriptGapStore } from "../src/store/transcript-gap-store.js";
import { LcmContextEngine } from "../src/engine.js";
import { createTestConfig, createTestDeps } from "./helpers.js";
import { FakeTranscriptHost } from "./transcript-delta-fake-host.js";
import type { AgentMessage } from "../src/openclaw-bridge.js";

// Only the disposable runner opts in. Never use an operator's ambient PG credentials.
describe.skipIf(process.env.LCM_DISPOSABLE_POSTGRES !== "1")(
  "native PostgreSQL engine",
  () => {
    let db: PostgresDatabase;
    const schema = `lcm_native_${randomBytes(8).toString("hex")}`;
    const dir = mkdtempSync(join(tmpdir(), "lcm-native-pg-"));
    beforeAll(async () => {
      await provisionPostgres({
        schema,
        organizationId: "org-one",
        agentId: "one",
      });
      db = await PostgresDatabase.open({
        schema,
        organizationId: "org-one",
        agentId: "one",
      });
    });
    afterAll(async () => {
      await db?.close();
      rmSync(dir, { recursive: true, force: true });
    });
    it("runs native message, summary DAG, cursor, replay and gap APIs", async () => {
      const conversations = new ConversationStore(db),
        summaries = new SummaryStore(db);
      const c = await conversations.createConversation({
        sessionId: "s",
        sessionKey: "agent:one:s",
      });
      const messages = await conversations.createMessagesBulk(
        Array.from({ length: 8 }, (_, i) => ({
          conversationId: c.conversationId,
          seq: i,
          role: i % 2 ? "assistant" : "user",
          content: `native canary paragraph ${i}`,
          tokenCount: 8,
        }))
      );
      await summaries.appendContextMessages(
        c.conversationId,
        messages.map((m) => m.messageId)
      );
      expect(await conversations.getMessageCount(c.conversationId)).toBe(8);
      expect(await summaries.getContextTokenCount(c.conversationId)).toBe(64);
      expect(await summaries.getContextTokenCount(-1)).toBe(0);
      await summaries.insertSummary({
        summaryId: "leaf",
        conversationId: c.conversationId,
        kind: "leaf",
        content: "native canary",
        tokenCount: 4,
      });
      await summaries.linkSummaryToMessages(
        "leaf",
        messages.map((m) => m.messageId)
      );
      await summaries.replaceContextRangeWithSummary({
        conversationId: c.conversationId,
        startOrdinal: 0,
        endOrdinal: 7,
        summaryId: "leaf",
      });
      expect(await summaries.getSummaryMessages("leaf")).toEqual(
        messages.map((m) => m.messageId)
      );
      expect(await summaries.getSummaryMessageSeqRange("leaf")).toEqual({
        minSeq: 0,
        maxSeq: 7,
      });
      expect(await summaries.getSummarySubtree("leaf")).toHaveLength(1);
      expect(
        await conversations.searchMessages({
          conversationId: c.conversationId,
          query: "canary",
          mode: "full_text",
          limit: 2,
        })
      ).toHaveLength(2);
      expect(
        await summaries.searchSummaries({
          conversationId: c.conversationId,
          query: "canary",
          mode: "full_text",
          limit: 2,
        })
      ).toHaveLength(1);
      const cursors = new TranscriptCursorStore(db);
      await cursors.upsert({
        conversationId: c.conversationId,
        cursor: "opaque-1",
        frontierEntryId: "e8",
        frontierSeq: 8,
        origin: "fresh",
      });
      expect((await cursors.get(c.conversationId))?.cursor).toBe("opaque-1");
      expect(await cursors.countRowsBetweenSeq(c.conversationId, 0, 7)).toBe(6);
      expect(
        await cursors.listContentMatchCandidatesAfterSeq(c.conversationId, 5)
      ).toHaveLength(2);
      await new UserReplayKeyStore(db).record(c.conversationId, [
        { entryId: "e8", idempotencyKey: "receipt-1", signature: "sig" },
      ]);
      expect(
        await new TranscriptGapStore(db).listForConversation(c.conversationId)
      ).toEqual([]);
    });
    it("runs real engine bootstrap, accepted-turn commit, replay, assembly and restart", async () => {
      const sessionId = "engine-session",
        sessionKey = "agent:one:engine-session";
      const target = { agentId: "one", sessionId, sessionKey };
      const host = new FakeTranscriptHost(sessionId);
      const deps = createTestDeps(createTestConfig(join(dir, "unused.db")), {
        readSessionTranscriptVisibleMessageDelta: host.readVisibleDelta,
        readSessionTranscriptRawDelta: host.readRawDelta,
        readVisibleSessionTranscriptMessageEntries: host.readVisibleEntries,
      });
      let engine = new LcmContextEngine(deps, db);
      await engine.bootstrap({ ...target, sessionTarget: target });
      const admission = host.append({
        role: "user",
        content: "PostgreSQL engine canary",
      } as AgentMessage);
      const terminal = host.append({
        role: "assistant",
        content: [{ type: "text", text: "persistent reply" }],
      } as AgentMessage);
      const anchor = (entryId: string) => ({
        ...target,
        storePath: "/synthetic/transcript",
        generation: host.generation,
        entryId,
        rawSeq: host.positionOf(entryId),
        effectiveParentId: null,
        activeMessagePosition: host.positionOf(entryId),
      });
      const params = {
        advancementKey: "pg-engine-turn",
        admission: {
          ...anchor(admission),
          logicalTurnId: "pg-engine-turn",
          role: "user" as const,
        },
        terminal: anchor(terminal),
        messages: host.visibleMessages().map((e) => e.message),
        sessionId,
        sessionKey,
      };
      expect(await engine.commitTurn(params)).toEqual({ status: "committed" });
      expect(await engine.commitTurn(params)).toEqual({ status: "duplicate" });
      const assembled = await engine.assemble({
        ...target,
        messages: [],
        tokenBudget: 4096,
      });
      expect(JSON.stringify(assembled.messages)).toContain(
        "PostgreSQL engine canary"
      );
      engine = new LcmContextEngine(deps, db);
      expect(await engine.commitTurn(params)).toEqual({ status: "duplicate" });
      expect(
        await db
          .prepare(
            "SELECT COUNT(*) AS count FROM turn_advancements WHERE advancement_key = ?"
          )
          .get("pg-engine-turn")
      ).toEqual({ count: 1 });
    });
    it("prepares and publishes native pending compaction, maintenance debt and focus overlay", async () => {
      const sessionId = "compact-session",
        sessionKey = "agent:one:compact-session";
      const host = new FakeTranscriptHost(sessionId);
      for (let i = 0; i < 24; i++)
        host.append({
          role: i % 2 ? "assistant" : "user",
          content:
            `paragraph ${i} ` + "deterministic synthetic content ".repeat(18),
        } as AgentMessage);
      const deps = createTestDeps(
        createTestConfig(join(dir, "unused.db"), {
          freshTailCount: 2,
          leafChunkTokens: 240,
          leafMinFanout: 2,
          condensedMinFanout: 2,
          condensedTargetTokens: 16,
          leafTargetTokens: 16,
          summaryProvider: "anthropic",
          summaryModel: "claude-opus-4-5",
        }),
        {
          readSessionTranscriptVisibleMessageDelta: host.readVisibleDelta,
          readSessionTranscriptRawDelta: host.readRawDelta,
          readVisibleSessionTranscriptMessageEntries: host.readVisibleEntries,
        }
      );
      const engine = new LcmContextEngine(deps, db);
      await engine.bootstrap({
        sessionId,
        sessionKey,
        sessionTarget: { agentId: "one", sessionId, sessionKey },
      });
      const c = (await engine
        .getConversationStore()
        .getConversationBySessionKey(sessionKey))!;
      const before = await engine
        .getSummaryStore()
        .getContextTokenCount(c.conversationId);
      const compact = await engine.compact({
        sessionId,
        sessionKey,
        sessionFile: "",
        tokenBudget: 1200,
        currentTokenCount: before,
        force: true,
      });
      expect(compact.ok, JSON.stringify(compact)).toBe(true);
      expect(compact.compacted, JSON.stringify(compact)).toBe(true);
      expect(
        await engine.getConversationStore().getMessageCount(c.conversationId)
      ).toBe(24);
      const batches = await db
        .prepare(
          "SELECT * FROM pending_compaction_batches WHERE conversation_id = ?"
        )
        .all(c.conversationId);
      expect(batches.length).toBeGreaterThan(0);
      expect(
        (
          await db
            .prepare(
              "SELECT COUNT(*) AS count FROM pending_summary_nodes WHERE conversation_id = ? AND status = 'promoted'"
            )
            .get(c.conversationId)
        )?.count
      ).toBeGreaterThan(0);
      await engine
        .getCompactionMaintenanceStore()
        .requestProactiveCompactionDebt({
          conversationId: c.conversationId,
          reason: "test",
          tokenBudget: 1200,
          currentTokenCount: before,
        });
      await engine.maintain({
        sessionId,
        sessionKey,
        sessionFile: "",
        runtimeContext: {
          allowDeferredCompactionExecution: true,
          tokenBudget: 1200,
          currentTokenCount: before,
        },
      });
      const focus = engine.getFocusBriefStore();
      const active = await focus.getActiveContextSummaries(c.conversationId);
      expect(active.length).toBeGreaterThan(0);
      const watermark = await focus.getCoveredWatermark(c.conversationId);
      const brief = await focus.createFocusBrief({
        ...watermark,
        conversationId: c.conversationId,
        sessionKey,
        prompt: "focus synthetic result",
        content: "Native PostgreSQL focus overlay",
        status: "active",
        tokenCount: 8,
        targetTokens: 16,
        sourceContextHash: "fixture",
        sources: active.map((r) => ({
          summaryId: r.summaryId,
          ordinal: r.ordinal,
          role: "active_input" as const,
        })),
      });
      expect((await focus.getActiveFocusBrief(c.conversationId))?.briefId).toBe(
        brief.briefId
      );
      const assembled = await engine.assemble({
        sessionId,
        sessionKey,
        messages: [],
        tokenBudget: 4096,
      });
      expect(JSON.stringify(assembled.messages)).toContain(
        "Native PostgreSQL focus overlay"
      );
    });

    it("registers the real asynchronous gateway factory and tenant-scoped recall tools", async () => {
      const host = new FakeTranscriptHost("gateway-session");
      host.append({
        role: "user",
        content: "gateway PostgreSQL canary",
      } as AgentMessage);
      gatewayTranscript.visible.mockImplementation(host.readVisibleDelta);
      gatewayTranscript.raw.mockImplementation(host.readRawDelta);
      gatewayTranscript.entries.mockImplementation(host.readVisibleEntries);
      let factory: (() => ContextEngine | Promise<ContextEngine>) | undefined;
      const hooks = new Map<string, () => Promise<void>>();
      const tools = new Map<
        string,
        (ctx: { sessionId: string; sessionKey: string }) => AnyAgentTool
      >();
      const log = {
        info: vi.fn(),
        warn: vi.fn(),
        error: vi.fn(),
        debug: vi.fn(),
      };
      const api = {
        id: "lossless-claw",
        name: "Lossless Claw",
        config: {},
        pluginConfig: {
          storageBackend: "postgres",
          postgresSchema: schema,
          postgresOrganizationId: "org-one",
          postgresAgentId: "one",
          largeFilesDir: dir,
        },
        runtime: {
          subagent: {
            run: vi.fn(),
            waitForRun: vi.fn(),
            getSessionMessages: vi.fn(),
            deleteSession: vi.fn(),
          },
          llm: {
            complete: vi.fn(async () => ({
              text: "gateway summary",
              provider: "anthropic",
              model: "claude-opus-4-5",
              usage: {},
              audit: { caller: { kind: "plugin", id: "lossless-claw" } },
            })),
          },
          modelAuth: {
            getApiKeyForModel: vi.fn(),
            resolveApiKeyForProvider: vi.fn(),
          },
          config: { current: () => ({}), loadConfig: () => ({}) },
          logging: { shouldLogVerbose: () => false, getChildLogger: () => log },
          channel: { session: { resolveStorePath: () => "/synthetic/store" } },
        },
        logger: log,
        resolvePath: () => dir,
        registerContextEngine: (_id: string, next: typeof factory) => {
          factory = next;
        },
        registerTool: (
          next: (ctx: {
            sessionId: string;
            sessionKey: string;
          }) => AnyAgentTool,
          meta: { name: string }
        ) => tools.set(meta.name, next),
        registerCommand: vi.fn(),
        registerHook: vi.fn(),
        registerHttpRoute: vi.fn(),
        on: (name: string, callback: () => Promise<void>) =>
          hooks.set(name, callback),
      } as unknown as OpenClawPluginApi;
      lcmPlugin.register(api);
      expect(factory).toBeTypeOf("function");
      try {
        const engine = await factory!();
        const target = {
          agentId: "one",
          sessionId: "gateway-session",
          sessionKey: "agent:one:gateway-session",
        };
        await engine.bootstrap({ ...target, sessionTarget: target });
        expect(
          JSON.stringify(
            (
              await engine.assemble({
                ...target,
                messages: [],
                tokenBudget: 4096,
              })
            ).messages
          )
        ).toContain("gateway PostgreSQL canary");
        const foreignGrep = tools.get("lcm_grep")!({
          sessionId: "foreign",
          sessionKey: "agent:two:foreign",
        });
        await expect(
          foreignGrep.execute("foreign-all", {
            pattern: "canary",
            mode: "full_text",
            allConversations: true,
          })
        ).rejects.toThrow(/authority/);
        const owned = (await new ConversationStore(
          db
        ).getConversationBySessionKey(target.sessionKey))!;
        await expect(
          foreignGrep.execute("foreign-id", {
            pattern: "canary",
            mode: "full_text",
            conversationId: owned.conversationId,
          })
        ).rejects.toThrow(/authority/);
        const grep = tools.get("lcm_grep")!(target);
        const result = await grep.execute("call-1", {
          pattern: "canary",
          mode: "full_text",
          scope: "messages",
        });
        expect(JSON.stringify(result)).toContain("gateway PostgreSQL canary");
        await expect(
          engine.bootstrap({
            sessionId: "cross-agent",
            sessionKey: "agent:two:cross-agent",
            sessionTarget: {
              agentId: "two",
              sessionId: "cross-agent",
              sessionKey: "agent:two:cross-agent",
            },
          })
        ).rejects.toThrow(/authority/);
      } finally {
        await hooks.get("gateway_stop")?.();
      }
    });

    it("retries failed cold initialization and closes a real pending connection before restart", async () => {
      const deps = createTestDeps(
        createTestConfig(join(dir, "unused.db"), {
          storageBackend: "postgres",
          postgresSchema: schema,
          postgresOrganizationId: "org-one",
          postgresAgentId: "one",
        })
      );
      const shared = createPostgresSharedInit(deps, true),
        open = PostgresDatabase.open.bind(PostgresDatabase);
      const failure = vi
        .spyOn(PostgresDatabase, "open")
        .mockRejectedValueOnce(new Error("synthetic cold open failure"));
      await expect(shared.waitForEngine()).rejects.toThrow(
        "synthetic cold open failure"
      );
      failure.mockRestore();
      expect(await shared.waitForEngine()).toBeInstanceOf(LcmContextEngine);
      await shared.close!();
      await shared.restart!();
      let release!: () => void, started!: () => void;
      const gate = new Promise<void>((r) => {
          release = r;
        }),
        arrived = new Promise<void>((r) => {
          started = r;
        });
      let connection: PostgresDatabase | undefined;
      const delayed = vi
        .spyOn(PostgresDatabase, "open")
        .mockImplementation(async (options) => {
          connection = await open(options);
          started();
          await gate;
          return connection;
        });
      const pending = shared.waitForEngine();
      const rejection = expect(pending).rejects.toThrow(/stopped/);
      await arrived;
      const stop = shared.close!();
      release();
      await rejection;
      await stop;
      delayed.mockRestore();
      expect(shared.getCachedEngine()).toBeNull();
      await expect(connection!.prepare("SELECT 1").get()).rejects.toThrow();
      await shared.restart!();
      expect(await shared.waitForEngine()).toBeInstanceOf(LcmContextEngine);
      await shared.close!();
    });
    it("routes identical session IDs to separate agent stores and validates every supplied authority", async () => {
      const secondOptions = {
        schema: `lcm_router_${randomBytes(6).toString("hex")}`,
        organizationId: "org-one",
        agentId: "two",
      };
      await provisionPostgres(secondOptions);
      const hosts = {
        one: new FakeTranscriptHost("overlapping-router-session"),
        two: new FakeTranscriptHost("overlapping-router-session"),
      };
      for (const agentId of ["one", "two"] as const)
        hosts[agentId].append({
          role: "user",
          content: `ONLY-${agentId}-ROUTER-CANARY`,
        } as AgentMessage);
      const select = (p: { sessionKey?: string; agentId?: string }) =>
        hosts[(p.agentId ?? p.sessionKey?.split(":")[1]) as "one" | "two"];
      const deps = createTestDeps(
        createTestConfig(join(dir, "unused.db"), {
          storageBackend: "postgres",
          postgresOrganizationId: "org-one",
          postgresSchemas: { one: schema, two: secondOptions.schema },
        }),
        {
          readSessionTranscriptVisibleMessageDelta: (p) =>
            select(p).readVisibleDelta(p),
          readSessionTranscriptRawDelta: (p) => select(p).readRawDelta(p),
          readVisibleSessionTranscriptMessageEntries: (p) =>
            select(p).readVisibleEntries(),
        }
      );
      const router = createPostgresRouter(deps, true);
      try {
        const engine = await router.waitForContextEngine!();
        for (const agentId of ["one", "two"]) {
          await engine.bootstrap({
            sessionId: "overlapping-router-session",
            sessionKey: `agent:${agentId}:router`,
            sessionTarget: {
              agentId,
              sessionId: "overlapping-router-session",
              sessionKey: `agent:${agentId}:router`,
            },
          });
          const assembled = await engine.assemble({
            sessionId: "overlapping-router-session",
            sessionKey: `agent:${agentId}:router`,
            messages: [],
            tokenBudget: 4096,
          });
          expect(JSON.stringify(assembled.messages)).toContain(
            `ONLY-${agentId}-ROUTER-CANARY`
          );
          expect(JSON.stringify(assembled.messages)).not.toContain(
            `ONLY-${agentId === "one" ? "two" : "one"}-ROUTER-CANARY`
          );
        }
        await expect(
          router.waitForScopedEngine!({
            agentId: "one",
            sessionKey: "agent:two:router",
          })
        ).rejects.toThrow(/authority/);
        await expect(
          engine.assemble({
            sessionId: "x",
            sessionKey: "agent:unknown:x",
            messages: [],
            tokenBudget: 4096,
          })
        ).rejects.toThrow(/configured agent/);
        const one = await router.waitForScopedEngine!({ agentId: "one" }),
          two = await router.waitForScopedEngine!({ agentId: "two" });
        expect(
          await two
            .getConversationStore()
            .searchMessages({
              query: "ONLY-one-ROUTER-CANARY",
              mode: "regex",
              limit: 100,
            })
        ).toEqual([]);
        expect(
          await one
            .getConversationStore()
            .searchMessages({
              query: "ONLY-two-ROUTER-CANARY",
              mode: "regex",
              limit: 100,
            })
        ).toEqual([]);
      } finally {
        await router.close!();
      }
    });

    it("rejects foreign successor/session identities without rebinding stored conversations", async () => {
      const config = createTestConfig(join(dir, "unused.db"));
      const engine = new LcmContextEngine(createTestDeps(config), db);
      const c = (await engine
        .getConversationStore()
        .getConversationBySessionKey("agent:one:s"))!;
      await expect(
        engine.handleSessionEnd({
          reason: "reset",
          sessionId: "s",
          sessionKey: "agent:one:s",
          nextSessionId: "next",
          nextSessionKey: "agent:two:next",
        })
      ).rejects.toThrow(/authority/);
      await expect(
        engine
          .getConversationStore()
          .rebindConversationSession(c.conversationId, "next", "agent:two:next")
      ).rejects.toThrow(/authority/);
      expect(
        (await engine.getConversationStore().getConversation(c.conversationId))
          ?.sessionKey
      ).toBe("agent:one:s");
      await expect(
        engine.bootstrap({
          sessionId: "mixed",
          sessionKey: "agent:two:mixed",
          sessionTarget: {
            agentId: "one",
            sessionId: "mixed",
            sessionKey: "agent:one:mixed",
          },
        })
      ).rejects.toThrow(/authority/);
    });
    it("preserves native SQLite timestamp conversion including offsets, whitespace and session timezone", async () => {
      const sqlite = new DatabaseSync(":memory:");
      try {
        await db.transaction(async () => {
          await db.exec("SET LOCAL TIME ZONE 'America/Los_Angeles'");
          for (const value of [
            "2026-11-04T12:00:00+02:00 ",
            "2026-11-04T12:00:00+02:00\t\n",
            "2026-11-04T12:00:00z ",
            "2026-11-04 12:00:00",
            "2026-11-04T12:00:00-07:00",
          ]) {
            const actual = await db
              .prepare("SELECT datetime(?) AS stamp, julianday(?) AS day")
              .get(value, value);
            const expected = sqlite
              .prepare("SELECT datetime(?) AS stamp, julianday(?) AS day")
              .get(value, value);
            expect(actual?.stamp).toBe(expected?.stamp);
            expect(actual?.day).toBeCloseTo(expected?.day as number, 8);
          }
        });
      } finally {
        sqlite.close();
      }
    });
    it("keeps nested rollback local and never acknowledges an aborted transaction after a swallowed SQL error", async () => {
      const conversations = new ConversationStore(db);
      const c = await conversations.createConversation({
        sessionId: "rollback",
        sessionKey: "agent:one:rollback",
      });
      await db.transaction(async () => {
        await conversations.createMessage({
          conversationId: c.conversationId,
          seq: 1,
          role: "user",
          content: "kept-before",
          tokenCount: 1,
        });
        await expect(
          db.transaction(async () => {
            await conversations.createMessage({
              conversationId: c.conversationId,
              seq: 2,
              role: "assistant",
              content: "rolled-back",
              tokenCount: 1,
            });
            throw new Error("nested abort");
          })
        ).rejects.toThrow("nested abort");
        await conversations.createMessage({
          conversationId: c.conversationId,
          seq: 2,
          role: "assistant",
          content: "kept-after",
          tokenCount: 1,
        });
      });
      expect(
        (await conversations.getMessages(c.conversationId)).map(
          (m) => m.content
        )
      ).toEqual(["kept-before", "kept-after"]);
      await expect(
        db.transaction(async () => {
          await conversations.createMessage({
            conversationId: c.conversationId,
            seq: 3,
            role: "user",
            content: "must roll back",
            tokenCount: 1,
          });
          try {
            await db
              .prepare(
                "INSERT INTO messages_fts(nonexistent_column) VALUES (?)"
              )
              .run("bad-index-write");
          } catch {
            /* Native FTS writes are best effort; transaction must still fail. */
          }
        })
      ).rejects.toThrow(/rolled back/);
      expect(await conversations.getMessageCount(c.conversationId)).toBe(2);
    });
    it("returns duplicate on same-receipt retry after a real concurrent separate-engine conflict", async () => {
      const other = await PostgresDatabase.open({
        schema,
        organizationId: "org-one",
        agentId: "one",
      });
      const sessionId = "race-session",
        sessionKey = "agent:one:race-session",
        host = new FakeTranscriptHost(sessionId);
      const deps = createTestDeps(createTestConfig(join(dir, "unused.db")), {
        readSessionTranscriptVisibleMessageDelta: host.readVisibleDelta,
        readSessionTranscriptRawDelta: host.readRawDelta,
        readVisibleSessionTranscriptMessageEntries: host.readVisibleEntries,
      });
      const first = new LcmContextEngine(deps, db),
        second = new LcmContextEngine(deps, other);
      await first.bootstrap({
        sessionId,
        sessionKey,
        sessionTarget: { agentId: "one", sessionId, sessionKey },
      });
      const admission = host.append({
          role: "user",
          content: "same receipt race",
        } as AgentMessage),
        terminal = host.append({
          role: "assistant",
          content: "one durable response",
        } as AgentMessage);
      const anchor = (entryId: string) => ({
        agentId: "one",
        sessionId,
        sessionKey,
        storePath: "/synthetic/race",
        generation: host.generation,
        entryId,
        rawSeq: host.positionOf(entryId),
        activeMessagePosition: host.positionOf(entryId),
        effectiveParentId: null,
      });
      const params = {
        advancementKey: "race-receipt",
        admission: {
          ...anchor(admission),
          logicalTurnId: "race-receipt",
          role: "user" as const,
        },
        terminal: anchor(terminal),
        messages: host.visibleMessages().map((e) => e.message),
        sessionId,
        sessionKey,
      };
      let arrivals = 0,
        release!: () => void;
      const barrier = new Promise<void>((resolve) => {
        release = resolve;
      });
      const spies = [db, other].map((connection) => {
        const prepare = connection.prepare.bind(connection);
        return vi.spyOn(connection, "prepare").mockImplementation((sql) => {
          const statement = prepare(sql);
          if (!/SELECT payload_hash/.test(sql)) return statement;
          return {
            ...statement,
            get: async (...args) => {
              const result = await statement.get(...args);
              if (!result) {
                if (++arrivals === 2) release();
                await barrier;
              }
              return result;
            },
          };
        });
      });
      try {
        const results = await Promise.allSettled([
          first.commitTurn(params),
          second.commitTurn(params),
        ]);
        spies.forEach((spy) => spy.mockRestore());
        expect(arrivals).toBe(2);
        const fulfilled = results.filter((r) => r.status === "fulfilled");
        expect(fulfilled).toHaveLength(1);
        expect(fulfilled[0]).toMatchObject({ value: { status: "committed" } });
        expect(results.filter((r) => r.status === "rejected")).toHaveLength(1);
        expect(await second.commitTurn(params)).toEqual({
          status: "duplicate",
        });
        expect(await first.commitTurn(params)).toEqual({ status: "duplicate" });
        const c = (await first
          .getConversationStore()
          .getConversationBySessionKey(sessionKey))!;
        expect(
          await first.getConversationStore().getMessageCount(c.conversationId)
        ).toBe(2);
        expect(
          await db
            .prepare(
              "SELECT COUNT(*) AS count FROM turn_advancements WHERE advancement_key = ?"
            )
            .get("race-receipt")
        ).toEqual({ count: 1 });
      } finally {
        spies.forEach((spy) => spy.mockRestore());
        await other.close();
      }
    });
    it("fences native leaf compaction when another engine accepts a turn during inference", async () => {
      const sessionId = "inference-race",
        sessionKey = "agent:one:inference-race",
        host = new FakeTranscriptHost(sessionId);
      for (let i = 0; i < 16; i++)
        host.append({
          role: i % 2 ? "assistant" : "user",
          content: `source ${i} ` + "synthetic durable source ".repeat(20),
        } as AgentMessage);
      const deps = createTestDeps(
        createTestConfig(join(dir, "unused.db"), {
          freshTailCount: 2,
          leafMinFanout: 2,
          leafChunkTokens: 1000,
        }),
        {
          readSessionTranscriptVisibleMessageDelta: host.readVisibleDelta,
          readSessionTranscriptRawDelta: host.readRawDelta,
          readVisibleSessionTranscriptMessageEntries: host.readVisibleEntries,
        }
      );
      const first = new LcmContextEngine(deps, db);
      const other = await PostgresDatabase.open({
        schema,
        organizationId: "org-one",
        agentId: "one",
      });
      const second = new LcmContextEngine(deps, other);
      try {
        await first.bootstrap({
          sessionId,
          sessionKey,
          sessionTarget: { agentId: "one", sessionId, sessionKey },
        });
        const c = (await first
          .getConversationStore()
          .getConversationBySessionKey(sessionKey))!;
        let entered!: () => void, release!: () => void;
        const atInference = new Promise<void>((resolve) => {
            entered = resolve;
          }),
          inference = new Promise<void>((resolve) => {
            release = resolve;
          });
        const work = first.getCompactionEngine().compactLeaf({
          conversationId: c.conversationId,
          tokenBudget: 1000,
          force: true,
          summarize: async () => {
            entered();
            await inference;
            return "bounded deterministic summary";
          },
        });
        // Attach rejection immediately so a fast conflict is never an unhandled rejection.
        const outcome = work.then(
          (value) => ({ value }),
          (error) => ({ error })
        );
        await atInference;
        const admission = host.append({
            role: "user",
            content: "new accepted turn during inference",
          } as AgentMessage),
          terminal = host.append({
            role: "assistant",
            content: "new durable reply",
          } as AgentMessage);
        const anchor = (entryId: string) => ({
          agentId: "one",
          sessionId,
          sessionKey,
          storePath: "/synthetic/inference",
          generation: host.generation,
          entryId,
          rawSeq: host.positionOf(entryId),
          activeMessagePosition: host.positionOf(entryId),
          effectiveParentId: null,
        });
        const key = "inference-new-turn";
        await second.commitTurn({
          advancementKey: key,
          admission: { ...anchor(admission), logicalTurnId: key, role: "user" },
          terminal: anchor(terminal),
          messages: host
            .visibleMessages()
            .slice(-2)
            .map((e) => e.message),
          sessionId,
          sessionKey,
        });
        release();
        expect(await outcome).toMatchObject({
          error: expect.objectContaining({
            message: expect.stringMatching(/revision conflict/),
          }),
        });
        expect(
          await first.getConversationStore().getMessageCount(c.conversationId)
        ).toBe(18);
        expect(
          (
            await first.getSummaryStore().getContextItems(c.conversationId)
          ).every((i) => i.itemType === "message")
        ).toBe(true);
        const retry = await first
          .getCompactionEngine()
          .compactLeaf({
            conversationId: c.conversationId,
            tokenBudget: 1000,
            force: true,
            summarize: async () => "fresh deterministic summary",
          });
        expect(retry.actionTaken).toBe(true);
        expect(typeof retry.tokensBefore).toBe("number");
        expect(typeof retry.tokensAfter).toBe("number");
        expect(
          await first.getConversationStore().getMessageCount(c.conversationId)
        ).toBe(18);
      } finally {
        await other.close();
      }
    });
    it("runs restart-persistent expansion grants and native gap totals as numbers", async () => {
      const c = (await new ConversationStore(db).getConversationBySessionKey(
        "agent:one:s"
      ))!;
      const manager = new ExpansionAuthManager(db);
      const grant = await manager.createGrant({
        issuerSessionId: "s",
        allowedConversationIds: [c.conversationId],
        tokenCap: 50,
      });
      await manager.bindSession("agent:one:subagent:child", grant.grantId);
      await manager.consumeTokenBudget(grant.grantId, 10);
      expect(
        await new ExpansionAuthManager(db).getRemainingTokenBudget(
          grant.grantId
        )
      ).toBe(40);
      expect(
        (
          await new ExpansionAuthManager(db).validateExpansion(grant.grantId, {
            conversationId: c.conversationId + 1000,
            summaryIds: [],
            depth: 1,
            tokenCap: 1,
          })
        ).valid
      ).toBe(false);
      const gaps = new TranscriptGapStore(db);
      const sourceMessages = await new ConversationStore(db).getMessages(
        c.conversationId
      );
      const gap = {
        firstEntryId: "missing-1",
        lastEntryId: "missing-3",
        firstVisibleSeq: 9,
        lastVisibleSeq: 11,
        entryCount: 3,
        prevMessageId: sourceMessages[0].messageId,
        nextMessageId: sourceMessages[1].messageId,
      };
      expect(await gaps.record(c.conversationId, [gap], "synthetic")).toBe(1);
      expect(await gaps.record(c.conversationId, [gap], "synthetic")).toBe(0);
      expect((await gaps.totals()).entries).toBe(3);
      expect(
        (await gaps.listForConversation(c.conversationId))[0].entryCount
      ).toBe(3);
    });

    it("keeps final engine projection epoch and messages on the same immutable snapshot", async () => {
      const engine = new LcmContextEngine(
        createTestDeps(createTestConfig(join(dir, "unused.db"))),
        db
      );
      const params = {
        sessionId: "s",
        sessionKey: "agent:one:s",
        messages: [],
        tokenBudget: 4096,
      };
      const baselineSpy = vi.spyOn(snapshots, "loadContextSnapshot");
      const before = await engine.assemble(params);
      const finalSnapshotCall = baselineSpy.mock.calls.length;
      baselineSpy.mockRestore();
      const c = (await engine
        .getConversationStore()
        .getConversationBySessionKey(params.sessionKey))!;
      const load = snapshots.loadContextSnapshot;
      let interleaved = false,
        snapshotCall = 0;
      const spy = vi
        .spyOn(snapshots, "loadContextSnapshot")
        .mockImplementation(async (connection, cid) => {
          const snapshot = await load(connection, cid);
          if (
            cid === c.conversationId &&
            ++snapshotCall === finalSnapshotCall &&
            !interleaved
          ) {
            interleaved = true;
            await db.transaction(async () => {
              await engine
                .getSummaryStore()
                .insertSummary({
                  summaryId: "interleaved-summary",
                  conversationId: cid,
                  kind: "condensed",
                  depth: 1,
                  content: "new interleaved canonical summary",
                  tokenCount: 8,
                });
              await engine
                .getSummaryStore()
                .linkSummaryToParents("interleaved-summary", ["leaf"]);
              await engine
                .getSummaryStore()
                .replaceContextRangeWithSummary({
                  conversationId: cid,
                  startOrdinal: 0,
                  endOrdinal: 0,
                  summaryId: "interleaved-summary",
                });
            });
          }
          return snapshot;
        });
      let assembled;
      try {
        assembled = await engine.assemble(params);
      } finally {
        spy.mockRestore();
      }
      expect(interleaved).toBe(true);
      expect(assembled!.messages).toEqual(before.messages);
      expect(assembled!.contextProjection?.epoch).toBe(
        before.contextProjection?.epoch
      );
      const next = await engine.assemble(params);
      expect(next.contextProjection?.epoch).not.toBe(
        before.contextProjection?.epoch
      );
      expect(JSON.stringify(next.messages)).toContain(
        "new interleaved canonical summary"
      );
    });
    it.skipIf(process.env.LCM_POSTGRES_VIA_POOLER === "1")(
      "enforces two-organization three-agent isolation using real restricted PostgreSQL login roles",
      async () => {
        const admin = new pg.Pool({ max: 1 });
        const stores: {
          schema: string;
          role: string;
          organizationId: string;
          agentId: string;
          db: PostgresDatabase;
          canary: string;
        }[] = [];
        try {
          for (let org = 0; org < 2; org++)
            for (let agent = 0; agent < 3; agent++) {
              const name = `lcm_role_${randomBytes(5).toString("hex")}`,
                organizationId = `isolation-org-${org}`,
                agentId = `agent-${agent}`;
              await provisionPostgres({
                schema: name,
                organizationId,
                agentId,
              });
              await admin.query(
                `CREATE ROLE "${name}" LOGIN; GRANT USAGE ON SCHEMA "${name}" TO "${name}"; GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA "${name}" TO "${name}"; GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA "${name}" TO "${name}"`
              );
              for (const table of POSTGRES_TABLES)
                await admin.query(
                  `GRANT ${
                    ["lcm_schema_version", "lcm_store_identity"].includes(table)
                      ? "SELECT"
                      : "SELECT,INSERT,UPDATE,DELETE"
                  } ON "${name}".${table} TO "${name}"`
                );
              const store = await PostgresDatabase.open({
                schema: name,
                organizationId,
                agentId,
                pool: { user: name, max: 2 },
              });
              const canary = `org${org}agent${agent}privatecanary`;
              const conversations = new ConversationStore(store),
                summaries = new SummaryStore(store);
              const c = await conversations.createConversation({
                sessionId: "overlapping-session-id",
                sessionKey: `agent:${agentId}:thread-one`,
              });
              const m = await conversations.createMessage({
                conversationId: c.conversationId,
                seq: 1,
                role: "user",
                content: canary,
                tokenCount: 1,
              });
              await summaries.appendContextMessage(
                c.conversationId,
                m.messageId
              );
              stores.push({
                schema: name,
                role: name,
                organizationId,
                agentId,
                db: store,
                canary,
              });
            }
          for (const current of stores) {
            const conversations = new ConversationStore(current.db);
            expect((await conversations.getMessages(1))[0].content).toBe(
              current.canary
            );
            for (const other of stores)
              if (other !== current) {
                expect(
                  await conversations.searchMessages({
                    query: other.canary,
                    mode: "full_text",
                    limit: 10,
                  })
                ).toEqual([]);
                await expect(
                  current.db
                    .prepare(`SELECT content FROM "${other.schema}".messages`)
                    .all()
                ).rejects.toMatchObject({ code: "42501" });
              }
            await expect(
              PostgresDatabase.open({
                schema: current.schema,
                organizationId: "wrong-organization",
                agentId: current.agentId,
                pool: { user: current.role },
              })
            ).rejects.toThrow(/identity mismatch/);
          }
          await admin.query(
            `ALTER ROLE "${stores[0].role}" SET default_transaction_read_only=on`
          );
          await expect(
            PostgresDatabase.open({
              schema: stores[0].schema,
              organizationId: stores[0].organizationId,
              agentId: stores[0].agentId,
              pool: { user: stores[0].role },
            })
          ).rejects.toThrow(/writable primary/);
        } finally {
          await Promise.all(stores.map((s) => s.db.close()));
          // Disposable server owns roles/schemas; preserve all unrelated cluster objects.
          await admin.end();
        }
      },
      30000
    );

    it("round-trips every live native table through restartable SQLite import and rollback export", async () => {
      const focusConversation = await new ConversationStore(
        db
      ).createConversation({
        sessionId: "focus-rowid-order",
        sessionKey: "agent:one:focus-rowid-order",
      });
      const focusStore = new (
        await import("../src/store/focus-brief-store.js")
      ).FocusBriefStore(db);
      const older = await focusStore.createFocusBrief({
        conversationId: focusConversation.conversationId,
        prompt: "z-older",
        content: "older same-second brief",
        status: "active",
        tokenCount: 4,
        targetTokens: 8,
      });
      const newer = await focusStore.createFocusBrief({
        conversationId: focusConversation.conversationId,
        prompt: "a-newer",
        content: "newer same-second brief",
        status: "active",
        tokenCount: 4,
        targetTokens: 8,
      });
      await db
        .prepare(
          "UPDATE focus_briefs SET brief_id=?,created_at=?,updated_at=? WHERE brief_id=?"
        )
        .run(
          "z-older-brief",
          "2026-11-04 00:00:00",
          "2026-11-04 00:00:00",
          older.briefId
        );
      await db
        .prepare(
          "UPDATE focus_briefs SET brief_id=?,created_at=?,updated_at=? WHERE brief_id=?"
        )
        .run(
          "a-newer-brief",
          "2026-11-04 00:00:00",
          "2026-11-04 00:00:00",
          newer.briefId
        );
      expect(
        (await focusStore.getLatestFocusBrief(focusConversation.conversationId))
          ?.briefId
      ).toBe("a-newer-brief");
      expect(
        (await focusStore.getActiveFocusBrief(focusConversation.conversationId))
          ?.briefId
      ).toBe("a-newer-brief");
      const exported = join(dir, "full-native-export.db");
      const result = await exportPostgresToSqlite(db, exported);
      const source = new DatabaseSync(exported, { readOnly: true });
      const data = exportSqlite(source);
      source.close();
      expect(exportHash(data)).toBe(result.sourceHash);
      for (const table of [
        "conversation_transcript_cursors",
        "transcript_user_replay_keys",
        "turn_advancements",
        "pending_compaction_batches",
        "pending_summary_nodes",
        "focus_briefs",
        "conversation_compaction_maintenance",
        "message_parts",
        "summary_parents",
        "conversation_transcript_gaps",
        "lcm_expansion_grants",
      ]) {
        expect(
          data.tables.find((t) => t.name === table),
          table
        ).toBeDefined();
      }
      const targetSchema = `lcm_import_${randomBytes(6).toString("hex")}`;
      const options = {
        schema: targetSchema,
        organizationId: "org-one",
        agentId: "one",
      };
      await provisionPostgres(options);
      let target = await PostgresDatabase.open(options);
      try {
        let chunks = 0;
        await expect(
          importSqliteExport(target, data, {
            chunkRows: 2,
            afterChunk: () => {
              if (++chunks === 2)
                throw new Error("synthetic import interruption");
            },
          })
        ).rejects.toThrow("synthetic import interruption");
        await expect(PostgresDatabase.open(options)).rejects.toThrow(
          /import is incomplete/
        );
        await target.close();
        target = await PostgresDatabase.open(options, {
          allowIncompleteImport: true,
        });
        expect(target.ready).toBe(false);
        expect(
          (await importSqliteExport(target, data, { chunkRows: 2 })).sourceHash
        ).toBe(result.sourceHash);
        // A completed replay is verifiable and introduces no duplicate rows.
        expect(
          (await importSqliteExport(target, data, { chunkRows: 2 })).sourceHash
        ).toBe(result.sourceHash);
        const restored = join(dir, "full-native-restored.db");
        expect(
          (await exportPostgresToSqlite(target, restored)).sourceHash
        ).toBe(result.sourceHash);
        const importedFocus = new (
          await import("../src/store/focus-brief-store.js")
        ).FocusBriefStore(target);
        expect(
          (
            await importedFocus.getLatestFocusBrief(
              focusConversation.conversationId
            )
          )?.briefId
        ).toBe("a-newer-brief");
        expect(
          (
            await importedFocus.getActiveFocusBrief(
              focusConversation.conversationId
            )
          )?.briefId
        ).toBe("a-newer-brief");
        const sqlite = new DatabaseSync(restored, { readOnly: true });
        const rollbackFocus = new (
          await import("../src/store/focus-brief-store.js")
        ).FocusBriefStore(sqlite);
        expect(
          (
            await rollbackFocus.getLatestFocusBrief(
              focusConversation.conversationId
            )
          )?.briefId
        ).toBe("a-newer-brief");
        expect(
          (
            await rollbackFocus.getActiveFocusBrief(
              focusConversation.conversationId
            )
          )?.briefId
        ).toBe("a-newer-brief");
        expect(sqlite.prepare("PRAGMA foreign_key_check").all()).toEqual([]);
        expect(exportHash(exportSqlite(sqlite))).toBe(result.sourceHash);
        sqlite.close();
        const afterImport = await importedFocus.createFocusBrief({
          conversationId: focusConversation.conversationId,
          prompt: "after-import",
          content: "new focus after migration",
          status: "active",
          tokenCount: 4,
          targetTokens: 8,
        });
        expect(
          (
            await importedFocus.getLatestFocusBrief(
              focusConversation.conversationId
            )
          )?.briefId
        ).toBe(afterImport.briefId);
        const highest = Math.max(
          ...data.tables
            .find((t) => t.name === "messages")!
            .rows.map((r) => r.message_id as number)
        );
        const conversation = (await new ConversationStore(
          target
        ).getConversationBySessionKey("agent:one:s"))!;
        const appended = await new ConversationStore(target).createMessage({
          conversationId: conversation.conversationId,
          seq: 99,
          role: "user",
          content: "after verified migration",
          tokenCount: 4,
        });
        expect(appended.messageId).toBeGreaterThan(highest);
      } finally {
        await target.close();
      }
    }, 30000);

    it("bounds snapshot serialized metadata and closes each immutable view", async () => {
      const conversations = new ConversationStore(db),
        summaries = new SummaryStore(db);
      const c = await conversations.createConversation({
        sessionId: "byte-bound",
        sessionKey: "agent:one:byte-bound",
      });
      const m = await conversations.createMessage({
        conversationId: c.conversationId,
        seq: 1,
        role: "user",
        content: "small raw content",
        tokenCount: 4,
      });
      await summaries.appendContextMessage(c.conversationId, m.messageId);
      await conversations.createMessageParts(m.messageId, [
        {
          sessionId: "byte-bound",
          partType: "text",
          ordinal: 0,
          textContent: m.content,
          metadata: JSON.stringify({ syntheticMetadata: "x".repeat(4096) }),
        },
      ]);
      expect(Buffer.byteLength(m.content)).toBeLessThan(1024);
      await expect(
        snapshots.loadContextSnapshot(db, c.conversationId, { maxBytes: 1024 })
      ).rejects.toThrow(/serialized JSON byte limit/);
      const snapshot = await snapshots.loadContextSnapshot(
        db,
        c.conversationId
      );
      const first = (await snapshot.conversationStore.getMessageById(
        m.messageId
      ))!;
      first.content = "modified external copy";
      expect(
        (await snapshot.conversationStore.getMessageById(m.messageId))?.content
      ).toBe(m.content);
      snapshot.close();
      await expect(
        snapshot.conversationStore.getMessageById(m.messageId)
      ).rejects.toThrow(/closed/);
      expect(await conversations.getMessageCount(c.conversationId)).toBe(1);
    });
    it("rolls back interrupted backend I/O and reconnects without false committed data", async () => {
      const conversations = new ConversationStore(db);
      const c = await conversations.createConversation({
        sessionId: "connection-failure",
        sessionKey: "agent:one:connection-failure",
      });
      await expect(
        db.transaction(async () => {
          await conversations.createMessage({
            conversationId: c.conversationId,
            seq: 1,
            role: "user",
            content: "unacknowledged synthetic turn",
            tokenCount: 4,
          });
          await db
            .prepare("SELECT pg_terminate_backend(pg_backend_pid())")
            .get();
        })
      ).rejects.toThrow();
      expect(await conversations.getMessageCount(c.conversationId)).toBe(0);
      expect(await db.prepare("SELECT 1 AS healthy").get()).toEqual({
        healthy: 1,
      });
    });

    it("resolves same-agent cross-conversation DAG sources and fences their revisions", async () => {
      const conversations = new ConversationStore(db),
        summaries = new SummaryStore(db);
      const source = await conversations.createConversation({
        sessionId: "graph-source",
        sessionKey: "agent:one:graph-source",
      });
      const root = await conversations.createConversation({
        sessionId: "graph-root",
        sessionKey: "agent:one:graph-root",
      });
      const message = await conversations.createMessage({
        conversationId: source.conversationId,
        seq: 42,
        role: "user",
        content: "own-thread DAG source",
        tokenCount: 4,
      });
      await summaries.insertSummary({
        summaryId: "cross-source-leaf",
        conversationId: source.conversationId,
        kind: "leaf",
        content: "own-thread leaf",
        tokenCount: 4,
      });
      await summaries.linkSummaryToMessages("cross-source-leaf", [
        message.messageId,
      ]);
      await summaries.insertSummary({
        summaryId: "cross-root",
        conversationId: root.conversationId,
        kind: "condensed",
        depth: 1,
        content: "own-thread parent",
        tokenCount: 4,
      });
      await summaries.linkSummaryToParents("cross-root", ["cross-source-leaf"]);
      await summaries.appendContextSummary(root.conversationId, "cross-root");
      const expanded = await new RetrievalEngine(
        conversations,
        summaries
      ).expand({
        summaryId: "cross-root",
        depth: 3,
        includeMessages: true,
        tokenCap: 1000,
      });
      expect(expanded.messages.map((m) => m.messageId)).toEqual([
        message.messageId,
      ]);
      expect(expanded.messages[0].content).toBe(message.content);
      const snapshot = await snapshots.loadContextSnapshot(
        db,
        root.conversationId
      );
      try {
        expect(
          (await snapshot.summaryStore.getSummaryParents("cross-root"))[0]
            .summaryId
        ).toBe("cross-source-leaf");
        expect(
          await snapshot.summaryStore.getSummaryMessageSeqRange("cross-root")
        ).toEqual({ minSeq: 42, maxSeq: 42 });
        expect(
          (await snapshot.conversationStore.getMessageById(message.messageId))
            ?.content
        ).toBe(message.content);
        expect(snapshot.revisions.map((r) => r.conversationId)).toEqual([
          source.conversationId,
          root.conversationId,
        ]);
        await conversations.createMessageParts(message.messageId, [
          {
            sessionId: "graph-source",
            partType: "text",
            ordinal: 0,
            textContent: "updated own-thread source",
          },
        ]);
        await expect(
          summaries.withTransactionAtRevision(
            root.conversationId,
            snapshot.revision,
            async () => {},
            snapshot.revisions
          )
        ).rejects.toThrow(/revision conflict/);
      } finally {
        snapshot.close();
      }
    });

    it("preserves sanitized phrase/literal/regex selection and explicitly records Porter stemming differences", async () => {
      const sqlite = new DatabaseSync(":memory:");
      runLcmMigrations(sqlite);
      const pgStore = new ConversationStore(db),
        sqliteStore = new ConversationStore(sqlite);
      const pgConversation = await pgStore.createConversation({
        sessionId: "fts-semantics",
        sessionKey: "agent:one:fts-semantics",
      });
      const sqliteConversation = await sqliteStore.createConversation({
        sessionId: "fts-semantics",
        sessionKey: "agent:one:fts-semantics",
      });
      const content = [
        "running storage sub agent",
        "runner databases sub-agent",
        "lcm_expand literal AND",
        "OR tokens with 100%literal",
      ];
      for (let seq = 0; seq < content.length; seq++)
        for (const [store, conversation] of [
          [pgStore, pgConversation],
          [sqliteStore, sqliteConversation],
        ] as const)
          await store.createMessage({
            conversationId: conversation.conversationId,
            seq,
            role: "user",
            content: content[seq],
            tokenCount: 4,
            createdAt: new Date(Date.UTC(2026, 10, 4, 0, 0, seq)),
          });
      const lookup = async (
        store: ConversationStore,
        conversationId: number,
        query: string,
        mode: "full_text" | "regex" = "full_text"
      ) => {
        const hits = await store.searchMessages({
          conversationId,
          query,
          mode,
          sort: "recency",
          limit: 10,
        });
        return Promise.all(
          hits.map(
            async (hit) => (await store.getMessageById(hit.messageId))!.content
          )
        );
      };
      const original = db.prepare.bind(db);
      let successfulFts = 0;
      const spy = vi.spyOn(db, "prepare").mockImplementation((sql) => {
        const statement = original(sql);
        if (!/messages_fts MATCH/.test(sql)) return statement;
        return {
          ...statement,
          all: async (...args) => {
            const rows = await statement.all(...args);
            successfulFts++;
            return rows;
          },
        };
      });
      try {
        for (const query of [
          "storage",
          '"sub agent"',
          "lcm_expand AND",
          "storage AND",
          'storage "***"',
        ])
          expect(
            await lookup(pgStore, pgConversation.conversationId, query),
            query
          ).toEqual(
            await lookup(sqliteStore, sqliteConversation.conversationId, query)
          );
        expect(successfulFts).toBeGreaterThan(0);
        expect(
          await lookup(
            pgStore,
            pgConversation.conversationId,
            "100%literal",
            "regex"
          )
        ).toEqual(
          await lookup(
            sqliteStore,
            sqliteConversation.conversationId,
            "100%literal",
            "regex"
          )
        );
        expect(
          await lookup(sqliteStore, sqliteConversation.conversationId, "run")
        ).toEqual([content[0]]);
        expect(
          await lookup(pgStore, pgConversation.conversationId, "run")
        ).toEqual([]);
        expect(
          (
            await pgStore.searchMessages({
              conversationId: pgConversation.conversationId,
              query: "storage",
              mode: "full_text",
            })
          )[0].rank
        ).toBeTypeOf("number");
      } finally {
        spy.mockRestore();
        sqlite.close();
      }
    });

    it("claims one immutable import source across competing maintenance processes", async () => {
      const source = new DatabaseSync(":memory:");
      runLcmMigrations(source);
      const sqliteStore = new ConversationStore(source),
        conversation = await sqliteStore.createConversation({
          sessionId: "claim",
          sessionKey: "agent:one:claim",
        });
      await sqliteStore.createMessage({
        conversationId: conversation.conversationId,
        seq: 1,
        role: "user",
        content: "source one",
        tokenCount: 4,
      });
      const firstData = exportSqlite(source);
      source.close();
      const secondData = structuredClone(firstData);
      secondData.tables.find((t) => t.name === "messages")!.rows[0].content =
        "source two";
      secondData.tables.find(
        (t) => t.name === "messages_fts"
      )!.rows[0].content = "source two";
      const options = {
        schema: `lcm_claim_${randomBytes(6).toString("hex")}`,
        organizationId: "org-one",
        agentId: "one",
      };
      await provisionPostgres(options);
      const first = await PostgresDatabase.open(options),
        second = await PostgresDatabase.open(options);
      let arrivals = 0,
        release!: () => void;
      const barrier = new Promise<void>((resolve) => {
        release = resolve;
      });
      const spies = [first, second].map((connection) => {
        const original = connection.prepare.bind(connection);
        return vi.spyOn(connection, "prepare").mockImplementation((sql) => {
          const statement = original(sql);
          if (!/SELECT source_hash,complete/.test(sql)) return statement;
          return {
            ...statement,
            get: async (...params) => {
              const row = await statement.get(...params);
              if (row?.source_hash === null) {
                if (++arrivals === 2) release();
                await barrier;
              }
              return row;
            },
          };
        });
      });
      try {
        const results = await Promise.allSettled([
          importSqliteExport(first, firstData),
          importSqliteExport(second, secondData),
        ]);
        spies.forEach((spy) => spy.mockRestore());
        expect(arrivals).toBe(2);
        expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
        expect(results.filter((r) => r.status === "rejected")).toHaveLength(1);
        const winner =
            results[0].status === "fulfilled" ? firstData : secondData,
          loser = winner === firstData ? secondData : firstData;
        expect(
          await first
            .prepare(
              "SELECT source_hash,complete FROM lcm_import_state WHERE id=1"
            )
            .get()
        ).toEqual({ source_hash: exportHash(winner), complete: 1 });
        expect((await importSqliteExport(first, winner)).sourceHash).toBe(
          exportHash(winner)
        );
        await expect(importSqliteExport(second, loser)).rejects.toThrow(
          /Different source/
        );
        expect(
          await new ConversationStore(first).getMessageCount(
            conversation.conversationId
          )
        ).toBe(1);
      } finally {
        spies.forEach((spy) => spy.mockRestore());
        await Promise.all([first.close(), second.close()]);
      }
    }, 30000);
    it("never overwrites a rollback destination that appears during export", async () => {
      const destination = join(dir, "raced-destination.db"),
        sentinel = "existing synthetic destination must survive";
      const original = db.prepare.bind(db);
      let inserted = false;
      const spy = vi.spyOn(db, "prepare").mockImplementation((sql) => {
        const statement = original(sql);
        if (!/FROM "conversations"/.test(sql)) return statement;
        return {
          ...statement,
          all: async (...params) => {
            if (!inserted) {
              inserted = true;
              writeFileSync(destination, sentinel);
            }
            return statement.all(...params);
          },
        };
      });
      try {
        await expect(
          exportPostgresToSqlite(db, destination)
        ).rejects.toMatchObject({ code: "EEXIST" });
      } finally {
        spy.mockRestore();
      }
      expect(inserted).toBe(true);
      expect(readFileSync(destination, "utf8")).toBe(sentinel);
      expect(existsSync(destination + ".partial")).toBe(true);
    });
  }
);
