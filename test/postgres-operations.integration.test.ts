import { inspectRuntimePrivileges } from "../src/db/postgres/privileges.js";
import { runPostgresCli } from "../src/cli/postgres.js";
import { runLcmMigrations } from "../src/db/migration.js";
import { ConversationStore } from "../src/store/conversation-store.js";
import { SummaryStore } from "../src/store/summary-store.js";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
vi.mock("openclaw/plugin-sdk/core", () => ({
  buildMemorySystemPromptAddition: () => undefined,
}));
vi.mock("openclaw/plugin-sdk/logging-core", () => ({
  redactSensitiveText: (text: string) => text,
}));
vi.mock("openclaw/plugin-sdk/session-transcript-runtime", () => ({}));
import { randomBytes, createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { DatabaseSync } from "node:sqlite";
import pg from "pg";
import {
  PostgresDatabase,
  provisionPostgres,
  runtimeRoleForOrganization,
} from "../src/db/postgres/database.js";
import {
  createMigrationBundle,
  verifyMigrationBundle,
} from "../src/db/postgres/bundle.js";
import {
  exportSqlite,
  exportHash,
  importSqliteExport,
  exportNativePostgres,
  writeSqliteExport,
} from "../src/db/postgres/transfer.js";
import { inspectPostgres } from "../src/db/postgres/doctor.js";
import { LcmContextEngine } from "../src/engine.js";
import { createTestConfig, createTestDeps } from "./helpers.js";

describe.skipIf(process.env.LCM_DISPOSABLE_POSTGRES !== "1")(
  "operational PostgreSQL cutover on synthetic copies",
  () => {
    const suffix = randomBytes(5).toString("hex"),
      role = `lcm_ops_${suffix}`;
    let directory: string, pool: pg.Pool;
    beforeAll(async () => {
      directory = await mkdtemp(path.join(tmpdir(), "lcm-ops-"));
      pool = new pg.Pool({ max: 1 });
      await pool.query(
        `CREATE ROLE "${role}" LOGIN; CREATE ROLE "${runtimeRoleForOrganization(
          "org1"
        )}" LOGIN; CREATE ROLE "${runtimeRoleForOrganization("org2")}" LOGIN`
      );
    });
    afterAll(async () => {
      await pool?.end();
      if (directory) await rm(directory, { recursive: true, force: true });
    });
    it("splits both fixture organizations, imports six agent stores under a limited role, verifies engine and backup/rollback parity", async () => {
      const fixturePath =
        process.env.LCM_SYNTHETIC_FIXTURE_MANIFEST ??
        "/tmp/lossless-claw-postgres-fixture/fixture-manifest.json";
      const manifest = JSON.parse(await readFile(fixturePath, "utf8")),
        bytes = await readFile(manifest.fixturePath);
      expect(createHash("sha256").update(bytes).digest("hex")).toBe(
        manifest.sha256
      );
      expect(manifest.seed).toBe(20261104);
      const fixture = JSON.parse(bytes.toString("utf8"));
      const { restoreSqlite } = await import(
        path.resolve("experiments/postgres/transfer.mjs")
      );
      const reports = [];
      for (const org of fixture.organizations) {
        const source = restoreSqlite(
          org,
          path.join(directory, `${org.organizationId}-source.db`)
        ) as DatabaseSync;
        const root = path.join(directory, `${org.organizationId}-source-files`);
        await mkdir(root);
        for (const row of source
          .prepare("SELECT file_id,storage_uri,byte_size FROM large_files")
          .all()) {
          const file = path.join(
            root,
            createHash("sha256").update(String(row.file_id)).digest("hex")
          );
          const data = Buffer.from(
            (
              `SYNTHETIC-${org.organizationId}-${row.file_id}-` +
              "x".repeat(Number(row.byte_size))
            ).slice(0, Number(row.byte_size))
          );
          await writeFile(file, data);
          source
            .prepare("UPDATE large_files SET storage_uri=? WHERE file_id=?")
            .run(file, row.file_id);
        }
        const exported = exportSqlite(source, { requireKnownTables: true });
        source.close();
        const destination = path.join(
          directory,
          `${org.organizationId}-bundle`
        );
        const prepared = await createMigrationBundle({
          data: exported,
          organizationId: org.organizationId,
          sourcePayloadRoot: root,
          destination,
        });
        const verified = await verifyMigrationBundle(
          destination,
          prepared.manifestHash,
          org.organizationId
        );
        expect(verified.agents.size).toBe(3);
        const runtimeRole = runtimeRoleForOrganization(org.organizationId);
        for (const [agentId, data] of verified.agents) {
          const schema = `lcm_${suffix}_${org.organizationId}_${agentId}`;
          const options = {
            schema,
            organizationId: org.organizationId,
            agentId,
          };
          await provisionPostgres(options, {
            runtimeRole,
            forImport: true,
          });
          await expect(PostgresDatabase.open(options)).rejects.toThrow(
            /incomplete/
          );
          const importer = await PostgresDatabase.open(options, {
            allowIncompleteImport: true,
          });
          try {
            await importSqliteExport(importer, data, { chunkRows: 250 });
            expect(exportHash(await exportNativePostgres(importer))).toBe(
              exportHash(data)
            );
          } finally {
            await importer.close();
          }
          // Direct role authentication; live pooler runtime-role gateway authentication is a separate runner proof.
          const runtime = await PostgresDatabase.open({
            ...options,
            pool: {
              user: runtimeRole,
              host: process.env.LCM_DIRECT_PGHOST ?? process.env.PGHOST,
              port: Number(process.env.LCM_DIRECT_PGPORT ?? process.env.PGPORT),
            },
          });
          try {
            const health = await inspectPostgres(runtime);
            expect(health.warnings).toEqual([]);
            expect(health.ok).toBe(true);
            expect(health.counts?.messages).toBe(1200);
            await expect(
              runtime
                .prepare("UPDATE lcm_store_identity SET organization_id=?")
                .run("foreign")
            ).rejects.toHaveProperty("code", "42501");
            await expect(
              runtime.prepare("UPDATE lcm_import_state SET complete=0").run()
            ).rejects.toHaveProperty("code", "42501");
            await expect(
              runtime.exec(`CREATE TABLE "${schema}".forbidden(value int)`)
            ).rejects.toHaveProperty("code", "42501");
            const deps = createTestDeps(
              createTestConfig(path.join(directory, "unused.db"), {
                storageBackend: "postgres",
                postgresSchema: schema,
                postgresOrganizationId: org.organizationId,
                postgresAgentId: agentId,
                largeFilesDir: verified.manifest.agents.find(
                  (a) => a.agentId === agentId
                )!.largeFilesDir,
              })
            );
            const engine = new LcmContextEngine(deps, runtime);
            const sessionId = `${org.organizationId}-${agentId}-thread1`,
              sessionKey = `agent:${agentId}:${sessionId}`;
            const assembled = await engine.assemble({
              sessionId,
              sessionKey,
              messages: [],
              tokenBudget: 100000,
            });
            expect(assembled.messages.length).toBeGreaterThan(0);
            const store = engine.getConversationStore(),
              c = (await store.getConversationBySessionKey(sessionKey))!;
            await store.withTransaction(async () => {
              const message = await store.createMessage({
                conversationId: c.conversationId,
                seq: 201,
                role: "user",
                content: `SYNTHETIC-POST-CUTOVER-${agentId}`,
                tokenCount: 8,
              });
              await engine
                .getSummaryStore()
                .appendContextMessages(c.conversationId, [message.messageId]);
            });
            const backupData = await exportNativePostgres(runtime),
              backupDir = path.join(
                directory,
                `${org.organizationId}-${agentId}-backup`
              );
            const backup = await createMigrationBundle({
              data: backupData,
              organizationId: org.organizationId,
              sourcePayloadRoot: destination,
              destination: backupDir,
            });
            const backupVerified = await verifyMigrationBundle(
                backupDir,
                backup.manifestHash,
                org.organizationId
              ),
              rollbackData = backupVerified.agents.get(agentId)!;
            const rollback = path.join(
              directory,
              `${org.organizationId}-${agentId}-rollback.db`
            );
            writeSqliteExport(rollbackData, rollback);
            const sqlite = new DatabaseSync(rollback, { readOnly: true });
            try {
              expect(exportHash(exportSqlite(sqlite))).toBe(
                exportHash(rollbackData)
              );
              expect(
                sqlite.prepare("SELECT COUNT(*) AS count FROM messages").get()
                  ?.count
              ).toBe(1201);
              expect(sqlite.prepare("PRAGMA foreign_key_check").all()).toEqual(
                []
              );
            } finally {
              sqlite.close();
            }
            reports.push({
              organizationId: org.organizationId,
              agentId,
              importedMessages: 1200,
              afterCutoverMessages: 1201,
              importHash: exportHash(data),
              backupHash: backup.manifestHash,
              rollbackHash: exportHash(rollbackData),
              files: backupVerified.manifest.agents[0].files.length,
            });
          } finally {
            await runtime.close();
          }
        }
      }
      const denied = new pg.Pool({
        user: runtimeRoleForOrganization("org1"),
        host: process.env.PGHOST,
        port: Number(process.env.PGPORT),
        database: process.env.PGDATABASE,
        max: 1,
      });
      try {
        await expect(
          denied.query(`SELECT * FROM "lcm_${suffix}_org2_agent1".messages`)
        ).rejects.toHaveProperty("code", "42501");
      } finally {
        await denied.end();
      }
      await expect(
        provisionPostgres(
          {
            schema: `lcm_cross_role_${suffix}`,
            organizationId: "org2",
            agentId: "one",
          },
          { runtimeRole: runtimeRoleForOrganization("org1") }
        )
      ).rejects.toThrow(/organization/);
      const runtimeIdentity = {
        schema: `lcm_${suffix}_org1_agent1`,
        organizationId: "org1",
        agentId: "agent1",
        pool: {
          user: runtimeRoleForOrganization("org1"),
          host: process.env.PGHOST,
          port: Number(process.env.PGPORT),
        },
      };
      const elevated = await PostgresDatabase.open(runtimeIdentity);
      try {
        await pool.query(
          `GRANT pg_read_all_data TO "${runtimeRoleForOrganization("org1")}"`
        );
        expect((await inspectRuntimePrivileges(elevated)).safe).toBe(false);
        await pool.query(
          `REVOKE pg_read_all_data FROM "${runtimeRoleForOrganization("org1")}"`
        );
        await pool.query(
          `CREATE TABLE public.synthetic_native_${suffix}(value text); GRANT SELECT ON public.synthetic_native_${suffix} TO "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
        const privileges = await inspectRuntimePrivileges(elevated);
        expect(privileges.safe).toBe(false);
        expect(privileges.unrelatedData).toHaveLength(1);
        await pool.query(
          `REVOKE SELECT ON public.synthetic_native_${suffix} FROM "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
        await pool.query(
          `GRANT SELECT(value) ON public.synthetic_native_${suffix} TO "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
        expect((await inspectRuntimePrivileges(elevated)).safe).toBe(false);
        expect((await inspectPostgres(elevated)).ok).toBe(false);
        await expect(
          provisionPostgres(
            {
              schema: `lcm_column_denied_${suffix}`,
              organizationId: "org1",
              agentId: "one",
            },
            { runtimeRole: runtimeRoleForOrganization("org1") }
          )
        ).rejects.toThrow();
        await pool.query(
          `REVOKE SELECT(value) ON public.synthetic_native_${suffix} FROM "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
        await pool.query(
          `GRANT CREATE ON SCHEMA public TO "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
        const ddl = await inspectRuntimePrivileges(elevated);
        expect(ddl.safe).toBe(false);
        expect(ddl.ddlNamespaces).toHaveLength(1);
        await pool.query(
          `REVOKE CREATE ON SCHEMA public FROM "${runtimeRoleForOrganization(
            "org1"
          )}"`
        );
      } finally {
        await elevated.close();
      }
      await writeFile(
        path.resolve(
          "experiments/postgres/.generated/native/operations-cutover.json"
        ),
        JSON.stringify(
          {
            seed: 20261104,
            sourceFixtureSha256: manifest.sha256,
            environment:
              "disposable local PostgreSQL; synthetic payload derivative; no inference",
            production: false,
            reports,
          },
          null,
          2
        ) + "\n"
      );
    }, 180000);
    it("rejects privileged or inherited runtime roles and leaves no half-provisioned schema", async () => {
      const schema = `lcm_role_reject_${suffix}`;
      await expect(
        provisionPostgres(
          { schema, organizationId: "org", agentId: "one" },
          { runtimeRole: "postgres" }
        )
      ).rejects.toThrow();
      expect(
        (
          await pool.query("SELECT 1 FROM pg_namespace WHERE nspname=$1", [
            schema,
          ])
        ).rows
      ).toEqual([]);
      const privilegedOrg = `privileged-${suffix}`,
        privilegedRole = runtimeRoleForOrganization(privilegedOrg);
      await pool.query(`CREATE ROLE "${privilegedRole}" SUPERUSER NOLOGIN`);
      await expect(
        provisionPostgres(
          { schema, organizationId: privilegedOrg, agentId: "one" },
          { runtimeRole: privilegedRole }
        )
      ).rejects.toThrow(/privileged/);
      const membershipOrg = `membership-${suffix}`,
        membershipRole = runtimeRoleForOrganization(membershipOrg);
      await pool.query(
        `CREATE ROLE "${membershipRole}" LOGIN; CREATE ROLE "lcm_parent_${suffix}"; GRANT "lcm_parent_${suffix}" TO "${membershipRole}"`
      );
      await expect(
        provisionPostgres(
          { schema, organizationId: membershipOrg, agentId: "one" },
          { runtimeRole: membershipRole }
        )
      ).rejects.toThrow(/memberships/);
      expect(
        (
          await pool.query("SELECT 1 FROM pg_namespace WHERE nspname=$1", [
            schema,
          ])
        ).rows
      ).toEqual([]);
    });
    it("keeps activation closed if a copied payload changes during the real CLI import, then resumes safely", async () => {
      const source = new DatabaseSync(":memory:");
      runLcmMigrations(source);
      const conversations = new ConversationStore(source),
        summaries = new SummaryStore(source);
      const c = await conversations.createConversation({
        sessionId: "gate",
        sessionKey: "agent:one:gate",
      });
      await conversations.createMessage({
        conversationId: c.conversationId,
        seq: 1,
        role: "user",
        content: "synthetic gate",
        tokenCount: 4,
      });
      const payloadRoot = path.join(directory, "gate-source-files");
      await mkdir(payloadRoot);
      const payload = Buffer.from("SYNTHETIC-ORIGINAL-PAYLOAD"),
        file = path.join(payloadRoot, "source.txt");
      await writeFile(file, payload);
      await summaries.insertLargeFile({
        fileId: "file_gate",
        conversationId: c.conversationId,
        storageUri: file,
        byteSize: payload.length,
      });
      const data = exportSqlite(source);
      source.close();
      const bundle = path.join(directory, "gate-bundle");
      const prepared = await createMigrationBundle({
        data,
        organizationId: "gate-org",
        sourcePayloadRoot: payloadRoot,
        destination: bundle,
      });
      const verified = await verifyMigrationBundle(
          bundle,
          prepared.manifestHash,
          "gate-org"
        ),
        copied = path.join(
          bundle,
          verified.manifest.agents[0].files[0].relativePath
        );
      const options = {
        schema: `lcm_gate_${suffix}`,
        organizationId: "gate-org",
        agentId: "one",
      };
      await provisionPostgres(options, { forImport: true });
      const actual = PostgresDatabase.open.bind(PostgresDatabase);
      let changed = false;
      const spy = vi
        .spyOn(PostgresDatabase, "open")
        .mockImplementation(async (options, maintenance) => {
          const db = await actual(options, maintenance),
            prepare = db.prepare.bind(db);
          vi.spyOn(db, "prepare").mockImplementation((sql) => {
            const statement = prepare(sql);
            if (!sql.includes("INSERT INTO lcm_import_chunks"))
              return statement;
            return {
              ...statement,
              run: async (...params) => {
                const result = await statement.run(...params);
                if (!changed) {
                  changed = true;
                  await writeFile(copied, "SYNTHETIC-CORRUPTED-COPY");
                }
                return result;
              },
            };
          });
          return db;
        });
      const env = {
        ...process.env,
        LCM_POSTGRES_ORGANIZATION_ID: options.organizationId,
        LCM_POSTGRES_AGENT_ID: options.agentId,
        LCM_POSTGRES_SCHEMA: options.schema,
      };
      try {
        await expect(
          runPostgresCli(["import", bundle, prepared.manifestHash], env)
        ).rejects.toThrow(/payload hash/);
      } finally {
        spy.mockRestore();
      }
      expect(changed).toBe(true);
      await expect(PostgresDatabase.open(options)).rejects.toThrow(
        /incomplete/
      );
      expect(await readFile(file)).toEqual(payload);
      await writeFile(copied, payload);
      expect(
        await runPostgresCli(["import", bundle, prepared.manifestHash], env)
      ).toHaveProperty("verifiedPayloads", true);
      const ready = await PostgresDatabase.open(options);
      await ready.close();
    });
    it("preserves deleted allocator high-water through real PostgreSQL import and SQLite rollback", async () => {
      const sqlite = new DatabaseSync(":memory:");
      runLcmMigrations(sqlite);
      sqlite.exec(
        "INSERT INTO conversations(conversation_id,session_id,session_key) VALUES(100,'deleted','agent:one:deleted'); DELETE FROM conversations WHERE conversation_id=100; INSERT INTO conversations(conversation_id,session_id,session_key) VALUES(1,'kept','agent:one:kept'); INSERT INTO messages(message_id,conversation_id,seq,role,content,token_count) VALUES(500,1,1,'user','synthetic deleted',1); DELETE FROM messages WHERE message_id=500"
      );
      const data = exportSqlite(sqlite);
      sqlite.close();
      const options = {
        schema: `lcm_high_${suffix}`,
        organizationId: "high-org",
        agentId: "one",
      };
      await provisionPostgres(options, { forImport: true });
      const db = await PostgresDatabase.open(options, {
        allowIncompleteImport: true,
      });
      try {
        await importSqliteExport(db, data);
        expect(exportHash(await exportNativePostgres(db))).toBe(
          exportHash(data)
        );
        const store = new ConversationStore(db);
        expect(
          (
            await store.createConversation({
              sessionId: "next",
              sessionKey: "agent:one:next",
            })
          ).conversationId
        ).toBe(101);
        expect(
          (
            await store.createMessage({
              conversationId: 1,
              seq: 1,
              role: "user",
              content: "synthetic next",
              tokenCount: 1,
            })
          ).messageId
        ).toBe(501);
        const output = path.join(directory, "highwater-rollback.db"),
          backup = await exportNativePostgres(db);
        writeSqliteExport(backup, output);
        const restored = new DatabaseSync(output);
        try {
          expect(
            restored
              .prepare(
                "INSERT INTO messages(conversation_id,seq,role,content,token_count) VALUES(1,2,'assistant','synthetic next again',1)"
              )
              .run().lastInsertRowid
          ).toBe(502);
        } finally {
          restored.close();
        }
      } finally {
        await db.close();
      }
    });
  }
);
