import { storageLifecycle } from "./storage-lifecycle.js";
import { PostgresDatabase } from "../db/postgres/database.js";
import { LcmContextEngine } from "../engine.js";
import type { LcmDependencies } from "../types.js";
import type { SharedLcmInit } from "./shared-init.js";

/** Lazy asynchronous gateway initialization; never provisions or migrates the cluster. */
export function createPostgresSharedInit(
  deps: LcmDependencies,
  enabled: boolean
): SharedLcmInit {
  let db: PostgresDatabase | undefined;
  let engine: LcmContextEngine | null = null;
  let initializing: Promise<LcmContextEngine> | undefined;
  let users = 0;
  const shared: SharedLcmInit = {
    stopped: false,
    storageBackend: "postgres",
    getCachedEngine: () => (shared.stopped ? null : engine),
    waitForEngine() {
      if (!enabled)
        return Promise.reject(
          new Error(
            "LCM database initialization is disabled during read-only plugin registration"
          )
        );
      if (shared.stopped)
        return Promise.reject(new Error("LCM PostgreSQL engine stopped"));
      if (initializing) return initializing;
      const attempt = (async () => {
        db = await PostgresDatabase.open({
          schema: deps.config.postgresSchema!,
          organizationId: deps.config.postgresOrganizationId!,
          agentId: deps.config.postgresAgentId!,
        });
        if (shared.stopped) {
          const stoppedDb = db;
          db = undefined;
          await stoppedDb.close();
          throw new Error(
            "LCM PostgreSQL stopped during connection initialization"
          );
        }
        try {
          engine = new LcmContextEngine(deps, db);
          return engine;
        } catch (error) {
          await db.close();
          db = undefined;
          throw error;
        }
      })();
      initializing = attempt;
      attempt.catch(() => {
        if (initializing === attempt) initializing = undefined;
      });
      return attempt;
    },
    async waitForPostgresDatabase() {
      await shared.waitForEngine();
      return db!;
    },
    async waitForDatabase() {
      throw new Error(
        "SQLite explorer, doctor, backup and CLI commands are unavailable on the PostgreSQL backend"
      );
    },
    retain() {
      users++;
      let released = false;
      return async () => {
        if (released) return;
        released = true;
        if (--users === 0) await shared.close?.();
      };
    },
    close: () => lifecycle.close(),
    restart: () => (enabled ? lifecycle.restart() : Promise.resolve()),
  };
  const lifecycle = storageLifecycle({
    stopped: () => shared.stopped,
    setStopped: (value) => {
      shared.stopped = value;
    },
    async close() {
      if (initializing) await initializing.catch(() => undefined);
      const current = db;
      db = undefined;
      engine = null;
      initializing = undefined;
      await current?.close();
    },
    async restart() {
      initializing = undefined;
    },
  });
  return shared;
}
