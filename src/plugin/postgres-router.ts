import { storageLifecycle } from "./storage-lifecycle.js";
import type { ContextEngine } from "../openclaw-bridge.js";
import type { LcmDependencies } from "../types.js";
import type { LcmContextEngine } from "../engine.js";
import { validatePostgresSchema } from "../db/postgres/database.js";
import { createPostgresSharedInit } from "./postgres-init.js";
import type { SharedLcmInit } from "./shared-init.js";

type Actor = Parameters<LcmContextEngine["assertStorageAuthority"]>[0];
/** Explicit agent-to-schema routing. Tool arguments never select a schema. */
export function createPostgresRouter(
  deps: LcmDependencies,
  enabled: boolean
): SharedLcmInit {
  const entries = Object.entries(deps.config.postgresSchemas ?? {});
  if (entries.length < 1 || entries.length > 8)
    throw new Error("PostgreSQL routing requires 1..8 explicit agent schemas");
  if (deps.config.postgresAgentId || deps.config.postgresSchema)
    throw new Error(
      "Use postgresSchemas or the single-agent PostgreSQL settings, not both"
    );
  if (!deps.config.postgresOrganizationId?.trim())
    throw new Error("PostgreSQL routing requires an organization identity");
  const schemas = new Set<string>();
  const children = new Map<string, SharedLcmInit>();
  for (const [agentId, schema] of entries) {
    if (
      !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/.test(agentId) ||
      typeof schema !== "string"
    )
      throw new Error("Invalid PostgreSQL agent-to-schema mapping");
    validatePostgresSchema(schema);
    if (schemas.has(schema))
      throw new Error("Each PostgreSQL agent must have a distinct schema");
    schemas.add(schema);
    children.set(
      agentId,
      createPostgresSharedInit(
        {
          ...deps,
          config: {
            ...deps.config,
            postgresSchemas: undefined,
            postgresAgentId: agentId,
            postgresSchema: schema,
          },
        },
        enabled
      )
    );
  }
  let contextEngine: Promise<ContextEngine> | undefined,
    users = 0;
  const scoped = async (actor: Actor): Promise<LcmContextEngine> => {
    if (shared.stopped) throw new Error("LCM PostgreSQL router stopped");
    const key =
      actor.sessionKey ??
      actor.parentSessionKey ??
      actor.childSessionKey ??
      actor.sessionTarget?.sessionKey ??
      actor.admission?.sessionKey;
    const agentId =
      actor.agentId ??
      actor.sessionTarget?.agentId ??
      actor.admission?.agentId ??
      /^agent:([^:]+):/.exec(key ?? "")?.[1];
    const child = agentId ? children.get(agentId) : undefined;
    if (!child)
      throw new Error(
        "PostgreSQL requires a configured agent identity for this gateway operation"
      );
    const engine = await child.waitForEngine();
    engine.assertStorageAuthority(actor);
    return engine;
  };
  const shared: SharedLcmInit = {
    storageBackend: "postgres",
    stopped: false,
    getCachedEngine: () => null,
    async waitForEngine() {
      if (entries.length !== 1)
        throw new Error(
          "An agent identity is required for the routed PostgreSQL engine"
        );
      return children.values().next().value!.waitForEngine();
    },
    waitForScopedEngine: scoped,
    async waitForPostgresDatabase(actor) {
      if (!actor)
        throw new Error(
          "An agent identity is required for PostgreSQL commands"
        );
      const engine = await scoped(actor);
      const child = children.get(engine.configuredPostgresAgentId!)!;
      return child.waitForPostgresDatabase!();
    },
    async waitForDatabase() {
      throw new Error("SQLite inspection is unavailable on PostgreSQL");
    },
    waitForContextEngine() {
      if (shared.stopped)
        return Promise.reject(new Error("LCM PostgreSQL router stopped"));
      if (contextEngine) return contextEngine;
      const attempt = (async () => {
        const first = await children.values().next().value!.waitForEngine();
        return {
          info: first.info,
          ingest: async (params) => (await scoped(params)).ingest(params),
          ingestBatch: async (params) =>
            (await scoped(params)).ingestBatch(params),
          bootstrap: async (params) => (await scoped(params)).bootstrap(params),
          assemble: async (params) => (await scoped(params)).assemble(params),
          compact: async (params) =>
            (await scoped(params)).compact({
              ...params,
              sessionFile: params.sessionFile ?? "",
            }),
          commitTurn: async (params) =>
            (await scoped(params)).commitTurn(params),
          afterTurn: async (params) => (await scoped(params)).afterTurn(params),
          maintain: async (params) => (await scoped(params)).maintain(params),
          prepareSubagentSpawn: async (params) =>
            (await scoped(params)).prepareSubagentSpawn({
              ...params,
              parentSessionKey: params.parentSessionKey ?? "",
            }),
          onSubagentEnded: async (params) =>
            (await scoped(params)).onSubagentEnded({
              ...params,
              reason: params.reason ?? "",
            }),
          control: async (params) => (await scoped(params)).control(params),
          getControlCapabilities: () => ({
            status: true,
            doctor: false,
            rotate: false,
          }),
          async dispose() {
            /* The registration owns the agent pools across runs. */
          },
        } satisfies ContextEngine;
      })();
      contextEngine = attempt;
      attempt.catch(() => {
        if (contextEngine === attempt) contextEngine = undefined;
      });
      return attempt;
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
      await Promise.all([...children.values()].map((child) => child.close!()));
      contextEngine = undefined;
    },
    async restart() {
      await Promise.all(
        [...children.values()].map((child) => child.restart!())
      );
      contextEngine = undefined;
    },
  });
  return shared;
}
