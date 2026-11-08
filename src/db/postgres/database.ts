import { AsyncLocalStorage } from "node:async_hooks";
import pg, { type PoolClient, type PoolConfig, type QueryResult } from "pg";
import type { SQLInputValue, StatementResultingChanges } from "node:sqlite";
import type { AsyncStorageDatabase } from "../storage.js";
import { compilePostgresStatement, POSTGRES_FUNCTIONS } from "./dialect.js";
import { POSTGRES_SCHEMA_V1 } from "./schema.js";

export type PostgresOptions = {
  /** One agent's schema. The login role must have access to this schema only. */
  schema: string;
  organizationId: string;
  agentId: string;
  pool?: PoolConfig;
};
function safeInteger(value: string): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number))
    throw new Error("LCM PostgreSQL integer exceeds JavaScript safe range");
  return number;
}
export function validatePostgresSchema(schema: string): string {
  if (!/^lcm_[a-z0-9_]{1,55}$/.test(schema))
    throw new Error(
      "LCM PostgreSQL requires an explicit lcm_ prefixed agent schema"
    );
  return schema;
}
function createPool(options: PostgresOptions): pg.Pool {
  validatePostgresSchema(options.schema);
  if (
    !options.organizationId?.trim() ||
    !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/.test(options.agentId ?? "")
  )
    throw new Error("LCM PostgreSQL requires organization and agent identity");
  const max = options.pool?.max ?? 4;
  if (!Number.isInteger(max) || max < 1 || max > 8)
    throw new Error("LCM PostgreSQL pool must have 1..8 connections");
  const pool = new pg.Pool({
    ...options.pool,
    max,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
    // Local parsers, never mutate pg's process-wide type registry.
    types: {
      getTypeParser: (oid: number, format?: string) => {
        if (oid === 20 || oid === 1700) return safeInteger;
        if (oid === 16) return (value: string) => (value === "t" ? 1 : 0);
        return pg.types.getTypeParser(oid, format as "text");
      },
    },
  });
  pool.on("connect", (client) =>
    client.on("error", () => {
      /* Active query/commit promises carry connection failures; avoid a second unhandled socket error. */
    })
  );
  pool.on("error", () => {
    /* pg discards failed idle clients; active operations receive errors. */
  });
  return pool;
}

/** Native async store driver. Every transaction owns a checked-out client. */
export class PostgresDatabase implements AsyncStorageDatabase {
  readonly dialect = "postgres" as const;
  readonly ready: boolean;
  private readonly pool: pg.Pool;
  private readonly context = new AsyncLocalStorage<{
    client: PoolClient;
    active: boolean;
  }>();
  private nextSavepoint = 0;
  private constructor(
    readonly schema: string,
    readonly organizationId: string,
    readonly agentId: string,
    pool: pg.Pool,
    ready: boolean
  ) {
    this.pool = pool;
    this.ready = ready;
  }

  static async open(
    options: PostgresOptions,
    maintenance: { allowIncompleteImport?: boolean } = {}
  ): Promise<PostgresDatabase> {
    const pool = createPool(options);
    try {
      const primary = await pool.query(
        "SELECT pg_is_in_recovery() AS replica, current_setting('transaction_read_only') AS read_only"
      );
      if (primary.rows[0].replica !== 0 || primary.rows[0].read_only !== "off")
        throw new Error(
          "LCM PostgreSQL requires a writable primary endpoint; replica reads are unsupported"
        );
      const { rows } = await pool.query(
        `SELECT version FROM "${options.schema}".lcm_schema_version`
      );
      if (rows.length !== 1 || rows[0].version !== 1)
        throw new Error("Unsupported LCM PostgreSQL schema version");
      const imported = await pool.query(
        `SELECT complete FROM "${options.schema}".lcm_import_state WHERE id=1`
      );
      if (
        imported.rows.length !== 1 ||
        (imported.rows[0].complete !== 1 && !maintenance.allowIncompleteImport)
      )
        throw new Error(
          "LCM PostgreSQL import is incomplete; gateway activation denied"
        );
      const identity = await pool.query(
        `SELECT organization_id,agent_id FROM "${options.schema}".lcm_store_identity`
      );
      if (
        identity.rows.length !== 1 ||
        identity.rows[0].organization_id !== options.organizationId ||
        identity.rows[0].agent_id !== options.agentId
      )
        throw new Error("LCM PostgreSQL schema identity mismatch");
      return new PostgresDatabase(
        options.schema,
        options.organizationId,
        options.agentId,
        pool,
        imported.rows[0].complete === 1
      );
    } catch (error) {
      await pool.end();
      throw error;
    }
  }

  prepare(sql: string) {
    const compiled = compilePostgresStatement(sql, this.schema);
    return {
      get: async (...params: SQLInputValue[]) =>
        (await this.query(compiled, params)).rows[0],
      all: async (...params: SQLInputValue[]) =>
        (await this.query(compiled, params)).rows,
      run: async (
        ...params: SQLInputValue[]
      ): Promise<StatementResultingChanges> => {
        if (/^\s*INSERT\s+INTO\s+"[^"]+"\.conversations\b/i.test(compiled)) {
          const columns = /\.conversations\s*\(([^)]+)\)/i
            .exec(compiled)?.[1]
            .split(",")
            .map((column) => column.trim());
          const key = columns
            ? params[columns.indexOf("session_key")]
            : undefined;
          if (
            typeof key !== "string" ||
            !key.startsWith(`agent:${this.agentId}:`)
          )
            throw new Error(
              "LCM conversation outside PostgreSQL agent authority"
            );
        }
        const insert =
          /^\s*INSERT\s+INTO\s+"[^"]+"\.(conversations|messages|conversation_transcript_gaps)\b/i.exec(
            compiled
          );
        const ids: Record<string, string> = {
          conversations: "conversation_id",
          messages: "message_id",
          conversation_transcript_gaps: "gap_id",
        };
        const returning = insert ? ` RETURNING ${ids[insert[1]]}` : "";
        const recoverable =
          /^(?:\s*INSERT\s+INTO\s+"[^"]+"\.(?:conversations|messages)\b|\s*UPDATE\s+"[^"]+"\.messages\s+SET\s+transcript_entry_id)/i.test(
            compiled
          );
        const result = await this.query(
          compiled + returning,
          params,
          recoverable
        );
        return {
          changes: result.rowCount ?? 0,
          lastInsertRowid: insert ? result.rows[0]?.[ids[insert[1]]] ?? 0 : 0,
        };
      },
    };
  }
  private async query(
    sql: string,
    params: SQLInputValue[] = [],
    recoverable = false
  ): Promise<QueryResult> {
    // Anonymous extended queries: no named prepared statements or session affinity.
    const scope = this.context.getStore();
    if (scope && !scope.active)
      throw new Error("LCM PostgreSQL transaction scope is already closed");
    if (recoverable && scope)
      return this.transaction(() => this.query(sql, params));
    try {
      return await (scope?.client ?? this.pool).query(sql, params);
    } catch (error) {
      // Preserve native duplicate-handling semantics while retaining PostgreSQL SQLSTATE.
      if (error instanceof Error && "code" in error && error.code === "23505")
        error.message = `UNIQUE constraint failed: ${error.message}`;
      throw error;
    }
  }
  async exec(sql: string): Promise<void> {
    await this.query(compilePostgresStatement(sql, this.schema));
  }

  async transaction<T>(operation: () => Promise<T> | T): Promise<T> {
    const scope = this.context.getStore();
    if (scope && !scope.active)
      throw new Error("LCM PostgreSQL transaction scope is already closed");
    const existing = scope?.client;
    if (existing) {
      const name = `lcm_sp_${++this.nextSavepoint}`;
      await existing.query(`SAVEPOINT ${name}`);
      try {
        const result = await operation();
        await existing.query(`RELEASE SAVEPOINT ${name}`);
        return result;
      } catch (error) {
        await existing.query(`ROLLBACK TO SAVEPOINT ${name}`);
        await existing.query(`RELEASE SAVEPOINT ${name}`);
        throw error;
      }
    }
    const client = await this.pool.connect();
    const owner = { client, active: true };
    let started = false,
      destroy = false;
    try {
      // SERIALIZABLE protects multi-process graph/context updates. Conflicts require whole-operation retry.
      await client.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
      started = true;
      await client.query(
        "SET LOCAL TIME ZONE 'UTC'; SET LOCAL statement_timeout = '10s'; SET LOCAL lock_timeout = '2s'; SET LOCAL idle_in_transaction_session_timeout = '30s'"
      );
      const result = await this.context.run(owner, operation);
      const committed = await client.query("COMMIT");
      started = false;
      if (committed.command !== "COMMIT")
        throw new Error(
          "LCM PostgreSQL transaction rolled back after a caught statement error"
        );
      return result;
    } catch (error) {
      if (started)
        try {
          await client.query("ROLLBACK");
        } catch {
          destroy = true;
        }
      throw error;
    } finally {
      owner.active = false;
      client.release(destroy);
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

/** Explicit provisioning only: the gateway never creates schemas or runs DDL. */
export async function provisionPostgres(
  options: PostgresOptions
): Promise<void> {
  const pool = createPool(options);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(`CREATE SCHEMA "${options.schema}"`);
    await client.query(
      `SET LOCAL search_path TO "${options.schema}", pg_catalog; SET LOCAL TIME ZONE 'UTC'`
    );
    await client.query(POSTGRES_FUNCTIONS);
    await client.query(POSTGRES_SCHEMA_V1);
    await client.query(
      `INSERT INTO "${options.schema}".lcm_store_identity VALUES($1,$2)`,
      [options.organizationId, options.agentId]
    );
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}
