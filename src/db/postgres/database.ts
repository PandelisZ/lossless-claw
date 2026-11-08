import {
  UNRELATED_DATA_PRIVILEGES,
  ROLE_DDL_NAMESPACES,
} from "./privileges.js";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { AsyncLocalStorage } from "node:async_hooks";
import pg, { type PoolClient, type PoolConfig, type QueryResult } from "pg";
import type { SQLInputValue, StatementResultingChanges } from "node:sqlite";
import type { AsyncStorageDatabase } from "../storage.js";
import { compilePostgresStatement, POSTGRES_FUNCTIONS } from "./dialect.js";
import { POSTGRES_SCHEMA_V1, POSTGRES_TABLES } from "./schema.js";

export type PostgresOptions = {
  /** One agent's schema. The login role must have access to this schema only. */
  schema: string;
  organizationId: string;
  agentId: string;
  pool?: PoolConfig;
};
/** Stable organization binding removes the check-then-grant race when two provisioners share a role name. */
export function runtimeRoleForOrganization(organizationId: string): string {
  if (!organizationId?.trim())
    throw new Error("Organization identity required");
  return `lcm_org_${createHash("sha256")
    .update(organizationId)
    .digest("hex")
    .slice(0, 32)}`;
}
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
/** Dedicated LCM credentials override PG defaults without changing native OpenClaw's environment. */
export function postgresEnvironment(
  env: NodeJS.ProcessEnv = process.env
): PoolConfig {
  const names = [
    "LCM_PGHOST",
    "LCM_PGDATABASE",
    "LCM_PGUSER",
    "LCM_PGPASSWORD",
    "LCM_PGPORT",
    "LCM_PGSSLMODE",
    "LCM_PGSSLROOTCERT",
  ];
  if (!names.some((name) => env[name] !== undefined)) {
    if (env.LCM_DISPOSABLE_POSTGRES !== "1")
      throw new Error(
        "Dedicated LCM PostgreSQL credentials are required; ambient native PostgreSQL credentials are not used"
      );
    if (!env.PGHOST || !env.PGDATABASE || !env.PGUSER || !env.PGPORT)
      throw new Error(
        "Disposable operator connections require an explicit PG endpoint"
      );
    return {
      host: env.PGHOST,
      database: env.PGDATABASE,
      user: env.PGUSER,
      port: Number(env.PGPORT),
      password: () => env.PGPASSWORD ?? "",
      ssl: false,
    };
  }
  if (
    !env.LCM_PGHOST?.trim() ||
    !env.LCM_PGDATABASE?.trim() ||
    !env.LCM_PGUSER?.trim()
  )
    throw new Error(
      "Dedicated LCM credentials require explicit host, database and user"
    );
  const port = Number(env.LCM_PGPORT ?? 5432);
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new Error("Invalid LCM PostgreSQL port");
  const result: PoolConfig = {
    host: env.LCM_PGHOST,
    database: env.LCM_PGDATABASE,
    user: env.LCM_PGUSER,
    port,
    // A function also prevents pg from falling back to a different service's PGPASSWORD or .pgpass.
    password: () => env.LCM_PGPASSWORD ?? "",
  };
  const mode = env.LCM_PGSSLMODE ?? "verify-full";
  if (mode === "verify-full") {
    if (!env.LCM_PGPASSWORD)
      throw new Error(
        "Dedicated LCM TLS credentials require their own password"
      );
    result.ssl = {
      rejectUnauthorized: true,
      ...(env.LCM_PGSSLROOTCERT
        ? { ca: readFileSync(env.LCM_PGSSLROOTCERT, "utf8") }
        : {}),
    };
  } else if (mode === "disable" && env.LCM_DISPOSABLE_POSTGRES === "1")
    result.ssl = false;
  else
    throw new Error(
      "LCM PostgreSQL requires verify-full TLS; disabling it is restricted to disposable tests"
    );
  return result;
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
  const requested = options.pool ?? {},
    disposable = process.env.LCM_DISPOSABLE_POSTGRES === "1";
  const fields = [
    "host",
    "database",
    "user",
    "password",
    "port",
    "ssl",
  ] as const;
  const allowed = new Set<string>([
    ...fields,
    "max",
    "connectionTimeoutMillis",
  ]);
  if (Object.keys(requested).some((key) => !allowed.has(key)))
    throw new Error("Unsupported LCM PostgreSQL pool option");
  const hasConnection = fields.some((key) => Object.hasOwn(requested, key));
  const complete =
    typeof requested.host === "string" &&
    !!requested.host.trim() &&
    typeof requested.database === "string" &&
    !!requested.database.trim() &&
    typeof requested.user === "string" &&
    !!requested.user.trim() &&
    requested.password !== undefined &&
    requested.password !== null &&
    requested.port !== undefined &&
    requested.ssl !== undefined;
  if (hasConnection && !complete && !disposable)
    throw new Error(
      "Partial LCM PostgreSQL connection overrides are unsupported"
    );
  let connection: PoolConfig;
  if (hasConnection) {
    if (complete)
      connection = {
        host: requested.host,
        database: requested.database,
        user: requested.user,
        password: requested.password,
        port: requested.port,
        ssl: requested.ssl,
      };
    else
      connection = {
        host: requested.host ?? process.env.PGHOST,
        database: requested.database ?? process.env.PGDATABASE,
        user: requested.user ?? process.env.PGUSER,
        port: requested.port ?? Number(process.env.PGPORT ?? 5432),
        password: requested.password ?? (() => process.env.PGPASSWORD ?? ""),
        ssl: requested.ssl ?? false,
      };
  } else connection = postgresEnvironment();
  if (Object.keys(connection).length) {
    if (
      !connection.host?.trim() ||
      !connection.database?.trim() ||
      !connection.user?.trim()
    )
      throw new Error(
        "Explicit LCM PostgreSQL connection identity is incomplete"
      );
    if (
      !Number.isInteger(connection.port) ||
      Number(connection.port) < 1 ||
      Number(connection.port) > 65535
    )
      throw new Error("Invalid explicit LCM PostgreSQL port");
    const password = connection.password;
    if (
      typeof password !== "function" &&
      (typeof password !== "string" || (!password && !disposable))
    )
      throw new Error("Explicit LCM PostgreSQL requires its own password");
    connection.password = async () => {
      let value;
      try {
        value = typeof password === "function" ? await password() : password;
      } catch {
        throw new Error("LCM credential provider failed");
      }
      if (typeof value !== "string" || (!value && !disposable))
        throw new Error("LCM credential provider returned no password");
      return value;
    };
    const ssl = connection.ssl;
    if (ssl === false && disposable) connection.ssl = false;
    else if (ssl === true) connection.ssl = { rejectUnauthorized: true };
    else if (
      ssl &&
      typeof ssl === "object" &&
      ssl.rejectUnauthorized !== false &&
      Object.keys(ssl).every((key) =>
        ["ca", "rejectUnauthorized"].includes(key)
      )
    )
      connection.ssl = {
        ...(ssl.ca ? { ca: ssl.ca } : {}),
        rejectUnauthorized: true,
      };
    else throw new Error("LCM PostgreSQL requires verified TLS");
  }
  const pool = new pg.Pool({
    ...connection,
    max,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
    query_timeout: 15_000,
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
  options: PostgresOptions,
  privileges: { runtimeRole?: string; forImport?: boolean } = {}
): Promise<void> {
  if (
    privileges.runtimeRole &&
    privileges.runtimeRole !==
      runtimeRoleForOrganization(options.organizationId)
  )
    throw new Error(
      "Runtime role must use the deterministic organization-specific name"
    );
  const pool = createPool(options);
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const runtimeRole = privileges.runtimeRole;
    if (runtimeRole) {
      if (!/^lcm_[a-z0-9_]{1,55}$/.test(runtimeRole))
        throw new Error("Runtime role must be an explicit lcm_ identifier");
      const result = await client.query(
        "SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1",
        [runtimeRole]
      );
      if (
        result.rows.length !== 1 ||
        Object.values(result.rows[0]).some(Boolean)
      )
        throw new Error(
          "Runtime role must already exist without privileged role attributes"
        );
      const ownership = await client.query(
        "SELECT 1 FROM pg_roles r WHERE r.rolname=$1 AND (r.rolname=current_user OR EXISTS(SELECT 1 FROM pg_shdepend d WHERE d.refclassid='pg_authid'::regclass AND d.refobjid=r.oid AND d.deptype='o'))",
        [runtimeRole]
      );
      if (ownership.rows.length)
        throw new Error(
          "Runtime role must not be the provisioning owner or own database objects"
        );
      const ddl = await client.query(ROLE_DDL_NAMESPACES, [runtimeRole]);
      if (ddl.rows.length)
        throw new Error(
          "Runtime role can create objects outside the operator-owned provisioning boundary"
        );
      const unrelated = await client.query(UNRELATED_DATA_PRIVILEGES, [
        runtimeRole,
      ]);
      if (unrelated.rows.length)
        throw new Error("Runtime role has data privileges outside LCM schemas");
      const accessible = await client.query(
        "SELECT nspname FROM pg_namespace WHERE left(nspname,4)='lcm_' AND has_schema_privilege($1,oid,'USAGE')",
        [runtimeRole]
      );
      for (const entry of accessible.rows) {
        validatePostgresSchema(entry.nspname);
        let identity;
        try {
          identity = await client.query(
            `SELECT organization_id FROM "${entry.nspname}".lcm_store_identity`
          );
        } catch {
          throw new Error(
            "Runtime role already accesses an unverified LCM schema"
          );
        }
        if (
          identity.rows.length !== 1 ||
          identity.rows[0].organization_id !== options.organizationId
        )
          throw new Error("Runtime role cannot be reused across organizations");
      }
      const memberships = await client.query(
        "SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=$1 LIMIT 1",
        [runtimeRole]
      );
      if (memberships.rows.length)
        throw new Error(
          "Runtime role with inherited memberships requires separate operator review"
        );
    }
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
    if (privileges.forImport)
      await client.query(
        `UPDATE "${options.schema}".lcm_import_state SET complete=0 WHERE id=1`
      );
    // Schemas are operator-owned. Gateway roles receive data DML, never provisioning/import privileges.
    await client.query(`REVOKE ALL ON SCHEMA "${options.schema}" FROM PUBLIC`);
    await client.query(
      `REVOKE ALL ON ALL TABLES IN SCHEMA "${options.schema}" FROM PUBLIC`
    );
    await client.query(
      `REVOKE ALL ON ALL SEQUENCES IN SCHEMA "${options.schema}" FROM PUBLIC`
    );
    await client.query(
      `REVOKE ALL ON ALL FUNCTIONS IN SCHEMA "${options.schema}" FROM PUBLIC`
    );
    if (runtimeRole) {
      const role = `"${runtimeRole}"`,
        schema = `"${options.schema}"`;
      await client.query(`GRANT USAGE ON SCHEMA ${schema} TO ${role}`);
      await client.query(
        `GRANT SELECT ON ALL TABLES IN SCHEMA ${schema} TO ${role}`
      );
      const mutable = POSTGRES_TABLES.filter(
        (name) =>
          !name.startsWith("lcm_import_") &&
          ![
            "lcm_schema_version",
            "lcm_store_identity",
            "lcm_migration_state",
          ].includes(name)
      );
      await client.query(
        `GRANT INSERT,UPDATE,DELETE ON ${mutable
          .map((name) => `${schema}."${name}"`)
          .join(",")} TO ${role}`
      );
      await client.query(
        `GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA ${schema} TO ${role}`
      );
      await client.query(
        `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ${schema} TO ${role}`
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}
