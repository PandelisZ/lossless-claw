import pg from "pg";
import { AsyncLocalStorage } from "node:async_hooks";
import { readFile } from "node:fs/promises";
export const schema = "lcm_experiment";
export const table = (name) => {
  if (!/^[a-z_]+$/.test(name)) throw new Error("Invalid table identifier");
  return `${schema}.${name}`;
};
export class Conflict extends Error {}
export class Database {
  #transaction = new AsyncLocalStorage();
  constructor(pool) {
    this.pool = pool;
    this.roundTrips = 0;
  }
  async query(sql, args = []) {
    this.roundTrips++;
    return (this.#transaction.getStore() ?? this.pool).query(sql, args);
  }
  async transaction(operation) {
    if (this.#transaction.getStore()) return operation();
    const client = await this.pool.connect();
    try {
      this.roundTrips++;
      await client.query("BEGIN");
      // Local settings survive transaction pooling without leaking to another client.
      this.roundTrips++;
      await client.query(
        "SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='2s'; SET LOCAL idle_in_transaction_session_timeout='10s'",
      );
      const result = await this.#transaction.run(client, operation);
      this.roundTrips++;
      await client.query("COMMIT");
      return result;
    } catch (error) {
      this.roundTrips++;
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
// Standard PG* variables are read only by pg, never logged or put in arguments.
export function createPool(options = {}) {
  if (
    options.max != null &&
    (!Number.isInteger(options.max) || options.max < 1 || options.max > 8)
  )
    throw new Error("Experimental pool must have 1..8 connections");
  const pool = new pg.Pool({
    max: 4,
    connectionTimeoutMillis: 3000,
    idleTimeoutMillis: 10000,
    maxLifetimeSeconds: 3600,
    ...options,
  });
  pool.on("connect", (client) => client.on("error", () => {}));
  pool.on("error", () => {}); // Active queries retain the error; idle disconnects discard clients.
  return pool;
}
export async function migrate(database) {
  await database.transaction(async () => {
    if (
      (await database.query("SELECT pg_is_in_recovery() AS replica")).rows[0]
        .replica
    )
      throw new Error("LCM requires the primary endpoint");
    const existing = (
      await database.query(
        "SELECT to_regclass('lcm_experiment.schema_version') AS name",
      )
    ).rows[0].name;
    if (existing) {
      const versions = (
        await database.query(
          "SELECT version FROM lcm_experiment.schema_version",
        )
      ).rows.map((r) => r.version);
      if (versions.length !== 1 || versions[0] !== 1)
        throw new Error("Unsupported LCM experimental schema version");
    }
    await database.query(
      await readFile(new URL("./schema.sql", import.meta.url), "utf8"),
    );
  });
}
