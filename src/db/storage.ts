import type {
  DatabaseSync,
  SQLInputValue,
  StatementResultingChanges,
} from "node:sqlite";

/** SQL execution is awaitable; PostgreSQL always performs nonblocking pooled I/O. */
export interface AsyncStorageDatabase {
  readonly dialect: "postgres";
  readonly ready: boolean;
  readonly organizationId: string;
  readonly agentId: string;
  prepare(sql: string): {
    get(
      ...params: SQLInputValue[]
    ): Promise<Record<string, unknown> | undefined>;
    all(...params: SQLInputValue[]): Promise<Record<string, unknown>[]>;
    run(...params: SQLInputValue[]): Promise<StatementResultingChanges>;
  };
  exec(sql: string): Promise<void>;
  transaction<T>(operation: () => Promise<T> | T): Promise<T>;
  close(): Promise<void>;
}
export type StorageDatabase = DatabaseSync | AsyncStorageDatabase;
export function isPostgresDatabase(
  db: StorageDatabase
): db is AsyncStorageDatabase {
  return "dialect" in db && db.dialect === "postgres";
}
