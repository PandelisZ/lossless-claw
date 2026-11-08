import { DatabaseSync } from "node:sqlite";
import {
  exportSqlite,
  importSqliteExport,
  exportNativePostgres,
  writeSqliteExport,
} from "../db/postgres/transfer.js";
import {
  createMigrationBundle,
  verifyMigrationBundle,
} from "../db/postgres/bundle.js";
import {
  PostgresDatabase,
  provisionPostgres,
  runtimeRoleForOrganization,
} from "../db/postgres/database.js";
import { inspectPostgres } from "../db/postgres/doctor.js";

/** Explicit experimental operator CLI. Credentials are read only by pg from process environment. */
export async function runPostgresCli(
  args: string[],
  env: NodeJS.ProcessEnv = process.env
) {
  const [command, ...paths] = args;
  const organizationId = env.LCM_POSTGRES_ORGANIZATION_ID ?? "",
    agentId = env.LCM_POSTGRES_AGENT_ID ?? "",
    schema = env.LCM_POSTGRES_SCHEMA ?? "";
  if (!command || ["help", "--help"].includes(command))
    return {
      usage: [
        "lcm-postgres split source-copy.db source-payload-root new-bundle-dir",
        "lcm-postgres verify bundle-dir expected-manifest-sha256",
        "lcm-postgres role-name",
        "lcm-postgres init",
        "lcm-postgres init-empty",
        "lcm-postgres import bundle-dir expected-manifest-sha256",
        "lcm-postgres doctor",
        "lcm-postgres backup new-bundle-dir source-payload-root",
        "lcm-postgres rollback bundle-dir expected-manifest-sha256 fresh-output.db",
      ],
      credentials:
        "LCM_PG* dedicated environment; PG* operator fallback only in disposable tests; init requires existing LCM_POSTGRES_RUNTIME_ROLE; all commands require LCM_DISPOSABLE_POSTGRES=1 in this experimental release",
    };
  if (command === "role-name") {
    if (paths.length || !organizationId.trim())
      throw new Error(
        "role-name requires only the organization environment identity"
      );
    return {
      organizationId,
      runtimeRole: runtimeRoleForOrganization(organizationId),
    };
  }
  if (env.LCM_DISPOSABLE_POSTGRES !== "1")
    throw new Error(
      "This operator CLI is restricted to explicitly disposable experiments"
    );
  if (!organizationId.trim())
    throw new Error("Explicit organization identity required");
  const arity: Record<string, number> = {
    split: 3,
    verify: 2,
    init: 0,
    "init-empty": 0,
    import: 2,
    doctor: 0,
    backup: 2,
    rollback: 3,
  };
  if (!(command in arity) || paths.length !== arity[command])
    throw new Error("Invalid command or arguments; see lcm-postgres --help");
  if (command === "split") {
    const source = new DatabaseSync(paths[0], { readOnly: true });
    let data;
    try {
      data = exportSqlite(source, { requireKnownTables: true });
    } finally {
      source.close();
    }
    return createMigrationBundle({
      data,
      organizationId,
      sourcePayloadRoot: paths[1],
      destination: paths[2],
    });
  }
  if (command === "verify" || command === "rollback") {
    const verified = await verifyMigrationBundle(
      paths[0],
      paths[1],
      organizationId
    );
    if (command === "rollback") {
      const data = verified.agents.get(agentId);
      if (!data)
        throw new Error(
          "Rollback requires an agent present in the verified bundle"
        );
      return writeSqliteExport(data, paths[2]);
    }
    return {
      verified: true,
      sourceHash: verified.manifest.sourceHash,
      agents: verified.manifest.agents.map((a) => ({
        agentId: a.agentId,
        rows: a.rows,
        files: a.files.length,
        exportHash: a.exportHash,
        largeFilesDir: a.largeFilesDir,
      })),
    };
  }
  const options = { schema, agentId, organizationId };
  if (command === "init" || command === "init-empty") {
    if (!env.LCM_POSTGRES_RUNTIME_ROLE)
      throw new Error("Init requires an existing limited runtime role");
    await provisionPostgres(options, {
      runtimeRole: env.LCM_POSTGRES_RUNTIME_ROLE,
      forImport: command === "init",
    });
    return {
      provisioned: true,
      importRequired: command === "init",
      schema,
      agentId,
      organizationId,
    };
  }
  // Verify files and identity before acquiring a database connection or starting an import.
  const verified =
    command === "import"
      ? await verifyMigrationBundle(paths[0], paths[1], organizationId)
      : undefined;
  const selected = verified?.agents.get(agentId);
  if (verified && !selected)
    throw new Error("Import agent not present in verified bundle");
  const db = await PostgresDatabase.open(options, {
    allowIncompleteImport: command === "import" || command === "doctor",
  });
  try {
    if (command === "import") {
      const result = await importSqliteExport(db, selected!, {
        beforeComplete: () =>
          verifyMigrationBundle(paths[0], paths[1], organizationId).then(
            () => undefined
          ),
      });
      return {
        ...result,
        verifiedPayloads: true,
        schema,
        agentId,
        largeFilesDir: verified!.manifest.agents.find(
          (a) => a.agentId === agentId
        )!.largeFilesDir,
      };
    }
    if (command === "doctor") return inspectPostgres(db);
    return createMigrationBundle({
      data: await exportNativePostgres(db),
      organizationId,
      sourcePayloadRoot: paths[1],
      destination: paths[0],
    });
  } finally {
    await db.close();
  }
}
