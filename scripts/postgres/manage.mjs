// Explicit maintenance helper for disposable/synthetic experiments. Never loads a DSN from arguments.
import { build } from "esbuild";
import { DatabaseSync } from "node:sqlite";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
if (process.env.LCM_DISPOSABLE_POSTGRES !== "1")
  throw new Error(
    "Maintenance is restricted to explicitly disposable experiments"
  );
const root = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../.."
  ),
  out = path.join(root, "experiments/postgres/.generated/native");
await mkdir(out, { recursive: true });
await build({
  stdin: {
    contents: `export {PostgresDatabase,provisionPostgres} from './src/db/postgres/database.ts';export {exportSqlite,importSqliteExport,exportPostgresToSqlite} from './src/db/postgres/transfer.ts';`,
    resolveDir: root,
  },
  bundle: true,
  platform: "node",
  format: "esm",
  external: ["pg"],
  outfile: path.join(out, "manage-store.mjs"),
});
const store = await import(path.join(out, "manage-store.mjs"));
const options = {
  schema: process.env.LCM_POSTGRES_SCHEMA,
  organizationId: process.env.LCM_POSTGRES_ORGANIZATION_ID,
  agentId: process.env.LCM_POSTGRES_AGENT_ID,
};
const [command, file] = process.argv.slice(2);
if (command === "init") {
  await store.provisionPostgres(options);
  console.log("Provisioned disposable LCM schema");
} else {
  if (!["import", "export"].includes(command) || !file)
    throw new Error(
      "Usage: manage.mjs init | import isolated-copy.db | export fresh-output.db"
    );
  const db = await store.PostgresDatabase.open(options, {
    allowIncompleteImport: command === "import",
  });
  try {
    if (command === "import") {
      const source = new DatabaseSync(file, { readOnly: true });
      let data;
      try {
        data = store.exportSqlite(source);
      } finally {
        source.close();
      }
      console.log(JSON.stringify(await store.importSqliteExport(db, data)));
    } else
      console.log(JSON.stringify(await store.exportPostgresToSqlite(db, file)));
  } finally {
    await db.close();
  }
}
