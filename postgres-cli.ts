#!/usr/bin/env node
import { runPostgresCli } from "./src/cli/postgres.js";
try {
  const result = await runPostgresCli(process.argv.slice(2));
  console.log(JSON.stringify(result, null, 2));
  if ("ok" in result && result.ok === false) process.exitCode = 2;
} catch (error) {
  // Never print connection objects, raw SQL errors/details or message payloads.
  const code =
    error && typeof error === "object" && "code" in error
      ? String(error.code)
      : "OPERATION_FAILED";
  console.error(
    JSON.stringify({
      ok: false,
      code,
      message:
        "PostgreSQL operation did not complete; source data was retained. Check command/identity, file hashes, permissions and server health.",
    })
  );
  process.exitCode = 1;
}
