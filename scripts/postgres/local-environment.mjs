// Local probes must never inherit a service's dedicated credentials or libpq password/certificate files.
export function discardAmbientPostgresSecrets() {
  for (const key of Object.keys(process.env)) {
    if (
      key.startsWith("LCM_PG") ||
      key.startsWith("PGSSL") ||
      ["PGPASSWORD", "PGPASSFILE", "PGSERVICE", "PGSERVICEFILE"].includes(
        key
      ) ||
      key.endsWith("_DATABASE_DSN") ||
      key === "DATABASE_URL"
    )
      delete process.env[key];
  }
}
export function requireLoopbackPostgres() {
  if (!["127.0.0.1", "localhost", "::1"].includes(process.env.PGHOST))
    throw new Error(
      "This probe requires an explicitly disposable loopback PostgreSQL endpoint"
    );
}
