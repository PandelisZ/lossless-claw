# Experimental native PostgreSQL engine

SQLite remains the default. This fork adds an explicitly selected asynchronous PostgreSQL engine, not a synchronous remote SQLite emulation. Nothing automatically provisions a schema, imports a database, or changes the production OpenClaw history service. Do not deploy this experiment yet.

The current branch also preserves the separate fork-main Turbopuffer prerelease metadata (`@pandelis/lossless-claw@1.3.0-turbopuffer.0`); this PostgreSQL work has not been published. Its measured baseline and native OpenClaw pin are unchanged.

The port starts from upstream commit `51c3c90bfe82fa649e4895dd1a2c4874b5e28f76` (package version 1.2.0). That checkout differs from the published 1.2.0 tag (`4bd12aa198c4903f6400a42fa00fa0efc6697063`). Frozen comparison modules are built from the exact checkout commit. Neither the native OpenClaw pin nor its compatibility declarations changed. The real local gateway proof uses OpenClaw **2026.9.2**.

## Select a backend

Provision a distinct schema for each organization/agent pair. Configure the plugin with:

```json
{
  "storageBackend": "postgres",
  "postgresOrganizationId": "example-org",
  "postgresSchemas": {
    "one": "lcm_example_one",
    "two": "lcm_example_two"
  }
}
```

For a single agent, use `postgresSchema` and `postgresAgentId` instead of `postgresSchemas`. The map supports 1–8 agents, with distinct schemas and one organization. The gateway routes using the trusted caller identity, never a model-supplied schema or conversation ID. Every supplied agent/session identity is validated; conflicting identities fail closed. Recall can cross an agent's own conversations but cannot cross agent schemas. Unmapped agents fail explicitly.

Connections use standard process environment `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` and PostgreSQL TLS settings. Never put a connection URL/password in plugin configuration or command arguments. No Rails persistence or jobs are involved. Every mapped agent has a bounded pool (default four, maximum eight; at most 32 default connections with eight configured agents). The mapped gateway currently uses a common organization role: restrict its grants to the configured LCM schemas. Application routing enforces the agent boundary; this is not a claim that the common role itself cannot access another mapped schema. Separate organization credentials/roles must not have cross-organization schema privileges.

The connection opens only a pre-provisioned, versioned schema whose stored organization and agent match. An incomplete import and a read-only/recovery server are rejected. Gateway initialization and native stores use actual Promise-based `pg` I/O. Failed initialization can be retried by the next request; there is no background retry loop. Resource close/restart is generation fenced.

## Implemented native paths

The same native `LcmContextEngine`, assembler, compactor, retrieval and gateway plugin run on either store. PostgreSQL implements conversation/message/structured-part/trust storage, FTS, summary/source/parent DAG edges, ordered context, large-file metadata, transcript cursors/gaps, user replay keys, turn advancement receipts, expansion grants, pending summary nodes and compaction batches, publication, focus briefs/sources, maintenance debt/leases and telemetry. These are live tables and live API paths, not auxiliary JSON preserved solely for migration.

The real gateway test starts a local OpenClaw process, calls its WebSocket agent RPC, and uses a deterministic HTTP model only at the inference boundary. It checks persisted accepted turns/cursors/receipts and inserts a synthetic summary through native stores: the following actual model request must contain this PostgreSQL-only context projection. It also exercises a second routed agent and checks that projection does not cross the boundary. The gateway is disposable and loopback only.

SQLite explorer, SQLite backup, doctor and rotation tools are deliberately unavailable on PostgreSQL. The control status API and native focus/refocus/unfocus command paths use the selected agent's PostgreSQL store. SQLite-only CLI commands reject the PostgreSQL selection rather than silently inspecting a local database. PostgreSQL operational doctor/backup UX, managed provisioning/credential delivery and production rollout wiring remain future work. File payloads still live under `largeFilesDir`: PostgreSQL stores metadata, not the referenced filesystem bytes. Back up and migrate those files separately, with their identity/access boundaries intact.

## Transactions, snapshots and failure contract

Transactions use a checked-out client, SERIALIZABLE isolation and local timeouts/time zone; nested calls use savepoints. They use no session-affinity state, named prepared statements or cross-agent locks. Independent conversations can progress concurrently. Explicit revision writes fence mutable context/graph commits, including reachable source conversations, after inference. Compaction never holds a PostgreSQL transaction while waiting for an LLM.

Each assembler or compactor operation obtains a bounded immutable, single-statement MVCC snapshot of resolved context, reachable summary/message DAG, structured parts, trust, files and focus. It is closed at operation end, refreshed after a mutation, and never reused across requests. Limits are 10,000 rows per family and 16 MiB **serialized JSON**, including metadata and JSON overhead; overflow rejects explicitly instead of silently truncating. This bounds an operation, not the total retained store. Very large reachable histories may exceed the prototype limit and require paging or a more selective snapshot in a later iteration.

Concurrent identical accepted-turn receipts produce one durable commit. An overlapping transaction can return a serialization/revision conflict; retry the same logical receipt/body after refreshing the transcript/revision. Once successful, replay returns the native `{status:"duplicate"}` result, not another commit. There is no false committed acknowledgement of an aborted transaction, and no automatic retry of uncertain writes. Backend termination is tested for rollback and subsequent reconnection. Restart verification compares actual gateway-created canonical message, context, DAG, cursor and receipt rows.

All reads currently require the primary. No replica reads, freshness algorithm, HA failover or PlanetScale production latency have been qualified. Existing OpenClaw HA/pooler/replica infrastructure is untouched. The dedicated OpenClaw PlanetScale PostgreSQL cluster is a plausible eventual target with separate LCM schemas and a limited role, subject to capacity, extension/DDL permissions, region latency and failover qualification. No DDL was run there.

## Search contract

The native sanitizer still treats FTS input as quoted literal phrases joined with AND; this does not introduce a raw PostgreSQL tsquery API. The adapter translates to `phraseto_tsquery`, a generated `tsvector` and GIN index, with `ts_rank_cd` translated to the native ascending rank convention. Scope, sort and limits are applied by the native stores. Literal/regex behavior and CJK LIKE fallback remain in those stores. Empty punctuation phrases match SQLite's ignored-empty-term behavior; all-empty input has no hits.

PostgreSQL `simple` tokenization is **not** SQLite `porter unicode61`: `run` matches `running` on the frozen SQLite Porter index and does not on PostgreSQL. Ranking values/tie order, headlines, Unicode segmentation and uncommon punctuation can differ. Tests compare supported phrase/literal/regex selections and explicitly assert stemming differences; no FTS5 ranking equivalence is claimed. PostgreSQL text/JSONB also cannot represent embedded NUL characters that SQLite text can retain. These differences preclude claiming universal byte/content compatibility for arbitrary historical databases without preflight validation.

## Synthetic migration and rollback

Only operate on quiesced synthetic copies in this experiment. The explicit helper is gated by `LCM_DISPOSABLE_POSTGRES=1` and uses identity/connection environment settings above:

```sh
mise exec -- node scripts/postgres/manage.mjs init
mise exec -- node scripts/postgres/manage.mjs import /absolute/path/isolated-agent-copy.db
mise exec -- node scripts/postgres/manage.mjs export /absolute/path/new-rollback.db
```

`import` requires a fresh target schema and an **already isolated one-agent SQLite copy**. It rejects foreign or missing agent session authorities rather than guessing ownership or discarding rows. Splitting an existing mixed-agent organizational database into safe per-agent copies is not implemented. Do not activate PostgreSQL against a mixed source or write concurrently to both stores.

The importer exports every native table from one source read transaction, hashes exact logical columns/rows, atomically claims that source, and records per-chunk receipts with data in the same transaction. A process interruption leaves the store unavailable to the gateway. Reopen via the maintenance helper and repeat the same import; a changed source hash is rejected. Completion requires full graph/content parity. Identity sequences, including hidden SQLite focus ordering, advance past imported IDs. Tests interrupt/reopen/resume, replay, race different source claims, and round-trip every live native table.

Rollback/export creates a new SQLite file, runs native migrations, exports in a consistent read transaction, verifies foreign keys and row hashes, and atomically publishes without overwriting any existing destination. A failed attempt retains its `.partial` file for inspection. Choose a new destination for a new attempt. An export is not a backup of external payload files. Before any future cutover, stop the writer, verify source/export hashes and files, switch configuration once, and retain the untouched source; rollback requires a fresh verified export of subsequent PostgreSQL writes before selecting SQLite again. No production migration was attempted.

## Reproduce evidence

```sh
mise exec -- npm ci
mise exec -- npm run typecheck
mise exec -- npm run build
mise exec -- npm test -- --maxWorkers=2
mise exec -- node scripts/postgres/run-local.mjs
```

The last command creates only uniquely labeled disposable PostgreSQL 18 and PgBouncer containers/network and cleans those resources. It runs native integration checks directly and through **live transaction pooling**, complete assembly comparisons with the frozen SQLite baseline, real gateway proofs, and a database restart. Restricted-role ACL tests run directly; the local PgBouncer user list only includes the synthetic superuser and does not qualify restricted-role authentication through the pooler. Neither resource is production or geographically networked.

See [native results](../experiments/postgres/NATIVE-RESULTS.md) for measured samples, payload hashes and source evidence. The older [standalone results](../experiments/postgres/RESULTS.md) remain historical and are not evidence that the previous auxiliary-preservation prototype could run the native engine. No Turbopuffer end-to-end adapter result is inferred from its benchmark-only PostgreSQL mirror.
