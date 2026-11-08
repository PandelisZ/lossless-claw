# PostgreSQL adoption tools (experimental, not deployed)

SQLite remains the default. These tools prepare a reviewable cutover on synthetic/disposable copies. They do not deploy, register a background job, change native OpenClaw history or Honcho, or automatically switch a live writer. The PostgreSQL operator CLI currently requires `LCM_DISPOSABLE_POSTGRES=1` for operations; help and the pure `role-name` command do not. This is deliberately not a production migration authorization.

## Credentials and privileges

A gateway must have its own complete `LCM_PGHOST`, `LCM_PGPORT` (default 5432), `LCM_PGDATABASE`, `LCM_PGUSER`, and `LCM_PGPASSWORD` namespace. The password stays in the existing process secret-injection path, never plugin JSON, source files, command arguments, manifests or logs. Missing dedicated credentials fail before opening a pool. Native `PG*`/OpenClaw DSN environment is unchanged. `PG*` operator fallback is allowed only in disposable mode with an explicit endpoint; password callbacks prevent `.pgpass` fallback.

`LCM_PGSSLMODE` defaults to `verify-full`; the optional `LCM_PGSSLROOTCERT` supplies a public CA file. TLS certificate verification is explicit even if Node's global TLS default is weakened. Disabling TLS is accepted only for explicitly disposable tests. Programmatic pools must supply a complete connection or tuning-only options: partial host/password/TLS overrides and connection strings are rejected. Explicit credentials are normalized to callbacks and verified TLS; client I/O has a 15-second query timeout. No session settings/named statements are needed for transaction pooling.

An organization runtime role name is deterministic:

```sh
# LCM_POSTGRES_ORGANIZATION_ID is a non-secret worker identity.
lcm-postgres role-name
```

Provision this fresh limited login role through an approved credential-custody path. Never place a password in DDL or shell arguments. `init` grants DML only for native LCM state, SELECT for identity/import metadata, sequence USAGE/SELECT and helper execution; the operator retains schema ownership. Runtime credentials cannot create tables or update schema identity/import completion. The deterministic organization binding prevents concurrent provisioners from accidentally reusing one role for two organizations. Existing privileged/inherited/owning roles and roles with non-LCM data privileges are rejected; existing accessible LCM schema identities must agree.

The same read-only runtime privilege inspector is used by doctor and Clawder startup. It checks privileged attributes, memberships, owned objects, non-LCM data access and metadata/DDL rights. Clawder also rejects visible LCM schemas outside its configured map. This is a strict prototype contract, not proof of PlanetScale's role-management/catalog behavior. Provider credentials or inherited roles that do not fit it require explicit qualification. Runtime SQL roles are shared among the configured agents of one organization; native trusted-actor schema routing enforces the agent boundary. Direct SQL permission tests qualify organization boundaries, not separate SQL roles for agents in a mapped gateway.

## Prepare a migration bundle

Stop the old writer and retain the original volume/database. Use a consistent quiesced SQLite copy and its LCM payload root. Do not use real customer messages as test fixtures. The operator commands use non-secret organization/schema/agent identity environment fields from the PostgreSQL guide; connection secrets remain in process memory.

```sh
lcm-postgres split /absolute/quiesced-source.db /absolute/source-files /absolute/new-bundle
lcm-postgres verify /absolute/new-bundle EXPECTED_MANIFEST_SHA256
```

`split` opens SQLite read-only, rejects unknown tables/NUL text, and assigns every native row to an agent using canonical authorities and graph references. It preserves same-agent cross-thread DAG edges, cursors, receipts, pending/focus state, grants and maintenance. Migration markers are copied to every partition; other rows are counted exactly once. Canonical grant issuers must agree with their live scoped targets. Stale grant bindings remain under their canonical agent, preserving the native unavailable-grant result. Ambiguous legacy identities and cross-agent graph/file/grant references fail rather than being guessed, dropped or shared.

Conversation/message AUTOINCREMENT high-water marks come from `sqlite_sequence` and are copied to every partition. Import and rollback preserve them, including previously allocated IDs whose highest row was explicitly deleted. Historical exports without this metadata cannot recover that allocator history; use newly generated bundles for cutover.

When optional SQLite FTS tables are absent, the logical export materializes their derived caches using the native message normalizer and summary content. It never writes to the source SQLite file. Logical export hashes include those caches; they are distinct from the raw database-file hash. Missing optional grant tables export as empty tables.

Referenced `large_files` bytes stream into exclusive new files, with size checks, SHA256, independent readback and source inode/mtime/ctime checks. Paths are confined to the supplied root. Source symlinks and cross-agent sharing of a payload inode are rejected. Linux `/proc/self/fd` canonical-path checks occur before reading an opened source descriptor, including after an ancestor swap; this payload workflow currently requires Linux/procfs and a quiesced, operator-controlled filesystem namespace. It is not a cross-platform secure-open API or a filesystem adversary sandbox. Output descriptors are checked, files/directories are synced, and the completion manifest is written last. A failed bundle remains for inspection, without a successful manifest/hash acknowledgement. Existing destinations are never overwritten.

The source export is unchanged. Only target `large_files.storage_uri` paths change; the manifest records reversibility and source/target logical hashes. This copies LCM payloads, not native OpenClaw attachments/workspaces or Honcho data. Keep the bundle at its final retained location: relocation makes its absolute payload metadata fail verification.

## Provision and import

```sh
# Operator connection, fresh schema, explicit runtime role from role-name.
lcm-postgres init
lcm-postgres import /absolute/new-bundle EXPECTED_MANIFEST_SHA256
# Switch the process credential namespace to the dedicated runtime role.
lcm-postgres doctor
```

Repeat for each agent/schema. `init` atomically creates a fresh schema gated as incomplete; the gateway cannot open it. `init-empty` is a separate explicit fresh-agent operation for an empty-origin store. Neither overwrites an existing schema. There is no gateway DDL or automatic source import.

The importer atomically claims one source, records rows plus chunk receipts in each transaction, preserves IDs/sequences, and verifies logical graph/content parity. A different source cannot claim the same target. Restart a failed import with the same verified bundle. Final file/export verification runs **before** completion is acknowledged, outside a database transaction: a changed payload during import leaves `complete=0`, including on replay, and a fresh gateway open is denied. No uncertain write is automatically retried. The old writer must remain stopped throughout; an already running process is not fenced by startup-only readiness checks.

`doctor` is read-only and reports public counts, invalid indexes/constraints, FTS content parity, conversation authority and runtime privilege findings. Message FTS is compared using the exact native externalized-content normalizer, not raw file-reference scaffolding. It does not repair data or print messages/credentials. Use runtime credentials for a gateway-readiness result; operator credentials intentionally report excessive privilege. This is a bounded diagnostic, not a general database repair/HA certification service.

## Back up and roll back

With writers stopped:

```sh
lcm-postgres backup /absolute/new-backup-bundle /absolute/current-payload-root
lcm-postgres verify /absolute/new-backup-bundle EXPECTED_BACKUP_MANIFEST_SHA256
# Select one agent through LCM_POSTGRES_AGENT_ID; create a new SQLite file.
lcm-postgres rollback /absolute/new-backup-bundle EXPECTED_BACKUP_MANIFEST_SHA256 /absolute/fresh-rollback.db
```

Backup combines a consistent native-table PostgreSQL snapshot with byte-verified payload copies. SQLite rollback restores all native rows, derived search data, FK integrity and allocator high-waters into a new file, with exact logical-hash verification and non-overwriting publication. Failed partial files remain for inspection. Select the restored database **and** the verified backup payload root before any explicit SQLite activation. The original source is retained; there is no automatic downgrade, pruning or old-source overwrite. These tests qualify the fork's native SQLite store; the production package/host cutover still needs its own staged approval.

## Build an unpublished candidate

```sh
# From a clean fork checkout. The script uses a fresh Git archive and npm lockfile.
mise exec -- node scripts/postgres/package.mjs /absolute/candidate-output
# To reproduce a pinned earlier implementation from a clean later checkout:
mise exec -- node scripts/postgres/package.mjs /absolute/candidate-output --commit FULL_SOURCE_SHA
```

The builder filters service credentials from child build environments, installs dependencies in the fresh fork root, builds all package surfaces and packs locally. It does not publish. The tarball and pin record source commit, lock SHA, Node/npm versions and raw archive SHA/integrity. The existing released fork name/version/channel remain unchanged; PostgreSQL candidates are distinguished by source/hash/build provenance, never by resolving the released Turbopuffer version from npm. A fresh installed-consumer probe checks root, package JSON, doctor API, retained TP path and the new `dist/experimental/postgres-operations.js` contract, imports both experimental APIs, checks CLI help and proves unconfigured PG cannot connect.

Clawder's explicit candidate lane is documented in its [experiment adoption guide](https://github.com/ComposalAI/vex/blob/pz/lcm-postgres-experiment/packages/clawder/experiments/lcm-postgres/ADOPTION.md). It verifies the fixed archive/build pin and startup metadata/privileges, keeps credentials out of config, retains removed `/lcm` commands/skills and readable tool details, and never changes the default package or native OpenClaw pin.

## Qualification boundary

See [operational results](../experiments/postgres/OPERATIONS-RESULTS.md) for precise runs and hashes. Both synthetic organizations/three agents/six conversations/200 messages are exercised; no real messages or production DDL are used. Live local transaction pooling authenticates a limited runtime role, and real two-agent gateway prompts retain isolation. The local streaming standby probe gates on replay, kills the primary with a write open, rejects replica startup, manually promotes and reconnects explicitly. It proves those mechanics and retained committed graph/receipt data only. It does **not** prove unplanned provider failover, arbitrary replication lag/RPO, automatic endpoint routing, PlanetScale network latency or production HA. Reads continue to require primary; read replicas remain unsupported. A named disposable hosted staging target is still needed for that evidence.
