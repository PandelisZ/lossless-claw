# Experimental PostgreSQL store

This is a development-only PostgreSQL backend prototype maintained in a fork of
Lossless Claw so the async storage boundary and semantic fixes can be reviewed
and contributed upstream in stages. It is self-contained in this repository:
there is no application checkout, private patch cache or hosted service dependency.

This directory preserves the original standalone experiment and its evidence.
It does not implement the new gateway path. The fork now separately contains
an opt-in [native PostgreSQL engine](../../docs/postgres.md) with complete live
cursor, receipt, pending-summary, focus and maintenance stores. SQLite remains
the production default. Standalone checks do not qualify native gateway code.

## Reproduce

Requires Node with `node:sqlite` (tested with Node 26.12.1), npm and Docker.
Run from this directory:

```sh
npm ci --ignore-scripts
npm test
```

The runner builds native exports from **an immutable Git archive of the original baseline**, generates a small
deterministic fixture and creates only uniquely named disposable PostgreSQL and
PgBouncer containers/networks. No production credentials, DSNs or data are needed.
Containers/volumes/config are removed in `finally`. `node_modules` and generated
source/data/results stay ignored. The comparison retains its own private dependency lockfile; native integration now has separate root dependencies/configuration.

The native source baseline is upstream main commit
`51c3c90bfe82fa649e4895dd1a2c4874b5e28f76`, version 1.2.0. `prepare.mjs` exports that exact Git commit into an experiment-owned ignored directory before building the reference, so parity never silently follows edited native code. An explicitly supplied source checkout still must pass the unchanged-source guard. This differs from the published v1.2.0 tag commit
`4bd12aa198c4903f6400a42fa00fa0efc6697063`; the baseline is explicit in generated
provenance and fixture metadata. The native engine has separate current-source qualification; these frozen measurements are historical evidence.

Default checks exercise real PostgreSQL directly and through **local PgBouncer
transaction pooling**, with two server connections, prepared-statement tracking
disabled and DISCARD ALL after transactions. Small before/after full-assembly
benchmarks are included. `LCM_FULL_BENCHMARK=1 npm test` additionally runs the
larger operation matrix and restart probe. The default scoped run does not need
those larger checks on every edit.

The fixture seed is 20261104: two synthetic organizations, three agents each,
six conversations per agent, 200 alternating user/assistant messages per
conversation, 400 characters each, eight-message leaves and four-source parent
fanout. File rows are metadata only. This fixture has its own source-baseline
hash; do not confuse it with fixtures generated from a different upstream commit.

## Implemented and checked

- Async `pg` pools (default four clients), SQL transactions, monotonic ordering,
  transcript-ID dedup and conversation revisions. No remote synchronous SQLite
  shim, global application lock, outbox or retry queue.
- Same-agent cross-thread recall with mandatory organization/agent predicates.
  Role-based organization RLS is exercised. Nonempty normalized conversation
  family lists take precedence over the current physical conversation, as native
  recall producers expect. Explicit empty/invalid lists deliberately fail closed
  instead of native fallback to the current/unconstrained conversation.
- Native `tsvector`/GIN search. A common ASCII query is compared with identical
  scope, recency sort, date bounds and limit. Porter stemming, accents, scores and
  snippets differ from the SQLite FTS5 dialect and are explicit limitations.
- Native timestamp parsing reused verbatim; imported naive SQLite timestamps
  are interpreted as UTC even on a pooled backend with another TimeZone. Only
  user messages retain allowlisted sender identity, matching native ingestion.
- Atomic custom turn commits for messages, parts, context and optional cursor /
  native-shaped receipt rows. Concurrent identical receipt calls either observe
  the saved result or return an explicit revision conflict; identical stale-revision
  retry returns the winner's exact result with no duplicate data.
- Operation-scoped read snapshots feeding the **unchanged native assembler**.
  One SQL statement captures revision, active context, reachable sources, structured
  parts, trust and required file metadata. Snapshots are disposed after each call;
  they are not shared across requests. Full runtime output, including diagnostics,
  is compared to native SQLite for normal/budget/prompt/structured-content cases.
- Native leaf and condensed compaction methods with custom SQL commit adapters.
  Revision fencing rejects another turn arriving during inference, and committed
  passes invalidate the snapshot before another read/pass. Inference holds no SQL
  transaction open. Fake inference exists only at the LLM boundary.
- A 256 KiB **serialized returned-message-array** bound, including metadata and
  JSON punctuation; tests cover an oversized recent message and a large tail.
  It is a conservative suffix, not optimal packing or a packet/wire-byte limit.
- Restartable synthetic SQLite export/import and fresh-copy rollback; full
  resolved context and expanded original-message DAG parity, not subtree metadata.

## Not implemented

All-row migration preservation in `auxiliary_rows` is **not live operational
coverage**. The native engine still constructs SQLite stores and uses direct
SQLite statements. Native transcript drain/resync, cursor/frontier/replay/trust/
gap transitions, native commitTurn admission/receipt protocol, pending-summary
worker/publisher/coordinator, focus overlays, telemetry and maintenance have not
been ported. Custom cursor/receipt writes are not replacements for their complete
native store APIs. Large-file bytes remain filesystem-owned. No gateway backend
selector, native tool-scope adapter or bootstrap integration is present.

Snapshots reject above 10,000 rows per record family or 16 MiB of PostgreSQL JSON
text instead of silently assembling partial state. Materialization/copying and
large-history behavior remain unqualified. Organization RLS does not itself
isolate agents sharing that role; application authority predicates do that here.
No hosted routing, production network, HA, replica freshness, cross-region
failover or real gateway activation is qualified by these local tests.

## Upstream contribution path

1. Review the async provider/atomic write contracts and dual-backend semantic
   tests independently of changing the default engine.
2. Introduce native async store interfaces and port the direct SQLite callers,
   then operational state stores in small reviewed changes with nonempty-state
   tests. Preserve lossless recovery and the current SQLite backend.
3. Integrate explicit opt-in engine registration, manifest/config/docs and tool
   authority together; qualify bootstrap, drain, commitTurn, afterTurn, assemble,
   recall, compaction, restart/reset/fork and rollback through the actual gateway.
4. Resolve search compatibility and large-history bounds before making a release
   or changing the default. Do not infer production readiness from this prototype.

The exact native OpenClaw version metadata is not changed by this experimental
package. A future integration that requires a newer host capability must follow
the repository's compatibility/changeset rules.
