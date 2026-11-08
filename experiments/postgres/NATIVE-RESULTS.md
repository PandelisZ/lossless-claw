# Native PostgreSQL engine qualification

This is a disabled-by-default fork experiment, not a production deployment or HA qualification. Native source baseline: `51c3c90bfe82fa649e4895dd1a2c4874b5e28f76`, package version 1.2.0. Reference exports use an immutable archive of that commit; the published tag is a separate baseline. The complete integration run is recorded in [native-checks-native.json](evidence/native-checks-native.json). A subsequent focused benchmark added all three complete-engine parity assertions; its measurement source inventory/command outcomes and metadata-collection caveat are in [native-focused-benchmark-checks.json](evidence/native-focused-benchmark-checks.json). Source digests use SHA256 of JSON.stringify(UTF-8 source), not raw sha256sum; the artifact manifest uses raw-byte SHA256.

## Complete assembly measurements

Twelve samples per variant, concurrency 1, real local I/O. SQLite is an on-disk WAL database. Every complete ContextAssembler result includes snapshot loading, focus lookup, native token/prompt/structured-content logic and disposal. `postgres-engine-batched` additionally includes the full native LcmContextEngine assembly lifecycle, its pressure probes, epoch and token accounting. It is not presented as a one-trip engine operation. Client I/O time includes protocol/decoding, excludes injected delay and is not isolated server CPU. Inference calls during these measurements: zero.

| Operation | Direct p50 / p95 / max ms | Local transaction pooler p50 / p95 / max ms | SQL trips |
| --- | --- | --- | --- |
| sqlite-baseline | 3.76/ 4.28/ 4.28 | 3.76/ 4.39/ 4.39 | 0 |
| sqlite-engine-baseline | 12.17/ 23.22/ 23.22 | 11.84/ 23.98/ 23.98 | 0 |
| postgres-unbatched | 28.57/ 38.44/ 38.44 | 34.86/ 43.72/ 43.72 | 232 |
| postgres-batched | 8.30/ 8.49/ 8.49 | 8.24/ 9.19/ 9.19 | 1 |
| postgres-engine-batched | 22.19/ 26.19/ 26.19 | 24.24/ 29.41/ 29.41 | 6 |
| postgres-unbatched (+2 ms synthetic/query) | 560.46/ 583.53/ 583.53 | 557.73/ 563.74/ 563.74 | 232 |
| postgres-batched (+2 ms synthetic/query) | 11.01/ 12.45/ 12.45 | 10.46/ 12.03/ 12.03 | 1 |
| postgres-engine-batched (+2 ms synthetic/query) | 37.22/ 40.83/ 40.83 | 36.25/ 39.04/ 39.04 | 6 |

The 232→1 comparison is complete assembler versus complete assembler. The earlier standalone 231-trip result omitted the native focus read. The full native engine is six trips and has separate SQLite-engine parity/timings. The 2 ms rows use an explicitly injected client timer for each SQL request: they are **not** physical network or production measurements. Localhost latency does not establish the eventual PlanetScale region/HA behavior.

Fixture seed: 20261104. Immutable source SHA: `597e3a64ffcd3c1121c34323f49fe865b9a2a7708897b76027d1e122f0e35ac0`. Source export contains 2 organizations × 3 agents × 6 conversations × 200 messages (7,200 messages / 2,880,000 content bytes, 7,039,271 export bytes). The timed one-agent/conversation slice has 200 messages / 80,000 content bytes, 95 context items and two deterministic structured parts. Slice logical hash: `a896b196ba7a73ef37daaa0529016b5fe5702cda2bd1e8de8c0e78d662d47d5a`. The common source export was read-only. The native snapshot serialized JSON limits include metadata/overhead, not only message text; a large-metadata/small-content regression rejects overflow.

Parity checks compare whole complete assembler and complete engine outputs to frozen SQLite for normal, token-budget and prompt/structured-content cases. Retrieval expands the same 200 original messages and 34 summary descendants on both stores. Historical ingest/dedup, scoped/sorted/limited FTS, context, compaction (199→11 trips), import and rollback evidence remains in [RESULTS.md](RESULTS.md) and its original evidence files. Those timings are for the historical standalone adapter; they are not mislabeled as timings of the new full native compaction engine.

## Live native and gateway checks

- 21 native integration cases passed directly on disposable PostgreSQL 18. Through live PgBouncer transaction pooling, 20 passed and the restricted-login-role ACL case was explicitly skipped because the synthetic pooler user list includes only postgres. Direct restricted-role tests cover two organizations × three agents with overlapping local IDs and negative cross-schema accesses.
- Tests exercise real bootstrap, transcript deltas, accepted-turn receipt commit/replay, concurrent separate-engine same-receipt conflict followed by duplicate replay, pending-summary promotion, maintenance debt, active focus overlay, persistent grants, gaps/trust/parts, full DAG resolution and source-conversation revision fences. A separate accepted turn during inference invalidates compaction without discarding raw messages.
- Both direct and pooler runs launch a real local OpenClaw 2026.9.2 gateway, use WebSocket agent RPC (no embedded fallback), and send three turns across two configured agents. The deterministic fake model is a real local HTTP server at the inference boundary. Agent one persists four messages, one cursor and two receipts; its second actual model request contains an LCM summary stored only in PostgreSQL. Agent two persists two messages, one cursor and one receipt; its actual model request does not contain the other agent summary.
- Restart of the owned PostgreSQL server preserves gateway-created message/context/summary/source/cursor/receipt rows, checked by logical state hash through the transaction pooler. Deliberate backend socket termination inside a write transaction rolls back and a fresh read reconnects. No HA, failover or replica evidence is claimed.
- All live native tables round-trip through SQLite import/export with exact logical row hashes and FK checks. Nonempty focus rows preserve hidden rowid ordering. Interrupted import resumes only against the same immutable source; competing sources cannot both claim the target. Rollback publication cannot overwrite a destination created during export.
- Full default SQLite run: 135 files / 2,146 tests passed, one file / 21 tests skipped (the PostgreSQL opt-in suite), three individual load-related timeouts. Those three files reran with a local 30 s timeout at concurrency 1: all 40 tests passed. No assertion or shared policy was changed. Five additional scoped lifecycle/config/cache checks passed. Typecheck, complete build and built CLI help passed. [Local check record](evidence/native-local-checks.json).

Evidence: [direct tests](evidence/native-tests-direct.json), [pooler tests](evidence/native-tests-pooler.json), [direct benchmark](evidence/native-benchmark-direct.json), [pooler benchmark](evidence/native-benchmark-pooler.json), [direct gateway](evidence/native-gateway-smoke-direct.json), [pooler gateway](evidence/native-gateway-smoke-pooler.json), [evidence hashes](evidence/native-manifest.json).

VCI attested zero tests: the fork has no `vci.toml`; its default run skipped the opt-in PG suite and refused attestation (`process.env.enumerate`). The explicit disposable runner above supplies the real integration evidence. Vex docs paths are not registered test units. No shared test policy changed and no CI skips are claimed.

A fresh read-only reviewer returned **Ready as a bounded experimental draft** after reviewing routing/lifecycle, snapshot/fence, receipt, import/publication code and final logs/artifacts. It verified nine artifact hashes and all 283 current source digests, including the separately requalified benchmark. It did not independently rerun the benchmark or qualify a hosted service.

## Recommendation and remaining work

PostgreSQL can now run the complete native engine storage/lifecycle in this bounded local experiment. It provides canonical atomic DAG/context updates and native indexed text retrieval without another durable service. Keep SQLite as the production default until target-network/load, operating limits, role provisioning and cutover work are qualified. PostgreSQL snapshots reduce network amplification substantially, but full native assembly still has six remote requests and local SQLite remains faster in these warm samples.

The practical Vex activation work is an approved fork build/package pin, explicit schemas and restricted credentials, safe per-agent extraction from mixed organization SQLite files, a stopped-writer verified migration/cutover, filesystem payload copy, operational PostgreSQL doctor/backup UX and hosted network/HA qualification. The gateway itself now has real opt-in wiring; no Vex production integration has consumed it. Search uses PostgreSQL simple tokenization/ranking and explicitly differs from SQLite Porter stemming; arbitrary NUL-containing text cannot round-trip into PostgreSQL text. Read replicas are unsupported; reads require primary. Snapshot caps (10,000 rows/family, 16 MiB serialized JSON) are prototype bounds, not a large-history qualification.

The Turbopuffer experiment is independent; its PostgreSQL benchmark mirror is not this adapter. No current comparable end-to-end Turbopuffer engine measurement was imported or inferred. Given the verified atomicity and now-live auxiliary paths, PostgreSQL is the more complete canonical-store candidate in this experiment, subject to those limits. See [the guide](../../docs/postgres.md) for configuration and the synthetic migration/rollback procedure.

## Reconciliation with the released fork main

After timing, fork main advanced to `716d22d` (the separate Turbopuffer release).
The PG branch incorporates that commit, preserving its `@pandelis/lossless-claw`
package identity/version/channel, all TP source and release metadata, and native
OpenClaw version declarations. The PG changeset names the actual fork package.
No new release/publication occurred. The frozen `51c3c90` SQLite baseline remains
unchanged. The historical full SQLite run above predates this reconciliation.

The affected command/release/TP/storage/lifecycle suite passed 120 tests across six
files; typecheck and the complete build passed. Native checks repeated directly
(21 passes) and through PgBouncer (20 passes, one explicitly skipped ACL case),
with all three complete assembler/engine parity cases, full DAG parity, both real
two-agent gateway proofs and restart verification passing. The `--checks-only`
runner skips new timing samples; the measured table remains the earlier frozen
run, not a claim of new timings after integration.

Current integration source digests/commands are in [native-checks-post-merge.json](evidence/native-checks-post-merge.json),
with [affected checks](evidence/native-post-merge-affected-checks.json),
[direct parity](evidence/native-parity-post-merge-direct.json) and
[pooler parity](evidence/native-parity-post-merge-pooler.json). Updated gateway and
test records have the `native-post-merge-` prefix. The original evidence remains
unchanged. This branch is still a draft and production remains SQLite.

The same fresh read-only reviewer returned **Ready for the reconciliation scope**, verifying all 17 artifact checksums and 286 current source digests, conflict resolutions and the affected test logs. It retained the bounded experimental verdict; no production/HA/replica readiness follows.
