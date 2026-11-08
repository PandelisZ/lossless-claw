# Historical standalone qualification

These results describe the earlier standalone store, before the native engine port. Current native gateway evidence is in [NATIVE-RESULTS.md](NATIVE-RESULTS.md). Statements below about missing native APIs apply to that earlier prototype.

Native source: `51c3c90bfe82fa649e4895dd1a2c4874b5e28f76` (upstream main, version 1.2.0).
This is a separate baseline from the published v1.2.0 tag; default native files and OpenClaw metadata are unchanged.

15 store and 16 focused tests passed directly and through real local PgBouncer transaction pooling, with zero failures or skips. Full engine activation is unimplemented; see [README](README.md).

Full assembly includes snapshot loading, exact native assembly, diagnostics and disposal. Same synthetic graph and whole-output equality to SQLite are checked before timing. Twenty read samples, concurrency 1, one warmup. Local Docker/host timings have no CPU/I/O isolation and are not production/WAN evidence.

| Full operation                 | Direct p50/p95 ms | Local PgBouncer p50/p95 ms | SQL trips |
| ------------------------------ | ----------------- | -------------------------- | --------- |
| sqlite-full-assembly           | 4.08/5.15         | 4.90/6.38                  | 0         |
| unbatched-full-assembly        | 43.24/50.89       | 62.10/137.58               | 231       |
| batched-full-assembly          | 7.26/14.84        | 10.09/13.36                | 1         |
| unbatched-full-leaf-compaction | 40.03/50.73       | 84.21/118.94               | 199       |
| batched-full-leaf-compaction   | 21.65/21.77       | 41.43/43.49                | 11        |

Compaction uses three separate conversations per mode and deterministic fake inference only at the LLM boundary; p95=max for that small sample. It includes the SQL commit and any snapshot refresh. The extra rows in the JSON add a synthetic 2 ms client timer per query; they do not measure physical RTT or production networking. Output bytes are JSON serialization sizes, not packet captures.

Fixture SHA-256: `597e3a64ffcd3c1121c34323f49fe865b9a2a7708897b76027d1e122f0e35ac0`. Seed 20261104, 7,200 messages / 2,880,000 message-content bytes. All data is synthetic; file metadata has no payload bytes.

Separately, 112 unchanged native SQLite tests passed across five selected test files (timestamp parsing, summary storage, structured assembly and compaction accounting). These native regressions are not PostgreSQL engine end-to-end evidence.

A fresh read-only reviewer approved this as a bounded experimental draft after verifying scope, source parity and supplied complete direct/pooler logs. It is not gateway-ready or release-ready.
