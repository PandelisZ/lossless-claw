# Experimental Turbopuffer context storage

This fork implements a canonical Turbopuffer backend **inside Lossless Claw** for
explicitly scoped context operations. It runs the unmodified v1.2.0
`ContextAssembler` and `CompactionEngine` instead of reimplementing their logic.
SQLite remains the plugin default. This is an upstream contribution candidate,
not a complete gateway database replacement or a production rollout.

## Use

Run `npm ci`, `npm run build:experimental-storage`, and import:

```js
import { TurbopufferContextStorage } from "@martian-engineering/lossless-claw/dist/experimental/context-storage.js";

const storage = new TurbopufferContextStorage({
  backend: "turbopuffer",
  namespacePrefix: "your-isolated-development-prefix",
  organizationId: trustedOrganizationId,
  agentId: trustedNativeAgentId,
  conversationId: trustedConversationId,
  apiKey: process.env.TURBOPUFFER_API_KEY,
  region: "aws-us-east-1",
});
await storage.initialize(); // Insert-if-absent; never resets existing state.
await storage.importTables(conversationScopedTables); // Explicit empty-store import.
const context = await storage.assemble({
  conversationId: trustedConversationId,
  tokenBudget: 16000,
  freshTailCount: 80,
  freshTailMaxTokens: 65536,
});
```

`appendTurn(stableToken, messages)` atomically appends messages and ordered context.
`compactLeaf(stableToken, config, input)` runs the real upstream leaf compactor,
then publishes its complete graph/context mutation. `searchMessages(input)` uses
the existing scoped FTS5/regex semantics against the current operation projection.
Stable operation tokens deduplicate retries, preserve returned Date values, and
reject different input under the same token. Imports reject foreign auxiliary
rows and rows without a supported conversation/message/summary owner link;
unimplemented auxiliary ownership is refused rather than guessed. Explicit `snapshot()` callers must
close their operation after use. Snapshot APIs and callbacks are trusted backend
code, not an agent-facing arbitrary SQL surface.

These are **programmatic options**, not new `plugins.entries.lossless-claw.config`
keys. No OpenClaw config/schema or minimum host version is changed. The ordinary
plugin still constructs its SQLite stores. Full hosted gateway activation is not
implemented and must not be inferred from this library import.

## Data and commit protocol

Turbopuffer owns all committed conversation table rows. Tables are sorted by
their actual primary keys, split into immutable blocks of up to 32 rows and stored
as non-filterable JSON attributes. The root embeds a manifest of block IDs,
parent commit and operation receipts. An immutable copy of every published/staged
manifest is stored separately. A single root patch, conditional on the observed
version **and** commit ID, atomically publishes the next complete state.

An operation reads the strong root once and bulk-fetches its immutable blocks.
It hydrates a new **SQLite `:memory:` read/write projection**, executes upstream
code locally and closes the projection. This SQLite database is never a durable
canonical store: deleting it loses no acknowledged conversation data, and a
fresh operation reconstructs it solely from Turbopuffer. There is no PostgreSQL,
Redis, Rails outbox, job queue, local database file or durable retry service
underneath this backend. CPU/memory/projection rebuilding are real costs, included
in all measurements.

For a write, all new blocks and the immutable manifest are staged first, then one
CAS updates the root. A losing writer leaves unreachable blocks; readers cannot
see them through the winning manifest. A lost CAS response is reconciled against
the current root's operation receipt. An unchanged root without the receipt is
`UnknownCommit`, not permission to blindly retry. A changed root without the
receipt is `StorageConflict`; a caller must reload and intentionally rebuild.
No inference callback is automatically repeated. Stale observations are fenced;
retired-host generation/lease authority is not implemented.

The root carries its manifest, reducing an ordinary operation to two network
round trips for the measured 23-block conversations. With over 128 blocks, bulk
reads use additional 128-ID batches. Only changed/new blocks are written, and
unchanged historical blocks remain retained. The current manifest/receipts are
rewritten on each commit. Search fetches this same pinned state and applies the
native upstream search algorithms: it is **not a provider BM25 performance claim**.
It avoids orphan candidate overfetch by searching committed state, at the cost of
fetching and indexing the complete bounded conversation projection.

## Paired benchmark against the PostgreSQL experiment

The benchmark runs real remote Turbopuffer in `aws-us-east-1` and the parallel
PostgreSQL adapter on a task-owned local PostgreSQL 18 container. The PG source
is copied read-only from its Git branch; its worktree, services and files are
never changed. This is a workload comparison, **not equal network geography**.

Both use exact upstream commit `4bd12aa198c4903f6400a42fa00fa0efc6697063`, seed
`20261104`, common export SHA-256
`af724fb7cd2504ac3e593b16090d63179bdd0ee25e3617991e25d8be14f2b5cf`, and the
same 200-message / 35-summary / 95-context-item conversation shape. Initial
assembly output compares byte-for-byte across SQLite, unbatched PostgreSQL,
batched PostgreSQL and this fork. Output SHA-256:
`d707aba9910cbf2e1e174db6ea36def0cf5a7ea95a8871de37c9f7275315ea01`.

| Full operation                     |   n | p50 ms | p95/max ms | Network calls per operation |
| ---------------------------------- | --: | -----: | ---------: | --------------------------: |
| SQLite assembly                    |   5 |   3.93 |       5.35 |                           0 |
| PostgreSQL unbatched assembly      |   5 |  42.59 |      57.36 |                     231 SQL |
| PostgreSQL batched assembly        |   5 |   8.08 |      11.17 |                       1 SQL |
| Turbopuffer fork assembly          |   5 | 231.63 |     251.05 |                      2 HTTP |
| PostgreSQL message FTS             |   5 |   0.96 |       2.10 |                       1 SQL |
| Turbopuffer + native projected FTS |   5 | 212.68 |     215.29 |                      2 HTTP |
| PostgreSQL batched leaf compaction |   3 |  26.93 |      30.30 |                      11 SQL |
| Turbopuffer fork leaf compaction   |   3 | 705.88 |     730.02 |                      4 HTTP |

### Current upstream main and the actual PostgreSQL fork

The table above preserves the release-pinned measurement. The contribution branch
is also rebased onto upstream `main` at
`51c3c90bfe82fa649e4895dd1a2c4874b5e28f76`. A fresh paired qualification uses
that **same native assembler/compactor/store source for all backends**, and the
actual PostgreSQL fork branch from
[fork PR #1](https://github.com/PandelisZ/lossless-claw/pull/1), commit
`1eebd7e273716799c6ffbbdf78bb1dd5b913f1b0`. The harness verifies native source
equality before running. The original exported fixture hash remains unchanged;
its source metadata still identifies the v1.2.0 release export. Data and native
algorithm provenance are recorded separately rather than conflated.

| Current-main full operation        |   n | p50 ms | p95/max ms | Network calls |
| ---------------------------------- | --: | -----: | ---------: | ------------: |
| SQLite assembly                    |   5 |   4.96 |       7.14 |             0 |
| PostgreSQL unbatched assembly      |   5 |  39.66 |      56.33 |       231 SQL |
| PostgreSQL batched assembly        |   5 |   8.88 |      11.21 |         1 SQL |
| Turbopuffer fork assembly          |   5 | 241.38 |     257.28 |        2 HTTP |
| PostgreSQL FTS                     |   5 |   1.23 |       3.01 |         1 SQL |
| Turbopuffer + projected native FTS |   5 | 223.81 |     227.64 |        2 HTTP |
| PostgreSQL batched leaf compaction |   3 |  38.15 |      44.87 |        11 SQL |
| Turbopuffer leaf compaction        |   3 | 713.89 |     739.14 |        4 HTTP |

[Current-main raw evidence](evidence/current-main-comparison.json) retains exact
implementation/source hashes. Initial assembly has the same byte-for-byte output
hash as the release-pinned run; normalized post-compaction assembly also matches.
Both runs' namespace cleanup succeeded, and their task-owned PostgreSQL containers
were removed. This compares the real batched PG fork, not the earlier JSONB mirror
or its statistics-sensitive slow query.

The compaction callback is deterministic **only at the inference boundary**;
summary selection, graph construction, persistence and context updates use real
upstream code and real backend I/O. Three independent conversations are compacted
once each. Post-compaction assembly compares all message content, token counts
and diagnostics after normalizing newly generated nonce-based summary IDs and
excluding only three debug digests derived from those IDs. The initial comparison
requires no normalization. An initial benchmark assertion incorrectly compared
those random-ID diagnostic hashes; investigation confirmed only those three
hashes differed, and the corrected paired run passed. The
[initial run and its cleanup evidence](evidence/initial-nonce-check.json) are retained
without treating that failed assertion as a completed qualification.

[Raw evidence](evidence/fork-comparison.json) includes every sample, exact PG Git
source, compiled fork bundle/source hashes, HTTP bytes/server time/billing,
round trips and cleanup. The final root was revision2, with 23 blocks and a
2,012-byte manifest, while retaining all 200 messages. Foreign agent/organization
canaries and out-of-scope conversation reads were rejected. The unique synthetic
namespace and task-owned PostgreSQL container were removed after collecting
evidence. Small samples do not establish production latency percentiles.

To reproduce, build this fork, export the PG experiment read-only, run its
`prepare.mjs` against a clean pinned source checkout, create a **new task-owned**
loopback PostgreSQL database named `lcm_fork_comparison`, and set its standard
`PGHOST=127.0.0.1`, `PGPORT`, `PGUSER`, `PGDATABASE` environment values:

```sh
LCM_TURBOPUFFER_EXPERIMENT=1 node bench/context-storage.mjs \
  --manifest /path/to/fixture-manifest.json \
  --postgres /path/to/private/read-only-pg-export \
  --postgres-source-sha <that-experiment-commit> \
  --output /tmp/lcm-fork-results
```

Provide `TURBOPUFFER_API_KEY` in process memory, never arguments or artifacts.
For the Composal experiment, `--key-helper` points to the already authorized
audited secret reader. All benchmark namespaces have unique `lcm-fork-exp-`
prefixes, and only these are explicitly deleted. Ordinary storage operations
never delete namespaces or historical records.

## Supported scope and remaining work

- Implemented: scoped table import, exact upstream assembly, append/context
  publication, upstream leaf compaction, FTS/regex message search, idempotent
  result receipts, CAS conflicts and unknown-outcome reconciliation. SQLite stays
  default, and no OpenClaw pin or upstream behavior tests are weakened.
- Bounded at 10,000 rows / 16 MiB per operation, 512 immutable blocks, 512 receipts
  and a 256 KiB manifest. Limits reject before publication and never truncate
  acknowledged data. Individual provider attribute limits still apply. These
  numbers are safeguards, not an unbounded lossless-storage claim.
- Gateway bootstrap/cursor/maintenance integration, cross-conversation numeric ID
  allocation and agent-wide recall across multiple roots are not complete. The
  current API binds a pre-existing conversation and imported IDs; appended IDs
  are local to that root. Do not present this as a drop-in `LcmContextEngine`
  replacement. Large-file rows retain metadata; actual file bytes remain outside
  this prototype, matching the common synthetic fixture.
- Safe GC is not implemented. Staging and losing commits leave retained garbage.
  A collector needs to fence paused writers before removing staged blocks and
  protect pinned readers and reconciliation receipts. A grace timer alone is
  insufficient. Receipt/chunk trees and bounded lazy graph loading need design.
- Strong reads never silently downgrade to eventual consistency. The provider's
  1.28 GB queried-byte floor applies to each charged read/conditional-write
  component even for these tiny namespaces. At the public $1/PB upper rate,
  two read queries contribute $2.56 per million operations before returned-data,
  write/storage charges and account minimums. This is not an invoice estimate.

This first contribution deliberately exposes a context-operation boundary rather
than claiming that the whole engine has been ported. It is useful to prove and
review incrementally in the fork. PostgreSQL batching is materially better than
its unbatched baseline in this local experiment; the remote Turbopuffer backend
remains viable but much slower in this topology. No production readiness or
deployment follows from either result.

## Qualification status

The release-pinned full upstream suite passed 134 files / 2,128 tests. A subsequent
broad rerun after import hardening had two timeout-only failures in unchanged
engine tests (5s test / 10s cleanup hook); that attempt is not called green. Both
failed files passed when rerun serially on the rebased branch (105 tests including
the storage tests), and the focused upstream storage/assembly/compaction selection
passed 156 tests. The final storage suite additionally tests concurrent callers
sharing one operation token: both must receive the **committed winner's** result,
not their different tentative results. Unknown responses never authorize blind
retries. The final rebased full suite passes **136 files / 2,150 tests** with one
worker. Typechecking, all build targets, and package dry-run inclusion of the
experimental bundle also pass; these are separate checks.

The same-token result bug was found and fixed during qualification: a losing
caller previously recognized the winning receipt but returned its own tentative
result. A dedicated real-provider regression in `bench/receipt-race.mjs` proves
matching replies and stable replay, with an independently cleaned synthetic
namespace. The historical paired timing artifacts identify the exact measured
source revisions; this later reconciliation fix does not silently replace them.
