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
Call `storage.close()` when finished to release its disposable read cache; active
read leases finish before their projection closes. Closing is not host-generation
fencing or cancellation of already-running writes. Stable operation tokens
deduplicate retries, preserve returned Date values, and
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

Every operation reads the strong root once. Cold operations bulk-fetch and verify
its immutable blocks. A per-instance cache retains at most one bounded manifest
of verified immutable blocks. Public snapshots and writes hydrate independent
**SQLite `:memory:` read/write projections**, reusing prepared inserts per table
shape. Read operations reuse a committed projection with `PRAGMA query_only=ON`
only when the freshly read root commit matches; they still execute the native
assembler/search each time. A changed root replaces the projection; reader leases
keep an older pinned projection alive until its readers finish. Failed strong
root reads reject rather than serving stale cached state. This SQLite database is
never a durable canonical store: deleting it loses no acknowledged conversation data, and a
fresh operation reconstructs it solely from Turbopuffer. There is no PostgreSQL,
Redis, Rails outbox, job queue, local database file or durable retry service
underneath this backend. CPU/memory/projection rebuilding are real costs, included
in all measurements.

For a write, new immutable blocks, the immutable manifest and the root CAS share
**one atomic namespace write batch**. Insert-if-absent applies only to immutable
IDs; the root patch independently checks its old version and commit. The provider
[guarantees atomic batches and conditional writes](https://turbopuffer.com/docs/guarantees).
A losing root patch can still insert unreachable immutable records, which readers
cannot see through the winning manifest. This is intentional, not a simulated
multi-document transaction condition. A pre-send crash publishes nothing; a lost
response after the batch commits is reconciled using the same operation receipts.
Only the root patch count establishes publication success, not aggregate affected
rows, which includes losing inserts. The live API omits some zero counters; the
parser accepts omitted zeroes only if known upsert/patch counts fully account for
the aggregate, otherwise it reconciles the unknown outcome. An unchanged root
without the operation receipt is
`UnknownCommit`, not permission to blindly retry. A changed root without the
receipt is `StorageConflict`; a caller must reload and intentionally rebuild.
No inference callback is automatically repeated. Stale observations are fenced;
retired-host generation/lease authority is not implemented.

The root carries its manifest. Fresh clients use two read round trips for the
measured 23-block conversations; unchanged warm reads use one strong root query.
With over 128 missing blocks, reads use additional 128-ID batches. A warm write
uses one strong root query and one atomic publication batch; a cold write also
fetches its missing blocks. Only changed/new blocks are written, and unchanged
historical blocks remain retained. The block cache trusts the protocol's immutable
content addresses; a fresh client independently verifies provider payloads. It is
not a background audit of out-of-protocol provider overwrites or deletions. The
current manifest/receipts are rewritten on each commit. Search fetches this same pinned state and applies the
native upstream search algorithms: it is **not a provider BM25 performance claim**.
It avoids orphan candidate overfetch by searching committed state. Cold/changed
roots still require constructing the complete bounded conversation projection;
unchanged warm reads reuse its in-memory index. Caches are disposable, scoped to
the explicit organization/agent/conversation instance, and contain no durable
acknowledged state. There is no process-global or cross-agent cache.

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

### Same-region EC2 qualification

A follow-up on November 7, 2026 (Pacific) runs the same harness, fixture and
native source from a fresh **t3.large (2 vCPU / 8 GiB)** in `us-east-1a` against
Turbopuffer `aws-us-east-1` over public HTTPS. PostgreSQL 18.6 is on that same
EC2 host, in a loopback-only Docker container limited to 2 CPUs / 768 MiB with
JIT disabled. Node is the same 26.12.1. PG fork source remains `1eebd7e`.
There is no PrivateLink, no production data and no production infrastructure
change. Provider AZ placement is unknown; same region does not imply same AZ.

| EC2 complete operation             |   n | p50 ms | p95/max ms | Network calls |
| ---------------------------------- | --: | -----: | ---------: | ------------: |
| SQLite assembly                    |   5 |  17.74 |      26.94 |             0 |
| PostgreSQL unbatched assembly      |   5 | 154.49 |     165.84 |       231 SQL |
| PostgreSQL batched assembly        |   5 |  26.99 |      32.40 |         1 SQL |
| Turbopuffer assembly               |   5 |  87.05 |     107.31 |        2 HTTP |
| PostgreSQL FTS                     |   5 |   3.55 |      10.48 |         1 SQL |
| Turbopuffer + projected native FTS |   5 |  73.78 |      83.39 |        2 HTTP |
| PostgreSQL batched leaf compaction |   3 |  54.22 |      78.74 |        11 SQL |
| Turbopuffer leaf compaction        |   3 | 367.83 |     380.15 |        4 HTTP |

The initial assembled output hash still matches byte-for-byte, normalized
post-compaction output matches, and organization/agent canaries remain absent.
All 200 messages remain retained with 23 blocks and a 2,012-byte manifest.
The same-region Turbopuffer path is substantially faster than the workstation
run, but the CPU also changed: EC2 SQLite is 17.74 ms versus 4.96 ms on the
workstation. Do not attribute the entire difference to network round-trip time.
On the **same EC2 host**, batched PG assembly is about 3.2x faster than this
Turbopuffer projection, and leaf compaction about 6.8x faster.

Across the entire live run, including setup and validation, 45 query requests
have a 19.50 ms median client time and 12 ms median provider-reported server
time. Fifteen writes have a 136.94 ms median client time and 130 ms median server
time. These aggregate distributions include import/publication work; they are
not per-compaction phase percentiles or pure database/network timings. The
remaining publication cost is therefore not explained solely by workstation WAN
latency. Sixty billed query components each report the 1.28 GB minimum, totaling
76.8 GB of billed queried units, not actual data scanned. Moving the client does
not remove this billing floor. No inference or embedding service was called.

[Raw samples and HTTP counters](evidence/ec2-us-east-1-comparison.json),
[host/runtime and binary hashes](evidence/ec2-us-east-1-host.txt), and
[execution/cleanup provenance](evidence/ec2-us-east-1-execution.json) retain the
exact evidence. Samples remain small (five reads / three independent compactions,
concurrency one), so p95 is simply the observed maximum. This is a bounded
same-region trial, not an outage/load/durability qualification. Earlier protocol
crash/race evidence is preserved separately; no new protocol claim follows here.

Reproduce with the existing benchmark on an isolated development EC2 host, using
its own disposable loopback PG database and synthetic fixture. Supply the key via
anonymous stdin into the Node process; the wrapper sets the environment only in
memory before importing the harness. Do not put the key in user data, SSH argv,
SSM commands, shell history, output files or instance tags. Record topology with:

```sh
# Append to the existing benchmark invocation; this changes provenance only.
--client-location 'EC2 us-east-1a; Turbopuffer aws-us-east-1 public HTTPS; PG loopback'
```

The instance had required IMDSv2, encrypted delete-on-termination storage, SSH
restricted to the initiating client's /32, and a 90-minute termination backstop.
Only its generated SSH key/security group and uniquely prefixed synthetic
namespace were used. Cleanup evidence records actual deletion/termination rather
than relying on the backstop. The normal plugin and production pins stay intact.

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
round trips and cleanup. These historical artifacts predate the cache/one-batch
optimization described below. The final root was revision2, with 23 blocks and a
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

## Optimization qualification on the same EC2 host

The old backend (`44aa5cd`) was rerun first on one fresh t3.large in `us-east-1a`.
The final backend (`4e72ee7`) then ran on **that same host**, with the disposable
PG database recreated from the same immutable fixture between runs. Native source
`51c3c90`, PG fork `1eebd7e`, Node 26.12.1 and PG 18.6/image digest stayed fixed.
All earlier evidence remains preserved. No production integration was enabled.

Three changes remove avoidable work: verified content-addressed block reuse,
prepared inserts per table shape, and a query-only committed read projection.
Every read still strongly checks the root; a root mismatch rebuilds its projection.
No assembled output is cached. Mutation projections remain independent. Atomic
block/manifest/root publication replaces two sequential writes with one batch.
The normal SQLite backend is unchanged.

| Complete operation                |   n | Old TP p50 ms | Final TP p50 ms | Final TP p95/max ms | PG p50 ms in final run | Final TP HTTP calls |
| --------------------------------- | --: | ------------: | --------------: | ------------------: | ---------------------: | ------------------: |
| Fresh-client assembly             |   5 |         83.90 |           85.37 |              120.02 |                  29.30 |                   2 |
| Unchanged-root warm assembly      |   5 |       83.90\* |           35.47 |               42.14 |                  29.30 |                   1 |
| Warm native projected message FTS |   5 |       66.69\* |           20.06 |               22.32 |                   4.96 |                   1 |
| Warm leaf compaction              |   3 |        370.53 |          206.81 |              207.98 |                  59.09 |                   2 |

\*The old implementation had no local read cache and rebuilt/fetched for every
operation. The final warm samples explicitly prime each client's blocks and
read projection outside the timed samples. Both paths still run the native
algorithms and include actual backend I/O. PG assembly remains one SQL call and
compaction eleven calls. The full initial assembly hash and normalized
post-compaction equality checks pass again, as do the agent/organization canaries.
SQLite p50 is 21.14 ms in the final run. Small samples make p95 the observed maximum.

Warm assembly is now about **1.21x PG latency** in the paired final run. This is
specifically an **unchanged-root** result. A new client remains about 85 ms, and
another append/compaction changes the root and invalidates the read projection.
Changed-root reads reuse any matching blocks but still hydrate and fetch missing
ones; that pattern is not separately timed here. Do not interpret the warm result
as a per-turn guarantee when every read follows a write. No retry, eventual read,
background publication or pre-durability acknowledgement was introduced.

Aggregate final-run writes still have 148 ms median provider server time (162.84 ms
client time, n=9 including initialization/import/publication), while query requests
have 15 ms median server time (21.35 ms client time, n=41). This bounds further
write-latency gains from client-only optimizations. These are aggregate distributions,
not exact compaction phases. The root check still costs one charged query component;
cache reuse does not remove provider queried-byte minimums. One warm assembly and
one warm compaction now use one and two request components respectively, versus
two and four in the old path. Raw billing counters are retained.

Run order/CPU scheduling can affect these small measurements: the baseline run's
PG assembly/compaction p50 were 26.14/40.80 ms and SQLite 14.64 ms, compared with
29.30/59.09/21.14 ms in the final run. The t3 instance was in unlimited-credit mode;
CloudWatch samples are retained. This is not a controlled hardware saturation or
steady-state tail-latency study. Report within-run PG ratios and cold/warm scope,
not a universal speedup or production readiness claim.

The canonical document format and schema are unchanged. Rollback to `44aa5cd`
requires no provider migration or data rewrite: stop the experimental caller,
reconcile any unknown publication outcome, and reopen with the older backend.
Its staged-write/root protocol can read these same committed blocks and receipts.
Disposable caches are discarded. Retained history/orphans are not garbage-collected
as part of either upgrade or rollback. Hosted gateway migration remains outside
this prototype's supported scope.

A separate **real API** protocol run verifies competing compactions and appends,
one winning root despite losing inserts, coherent graph reads from a fresh client,
a pre-send injected transport failure leaving the root unchanged, a successful
write with its response lost followed by another writer advancing the head,
fresh-client receipt replay, and same-token callers receiving the committed result.
It does not crash Turbopuffer servers or prove arbitrary service-outage recovery.
Documented provider atomicity, rather than a mock, supplies batch visibility.

The initial protocol instrumentation assertion failed because the live API omitted
zero `rows_patched` counters on losing requests. Publication failed closed and
reconciled correctly, but the strict parser did not return those counts to the
probe. The parser now permits omitted zeroes only when the aggregate is fully
accounted for by known operation counts; an ambiguous positive aggregate is never
accepted as a successful root CAS. The corrected live probe passes every assertion.
The failed attempt and its successful namespace cleanup are retained as evidence.

Evidence:

- [Same-host baseline](evidence/optimized-ec2-baseline.json),
  [final cold/warm comparison](evidence/optimized-ec2-results.json),
  [intermediate block-cache measurement](evidence/optimized-ec2-block-cache-intermediate.json).
- [Final atomic protocol run](evidence/optimized-ec2-protocol.json),
  [initial counter assertion](evidence/optimized-ec2-initial-counter-check.json).
- [Host/runtime](evidence/optimized-ec2-host.txt),
  [final source/bundle hashes](evidence/optimized-ec2-final-hashes.txt),
  [checks and CPU-credit data](evidence/optimized-ec2-validation.json),
  [verified resource cleanup](evidence/optimized-ec2-execution.json).

Affected validation passes 133 tests across native assembly/compaction and storage;
the final storage suite passes 12 tests, including read leases across root changes,
read-only enforcement, cache shutdown and strong-root failure. Typecheck and all
build targets pass. The historical full 2,150-test qualification is not silently
re-attested for this new runtime revision. Reproduce with the same paired harness;
`bench/atomic-publication.mjs --output <path>` runs the separate live protocol probe
with explicit experiment opt-in and an API key supplied only in memory.

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
