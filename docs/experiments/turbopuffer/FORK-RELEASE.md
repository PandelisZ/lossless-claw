# Pandelis fork release: 1.3.0-turbopuffer.0

Publish identity: `@pandelis/lossless-claw`, npm channel `turbopuffer`.
Repository: `PandelisZ/lossless-claw`. Original Martian Engineering attribution,
MIT license and upstream changelog remain preserved. This is a fork release,
not an upstream npm release or a Vex production rollout.

## Final review

Reviewed the experimental transport, immutable block/root publication, scoped
imports, idempotent receipts, query-only read cache and its reader leases against
the documented bounded API. Real-provider race/crash-boundary evidence remains
in Git, while raw evidence dumps are excluded from the npm tarball.

Fork packaging findings were fixed: package diagnostics and update hints used
hardcoded upstream package names. They now derive the package name from this
manifest, scan this scope's installed copies, and suggest its `turbopuffer`
channel. Existing diagnostic tests now exercise the fork identity. The inherited release helper now explicitly recognizes the npm-only
`turbopuffer` prerelease channel; its stable/beta guards remain tested. The
changelog rollback marker identifies the upstream baseline, not a previous
release in this new npm scope. Host version
requirements, plugin id and config schema remain unchanged because this release
introduces no new OpenClaw host API/configuration requirement.

The tested API supports scoped import, exact native assembly, append, leaf
compaction, native FTS/regex search, atomic publication and receipt reconciliation.
It does **not** activate Turbopuffer for the entire OpenClaw engine. Gateway
bootstrap/cursors/maintenance, agent-wide multi-root recall and numeric ID
allocation, large-file bytes, host-generation fencing and safe GC remain outside
this initial release. Bounds fail before publication instead of truncating data.
Those limits are part of this prerelease's public documentation, not release
readiness claims for an unimplemented gateway adapter.

## Validation and publication

The release is built with mise-managed Node/npm. Typecheck, all build targets,
the full JavaScript suite, scoped fork diagnostics/storage tests, and Go TUI
tests must pass before merge. The packed artifact is installed in a disposable
consumer: its experimental API imports, SQLite remains default and CLI help runs.
Package/lockfile name and version match; registry authentication is the `pandelis`
account. No credentials are stored in source or artifacts.

The first full-suite attempt was invalidated by dependency-directory mutation
during lockfile verification; it is not a passing qualification. Dependencies
were restored and the final suite was run without concurrent install commands.

Publish from the exact merged fork commit using `--access public --tag turbopuffer`.
Verify registry version, channel, tarball integrity and Git identity, then install
that registry artifact in a fresh consumer. Vex can consume the exact scoped
version; changing its pin or activating a new backend is a separate change.

The canonical Turbopuffer document format is unchanged from the measured
prototype. Reverting the caller to the older prototype requires no data rewrite;
reconcile unknown operations before changing callers. SQLite stays the ordinary
plugin default. No automatic data deletion or production migration accompanies
this release.
