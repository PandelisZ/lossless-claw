---
"@martian-engineering/lossless-claw": minor
---

Add an explicitly selected experimental Turbopuffer context-operation backend.
SQLite remains the default. The experimental backend stores immutable conversation
blocks and commits through a versioned root, and runs the existing assembler and
leaf compactor against disposable in-memory projections. Verified block reuse,
query-only read projections and one atomic immutable/root publication batch reduce
round trips while preserving strong root checks and CAS receipts. It is not a complete
OpenClaw gateway storage replacement.
