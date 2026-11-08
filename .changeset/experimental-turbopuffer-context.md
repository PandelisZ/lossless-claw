---
"@martian-engineering/lossless-claw": minor
---

Add an explicitly selected experimental Turbopuffer context-operation backend.
SQLite remains the default. The experimental backend stores immutable conversation
blocks and commits through a versioned root, and runs the existing assembler and
leaf compactor against disposable in-memory projections. It is not a complete
OpenClaw gateway storage replacement.
