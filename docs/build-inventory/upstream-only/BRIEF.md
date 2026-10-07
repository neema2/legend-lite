# Homework: make every upstream generator's inputs come from upstream alone

## Goal (the user, 2026-10-05)
legend-lite's bump must be self-contained: every file derived from the pinned upstream (legend-engine, legend-pure) is
generated from the pinned checkouts ALONE, and only the bump regenerates it. Our own decisions (which natives we
implement, how each dynafunction resolves, the Lite extensions) must never be an input of these generators. They
belong in hand-owned registrations or hand-written code, as docs/COMPILER_DESIGN_2026_09_25.md tenets 4 and 5 say
("nothing about upstream is typed by hand"; "everything the platform decides for itself is a registration").

For your generators, establish without guessing:
1. Every input that is ours (not upstream), and exactly where the program reads it (`path:line`).
2. WHY it reads each one: what the output uses it for.
3. Whether it can change the output bytes, and how. Read the code path from the input to the written text. The
   caller is also running experiments; say what experiment would confirm each claim.
4. The concrete upstream-only design: what the generator should emit instead, where our part goes (a hand file, a
   registry row, hand code that joins the two at class init, or a test), and what in core consumes the output today
   and would have to change. Give the files and lines that change.
5. Feasibility risks, each with what would settle it.

## Rules
- Read-only. Never edit, commit or build (`bazel build`, `bazel test`, `bazel run`). You MAY run `bazel query`,
  `bazel cquery` and `bazel aquery` (always `env -C <repo> bazel ...`, never `cd`), git, grep, and read any file.
- The repo: the build/rebuild checkout (branch build/rebuild).
- Prior dossiers (verify, don't copy): docs/ (plan branch) build-inventory/generators/
  (G1 spec chain, G3 upstream-facing). Design context: docs/ (plan branch) GENERATORS.md
  and docs/COMPILER_DESIGN_2026_09_25.md in the repo.
- Every claim cites `path:line`, a query or a commit. Write OPEN with what would settle it, never a guess.
- Write runs/homework/<your id>.md. Return to the caller only a 10-line summary.
