# Legend Studio, rebuilt light

Write Legend models as text, compiled as you type, then save, review, commit and release them. It
speaks upstream legend-sdlc's and legend-depot's APIs (plus lite's text routes), and it runs with no
server at all. Design: `docs/STUDIO_DESIGN_2026_10_02.md`.

```bash
bazel run //studio:serve                      # http://127.0.0.1:8200/demo/index.html  (level 0: no server)
bazel run //sdlc-server:server -- --port 6100 --repo /path/to/repo   # the model home (level 1)
#   then http://127.0.0.1:8200/demo/index.html?config=./config-server.json
bazel test //studio:all //sdlc-client:all //depot-client:all
bazel test //studio:verify_test               # the whole loop in the pinned Chromium, at level 0 and level 1
```

## What runs where

| Piece | Code | In the page (level 0) | On a server (level 1-2) |
|---|---|---|---|
| The editor | `studio/` (TypeScript, Monaco) | yes | the same page |
| The compiler (live errors) | `//wasm:planner`'s `compileOrError` | WebAssembly, in a worker | the same |
| The SDLC (projects, workspaces, saves, reviews, versions) | `sdlc-server/` rules (Java, written once) | compiled to WebAssembly, records in IndexedDB | `//sdlc-server:server` at `/sdlc/api`, over a real git repository |
| Depot (published versions, dependencies) | `depot-server/` rules (Java, written once) | the same module, over the page's own versions | the same server at `/depot/api` |

Studio talks to `sdlc-client/` and `depot-client/`, one typed client each. Where they answer from is
only configuration (`demo/config.json`: `"sdlc": "page"`; `config-server.json`: a URL; or `?sdlc=<url>`).

## The loop

1. **Setup.** Pick or create a project and a workspace. "Load demo projects" publishes the dogfood
   model (design S18) the way a person would.
2. **Edit.** Each element is its own file of Pure text, comments kept. A new element starts from a
   template by kind. Renaming the element in its text renames the file (a delete plus a create).
3. **Compile.** The model compiles in the tab as you type (and on F9), with the dependencies' files
   from Depot. Errors show in Problems, in the editor, and on the explorer and tabs.
4. **Save** (Ctrl+S). One revision of every changed file, guarded by upstream's revision lock: a save
   from a stale revision is refused. Imports are refused, as upstream SDLC refuses them (v0, design
   S20).
5. **Review.** Create a review, then commit it. The workspace merges onto the project line as one
   merge commit, and the workspace closes, as upstream's does. The commit is refused if it conflicts,
   or if the project line would not compile.
6. **Release.** Major, minor or patch of the project line's head, numbered after the latest version.
   Only a revision that compiles with its dependencies is released.
7. **Dependencies.** Pick a project and version from Depot. The closure is upstream's nearest-wins,
   and the in-tab compile and the SDLC's gates both use it.

## Tests

- `//studio:workspace_test`: the workspace model over the real SDLC and compiler modules. It covers
  the save diff, renames, the lock, and errors mapped to file and line.
- `//studio:demo_test`: the dogfood model published through every gate. The diamond resolves
  nearest-wins, and a trading workspace compiles in the tab with its dependencies.
- `//studio:verify_test`: the loop in Chromium, at level 0 and against the model home.
- `//sdlc-client:all`, `//depot-client:all`: one conformance suite each, run against the page's
  module and the server over HTTP.

## Not yet (design §3)

- Imports in files (v1, `docs/function-resolution/README.md`).
- Opening existing upstream projects' JSON-only elements: needs the model JSON reader and printer
  (S19, S20). Until then `entityChanges` answers 501.
- Form editors.
- Group workspaces, workspace update and conflict resolution.
- The GitHub backend (S16).
- Depot's `pureModelContextData` for the engine's pointers (S9).
- Query opening models by coordinates.
