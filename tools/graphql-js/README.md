# The graphql-js differential

The validity oracle of lite's GraphQL (docs/GRAPHQL_STUDY_2026_10_18.md §9.3, G1; PR1b): graphql-js, the
reference implementation, judges whether a document is valid against the generated SDL. `differential.mjs`
writes the base documents of `documents.json` as they are and under a seeded set of mutations (a near-miss
field, Hasura's `_gt`, a bare value, an object without a selection, an unknown argument, a fragment on the wrong
type, a string for an Int, two keys in one sort object, a selection on a scalar, two fields under one key, an
undefined variable, an unused fragment, a mutation operation, an empty selection, an argument twice, an
introspection near-miss), with graphql-js's verdict on each, as JSON lines. `GqlValidityTest` in core reads those
lines and asserts that lite rejects everything graphql-js rejects (lite's acceptance a subset of graphql-js's,
never more), and that every stricter refusal is one of §6.3's named rules.

A hand tool, outside the Bazel build (the npm package is pinned in `package.json`; the lock is committed):

```
cd tools/graphql-js && npm install
node differential.mjs --schema ../../core/src/test/resources/graphql-parity/schema/demo.graphql \
  --documents documents.json --out ../../core/src/test/resources/graphql-parity/validity/validity.jsonl \
  --seed 20261019 --mutations 8
```

Rerun it when the golden schema or the base documents change; the test's assertions are about the committed
lines, so the file is the record. The seed makes the run reproducible.
