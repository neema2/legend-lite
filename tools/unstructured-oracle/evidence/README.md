# tools/unstructured-oracle/evidence: what a ledger row does not carry

The numbers `projects/unstructured/evals/DECISIONS.md` and the study read from the arms' whole lists. They hold pointers, ranks and scores, and
no passage text.

| File | What | Made by |
|---|---|---|
| `ranks.tsv` | For each resolved text case, per arm that runs: `best`, the best score with no floor (on hybrid, the fused relevance before the recency blend), and each accepted and superseded pointer's `rank` and `score` in the arm's whole list, `-` where the arm does not list it. Keyword and vector lists have no depth limit here, so "listed" in the study means a rank within the arm's depth; hybrid's list is what its two depth-limited arms fuse | `run.sh --write` |
| `passages.tsv` | Per source: passage rows, distinct hashes, hashes at more than one date, the median passage length in characters | `run.sh --write` |
| `ledger_before_cut_rule.tsv` | The ledger as shown before the cut became `max(5, accepted pointers)`, kept because the first committed ledger, `e42d8177a`, was already made under the rule (`DECISIONS.md`, entry 6) | once, by the judge at the time |
