# projects/unstructured/corpus/loans: synthetic credit agreements

**Entirely fictional and committed.** Every borrower, agent and agreement is invented. They are shaped like syndicated
credit agreements and are not legal drafting anyone should copy. Rebuilt, byte for byte, by
`python tools/synth-corpus/gen_loan_docs.py` (seeded; needs `reportlab==4.2.5`, because a PDF's bytes depend on the version, in the environment `tools/unstructured-corpus/requirements.txt` makes, which holds reportlab's `chardet` at `>=7.0.0`: every earlier release is LGPL).

| Path | What |
|---|---|
| `agreements/CA-nnn.pdf`, `.txt` | One agreement, the same words in both. Offsets index into the `.txt`, where each clause is one blank-line-delimited block. |
| `ground_truth/LN_*.csv` | One file per `store::Lending` table, with the model's columns. |
| `ground_truth/spans.csv` | For each `Covenant` and `EventOfDefault`, the exact clause (`FILE_PATH`, `CHAR_START`, `CHAR_END`) that states it. |

12 agreements across 6 borrowers: each borrower has an original agreement and an amended and restated one, with different
terms. Each agreement has 1 to 3 facilities, collateral, 4 to 7 covenants (a step-down schedule and an equity cure in every
one) and 2 to 3 events of default, always with a grace period on non-payment and a cross-default threshold.

Reading it correctly:

- A covenant's `threshold` is its first level. `LN_COVENANT_STEP` has every level of the schedule, the first included.
- `equityCureAllowed` is true for a covenant whose clause is made "Subject to Section 7.11(d)". The cure right is in that clause.
- An unused section number is written `[Reserved]`, as real agreements do, so that a document's sections are all its rows.
- `gracePeriodDays` is the number the clause states, in Business Days for non-payment and calendar days for insolvency.
