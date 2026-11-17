# projects/unstructured/corpus/emails: synthetic sell-side emails

**Entirely fictional and committed.** Every firm, person, issuer and deal is invented. Addresses are under the reserved
`.example` domain. Rebuilt, byte for byte, by `python tools/synth-corpus/gen_emails.py` (standard library only, seeded).

**The identifier scheme is fictional too.** A deal's identifier is `SYN-` and eight letters or digits. The hyphen means
it cannot be a real CUSIP or ISIN, and the corpus uses no tickers.

| Path | What |
|---|---|
| `bodies/MSG-nnnn.txt` | One email's body. Offsets index into this file. |
| `ground_truth/SA_*.csv` | One file per `store::Sales` table, with the model's columns. `SA_EMAIL` has no `BODY`: the body is the file above. |
| `ground_truth/spans.csv` | For each `AllocationRequest`, the exact paragraph (`FILE_PATH`, `CHAR_START`, `CHAR_END`) stating its size and limit. |

8 clients, 12 contacts, 5 salespeople, 10 deals, 60 emails in 18 threads, 15 allocation requests.

Reading it correctly:

- A request that was revised points at the revising email. The superseded numbers are still in the thread, on purpose.
- A declined request has an allocated amount of 0. One still open has none.
- Every enum value is checked against `model.pure`, and `load.py` refuses any other.
