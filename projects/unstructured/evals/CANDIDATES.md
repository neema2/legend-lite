# Answer-key candidates (stage 1)

**Candidates, not the key.** Nothing here is frozen, and no `answer_key.yaml` exists yet. Each case was found by reading the
corpus files, with grep. No case came from a search the system runs.

A pointer is `path [charStart:charEnd] sha256 — “snippet”`. Paths are relative to the corpus's own directory; the snippet is
at most 25 words of the span, verbatim. Every pointer was re-read from its file and its sha256 checked; each span is also a
row of `CO_PASSAGE`, except the table rows described below.

**Superseded** lists what an out-of-date answer would cite. **Near miss** lists a passage that reads like the answer and is about something else. A case must not be answered from either.

| Type | Cases | | Corpus | Cases |
|---|---|---|---|---|
| lookup | 14 | | EDGAR 13F | 14 |
| number | 14 | | EDGAR 10-K / 10-Q / 8-K | 20 |
| multi-hop | 11 | | SALES | 24 |
| time-aware | 17 | | LENDING | 22 |
| negative | 9 | | Total | 80 |
| keyword-only | 15 | |  |  |

The owner's ten are C01 to C10, and count in their corpus above.

## Open before this becomes the key

1. **C09 needs a ruling:** largest by row or by issuer.
2. **13F holdings are not passages.** `holdings.csv` loads into `ED_HOLDING`, not `CO_PASSAGE`, so a passage search cannot reach
   those rows. A table-row pointer is one line of the file. The key has to say whether these cases are judged on a table route.
3. **C04's clients** come from email metadata, not from any passage.
4. **A second reviewer** has to be named in the key before it freezes.
5. **Byte-identical clauses in different agreements.** The judge is to dedupe by sha256 before any cut and identify a result by
   `(path, charStart)`. C08 has twelve pointers and eight distinct hashes, so a deduped list can return at most eight of them. In
   C63, C75 and C76 the kept copy can be another borrower's, and the case then misses although the text is right. The key has to
   say what a hash hit credits: every location holding that hash, or only the one that survived the dedupe.

## The owner's ten

### C01 · time-aware · SALES

**Q:** What size and limit did Quillon put in for the Kestrelline 5.125% 2034s, and was that their final order?

**Why hard:** A superseded order ($25mm at 99.55, soft) sits earlier in the same thread, and the desk's acknowledgement repeats it.

**Answer:**
- SALES `bodies/MSG-0005.txt` [53:167] `9f9f9e400d3ba5c3d0a54e9f1f88438b59496e6cb479e24183723b82b053a281` — “Please put us in for $20mm of the Kestrelline 5.125% 2034s at a 99.75 limit. That limit is hard: we drop above it.”
- SALES `bodies/MSG-0005.txt` [169:216] `17d349212b20333170d0b1c037ecdedbc1381e73a4069e7e0f412d416db44195` — “Please replace our earlier order with this one.”

**Superseded:** moved to `answer_key.yaml`.

### C02 · lookup · SALES

**Q:** Why was Soren's order in the Tarnwick 7.875% 2031s paused?

**Why hard:** Two compliance holds read alike (MSG-0021, MSG-0044), and only the greeting carries the first name Soren.

**Answer:**
- SALES `bodies/MSG-0021.txt` [8:173] `a3344641ce350201b183c7e2af9786868f371b9da21ef0af10e2523db72b6a68` — “I have to pause your order in Tarnwick 7.875% 2031s. Compliance has placed it on hold: the account is pending its annual sanctions re-screen, which”

### C03 · multi-hop · SALES

**Q:** What was the final book size and coverage on the Kestrelline deal, and what was Quillon allocated?

**Why hard:** Book and allocation are separate paragraphs; an earlier $1.95bn book update is a trap, and four other deals' notices have this wording with other figures.

**Answer:**
- SALES `bodies/MSG-0006.txt` [8:117] `b1e91382e7d64cdb282111c52b91b2e1bef0590bc3324301c399fa1ba248a168` — “Allocations are out on Kestrelline 5.125% 2034s. Final book $2.85bn, 3.8x covered on $750mm, re-offer 99.714.”
- SALES `bodies/MSG-0006.txt` [119:278] `dc5dfedc471bcc5f55b256784204e294495c2bca8080abae9fcf15baccd29295` — “You are done on $5.25mm of your $20mm (26.3%). Syndicate allocated strictly pro rata to order size once the book closed, and no account was”

### C04 · multi-hop · SALES

**Q:** Which clients were declined on any deal, and on which?

**Why hard:** Three unconnected threads, none of which uses the word declined.

**Answer:**
- SALES `bodies/MSG-0010.txt` [9:109] `20d325d6a3d3c5d07343d1443822de2144aba68ca6ba1052246ecc4f36ac973a` — “Coming back on your $15mm order in Kestrelline 5.125% 2034s. Unfortunately we cannot allocate to it.”
- SALES `bodies/MSG-0035.txt` [11:100] `420d468bef9f63048c4a2d23454422146ff28faa24e367e30dda4d474b6b5c4f` — “Coming back on your $3mm order in Lumencrest IPO. Unfortunately we cannot allocate to it.”
- SALES `bodies/MSG-0060.txt` [12:102] `525d93d357118d66e800132cc66b42c25282ad1351749e7ed23e7002d64965f9` — “Coming back on your $10mm order in Caldrith block. Unfortunately we cannot allocate to it.”

**Note:** No passage names the client. Each email's greeting names a contact, and the client comes from `SA_EMAIL.CLIENT_ID`: Velmara (MSG-0010), Hallerbrunn (MSG-0035), Sorrelmead (MSG-0060). The passages can prove the deal and the refusal, not the client.

### C05 · number · LENDING

**Q:** In CA-003, what is the Senior Secured Net Leverage threshold for the quarter ending September 30, 2025?

**Why hard:** The last of four step-down rows; CA-009, the same borrower's restated agreement, has a near-identical clause with different levels.

**Answer:**
- LENDING `agreements/CA-003.txt` [6889:7618] `f2dd00e6d5d7926496c64f71956324ff49c548cb635ec28794b0f28dc31cf9b1` — “Fiscal Quarter ending September 30, 2025 and each Fiscal Quarter thereafter: 4.00:1.00”

### C06 · number · LENDING

**Q:** How many times may CA-003's Cure Right be exercised over the life of the agreement?

**Why hard:** The same sentence also gives 'three of any four' quarters, and eleven other agreements have near-identical cure clauses.

**Answer:**
- LENDING `agreements/CA-003.txt` [8187:8876] `3b6cc8dd84e5ad3e55a44c9e0f817c41011891901a80bcd1e39a875cb580b60b` — “The Cure Right may not be exercised in more than three of any four consecutive Fiscal Quarters or more than five times during the term”

### C07 · lookup · LENDING

**Q:** Which agreements carry a maximum capital expenditures covenant, and at what amounts?

**Why hard:** Clause (c) of Section 7.11 is a liquidity test or [Reserved] in most agreements: same reference, different content.

**Answer:**
- LENDING `agreements/CA-003.txt` [7938:8185] `eab0a50eed37b56b013d43175a1517162fe8c99181638cb25012f81d3cead105` — “(c) Maximum Capital Expenditures. The Borrower shall not make, or permit its Subsidiaries to make, Capital Expenditures in any fiscal year exceeding $70,000,000 in the”
- LENDING `agreements/CA-004.txt` [7925:8173] `6d5fa8acef5df60c489ea48c743fc6ec0bd7efa5db431fa82aaa548753b6b96e` — “(c) Maximum Capital Expenditures. The Borrower shall not make, or permit its Subsidiaries to make, Capital Expenditures in any fiscal year exceeding $130,000,000 in the”

### C08 · number · LENDING

**Q:** What is the cross-default threshold in each agreement that has one?

**Why hard:** Twelve near-identical clauses that differ only in the figure, so a top-k cut will miss some: this is a recall test.

**Answer:**
- LENDING `agreements/CA-001.txt` [10247:10582] `1ce32b4187e2e06a90ec3ff0215d51ca790c5184e279b2a0bf481f94240b122f` — “aggregate principal amount of more than $70,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-002.txt` [8562:8897] `049ca158da63f58eb1a50186e487442f13640240dc51ba91a4369aeb0470736e` — “aggregate principal amount of more than $60,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-003.txt` [9180:9515] `c0fd622b5e9ea1a8a05810410468df3bd4b4970178b34fbc6bb57f0513a8b6b6` — “aggregate principal amount of more than $65,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-004.txt` [9152:9487] `7eed77bbb707bd194de170197147d4820afe9a7b504fb57db9cba89c5324659e` — “aggregate principal amount of more than $75,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-005.txt` [9512:9847] `4b00d2ad10caf0abd5ea4c632103c0ea2b94a7e09d18f32718124d02c8cb9146` — “aggregate principal amount of more than $25,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-006.txt` [8937:9272] `c0fd622b5e9ea1a8a05810410468df3bd4b4970178b34fbc6bb57f0513a8b6b6` — “aggregate principal amount of more than $65,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-007.txt` [9531:9866] `b3ad7eb0eda33d4f0060f32e9ca7c7a719982eb8eee8b689422d79f2b5488de7` — “aggregate principal amount of more than $45,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-008.txt` [7967:8302] `b3ad7eb0eda33d4f0060f32e9ca7c7a719982eb8eee8b689422d79f2b5488de7` — “aggregate principal amount of more than $45,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-009.txt` [9896:10231] `7eed77bbb707bd194de170197147d4820afe9a7b504fb57db9cba89c5324659e` — “aggregate principal amount of more than $75,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-010.txt` [8283:8618] `722a319a29d79fa0d1f21f756e1f6015d174606a99add9cdf2ed72cb2f5c6ed8` — “aggregate principal amount of more than $80,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-011.txt` [9363:9698] `c51ef7edec1050353aa7b744caa247d641f7cd06a78bee689a802ca7e52a4e38` — “aggregate principal amount of more than $85,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”
- LENDING `agreements/CA-012.txt` [9002:9337] `722a319a29d79fa0d1f21f756e1f6015d174606a99add9cdf2ed72cb2f5c6ed8` — “aggregate principal amount of more than $80,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”

**Note:** Twelve pointers but only eight distinct hashes: CA-003/006, CA-004/009, CA-007/008 and CA-010/012 are byte-identical. See open item 5.

### C09 · number · EDGAR 13F

**Q:** What was Berkshire's largest holding by value in its most recent 13F, and did the total match the cover page?

**Why hard:** Largest single row (American Express, $50.4bn) and largest issuer once rows are combined (Apple, $65.95bn across 12 rows) differ; the earlier 13F is a near-duplicate.

**Answer:**
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [737:779] `f89be9d023f55724ecd2808f20d7e205835b402dd3a76015cf0c1dcfeff6c046` — “APPLE INC,037833100,200237120,692000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [780:824] `6ed598d7a696b446d4250a29572357aa6582a891655f8d473a456cf887e0de88` — “APPLE INC,037833100,1111142400,3840000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [825:864] `e0d961807be735b89ecbbae13e557a407b97b9a0e0b6ce95f5e3e400ff2c43cf` — “APPLE INC,037833100,9838240,34000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [865:911] `40967fe2e4dfdd79af6f6ccebcdf548a4692a8c3c91b709b2049f66ce2a1f994` — “APPLE INC,037833100,10047736640,34724000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [912:955] `418ac647b0cfb703e82098ffb5024096867c5f846bfc88d1ed67f82dbd25ea13` — “APPLE INC,037833100,788216640,2724000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [956:1001] `33b4a4a2c1c7fb410e340226a9e5b0e4630df50c26ddcec2d7786f471c2c2a3e` — “APPLE INC,037833100,4494339520,15532000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1002:1048] `47af7987af70e3a96f34543136d32095cd9973606870c90dc8a1311666dbe88f` — “APPLE INC,037833100,17808079008,61542988,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1049:1093] `37da4d6f02a3b264c9650516a361be238ae2a3c0f12277cbc6e1d1623770152e` — “APPLE INC,037833100,2084549440,7204000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1094:1139] `e2e6a0c77381f49171b4a77db88b0aa8420b656274e3b3ca6a20fe68ba730689` — “APPLE INC,037833100,4187617920,14472000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1140:1186] `f78769c01afc6c031c48e5accde94a2ddc067d884294e7fd57dc1a4a9ae8e362` — “APPLE INC,037833100,23341172315,80664820,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1187:1230] `a691b0394de95c2df4a845754e65cce5b06a9b0946087d26281b34d7b0d3315a` — “APPLE INC,037833100,784744320,2712000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1231:1275] `80e8c60e600948cccf40cd42a899ba99bea5d4ee3997cf03d4638991cfd77ae0` — “APPLE INC,037833100,1092623360,3776000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [571:624] `2db085a0b0b34e2cd2ed9e8405d5ea66605b072b5c0a45cef023ce4323d47bcc` — “AMERICAN EXPRESS CO,025816109,388967882,1149942,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [625:682] `5e22d73402c418ff30d21fa2622398a6c191a25362d30218348acd0582f0f4b5` — “AMERICAN EXPRESS CO,025816109,50419898471,149061045,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [683:736] `6d3d5dcb964204d9ed79f0c31b70f447af1678d22045109b586af84bcf20476c` — “AMERICAN EXPRESS CO,025816109,473452922,1399713,,DFND”
- EDGAR `13F-HR/1067983_0001193125-26-352200/cover.txt` [792:1590] `3cc6a6b66f66317353689991ce9cb30be1293ace75a37f7e3e5de17aacd6ea7f` — “formData.summaryPage.tableValueTotal: 299253556246”

**Note:** **Needs your ruling: by row or by issuer?** Apple's 12 rows and American Express's 3 are both listed until you decide; by row, only the $50.4bn American Express row answers. Checking the total means summing all 89 rows, which are not listed here: `corpus_test` proves the sum equals the cover page's 299,253,556,246.

### C10 · lookup · EDGAR 10-K / 10-Q / 8-K

**Q:** In Apple's latest 10-K Item 7, what did management say drove the change in Services revenue?

**Why hard:** The Q2 10-Q gives a different list (it adds the App Store) in an almost identical sentence, and the Q3 10-Q repeats the 10-K's.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [6908:7682] `ee33c2f1e9e36bf6c6a5ef9ec621aff736d21857c97725cc4d50cb1b40d9e3b6` — “Services net sales increased during 2026 compared to 2025 primarily due to higher net sales from advertising and cloud services.”

**Near miss, not superseded:**
- EDGAR `10-Q/320193_0000320193-26-000013/item_2.txt` [10406:11187] `35b7bc14355b7f0ec23285e9377048e7ea80c0f0916591da1233ebb5e6586618` — “Services net sales increased during the second quarter and first six months of 2026 compared to the same periods in 2025 primarily due to higher”

**Note:** Typed lookup rather than time-aware: the 10-Q covers a different period, so it is a near miss and nothing is superseded.

## EDGAR 13F

### C11 · time-aware

**Q:** How many Delta Air Lines shares did Berkshire report in its most recent 13F?

**Why hard:** The prior quarter's row is identical in shape, with 39,809,456 shares.

**Answer:**
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [2639:2694] `eb734196b3b5ea73e6a6b6cf120f22f60f9015f15438ce3967d0006db7b201fb` — “DELTA AIR LINES INC,247361702,5368591200,57320000,,DFND”

**Superseded:** moved to `answer_key.yaml`.

### C12 · time-aware

**Q:** What is Berkshire's current Nucor position in shares?

**Why hard:** It more than halved between the two filings, and the two rows differ only in digits.

**Answer:**
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [4126:4170] `f094ee5df561a0d8077b76b10cd910beccbae9a7df8b5a995226e5e9ccfbe7ab` — “NUCOR CORP,670346105,413814258,1857752,,DFND”

**Superseded:**
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [4203:4247] `686251945143fdc82b96ca715076e80c18092417fb9945dee000206a6d25d3d4` — “NUCOR CORP,670346105,660686383,3907075,,DFND”

### C13 · time-aware

**Q:** How many positions does Berkshire's most recent 13F list?

**Why hard:** The two cover pages are near-duplicates: 89 entries against 90, with only the digits differing.

**Answer:**
- EDGAR `13F-HR/1067983_0001193125-26-352200/cover.txt` [792:1590] `3cc6a6b66f66317353689991ce9cb30be1293ace75a37f7e3e5de17aacd6ea7f` — “formData.summaryPage.tableEntryTotal: 89”

**Superseded:**
- EDGAR `13F-HR/1067983_0001193125-26-226661/cover.txt` [792:1590] `73dfa30f43dd7fbb3a58fb96d81c937fb93daba17116c7a5b67c1e4143fa57ff` — “formData.summaryPage.tableEntryTotal: 90”

### C14 · multi-hop

**Q:** By how many shares did Berkshire's Lennar class B position change between its two 13Fs?

**Why hard:** Needs the row from each filing; the CUSIP differs from the class A rows, which share the issuer name.

**Answer:**
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [3197:3240] `6520eb6737bf9f004138c149063dc5e68fead321ecdb8e48adb5af116cf2958c` — “LENNAR CORP,526057302,19995576,237703,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [3076:3119] `0927931e86ff16fe9220cd2a965214cccefccbe8c01abdb90952d641fe7600ca` — “LENNAR CORP,526057302,26445959,298117,,DFND”

### C15 · number

**Q:** What was Renaissance Technologies' largest position by reported value?

**Why hard:** A 3,140-row table in which Alphabet's combined value comes close; no passage states the ranking.

**Answer:**
- EDGAR table row `13F-HR/1037389_0001037389-26-000059/holdings.csv` [102511:102564] `f28281a490832ec2c049581291dc7205f32817ff9551788752a3dddcae9eba8e` — “NVIDIA CORPORATION,67066G104,1418869170,7091256,,SOLE”

### C16 · multi-hop

**Q:** What share of Renaissance's reported portfolio was its NVIDIA position?

**Why hard:** Needs a table row and the cover page's total, which are in different files.

**Answer:**
- EDGAR table row `13F-HR/1037389_0001037389-26-000059/holdings.csv` [102511:102564] `f28281a490832ec2c049581291dc7205f32817ff9551788752a3dddcae9eba8e` — “NVIDIA CORPORATION,67066G104,1418869170,7091256,,SOLE”
- EDGAR `13F-HR/1037389_0001037389-26-000059/cover.txt` [772:1524] `223bc93b2d8cdd9eb383ffea070596232cae2ebed36439e2c8499467eefea7b2` — “formData.summaryPage.tableValueTotal: 72617871974”

### C17 · keyword-only

**Q:** Which issuer is CUSIP 91307C102 in Renaissance's 13F?

**Why hard:** An opaque identifier: nothing about it is semantic.

**Answer:**
- EDGAR table row `13F-HR/1037389_0001037389-26-000059/holdings.csv` [142819:142881] `aa8bc94121495e1a23ba249c3c1059e373762915acb965091b148a3d15410b70` — “UNITED THERAPEUTICS CORP DEL,91307C102,925846052,1708739,,SOLE”

### C18 · keyword-only

**Q:** Which filer has 13F file number 028-06262?

**Why hard:** A bare number among near-identical cover-page fields, and the filer's name is in the passage before it.

**Answer:**
- EDGAR `13F-HR/1037389_0001037389-26-000059/cover.txt` [772:1524] `223bc93b2d8cdd9eb383ffea070596232cae2ebed36439e2c8499467eefea7b2` — “formData.coverPage.form13FFileNumber: 028-06262”
- EDGAR `13F-HR/1037389_0001037389-26-000059/cover.txt` [0:771] `5e8c9628032800d165f85a3d85571cd7ae9e41489ad19d625c8bf9f823cf8794` — “filingManager.name: RENAISSANCE TECHNOLOGIES LLC”

### C19 · keyword-only

**Q:** Which issuer name does Berkshire's most recent 13F use for CUSIP 060505104, and how many rows does it take?

**Why hard:** The name changes between filings (BANK AMERICA CORP, then BANK OF AMER CORP): a question in plain English matches neither.

**Answer:**
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1276:1327] `144a30bf31e2fb0a6b85b87b937dc44538e4d3e6858b43da76a75fd94fb3fd47` — “BANK OF AMER CORP,060505104,223361600,3920000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1328:1380] `53184150d6b6ac72f7ce6cc6fb8d4f9479604bca2623224131587cb5beb0f889` — “BANK OF AMER CORP,060505104,678062000,11900000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1381:1429] `d364abf8f9ff9087aa994f231d6fddc27e90eee3e9d2b82636f1b9d5cc5e4dbc` — “BANK OF AMER CORP,060505104,8290590,145500,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1430:1481] `3a3a3d6e4471f50e371380382cbae28231cd2bae3df2451e0516487d8b00f237` — “BANK OF AMER CORP,060505104,119658000,2100000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1482:1535] `ce395d921be9fe835110f15e6e7c81d0327578d0c083ad61cfdd672a28597923` — “BANK OF AMER CORP,060505104,1196580000,21000000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1536:1591] `0c9d20246663da76dc608c780324c4344c1cc9db214ffc0214dca98deb03d7e5` — “BANK OF AMER CORP,060505104,17595909185,308808515,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1592:1646] `1d56997591035a83c8d43af005faca41240550b2e1bdafdb25bac7fa1d2004eb` — “BANK OF AMER CORP,060505104,7163525600,125720000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-352200/holdings.csv` [1647:1698] `ab3046f307d948487363b456000847ff938f6cb5508a37f93aaec31011eda0af` — “BANK OF AMER CORP,060505104,558404000,9800000,,DFND”

**Superseded:**
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1273:1324] `905397415d19778d0df8059b9abb60fdd150226e0d685e0c912fed1596b9ad24` — “BANK AMERICA CORP,060505104,191100000,3920000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1325:1377] `c4add5c2265f89cbaff88f65c29989b0ba29f24929d2ab6a2c7c53b7e6b9e0b7` — “BANK AMERICA CORP,060505104,580125000,11900000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1378:1426] `f39b0df512889253d5bbbebe9f824357f402aa7b52d6e47dd08cd4b716ac9d8f` — “BANK AMERICA CORP,060505104,7093125,145500,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1427:1480] `204ebe0767ca9ecb0d7b10695d798368605fe3ba265b32ba8870218d35dc4142` — “BANK AMERICA CORP,060505104,1376634919,28238665,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1481:1532] `34574b286cf3bc2a27d584d8618808517f94b6b9af233d25648d72495c720a21` — “BANK AMERICA CORP,060505104,102375000,2100000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1533:1586] `965d2e37284affe9b6c28bc0e4bf3defb0fd198797a97691216de3bc260229e8` — “BANK AMERICA CORP,060505104,1023750000,21000000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1587:1642] `d164ab83a8ef4fa9b4f7064a219459912863e7a03941c3164392da242d1f26b5` — “BANK AMERICA CORP,060505104,15151500000,310800000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1643:1697] `c775c970eb7724c9e8f64bdc803f239703991f87208427c1b4b669276fc7b53c` — “BANK AMERICA CORP,060505104,6128850000,125720000,,DFND”
- EDGAR table row `13F-HR/1067983_0001193125-26-226661/holdings.csv` [1698:1749] `3609019750188c4f2d830c15071fd87218344235ad55922edd2508096694e8e1` — “BANK AMERICA CORP,060505104,477750000,9800000,,DFND”

### C20 · number

**Q:** What total value did Renaissance report on its cover page?

**Why hard:** An unformatted 11-digit field beside the 3,140 entry count.

**Answer:**
- EDGAR `13F-HR/1037389_0001037389-26-000059/cover.txt` [772:1524] `223bc93b2d8cdd9eb383ffea070596232cae2ebed36439e2c8499467eefea7b2` — “formData.summaryPage.tableValueTotal: 72617871974”

### C21 · lookup

**Q:** On what date was Berkshire's most recent 13F signed?

**Why hard:** The earlier cover page has the same signature block with a different date.

**Answer:**
- EDGAR `13F-HR/1067983_0001193125-26-352200/cover.txt` [792:1590] `3cc6a6b66f66317353689991ce9cb30be1293ace75a37f7e3e5de17aacd6ea7f` — “formData.signatureBlock.signatureDate: 08-14-2026”

### C22 · negative

**Q:** How many Tesla shares did Berkshire hold at June 30, 2026?

**Why hard:** Renaissance's 13F has a Tesla row, so a retriever that blurs filers will return it.

**Expected: the empty set.** Checked:
- regex `tesla` over 1 file(s) under `EDGAR/13F-HR/1067983_0001193125-26-352200/holdings.csv`: 0 hits
- regex `tesla` over 1 file(s) under `EDGAR/13F-HR/1067983_0001193125-26-226661/holdings.csv`: 0 hits

### C23 · negative

**Q:** Which of Renaissance's positions are reported as put options?

**Why hard:** The table has a put_call column, which invites an answer, but it is empty on all 3,140 rows.

**Expected: the empty set.** Checked:
- regex `,(PUT|CALL|Put|Call),` over 1 file(s) under `EDGAR/13F-HR/1037389_0001037389-26-000059/holdings.csv`: 0 hits

## EDGAR 10-K / 10-Q / 8-K

### C24 · time-aware

**Q:** What is JPMorgan's most recently reported supplementary leverage ratio?

**Why hard:** A matching bullet in the 10-K says 5.8%, and the 10-Q's table row carries seven periods, of which the first is the latest.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [15855:16627] `cba9f2848ecb0d66438d30cf728cefa79698a4a50bfe0926d0f89e8bddcd4f55` — “•SLR was 5.5%.”
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [2284:3012] `1ed9c5fb0b822eceb46d835987c95d6b332d1e159f1c19c1e377c4563bd25ff9` — “Supplementary leverage ratio (“SLR”)(c)  5.5  5.5  5.6  5.8  5.8  5.5  5.8”

**Superseded:** moved to `answer_key.yaml`.

### C25 · time-aware

**Q:** What is JPMorgan's latest Standardized CET1 ratio?

**Why hard:** A matching bullet in the 10-K gives 14.6%.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [15855:16627] `cba9f2848ecb0d66438d30cf728cefa79698a4a50bfe0926d0f89e8bddcd4f55` — “•CET1 capital was $304 billion, and the Standardized and Advanced CET1 ratios were each 14.0%.”

**Superseded:** moved to `answer_key.yaml`.

### C26 · time-aware

**Q:** What full-year 2026 net interest income does JPMorgan expect?

**Why hard:** January's $103 billion and October's $106.2 billion are the same sentence with different numbers; the 10-Q also has a Q4 figure.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [20798:21443] `79858ddd877db48ff7724ee586fc9adbf4a80fa802189b61ed827a994fe97c78` — “•Management expects net interest income to be approximately $106.2 billion and net interest income excluding Markets to be approximately $97.2 billion, market dependent.”

**Superseded:**
- EDGAR `10-K/19617_0001628280-26-008131/item_7.txt` [14942:15519] `72162f2e9957b52001b31d89a3f9636a22831eb0b73ac546bff3aec34ac6872d` — “•Management expects net interest income to be approximately $103 billion and net interest income excluding Markets to be approximately $95 billion, market dependent.”

### C27 · number

**Q:** What were Apple's Services net sales for fiscal 2026?

**Why hard:** Quarterly and year-to-date figures for the same line sit in two 10-Qs, and 2025's is in the same row.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [6184:6906] `8946a0cedeaa4dc8726e89dfbed8aaf2022742b7dea5d98c25010f29e766a1fb` — “Services (1)  123,301  13%  109,158  14%  96,169”

### C28 · number

**Q:** What was Apple's effective tax rate for fiscal 2026?

**Why hard:** The row holds three years' rates, and only the first is 2026.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [9193:9864] `9f64f70f73aee03155b33fadecf3c7e1df499ddb75be716b7f204adfc411a661` — “Effective tax rate  17.0%  15.6%  24.1%”

### C29 · lookup

**Q:** What drove the increase in Apple's R&D expense in fiscal 2026?

**Why hard:** The Risk Factors discuss artificial intelligence at length without giving this driver.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [9193:9864] `9f64f70f73aee03155b33fadecf3c7e1df499ddb75be716b7f204adfc411a661` — “R&D expense increased during 2026 compared to 2025 primarily due to higher infrastructure-related costs, including investments in artificial intelligence, and headcount-related expenses.”

### C30 · lookup

**Q:** Why did Apple's Greater China net sales rise in 2026?

**Why hard:** Five near-identical regional paragraphs, each with its own products and currency note.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [5414:6182] `fbe8c2c996dc002963f7edd4e533d513f9a9af238a23dadc7df8cc2bbe369ad8` — “Greater China net sales increased during 2026 compared to 2025 primarily due to higher net sales of iPhone and Mac. The strength in the renminbi”

### C31 · multi-hop

**Q:** What were Apple's fiscal 2026 Services net sales and Services gross margin percentage?

**Why hard:** Two tables with the same row label, 'Services', in different passages.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [6184:6906] `8946a0cedeaa4dc8726e89dfbed8aaf2022742b7dea5d98c25010f29e766a1fb` — “Services (1)  123,301  13%  109,158  14%  96,169”
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [7683:8429] `1018681a296a93fe9e6d203108b43d22b271ccadd95a3ef2d763493029ec9ae1` — “Services  76.2%  75.4%  73.9%”

### C32 · keyword-only

**Q:** When were the initial results of the Section 232 semiconductor investigation published?

**Why hard:** A statute number that an embedding treats like any other; three filings state it, and all three are accepted.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [2183:4821] `0745cc758afd521e0cd56563382fd2900d130a55ed4e40f5f4376f9ae62689a5` — “On January 14, 2026, initial results were published of the previously announced U.S. Department of Commerce investigation under Section 232 of the Trade Expansion Act”
- EDGAR `10-Q/320193_0000320193-26-000013/item_2.txt` [4430:6990] `9e21b717ede0a257f1ee5336e38a9491bc5b8e65ff842cd7fcf002a4c98687e5` — “On January 14, 2026, initial results were published of the previously announced U.S. Department of Commerce investigation under Section 232 of the Trade Expansion Act”
- EDGAR `10-Q/320193_0000320193-26-000020/item_2.txt` [4596:7246] `1fafd1ab3b3a7494002a07c8a85bf47efe7bb651ea491305a6127627b52bc176` — “On January 14, 2026, initial results were published of the previously announced U.S. Department of Commerce investigation under Section 232 of the Trade Expansion Act”

### C33 · keyword-only

**Q:** Which registration form carries File No. 333-285537?

**Why hard:** A file number with no semantic content.

**Answer:**
- EDGAR `8-K/19617_0001193125-26-426704/item_8.01.txt` [521:825] `12dfcd085e2dcbde892f9085e50ecb59af7804902f9eb8beb10aafcf3d775eb8` — “The offerings of the Notes were registered under the Securities Act of 1933, as amended, pursuant to a registration statement on Form S-3 (File No.”

### C34 · number

**Q:** What principal amount of JPMorgan's notes due 2037 closed on October 20, 2026?

**Why hard:** Three tranches in one sentence, two of them due 2030.

**Answer:**
- EDGAR `8-K/19617_0001193125-26-426704/item_8.01.txt` [0:519] `9bb7ee68709858327e7b07b8d2f4e3c0fdcf3ff2f684d15a76af9f9c08f762a9` — “(iii) $3,750,000,000 aggregate principal amount of Fixed-to-Floating Rate Notes due 2037 (together with the Floating Rate Notes and the 2030 Fixed-to-Floating Rate Notes, the “Notes”).”

### C35 · lookup

**Q:** Which fiscal quarter did Apple's 8-K of July 30, 2026 report on?

**Why hard:** Three 8-Ks differ only in two dates and an ordinal.

**Answer:**
- EDGAR `8-K/320193_0000320193-26-000018/item_2.02.txt` [0:640] `9a63b84eff24addc313f59f3466ca47806b6ee8118c1c32d3000655d3cf46da0` — “On July 30, 2026, Apple Inc. (“Apple”) issued a press release regarding Apple’s financial results for its third fiscal quarter ended June 27, 2026. A”

### C36 · keyword-only

**Q:** How much HQLA did JPMorgan report at December 31, 2025?

**Why hard:** An acronym; the 10-Q has its own HQLA figure for a later date.

**Answer:**
- EDGAR `10-K/19617_0001628280-26-008131/item_7.txt` [10287:11028] `5686a1160a62165dd7478f93e53ad364acc01c71171cc6b1c937519ac6bc2f7d` — “•As of December 31, 2025, the Firm had eligible end-of-period High Quality Liquid Assets (“HQLA”) of approximately $915 billion and unencumbered marketable securities with a”

### C37 · lookup

**Q:** When is JPMorgan's Apple Card transaction expected to close?

**Why hard:** Later footnotes in the 10-Q give dollar amounts 'associated with the Apple Card transaction', not its timing.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [3775:4522] `eb8c41aab7959af473eaccc876ef09035b597045b25b631d54fa8a1e7898b468` — “with an expected closing date approximately 24 months thereafter. Refer to Notes 4, 13, 27 and 28 of JPMorganChase’s 2025 Form 10-K for additional information.”

### C38 · number

**Q:** How did JPMorgan's average total VaR change in the third quarter of 2026 against the second?

**Why hard:** The next paragraph gives a different comparison (+$15 million against the prior year) in the same words.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_3.txt` [5608:6153] `918f7b22c0bec7c685664396bce14702fb16b79b805a2aebd0784c040d04446b` — “Average total VaR for the three months ended September 30, 2026 decreased by $1 million, when compared with June 30, 2026, driven by the sale”
- EDGAR `10-Q/19617_0001628280-26-069523/item_2.txt` [239839:240384] `918f7b22c0bec7c685664396bce14702fb16b79b805a2aebd0784c040d04446b` — “Average total VaR for the three months ended September 30, 2026 decreased by $1 million, when compared with June 30, 2026, driven by the sale”

### C39 · number

**Q:** What one-day foreign-exchange VaR did Apple report for fiscal 2026?

**Why hard:** The sentence gives two years, and the first number is the current one.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7a.txt` [2214:3002] `779350959c29b9532e6ba37ac0597b29be9f48fd19eb94ad02cbf77e9bc50d72` — “the Company estimates, with 95% confidence, a maximum one-day loss in fair value of $560 million and $590 million as of September 26, 2026 and”

### C40 · negative

**Q:** What did JPMorgan's 8-K of October 13, 2026 report under Item 2.02?

**Why hard:** The filing is in the corpus but declares only Items 7.01 and 9.01, so no text was extracted; other JPMorgan 8-K text is the distractor.

**Expected: the empty set.** Checked:
- no file matches `8-K/19617_0001628280-26-065616/*.txt`

### C41 · lookup

**Q:** Where does JPMorgan's latest 10-Q discuss its risk factors?

**Why hard:** The correct passage is only a cross-reference, and the 10-K's substantive risk text reads as a better match.

**Answer:**
- EDGAR `10-Q/19617_0001628280-26-069523/item_1a.txt` [0:402] `b8122e9d93571ca6f3efd84485b04b1ccf94c35a073a42bf0b6fe61c5509294a` — “Refer to Part I, Item 1A: Risk Factors on pages 9–31 of JPMorganChase’s 2025 Form 10-K and Forward-Looking Statements on page 91 of this Form”

### C42 · keyword-only

**Q:** In which fiscal quarter did Apple announce the MacBook Neo?

**Why hard:** A product name in a bullet list; the quarter is a heading lines above.

**Answer:**
- EDGAR `10-K/320193_0000320193-26-000026/item_7.txt` [723:1368] `8a24a37762cdd95d1c2a20d7d8cbc5f2ccd2c19affc0991414cefabd7e6f703e` — “•MacBook Neo, MacBook Air and MacBook Pro”

## SALES

### C43 · time-aware

**Q:** How big was the book on the Kestrelline deal?

**Why hard:** A confident $1.95bn update appears in two earlier emails; the final $2.85bn is one clause of an allocation notice.

**Answer:**
- SALES `bodies/MSG-0006.txt` [8:117] `b1e91382e7d64cdb282111c52b91b2e1bef0590bc3324301c399fa1ba248a168` — “Allocations are out on Kestrelline 5.125% 2034s. Final book $2.85bn, 3.8x covered on $750mm, re-offer 99.714.”

**Superseded:** moved to `answer_key.yaml`.

### C44 · time-aware

**Q:** What was the latest price guidance on the Kestrelline new issue?

**Why hard:** The first pitch says T+135, and the two later emails word the update differently.

**Answer:**
- SALES `bodies/MSG-0008.txt` [122:149] `cbb76caa3b81fc2e0c21890b8b201bb6555e302cecd3b94ea156c98b2e0f2793` — “Guidance is now T+120 area.”
- SALES `bodies/MSG-0011.txt` [123:151] `8b5cc6fedf26d6cf1358119c0cf1a45bfc6a527e1c8be740f6d7fccb89f6acf0` — “We have moved to T+120 area.”

**Superseded:** moved to `answer_key.yaml`.

### C45 · time-aware

**Q:** Is Ostermark's Tarnwick order still on compliance hold?

**Why hard:** The hold email has far more words on the subject than the short clearance that replaces it.

**Answer:**
- SALES `bodies/MSG-0022.txt` [8:84] `969496137f138b48737aa198426323726bafe7b94e7c7b8a630dd453fc468429` — “Compliance cleared the hold on your Tarnwick 7.875% 2031s order at 13:20 ET.”

**Superseded:** moved to `answer_key.yaml`.

### C46 · time-aware

**Q:** At what price did the Lumencrest IPO price?

**Why hard:** Price talk gives a range, and Hallerbrunn's and Sorrelmead's limits are other dollar prices nearby.

**Answer:**
- SALES `bodies/MSG-0032.txt` [12:111] `b5161fb6e248eb6c8d5ea60f62b0454fce8d55e2df94db9257f69b19f184a112` — “Allocations are out on Lumencrest IPO. Final book $1.68bn, 4.0x covered on $420mm, re-offer $18.00.”
- SALES `bodies/MSG-0035.txt` [102:269] `0501c7ecd3c3744c68e246ead80d2836f74af56711c04a74103c1a2f0ab1c9aa` — “The institutional tranche is closed to private-bank accounts under the syndicate's allocation policy, and the priced level of $18.00 is through your limit in any”

**Superseded:**
- SALES `bodies/MSG-0029.txt` [119:214] `7946d7df4133903342706be96a77684043d60089c85491a3cdb1be5dfeda253e` — “Initial talk the $16.00 to $18.00 range. Syndicate expects decent demand, so earlier is better.”

### C47 · lookup

**Q:** By when did accounts have to reconfirm their orders after the Kestrelline reprice?

**Why hard:** Byte-identical sentences in two emails: both are accepted, and they collapse to one hash.

**Answer:**
- SALES `bodies/MSG-0004.txt` [122:212] `831fc4152db9e39888ff99ad3b58a7fdc78d21f65927c528ed35bfc03bf71a97` — “Existing orders stand unless you tell us otherwise. Please reconfirm or amend by 14:30 ET.”
- SALES `bodies/MSG-0009.txt` [126:216] `831fc4152db9e39888ff99ad3b58a7fdc78d21f65927c528ed35bfc03bf71a97` — “Existing orders stand unless you tell us otherwise. Please reconfirm or amend by 14:30 ET.”

### C48 · lookup

**Q:** Why did Velmara's Kestrelline order drop?

**Why hard:** The reason depends on a reprice the email only mentions; Quillon's order in the same deal survived it.

**Answer:**
- SALES `bodies/MSG-0010.txt` [111:216] `91b580be6ec10e4fa56f1ce32517a153d980495c1de611fa2d5394fd2bc11e9a` — “The deal re-offered at 99.714, which is above your 99.65 hard limit, so the order dropped at the reprice.”

### C49 · lookup

**Q:** Why could Hallerbrunn not be allocated on Lumencrest?

**Why hard:** Two reasons in one sentence, and Sorrelmead's scale-back is a near neighbour.

**Answer:**
- SALES `bodies/MSG-0035.txt` [102:269] `0501c7ecd3c3744c68e246ead80d2836f74af56711c04a74103c1a2f0ab1c9aa` — “The institutional tranche is closed to private-bank accounts under the syndicate's allocation policy, and the priced level of $18.00 is through your limit in any”

### C50 · lookup

**Q:** Why was the order in the Caldrith block put on hold?

**Why hard:** The other compliance hold has the same opening words and a different reason.

**Answer:**
- SALES `bodies/MSG-0044.txt` [6:212] `063d6a43f3f5a536495923acfd370b826073e76f95fa6382d64882fe21a4513a` — “I have to pause your order in Caldrith block. Compliance has placed it on hold: the issuer went on our restricted list this afternoon after”

### C51 · multi-hop

**Q:** What did Brantoak ask for on Tarnwick, and what did it get?

**Why hard:** A request and a fill in separate emails a day apart; Ostermark's order is in the same deal.

**Answer:**
- SALES `bodies/MSG-0016.txt` [39:132] `35d49d4e3ed0f04b4cf121bab66f2879a3d3734a767694ad597b216d393fc20c` — “Order is $30mm, limit 99.50. There is some give if the book is strong, but speak to me first.”
- SALES `bodies/MSG-0017.txt` [114:272] `80701a7cc069b2aaf15f333b3b9c2fc22e93d2723c6b2886861e91932b1e632c` — “You are done on $9.7mm of your $30mm (32.3%). Syndicate allocated strictly pro rata to order size once the book closed, and no account was”

### C52 · multi-hop

**Q:** What did Sorrelmead ask for on the Lumencrest IPO, and how much was allocated?

**Why hard:** Limit and scale-back are in different emails, and the limit is above the priced level.

**Answer:**
- SALES `bodies/MSG-0030.txt` [48:142] `1e05d9ee5f6fc1c763f84b6f32a3c5095cee59b50294371f091ae913d81f28f7` — “Order is $40mm, limit $18.25. There is some give if the book is strong, but speak to me first.”
- SALES `bodies/MSG-0032.txt` [113:314] `25b6f0dbf77b9cfec07c5843d2cbdcd1a28adb135847c4eb56c4fffa9fd6a27b` — “You are done on $10mm of your $40mm (25.0%). Long-only accounts were scaled back pro rata to demand across the institutional tranche, so this reflects”

### C53 · multi-hop

**Q:** What was Ysolde's limit on Ashgrove and what fill rate did it receive?

**Why hard:** Quillon's Ashgrove order was filled in full under a different rule.

**Answer:**
- SALES `bodies/MSG-0026.txt` [49:142] `6d6c000e6048da101bb6c935900ecec18029c127fc82e1f0296597ae66cce0f2` — “Order is $60mm, limit 99.90. There is some give if the book is strong, but speak to me first.”
- SALES `bodies/MSG-0027.txt` [112:272] `961f0ddb7a959c892d6393fb4cb24f56fb9efabd7e9b9288868bd15aadfc0ea2` — “You are done on $19.35mm of your $60mm (32.3%). Syndicate allocated strictly pro rata to order size once the book closed, and no account was”

### C54 · number

**Q:** What share of Velmara's Bexford commitment was allocated?

**Why hard:** The allocation appears twice in one sentence, as dollars and as a percentage.

**Answer:**
- SALES `bodies/MSG-0047.txt` [106:248] `91dcf79395b130de1630f1700449012b6b0339bfdd6549f1faa7546985200f70` — “You are done on $10.8mm of your $20mm (54.0%). Commitments were cut back pro rata across the CLO and institutional accounts at the final book.”

### C55 · number

**Q:** What was the minimum-ticket threshold on the Tarnwick deal?

**Why hard:** The same sentence with $2mm appears for Kestrelline and Ashgrove.

**Answer:**
- SALES `bodies/MSG-0023.txt` [87:176] `69aeb1ce36a095a747c890ff64ac185982e6c5d38171f19668c7909a54ef96e2` — “Tickets at or below $5mm were filled in full under the syndicate's minimum-ticket policy.”

**Note:** This is not a superseded value: the two thresholds are different deals' policies.

### C56 · keyword-only

**Q:** Which deal carries identifier SYN-G235E9Y6, and when does it settle?

**Why hard:** An invented token that embeddings cannot place; the issuer's name is in the previous paragraph.

**Answer:**
- SALES `bodies/MSG-0038.txt` [10:64] `77c20aaa217849fcfa1df6795a33c55953d15ea0223be44da9013eed4c63b0f6` — “Settlement instructions for Portwain follow-on, $12mm:”
- SALES `bodies/MSG-0038.txt` [66:222] `6c0a7dfd1ba3f266cec29d9227752434e95e982f5f65040be72c70809f907302` — “Trade date 2026-06-02, settling T+1. Deliver versus payment through Merrowfield Clearing, DTC participant 9X41, reference D-05/C02. Identifier SYN-G235E9Y6.”

### C57 · keyword-only

**Q:** What does settlement reference D-03/C01 relate to?

**Why hard:** The four settlement emails are the same template apart from these tokens, and the deal is named one paragraph earlier.

**Answer:**
- SALES `bodies/MSG-0057.txt` [11:66] `ec735bab7266a753d60966f3d1a3c63a098d1fcc0ddd5aa90a2f34021536c20e` — “Settlement instructions for Ashgrove 4.75% 2036s, $2mm:”
- SALES `bodies/MSG-0057.txt` [68:224] `042f7924796ee6c0e8b7ca1f436996026f389735aa36a9049e66e1f3638812e2` — “Trade date 2026-05-05, settling T+3. Deliver versus payment through Merrowfield Clearing, DTC participant 9X41, reference D-03/C01. Identifier SYN-3FQX1A1V.”

### C58 · keyword-only

**Q:** What did the Holvane block clear at for Quillon?

**Why hard:** A deal name; the two blocks' emails are the same template.

**Answer:**
- SALES `bodies/MSG-0041.txt` [7:75] `bb5e1b15997c9c5dcdf18ecd04875a635bf21dbf7cc7d062bfc71c77c8a01d7e` — “Good news on Holvane block: you are filled in full, $18mm at $27.60.”

### C59 · keyword-only

**Q:** On which deal was talk 'S+350 at 99 OID'?

**Why hard:** Loan jargon that embeds poorly, and the issuer is in the previous paragraph.

**Answer:**
- SALES `bodies/MSG-0045.txt` [12:158] `6bc5c5e31a542358aaa7a0ea1751e1725bb60a5a11de24a63af847b68d8d1486` — “Wanted you to see this before the screens fill up. Bexford Health, $600mm 6-year term loan B, first lien, proceeds fund the Northgate acquisition.”
- SALES `bodies/MSG-0045.txt` [160:232] `d9965d6901831b4d3324fba7c9a9f5370db1596a482c66009d5d24ea7ca00f04` — “Talk is S+350 at 99 OID. Happy to walk you through the credit if useful.”

### C60 · negative

**Q:** What was the final re-offer price on the Wexcombe 2031s?

**Why hard:** The deal has not priced; Ysolde's 99.50 limit looks like an answer.

**Expected: the empty set.** Checked:
- regex `Allocations are out on Wexcombe|Good news on Wexcombe|on Wexcombe[^\n]*re-offer` over 60 file(s) under `SALES/bodies/*.txt`: 0 hits

### C61 · negative

**Q:** How much was Hallerbrunn allocated on the Nimbrel follow-on?

**Why hard:** An indication exists but no order, and the follow-on has not been launched.

**Expected: the empty set.** Checked:
- regex `Allocations are out on Nimbrel|Good news on Nimbrel|your \$\S+ order in Nimbrel` over 60 file(s) under `SALES/bodies/*.txt`: 0 hits

### C62 · negative

**Q:** What are the settlement instructions for the Bexford term loan?

**Why hard:** An allocation exists, and four other deals' settlement emails look right.

**Expected: the empty set.** Checked:
- regex `Settlement instructions for Bexford` over 60 file(s) under `SALES/bodies/*.txt`: 0 hits

## LENDING

### C63 · time-aware

**Q:** What is Hartsmere's current cross-default threshold?

**Why hard:** CA-001 (2023) and CA-007 (2025) are one borrower's agreements, with a word-for-word clause and different figures.

**Answer:**
- LENDING `agreements/CA-007.txt` [9531:9866] `b3ad7eb0eda33d4f0060f32e9ca7c7a719982eb8eee8b689422d79f2b5488de7` — “aggregate principal amount of more than $45,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”

**Superseded:** moved to `answer_key.yaml`.

**Note:** CA-008, another borrower's agreement, has this clause byte for byte. See open item 5.

### C64 · time-aware

**Q:** Under Vennick's current credit agreement, what is the maximum Senior Secured Net Leverage Ratio for the first test period?

**Why hard:** The 2023 agreement's first row is the same shape at 5.50; the restated one starts at 4.00.

**Answer:**
- LENDING `agreements/CA-008.txt` [5920:6657] `db05f9069c4a6a881aaefb91a379b9451ab09d44887f412332960b912407fc69` — “Fiscal Quarter ending September 30, 2025 through the Fiscal Quarter ending December 31, 2025: 4.00:1.00”

**Superseded:** moved to `answer_key.yaml`.

### C65 · time-aware

**Q:** Does Ardlow's current agreement still cap capital expenditures?

**Why hard:** The 2023 agreement states the cap clearly; the answer is that the same clause letter is now a liquidity test.

**Answer:**
- LENDING `agreements/CA-009.txt` [8797:8900] `b92c85e61622b0470a2889f88dcb7270acaed769280d03e5aa649799bdf7dc6f` — “(c) Minimum Liquidity. The Borrower shall not permit Liquidity at any time to be less than $85,000,000.”

**Superseded:**
- LENDING `agreements/CA-003.txt` [7938:8185] `eab0a50eed37b56b013d43175a1517162fe8c99181638cb25012f81d3cead105` — “(c) Maximum Capital Expenditures. The Borrower shall not make, or permit its Subsidiaries to make, Capital Expenditures in any fiscal year exceeding $70,000,000 in the”

**Note:** The honest answer is no. The passage shows what now occupies clause (c); the words “Capital Expenditures” appear nowhere in CA-009.

### C66 · time-aware

**Q:** What is Cobbleford's current general lien basket?

**Why hard:** The figure fell from $175,000,000 to $45,000,000 in a clause otherwise unchanged.

**Answer:**
- LENDING `agreements/CA-010.txt` [5282:5580] `2e772a5c0f4c6ca9fa893a2084b780f6fc56f97fbb8f0ef91c77ac92d0ceca3b` — “other Liens securing obligations not exceeding $45,000,000 at any time outstanding (the “General Lien Basket”).”

**Superseded:**
- LENDING `agreements/CA-004.txt` [5946:6245] `f5c10e54f59f3fba5b290b454b4e07583ade946b5bfa1efdca99981c1068b75e` — “other Liens securing obligations not exceeding $175,000,000 at any time outstanding (the “General Lien Basket”).”

### C67 · time-aware

**Q:** What is Trevanion's current cross-default threshold?

**Why hard:** The figure rose from $25,000,000 to $85,000,000 in a word-for-word clause.

**Answer:**
- LENDING `agreements/CA-011.txt` [9363:9698] `c51ef7edec1050353aa7b744caa247d641f7cd06a78bee689a802ca7e52a4e38` — “aggregate principal amount of more than $85,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”

**Superseded:**
- LENDING `agreements/CA-005.txt` [9512:9847] `4b00d2ad10caf0abd5ea4c632103c0ea2b94a7e09d18f32718124d02c8cb9146` — “aggregate principal amount of more than $25,000,000 (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a”

### C68 · time-aware

**Q:** What is the first maximum Total Net Leverage level in Lissadell's current agreement?

**Why hard:** The restated agreement starts at 4.75, and the 2023 one at 5.50, in the same table shape.

**Answer:**
- LENDING `agreements/CA-012.txt` [6982:7695] `948329cb942091c95eb5d75329eb671a042cd0064595a2650a226dcb5fe03089` — “Fiscal Quarter ending December 31, 2025 through the Fiscal Quarter ending March 31, 2026: 4.75:1.00”

**Superseded:** moved to `answer_key.yaml`.

### C69 · number

**Q:** In CA-012, what is the maximum Total Net Leverage Ratio for the quarter ending December 31, 2026?

**Why hard:** A middle row of a step-down: neighbouring rows differ by one figure, and the date must be read as a range.

**Answer:**
- LENDING `agreements/CA-012.txt` [6982:7695] `948329cb942091c95eb5d75329eb671a042cd0064595a2650a226dcb5fe03089` — “Fiscal Quarter ending December 31, 2026 through the Fiscal Quarter ending March 31, 2027: 3.75:1.00”

### C70 · multi-hop

**Q:** In CA-011, which leverage test may be cured, and how long does the Borrower have to do it?

**Why hard:** The test and the cure are separate clauses joined only by a cross-reference.

**Answer:**
- LENDING `agreements/CA-011.txt` [7456:8083] `875cccc3249a384b2b6a483b332ffd2e73c0993123424e5e8ebac1fb8621f2cc` — “(a) Maximum Senior Secured Net Leverage Ratio. Subject to Section 7.11(d), the Borrower shall not permit the Consolidated Senior Secured Net Leverage Ratio as of”
- LENDING `agreements/CA-011.txt` [8391:9059] `554ccbb6fef8877df30ea0fa1bd4b7c8e7e50999387e058949a8c80551e91277` — “the Borrower shall have the right, until the tenth Business Day after the date on which financial statements for such Fiscal Quarter are required to”

### C71 · multi-hop

**Q:** In CA-012, what is the first Total Net Leverage level, and how long after the financial statements are due may a breach be cured?

**Why hard:** The covenant and its cure are separate clauses, and the window differs by agreement: the tenth Business Day in CA-003 and CA-011, the fifteenth here.

**Answer:**
- LENDING `agreements/CA-012.txt` [6982:7695] `948329cb942091c95eb5d75329eb671a042cd0064595a2650a226dcb5fe03089` — “Fiscal Quarter ending December 31, 2025 through the Fiscal Quarter ending March 31, 2026: 4.75:1.00”
- LENDING `agreements/CA-012.txt` [8008:8699] `45637a9b9bd257a26d671905668fd51fffa95e389f30ccdb8f90b2742ea03835` — “the Borrower shall have the right, until the fifteenth Business Day after the date on which financial statements for such Fiscal Quarter are required to”

### C72 · multi-hop

**Q:** What are the commitment and the margin on CA-009's Delayed Draw Term Facility?

**Why hard:** Sections 2.01(c) and 2.08(c) share a clause letter and a facility name.

**Answer:**
- LENDING `agreements/CA-009.txt` [3469:3828] `f4f1fb69fad2721bb83f2f6f9a918452e9dc1ca856ec1042da71226dd7061fbf` — “the aggregate of all such commitments being $475,000,000 on the date hereof. All amounts outstanding under the Delayed Draw Term Facility are due on September”
- LENDING `agreements/CA-009.txt` [4404:4798] `c65b6425f536de0735e33a65eb4eabc7047d38ab43d3bdab345e483df05759e8` — “(c) Delayed Draw Term Facility. Loans under the Delayed Draw Term Facility bear interest at Term SOFR plus 2.50% per annum, subject to a Term”

### C73 · keyword-only

**Q:** What is the “General Lien Basket” in CA-007?

**Why hard:** A defined term in quotes, repeated in other agreements with other amounts.

**Answer:**
- LENDING `agreements/CA-007.txt` [6905:7204] `6ad7f675d774a05e07ef7d8ade4f8223a00ab9848824d0e3f2458a40c7556662` — “other Liens securing obligations not exceeding $185,000,000 at any time outstanding (the “General Lien Basket”).”

### C74 · keyword-only

**Q:** Which agreements define a “General Debt Basket”?

**Why hard:** An exact defined term: five agreements have it, and the other seven have no such basket.

**Answer:**
- LENDING `agreements/CA-001.txt` [6954:7264] `705a7938b7abe788b0cfca2a823a42733abe02f8556d4acb2678d667144d9063` — “other Indebtedness in an aggregate principal amount at any time outstanding not exceeding $170,000,000 (the “General Debt Basket”).”
- LENDING `agreements/CA-002.txt` [5736:6045] `3a0bb0260a7731bf7d9048813060a497b82b10575885392e33de5c46bdaefb31` — “other Indebtedness in an aggregate principal amount at any time outstanding not exceeding $45,000,000 (the “General Debt Basket”).”
- LENDING `agreements/CA-005.txt` [6253:6562] `bba7097c6def22f9f63aaa4243ce3c126579fcdbf5790f39f7ed7c4e152f2e57` — “other Indebtedness in an aggregate principal amount at any time outstanding not exceeding $25,000,000 (the “General Debt Basket”).”
- LENDING `agreements/CA-009.txt` [7190:7499] `741700245575b68b41b3da5f1be1001871a2f8663d1303fa79a9cb0be679722d` — “other Indebtedness in an aggregate principal amount at any time outstanding not exceeding $70,000,000 (the “General Debt Basket”).”
- LENDING `agreements/CA-012.txt` [5857:6167] `5467ed9c55fb58b86e93ab7c16b70583180732b7d1a3e7d2d122e3410795ef01` — “other Indebtedness in an aggregate principal amount at any time outstanding not exceeding $130,000,000 (the “General Debt Basket”).”

### C75 · keyword-only

**Q:** What does Section 7.05(a) of CA-012 limit, and to how much?

**Why hard:** A clause reference, and the block's text does not contain it.

**Answer:**
- LENDING `agreements/CA-012.txt` [6569:6815] `ce16ae2533c66d5f0b54065c830a25874ad52ce56c7a182e15f1f5e48cca38aa` — “in an aggregate amount in any fiscal year not exceeding $60,000,000.”

**Note:** The text reads only '(a) ...'. 'Section 7.05(a)' is in the passage's `SECTION_OR_DOC` label, so a keyword arm finds it only if the label is indexed. CA-005 has this clause byte for byte: see open item 5.

### C76 · keyword-only

**Q:** What does “Threshold Amount” mean in CA-006?

**Why hard:** One short definition among twelve with the same words.

**Answer:**
- LENDING `agreements/CA-006.txt` [2606:2643] `278e82a0d346310251a64a237199d4f53d34b3ebe10daa1a70c86cf39758aed6` — ““Threshold Amount” means $65,000,000.”

**Note:** CA-003 has this definition byte for byte, and the question names CA-006. See open item 5.

### C77 · lookup

**Q:** Which agreement has an insolvency event of default, and how long may an involuntary proceeding stay undismissed?

**Why hard:** Only one of twelve agreements has this clause, and clause (c) in the others is something else.

**Answer:**
- LENDING `agreements/CA-012.txt` [9339:9546] `c049bbba89022e88b31544f0bc273a4d7616fe568f447c52688417534991f121` — “or any such proceeding is instituted against it and remains undismissed for 60 calendar days.”

### C78 · negative

**Q:** What is the minimum tangible net worth covenant in CA-001?

**Why hard:** It is the kind of covenant a credit agreement has; a retriever will offer the nearest financial test.

**Expected: the empty set.** Checked:
- regex `tangible net worth` over 12 file(s) under `LENDING/agreements/*.txt`: 0 hits

### C79 · negative

**Q:** Which agreements are governed by Delaware law?

**Why hard:** Delaware is everywhere as the borrower's jurisdiction, and every governing-law clause says New York.

**Expected: the empty set.** Checked:
- regex `STATE OF DELAWARE` over 12 file(s) under `LENDING/agreements/*.txt`: 0 hits

### C80 · negative

**Q:** What is the EURIBOR margin on the euro tranche in any agreement?

**Why hard:** The interest clauses look like the answer, and they all say Term SOFR.

**Expected: the empty set.** Checked:
- regex `euribor|euro tranche` over 12 file(s) under `LENDING/agreements/*.txt`: 0 hits
