# Phase 1 measured baseline (main @ 4dfbb5b7)

Series packages: exec executionplan testexecutionplan testplan testexec planner result setup testdatagen door.
Older packages for comparison: compiler lowering resolver parser normalizer sql lexer server protocol builtin.

## Process narrative in code (per 1,000 code lines)
- Dated references: series 12-48 (exec 48, executionplan 41, testplan 37) vs older 2-20 (resolver 2.4, parser 3.4, compiler 7.2, sql 18).
- docs/ citations: series 2-14 vs older 0.7-5.
- "the user": series 3-9 vs older 0.2-1.6 (server 6.7).
- Comment share of non-blank lines is similar everywhere (19-28%): it is the content, not the volume, that differs.

## Inline fully qualified names (per 1,000 code lines)
- Codebase-wide habit: compiler 113, builtin 256, resolver 82, lowering 97.
- Series: testplan 202, setup 197, testexec 184, planner 150, exec 85, executionplan 33.

## @Nullable (per 1,000 code lines)
- Series test-side packages 45-52 (testexecutionplan 52, testexec 48, testplan 45) vs older 3-23. Product-side series packages 17-21.

## case "Literal" string dispatch (count)
- exec 30, result 30, testexecutionplan 37, testexec 12; older: lowering 45, parser 111 (keywords, expected), protocol 139.

## Complexity (lizard)
- Series packages are no worse than older ones on average CCN (2.8-5.2 vs 4.0-6.8).
- Outlier: testplan: 46 functions with more than 6 parameters (8% of its functions; resolver 106 = 9%; others ~1-3%),
  13 functions with CCN > 30, max CCN 92 (DatabaseJudge::databaseVerdict, 160 NLOC, 13 params).
  SqlTextVerdicts::rowsLegAndVerdict has three overloads with 15, 17 and 18 parameters.
- Other hot spots: BodyClass::frameProgram CCN 76; AssertVerdicts::adjudicate CCN 65 / 223 NLOC;
  MirJson::readExpr CCN 58, MirJson::expr CCN 51; JdbcDecode::byPureType CCN 47.

## Textual copy-paste (jscpd, >= 80 tokens and 10 lines; re-run with --max-lines 100000 after a verifier found jscpd's default skips files over 1,000 lines)
- Series packages 0-1.26% (exec 0.21, executionplan 0.29, testexecutionplan 0, testplan 0.65, testexec 0.46, planner 0, result 1.26).
- Older packages 0.36-3.77% (compiler 0.46, lowering 0.36, resolver 0.81, sql 3.77).
- Copy-paste is not the problem in the series; structural duplication is (see the reviews).

## Scale
- 1,020 commits in the series range, 1,005 authored as Claude. 89 of them touched ArchitectureTest.java.
- core main 398K lines, core test 198K; root-level guard/census tests 21.8K lines.
- docs/ 395K lines in 1,410 files (as large as core's main code). AGENTS.md 536 lines, 5,776 words.
