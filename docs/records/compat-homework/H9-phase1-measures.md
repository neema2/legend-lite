# H9: phase 1's measures (the compatibility program's slice 0.4 (f), (g), (i), and the Operand's coverage)

B1 (`session_0195rQEnAa5CYXHygAxEFh3Y`), 2026-11-19, on the scratch branch `claude/compat-b1-homework` (never merged).
Asked by the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`) under the user's GO of 2026-11-19 ("do as much
homework, research, measurements right now up front as possible and write down all the learning/outcomes/details
immediately"). The plan is r5, `docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program` (r6 at `af6d5a9a`). The
branch is cut from `claude/compat-s0` (PR #123's head, `ef9e9bfa3`), so the probes run against the hardened net.

Every number here is reproducible from a committed probe or a committed table. Counts first, then the details.

## The counts

| item | the count | what it decides |
|---|---|---|
| 0.4 (f) BARE, by position | **95 of 98 at the literal root**; 3 in a relation column (1 never executed); 0 in a mapped list, a Pair or a new instance | 1.3 is the 4-day case |
| 0.4 (f) COMPOSED and FEEDS over Float, Decimal or DateTime, content read | **0 of 22**; one COMPOSED Pair print (PCT `testPairCollectionToString`) that 1.3's rule refuses | 1.3's refusal list: that one test |
| 0.4 (g) G7 over the net | (filled below) | the messages 1.2 changes under A3 |
| 0.4 (i) Floats in database-written JSON | (filled below) | whether a product fix is needed before a formatter |
| Operand's coverage | (filled below) | whether 1.2's `Operand` carries every literal `RaiseSite.operands()` holds |

## 0.4 (f) The prints, by position and by use

**Source.** H2's census, `docs/records/compat-homework/H2-text-in-queries.md` and its data
(`H2-data/pct-comb.tsv`, `H2-data/corpus-comb.tsv`, branch `claude/compat-homework-h2` at `a0d5f1ea`). The probe here
is a filter over those rows (`H9-probes/bare_positions.py.txt`); no new parse.

**Reproducing H2's 98.** The rows of class `RETURNED-BARE` whose function is a spelling function (`toString`, `format`,
`formatDate`, `char`; no other spelling function occurs on the query side) are 103: PCT essential 89, standard 3,
unclassified 9, the corpus 2. H2's table excludes the 5 whose operand is a String (identity prints: essential's
`testStringToString` 2, `format` over Strings alone 3: its column "text-only toString/format"). That leaves 98:
essential 84, standard 3, unclassified 9, the corpus 2.

### The 98 BARE sites, by position

| position | sites | tests |
|---|---|---|
| **at the literal root** (the print is the query's answer: `$f->eval(|x->toString())`) | **95** | PCT essential 84, standard 2, unclassified 9 |
| inside a returned structure | **3** | see below |
| of which in a mapped list, a Pair's side or a new instance's property | **0** | |

The 3 nested ones are all a relation's COLUMN:

| test | site | type, origin | note |
|---|---|---|---|
| PCT standard `testDayOfWeek_Relation` | `extend(~dayOfWeek: x|$x.date->toOne()->dayOfWeek()->toString())` | enum `DayOfWeek`, DATA | an enum's name: the same text in every audience (R0) |
| corpus `testStrictDateProject` | `project(~[id:t|$t.id, date:t|$t.date->toString()])` | StrictDate, DATA | a projected column: 1.3's "a TDS column's declared type changes with the print" |
| corpus `testToSQLStringChar` | `toSQLString(|Person.all()->project(p|char($p.age->toOne()), 'char'), ...)` | Integer `char`, DATA | **never executed**: the query is only `toSQLString`'s argument; its assert is SQL text (CP-4's class) |

**The 95 at the root, by type** (every operand a literal, or an instance PCT builds from literals):

| type | sites | by function |
|---|---|---|
| Float | 34 | `toString` 14, `format` 20 (`%f`, `%.Nf`, `%s`) |
| DateTime | 24 | `toString` 13, `format` 10 (`%s`, `%t`, `%t{...}`, `%r`), `formatDate` 1 |
| StrictDate or Date | 8 | `toString` 6, `format` 1, `formatDate` 1 |
| Integer | 15 | `char` 9, `format` 4 (`%d`, `%05d`), `toString` 2 |
| Boolean | 4 | `toString` 2, `format` 2 |
| Pair | 3 | `toString` 2, `format` 1 |
| List | 3 | `toString` 2, `format` 1 |
| a class, an enumeration, an enum value | 4 | `toString` (one over a class whose `toString` the test defines: `ClassWithComplexToString`) |

**So 1.3 is the 4-day case:** 95 of 98 at the literal root, 3 in a relation column (one never executed), none in a
mapped list, a Pair or a new instance.

### The 8 COMPOSED and 14 FEEDS sites: which print a Float, a Decimal or a DateTime and have their content read

**None.** Every one of the 22 is over an Integer, an enum, a Pair or an empty list:

| class | test | type, origin | what reads it |
|---|---|---|---|
| FEEDS | PCT essential `testRemoveDuplicatesPrimitiveNonStandardFunction` (4 sites) | Integer and String, a mixed literal list | `removeDuplicates`' comparator `==` |
| FEEDS | PCT essential `testRemoveDuplicatesByPrimitive` (2) | Integer and String | `removeDuplicatesBy`'s key |
| FEEDS | PCT essential `testRemoveDuplicatesEmptyListExplicit` (3) | the empty list (no element is ever printed) | comparator and key |
| FEEDS | PCT relation `testExtendFilter` (1) | Integer, a column | `filter ==` after `+` with a String column |
| FEEDS | PCT relation `testMixColumnNamesRenameExtend` (2) | Integer, a column | `filter contains` after `+` |
| FEEDS | corpus `testFilterUsingToStringFunction` (1) | Integer, a column | `filter ==` |
| FEEDS | corpus `testQualifierProperty` (1) | enum `PartyType`, in a derived property | `filter startsWith` after `+` |
| COMPOSED | PCT essential `testMatchManyWithMany` (1) | Integer, `size()` | `'address:' + ...` returned |
| COMPOSED | PCT essential `testPairCollectionToString` (1) | **Pair**, computed | `'(' + $pairs->map(p|$p.toString())->joinStrings(',') + ')'` returned |
| COMPOSED | PCT grammar `testMultiPlusWithFunctionExpressions` (3) | Integer, literal and computed | `toString(1) + ' ' + ...` returned |
| COMPOSED | PCT relation `testSimpleRelationProject`, `testSimpleExtendStrShared`, `testSimpleExtendStr_MultipleExpressions` (1 each) | Integer, a column | `$c.str + $c.val->toString()` returned |

**One finding for 1.3's rule.** `testPairCollectionToString` COMPOSES a Pair's print into a returned text, and a Pair's
text differs by audience (R5: its JSON object in LITE, `<a, b>` in PURE and ENGINE). Under 1.3's rule ("refused by name
where its type's text differs") it is the one upstream composed print the rule refuses, unless the translator rewrites
a Pair's print as a structural text it can spell in every audience. Its expected value is `'(<a, b>,<a, 1>)'`. It is
not over a Float, a Decimal or a DateTime, so it is outside 0.4 (f)'s question as asked, but it is the case 1.3's
"expected: none beyond H2's 14 Integer and enum sites" misses. Of the other 21: 18 print an Integer or an enum, whose
text is the same in every audience (R0), and the 3 over the empty list print nothing (no element exists).
