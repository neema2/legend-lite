# GraalVM Web Image for the tab: the spike, and why the tab stays on TeaVM (2026-10-10)

**Question.** The tab's planner is TeaVM's compile of `planner.TabExports`. TeaVM cannot compile the JDK's own class
library, so it ships its own rewrite of `String`, `Double` and the rest, and protocol leg 5 found three places where
the rewrite answers differently from the JDK (`String.isBlank`, `Double.toString`, `parseDouble`; PARK-23 is the one
left open). GraalVM's Web Image (Native Image's WebAssembly backend) compiles the real JDK library. Would building the
tab with it remove that whole class of difference, and at what cost?

**Answer.** It would, measurably, and it plans about nine times faster; but it ships a module about four times
larger, it is still experimental and only in Oracle GraalVM (under Oracle's licence), and it does not build the same
bytes twice. **The user, 2026-10-10: the tab stays on TeaVM, made exact where lite needs it by a conformance test and
by fixes in TeaVM's class library.** Web Image is worth another look when the conditions at the end hold.

**Method.** Oracle GraalVM 25.4.4.1.1 and Binaryen 133, by hand, outside Bazel; a 50-line adapter
(`WebImageExports`) that calls the very `TabExports` methods TeaVM exports, so both compilers compile the same Java;
the repo's own differentials run against the result in Node 22.16 and the pinned Chromium 153; probes of the class
library; the module taken apart function by function. Every file, command and number:
[`build-inventory/program/evidence/webimage/`](build-inventory/program/evidence/webimage/README.md).

## What was found

1. **Correct on every check.** The planner differential 69/69, the timezone probe 8/8, the round trip in the tab
   9,423/9,423 byte for byte, in Node and in Chromium. It built on the first try (35 seconds).
2. **The JDK's class library, really.** A million random doubles through `Double.toString`, `parseDouble`,
   `BigDecimal.doubleValue` and `Float.toString`, and `isBlank` over every `char`: Web Image's answers equal the JVM's
   in all seven families. TeaVM 0.15, run as the control, differs in five of them, so the probe sees a difference
   when there is one.
3. **About nine times faster at planning.** 69 warm plans: about 0.2 s against about 1.9 s in Chromium; the first
   plan about 0.2 s against about 2 to 3 s. Parsing and printing models (the round trip) is about the same speed in
   both. TeaVM's top optimisation level (`FULL`) closes little of it (69 warm plans 1.5 s against 1.85 s) and makes
   TeaVM's module 70% larger, so the gap is not a TeaVM setting.
4. **About four times larger.** 4.97 MB compressed (Brotli) against TeaVM's 1.32 MB. A third of it is the startup
   heap, which Web Image builds with instructions rather than storing as data (152,000 objects: a description of each
   of 8,188 types, 49,500 strings, the JDK's module tables); a quarter is lite's own code, which Web Image translates
   more bulkily than TeaVM; the JDK's code is 13%. `prelude.pure` is under 1% (300 KB of text, 42 KB compressed); all
   the Pure text in the module, under 5%.
5. **Little of it can be taken back.** Graal's own settings (`-Os`, `-O1`, inlining off) change it by under 0.3%.
   Binaryen's `wasm-opt -O1` afterwards takes 11% (to 4.40 MB) and passes every check; `-Oz` is no smaller and breaks
   every call in Node 22 (its inlining pass). Brotli is the best format (zstd 9% larger, gzip 42%). Removing lite's
   two `ServiceLoader` uses and its `String.format` calls takes 0.5%: the JDK reaches both on its own paths
   (`ResourceBundle`; `java.net.URI`'s error messages). Fewer types (two thirds of lite's 3,737 types in the build
   are lambda classes) would take about 5%, for rewriting thousands of lambdas. A planner/protocol split into two
   modules is the one large lever left.
6. **Not reproducible.** The same source built twice gives different modules: the planner at 19,503,297 and
   19,437,759 bytes, the same with one build thread, and a 4,000-function probe with no `invokedynamic` in it. The
   order of the functions, which functions exist, which overload gets which name, and the memory address in the names
   of the classes Java makes for `switch` on types and string `+` all change. TeaVM builds the same input to the same
   bytes. A small source change also moves 95% of Web Image's bytes, so sending only what changed between versions
   saves little.
7. **Oracle's licence, and experimental.** Web Image is in Oracle GraalVM 25.1 and later, not in the Community
   Edition the repo pins (25.0.2). Oracle's terms count what Native Image produces as part of Oracle's program, free
   to redistribute only without charge: the module the tab ships would carry that condition to anyone who bundles
   lite into something they sell. Its own documentation calls it early and experimental; `-H:-AutoRunVM`, which the
   adapter needs, warns that it is experimental; it needs Binaryen (`wasm-as`) beside it.

## The decision, and what follows

The user, 2026-10-10, between Web Image ("the non-determinism and bigger file size, for planning speed and the exact
JDK") and TeaVM made exact by tests: **TeaVM with tests.** The licence and the reproducible build decide it; the speed
is real but not needed now (a warm plan is about 27 ms on TeaVM, and the tab warms the planner ahead of the first
query); exactness is what lite needs, and a test can hold TeaVM to it where lite uses the JDK.

What it costs to choose TeaVM: lite's exactness in the tab is as wide as its tests, not guaranteed by construction.
An answer in a corner no differential covers can still differ from the JVM.

What follows, in order (status 2026-10-10: step 1 done, `//wasm:conformance_test` with its ledger
`wasm/conformance-known.tsv`; step 2 done for the numbers, `third_party/teavm_classlib`: every number family the
conformance test probes (a double's and a float's text both ways, BigDecimal) equals the JDK in the tab, built as
recorded under step 2; step 3 done but for one speed clause, PARK-23 restated):
1. **A conformance test of TeaVM's class library** against the JVM, over the JDK methods lite's browser code calls
   (488 by class and name, the riskiest first: text and numbers, `Character`, regex, `java.time`, the order maps and
   sets iterate in), run in the wasm lane. It measures the gap before anything is fixed.
2. **The fixes, in TeaVM itself**, offered upstream (Apache 2.0; TeaVM is active and takes class-library fixes from
   outside): `isBlank`; `Double.toString`/`Float.toString` by a shortest-exact algorithm and `parseDouble` by a fast
   exact one, written from the published algorithms (Schubfach or Ryu; Eisel and Lemire), not from the JDK's code.
   They close TeaVM's own issue #735. Until a release carries them, the corrected classes ride in our build ahead of
   TeaVM's own (a Bazel change, reviewed), and go when we upgrade.

   **As built (2026-10-10), a deviation from the plan above, recorded:** not Schubfach/Ryu and Eisel-Lemire. Writing
   is a fast route for a double of at most 15 digits within 10^±22 (one correctly rounded operation decides; at
   TeaVM's old speed) and an exact route over big integers for the rest (a bisection on the scale, then the
   specification's choice); reading is Clinger's fast case, else big integers. Correctness first, from the
   specification, simple enough to check line by line; the published fast algorithms carry proofs whose every constant
   would have had to be right with no code to check against. The cost is the exact route's speed in the tab only
   (about 3.5 times TeaVM's old, inexact writer on 17-digit doubles), which is PARK-23 as restated.
   **How the fixes are written: clean room (the user, 2026-10-10: "make sure we don't plagiarize GPL or other
   licensed code").** A fix is written from what the behaviour IS, never from another implementation's code:
   - *What may be read:* the Java SE API specification (the Javadoc: what a method must return); the published
     papers (Giulietti, "The Schubfach way to render doubles", 2020; Adams, "Ryū: fast float-to-string conversion",
     PLDI 2018; Clinger, "How to read floating point numbers accurately", PLDI 1990; Lemire, "Number parsing at a
     gigabyte per second", 2021, and the Eisel-Lemire algorithm it describes); the Unicode Character Database's data
     files (the Unicode licence: permissive, with its notice) for character tables; the IANA time zone database (public
     domain) for zone rules; and the JDK's behaviour as a black box, which the conformance test compares against.
   - *What may not:* OpenJDK's source (GPL 2 with the Classpath Exception) for anything being reimplemented, including
     recalled from memory; and any other implementation's code, whatever its licence (Ryu's C, fast_float,
     double-conversion, ...): the papers, not their code. A permissively licensed implementation is only ever taken
     whole, as a dependency carrying its own licence notice, by an explicit decision, never pasted in.
   - *TeaVM's own files* are Apache 2.0: a fix edits them in place under their header, and is offered back under the
     same licence.
   - *Provenance:* every new algorithm names in its header the papers and specifications it was written from, and its
     commit says so; test inputs are generated, not copied from OpenJDK's tests (GPL too).
   - *Review:* the audit before landing checks each fix's provenance note and that its code follows the papers'
     structure rather than any implementation's.
3. **PARK-23 moves into TeaVM.** The fast exact double conversion it asks for becomes TeaVM's, so every
   `Double.toString` in the tab is exact, the JDK's own uses included, not only the call sites lite routes through
   `PortableText`. `PortableText` and its bans stay until the conformance test shows TeaVM exact.

**Look at Web Image again when** it is in GraalVM's Community Edition under an open licence, builds the same bytes
twice, and is no longer experimental. The evidence directory has everything a second look needs: the adapter, the
harness that runs the repo's differentials against any build, and the probes.
