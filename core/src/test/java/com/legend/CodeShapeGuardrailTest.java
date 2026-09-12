// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * CODE-SHAPE GUARDRAILS — build-failing, not aspirational. Added after
 * the audit-window review found a 1,207-line method and a 4,974-line
 * file that no scoreboard had ever surfaced: the corpus measures
 * behavior, THIS measures whether the next reader can hold the code.
 *
 * <p>The allowlists are a BURN-DOWN ledger, not a loophole: every entry
 * names a known offender with a planned split. Shrink them; never grow
 * them — a new entry needs the same justification a corpus regression
 * would.
 */
class CodeShapeGuardrailTest {

    private static final int METHOD_LIMIT = 250;
    private static final int FILE_LIMIT = 3500;

    /** Known oversized METHODS, pending their planned splits — ceilings
     * at measured size + small slack; SHRINK only. */
    private static final Map<String, Integer> METHOD_ALLOWLIST = Map.of();

    /** Known oversized FILES, pending their planned splits. */
    private static final Map<String, Integer> FILE_ALLOWLIST = Map.of(
            // sat exactly AT the 3500 ceiling before the upstream explicit-src
            // relation-mapping forms landed (2026-08-05); the row-expression binding
            // added ~4 lines. Next real touch should extract the relation-col binding
            // family into its own normalizer helper instead of growing this again.
            "MappingNormalizer.java", 3510);
    // SpecParser.java's entry (3525, dialect-quarantine growth) was RETIRED
    // at the 4.138.2 re-pin: the named seam was taken — the island
    // char-scanner family moved to IslandScan.java (3539 -> ~3200 lines).

    /** Mutable instance fields that are DELIBERATE: hand-rolled parser
     * cursors (Lexer/ElementParser/SpecParser walk positions and scope
     * stacks), per-resolution frames, fresh-name counters and one
     * memo cache. Everything else must be final or become part of an
     * explicit frame object. */
    private static final Set<String> MUTABLE_FIELD_ALLOWLIST = Set.of(
            // (Lowerer.relationDepth row DELETED 2026-08-24 — the audit
            // found the counter write-only; the allowlist justification
            // was a rationalization, exactly what this guardrail is for)
            // renderer nesting cursor: NAMED-frame (view) subselect depth —
            // same lifecycle as a parser cursor, scoped to one render()
            // the inliner's UNROLL BUDGET spend (batch 149): a per-compile
            // counter of call expansions inside a named budget object —
            // same lifecycle as the inliner (the compile artifact); it
            // exists so a program-sized tree walk walls instead of hanging
            "UserCallInliner.spent",
            "EngineStyleH2.frameDepth",
            // anonymous-subselect nesting cursor (bare DISTINCT-key
            // spelling scope) — same lifecycle as frameDepth
            "EngineStyleH2.anonDistinctDepth",
            // mapping-section context for AggregationAware span-shift
            // emulation: set once at parse entry / per ~modelOperation
            // view — same lifecycle as a parser cursor
            "MappingProtocolParser.sectionStartLine",
            "MappingProtocolParser.aggLambdaShift",
            // JSON writer's field/value alternation cursor — same
            // lifecycle as a parser cursor, scoped to one document write
            "Json.awaitingFieldValue",
            // parser cursors + scope state
            "Lexer.pos", "Lexer.islandDepth", "Lexer.types", "Lexer.starts",
            "Lexer.ends", "Lexer.count",
            "ElementParser.pos", 
            // DatabaseProtocolParser: the same parser-cursor shape
            "DatabaseProtocolParser.pos",
            // GqlParser: the raw-text scanner cursor, same family as the
            // other parser cursors above
            "GqlParser.pos",
            "MappingProtocolParser.pos",
            // ConnectionSectionGrammar.Cursor / RuntimeSectionGrammar
            // .SliceCursor: the re-lex feeds' parser cursors — same shape,
            // scoped to one island/SPI parse
            "ConnectionSectionGrammar.pos",
            "RuntimeSectionGrammar.pos",
            "ServiceSectionGrammar.pos",
            "DataSpaceSectionGrammar.pos",
            "PersistenceSectionGrammar.pos",
            "FunctionActivatorSectionGrammar.pos",
            // the SHARED SliceCursor (ElementwiseSectionGrammar's SPI feed)
            "SliceCursor.pos",
            // DiagramSectionGrammar.Raw: the RAW section's char walker —
            // a parser cursor (position + its own line/col tracking, since
            // diagram content never reaches the shared lexer)
            "DiagramSectionGrammar.i", "DiagramSectionGrammar.line",
            "DiagramSectionGrammar.col",
            // parse-surface MODE, set once at construction by the factory that owns
            // it (at() = the engine-strict drop-in surface) — never flipped mid-parse
             
            "SpecParser.pos",
            // the shared minimal JSON reader's walk position — a parser
            // cursor, same family as Lexer.pos
            "Json.i",
            // ';'-ambiguity context for unbraced lambda code blocks: inside
            // call args/collections a ';' is unambiguously the lambda's —
            // a cursor-adjacent depth, same lifecycle as pos
            "SpecParser.boundedDepth",
            // per-resolution frames + counters
            "StoreResolver.freshVarCounter", "StoreResolver.temporal",
            // driver-scoped emission opt-in (builder-style, set once
            // before lower()): engine-parity join-distinct exists form —
            // the standalone-SQL surface constructs without it
            "Lowerer.engineExistsJoinForm",
            // Phase 2a (batch 136): the two lowering MODES that were
            // thread-locals — verbatim equality (save/set/restore around
            // the two mapping-definition sites) and the engine-text
            // option (builder-style, set once before lower())
            "Lowerer.verbatimEquality", "Lowerer.engineText",
            // the query's execution feature flags (Feature): builder-style,
            // set once by the executor before lower(), same lifecycle as
            // engineText — a flag selects an emission during the lowering
            "Lowerer.features", "Lowerer.legacyNullUnsafeEquals",
            // Phase 2b (batch 137): the execution-trace stamp is state of the
            // execution ENVIRONMENT (one ExecutionTrace per ExecEnv), a
            // thread-local before
            "ExecutionTrace.last",
            // builder-flag, same lifecycle as engineExistsJoinForm: set
            // once by the driver before lower(), read during the lowering
            "Lowerer.streamingGraphRoot",
            // JSON source frames (XStore §1): the execution context's
            // JsonModelConnection map, set per from()-scope — same
            // per-resolution frame lifecycle as `temporal`
            "ClassSources.jsonSources",
            // serialize(..., config) type-key config: set at the serialize
            // arm, consumed at the envelope build — same per-resolution
            // frame lifecycle as `temporal`
            "StoreResolver.serializeTypeCfg",
            // graphFetchChecked flag: set beside serializeTypeCfg at the
            // serialize arm, consumed at the envelope build — same
            // per-resolution frame lifecycle
            "StoreResolver.checkedEnvelope",
            "SyntheticHeads.count", "Lowerer.tdsCounter", "Lowerer.aliasCounter",
            "UserCallInliner.fresh", "Bindings.contravariantDepth",
            // walk-scoped mode flag: inside a postprocessor-CONFIG
            // property user calls stand for structural extraction —
            // same lifecycle as a parser cursor, scoped to one rewrite
            "UserCallInliner.configMode",
            // §4AD slice 1: filter-predicate nesting depth during the
            // one liftFilteredHeads walk (value-position scope gate) —
            // same lifecycle as a parser cursor
            "SyntheticHeads.filterPredDepth",
            // §4AD slice 1: the walk's pre-scanned single-pred head set
            // (scalar-lift scope gate) — set once at walk entry, read
            // during the same walk; same lifecycle as a parser cursor
            "SyntheticHeads.scalarLiftHeads",
            // NormalizeRequired inline α-rename counter — same lifecycle as
            // UserCallInliner.fresh (fresh-name generation per compile)
            "AlphaRename.nrFresh",
            // render mode toggle + import memo
            "AnsiSqlRenderer.inlineMode", "ModelOrchestrator.cachedImports",
            // normalizer emission frame: Pipeline IS the frame object; expr
            // is the accumulating pipeline AST
            "Pipeline.expr",
            // regex artifact, NOT mutable: implicitly public-static-final
            // interface constant (interfaces cannot spell 'final' on fields)
            "TokenStreamCursor.IDENTIFIER_TOKENS",
            // nested-class cursor: PureDateLiteral's date parser (keys are
            // filename-scoped; the old runner's ExecJson cursor died with
            // it in batch 115)
            "PureDateLiteral.pos");

    private static final Pattern SIG = Pattern.compile(
            // 4-space (top-level class members) OR 8-space (nested-class
            // members — F1.7: the 4-space anchor left nested methods
            // unscanned); statement keywords excluded so 8-space method
            // BODY lines (multiline `return foo(...)` calls) cannot
            // false-match as signatures
            "^(?:    |        )(?! )(?!return |throw |if |while |for "
            + "|switch |case |else |new |yield |assert )"
            + "(?:private |public |protected |static |final |synchronized )*"
            + "[\\w.<>\\[\\], ?]+ (\\w+)\\(");

    /** Class-level non-final instance fields, ANY visibility — package-
     * private has no keyword, so the visibility group is optional (audit 15:
     * the private-only pattern let two refactors widen mutable fields out
     * of the scan unaudited). */
    private static final Pattern MUTABLE_FIELD = Pattern.compile(
            "^    (?! )(?:(?:private|protected|public) )?"
            + "(?!static |final |private |protected |public |record |class"
            + " |interface |enum |abstract |sealed |non-sealed )(?! )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Nested-class fields (8-space indent). A visibility modifier is
     * REQUIRED here to distinguish fields from method locals — declare
     * nested-class fields private/protected/public, never package-private. */
    private static final Pattern NESTED_MUTABLE_FIELD = Pattern.compile(
            "^        (?! )(?:private|protected|public) (?!static |final )(?! )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Static mutable state is banned outright — NO allowlist. */
    private static final Pattern STATIC_MUTABLE_FIELD = Pattern.compile(
            "^\\s*(?:(?:private|protected|public) )?static (?!final )"
            + "[\\w.<>\\[\\], ?]+ (\\w+)( =.*)?;");

    /** Strip string/char literals and comments so braces inside them
     * never skew the counts (parseDerivedProperty false-positived at
     * 3,064 lines from a brace inside a string). */
    private static String sanitize(String line) {
        StringBuilder out = new StringBuilder(line.length());
        boolean inStr = false;
        boolean inChar = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if ((inStr || inChar) && c == '\\') {
                i++;
                continue;
            }
            if (!inChar && c == '"') {
                inStr = !inStr;
                continue;
            }
            if (!inStr && c == '\'') {
                inChar = !inChar;
                continue;
            }
            if (!inStr && !inChar) {
                if (c == '/' && i + 1 < line.length()
                        && line.charAt(i + 1) == '/') {
                    break;
                }
                out.append(c);
            }
        }
        return out.toString();
    }

    private static List<Path> mainSources() throws IOException {
        Path root = Path.of("src/main/java");
        try (Stream<Path> s = Files.walk(root)) {
            List<Path> out = s.filter(p -> p.toString().endsWith(".java"))
                    .toList();
            GuardCoverage.assertFloor(/* 499->498: HostEval DELETED, Phase 1 batch 2 */ "CodeShapeGuardrailTest",
                    out.size(), 498);
            return out;
        }
    }

    // F1.8: unreferenced private methods. Counting rule (Tier-2 audit
    // 2026-08-18): a private method group is dead when its name has
    // ZERO mentions outside its own declaration+body spans — masking
    // the spans makes RECURSION invisible to the count (the audit's
    // probe: a dead recursive method was green because `uses` counted
    // its own self-call). The sound scanner + span rule found 11 dead
    // methods (the 9 backlogged F0.1 sites plus 2 the old rule could
    // not see); ALL were deleted the same day — the pin is ZERO and
    // stays there: dead private code is deleted, never backlogged.
    private static final int DEAD_PRIVATE_METHODS = 0;

    @Test
    void deadPrivateMethodsOnlyShrink() throws IOException {
        List<String> dead = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString();
            // strip comments AND string/char literals with a real
            // scanner — brace-walking must not see a "{" inside a
            // string, and regex-order stripping mangles quotes ('"'
            // char literals, // inside string URLs) into unbalanced
            // braces (Tier-2 audit: the first cut false-flagged 25
            // live methods exactly this way)
            String code = blankNonCode(Files.readString(p));
            // each declaration's span = signature through matching
            // close-brace; a self-call sits INSIDE the span, so masking
            // the spans makes recursion invisible to the use count
            // (ADVERSARIAL_TENET_AUDIT §3 probe: a dead RECURSIVE
            // method was green — `uses` counted its own self-call)
            Matcher d = Pattern.compile(
                    "(?m)^\\s*private\\s+(?:static\\s+|final\\s+"
                    + "|synchronized\\s+|@[\\w.]+\\s+|<[^>]+>\\s+)*"
                    + "[\\w.<>\\[\\], ?@]+\\s+(\\w+)\\(")
                    .matcher(code);
            java.util.Map<String, List<int[]>> spans =
                    new java.util.HashMap<>();
            while (d.find()) {
                int open = code.indexOf('{', d.end());
                int semi = code.indexOf(';', d.end());
                if (open < 0 || (semi >= 0 && semi < open)) {
                    continue;
                }
                int depth = 1;
                int i = open + 1;
                while (i < code.length() && depth > 0) {
                    char ch = code.charAt(i);
                    if (ch == '{') {
                        depth++;
                    } else if (ch == '}') {
                        depth--;
                    }
                    i++;
                }
                spans.computeIfAbsent(d.group(1),
                        k -> new ArrayList<>()).add(new int[]{d.start(), i});
            }
            for (var e : spans.entrySet()) {
                StringBuilder masked = new StringBuilder(code);
                for (int[] s : e.getValue()) {
                    for (int i = s[0]; i < s[1]; i++) {
                        masked.setCharAt(i, ' ');
                    }
                }
                String outside = masked.toString();
                int uses = countMatches(outside,
                        "\\b" + Pattern.quote(e.getKey()) + "\\s*\\(");
                int refs = countMatches(outside,
                        "::" + Pattern.quote(e.getKey()) + "\\b");
                if (uses + refs == 0) {
                    dead.add(cls + "." + e.getKey());
                }
            }
        }
        assertTrue(dead.size() <= DEAD_PRIVATE_METHODS,
                "unreferenced private methods grew to " + dead.size()
                + " (pinned at " + DEAD_PRIVATE_METHODS + "): " + dead
                + " — delete the dead code or reference it");
    }

    /** Comments, string literals (incl. text blocks), and char
     * literals become spaces (newlines kept); code text and its brace
     * structure survive exactly. A single state-machine pass — regex
     * stripping cannot handle {@code '"'} or {@code //} inside a
     * string without corrupting quote balance. */
    private static String blankNonCode(String src) {
        StringBuilder out = new StringBuilder(src.length());
        int i = 0;
        int n = src.length();
        while (i < n) {
            char c = src.charAt(i);
            char c1 = i + 1 < n ? src.charAt(i + 1) : '\0';
            if (c == '/' && c1 == '/') {
                while (i < n && src.charAt(i) != '\n') {
                    out.append(' ');
                    i++;
                }
            } else if (c == '/' && c1 == '*') {
                out.append("  ");
                i += 2;
                while (i < n && !(src.charAt(i) == '*' && i + 1 < n
                        && src.charAt(i + 1) == '/')) {
                    out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                    i++;
                }
                if (i < n) {
                    out.append("  ");
                    i += 2;
                }
            } else if (c == '"' && c1 == '"' && i + 2 < n
                    && src.charAt(i + 2) == '"') {
                out.append("   ");
                i += 3;
                while (i < n && !(src.charAt(i) == '"' && i + 2 < n
                        && src.charAt(i + 1) == '"'
                        && src.charAt(i + 2) == '"')) {
                    if (src.charAt(i) == '\\') {
                        out.append("  ");
                        i += 2;
                    } else {
                        out.append(src.charAt(i) == '\n' ? '\n' : ' ');
                        i++;
                    }
                }
                if (i < n) {
                    out.append("   ");
                    i += 3;
                }
            } else if (c == '"' || c == '\'') {
                out.append(' ');
                i++;
                while (i < n && src.charAt(i) != c) {
                    if (src.charAt(i) == '\\') {
                        out.append("  ");
                        i += 2;
                    } else {
                        out.append(' ');
                        i++;
                    }
                }
                if (i < n) {
                    out.append(' ');
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    private static int countMatches(String code, String regex) {
        Matcher m = Pattern.compile(regex).matcher(code);
        int n = 0;
        while (m.find()) {
            n++;
        }
        return n;
    }

    @Test
    void noMethodBeyondTheLimit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            List<String> lines = Files.readAllLines(p);
            for (int i = 0; i < lines.size(); i++) {
                Matcher m = SIG.matcher(lines.get(i));
                if (!m.find() || lines.get(i).contains(";")) {
                    continue;
                }
                int depth = 0;
                boolean started = false;
                int j = i;
                while (j < lines.size()) {
                    String ln = sanitize(lines.get(j));
                    depth += count(ln, '{') - count(ln, '}');
                    if (ln.indexOf('{') >= 0) {
                        started = true;
                    }
                    if (started && depth == 0) {
                        break;
                    }
                    j++;
                }
                int len = j - i + 1;
                String key = cls + "." + m.group(1);
                int limit = METHOD_ALLOWLIST.getOrDefault(key, METHOD_LIMIT);
                if (len > limit) {
                    violations.add(key + " is " + len + " lines (limit "
                            + limit + ") — split it; the numbered-comment"
                            + " sections are the seams");
                }
                i = j;
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    void noFileBeyondTheLimit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            long len = Files.lines(p).count();
            String name = p.getFileName().toString();
            long limit = FILE_ALLOWLIST.getOrDefault(name, FILE_LIMIT);
            if (len > limit) {
                violations.add(name + " is " + len + " lines (limit "
                        + limit + ")");
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    /** Convergence slice 2 EXECUTED (SQL-IR backend-agnosticism):
     * the 62 origin-unstamped sites burned to 7, every survivor a
     * 4-arg construction CARRYING its origin (rebuild-transports +
     * three adjudicated stamps). The 2-arg M1 door survives for TEST
     * fixtures and the of(outs,name) unclaimed-name fallback — a
     * production null-origin reaching the strict H2 renderer WALLS
     * (slice 3), so any residue surfaces as a counted decline, never
     * a guessed spelling. Count only SHRINKS. */
    @Test
    void unstampedColumnConstructionOnlyShrinks() throws IOException {
        int count = 0;
        for (Path p : mainSources()) {
            if (p.getFileName().toString().equals("SqlExpr.java")) {
                continue;
            }
            String src = Files.readString(p);
            int i = 0;
            while ((i = src.indexOf("new SqlExpr.Column(", i)) >= 0) {
                count++;
                i++;
            }
        }
        assertTrue(count <= 7, "raw SqlExpr.Column construction sites"
                + " grew: " + count + " > 7 — new references go through"
                + " the stamped doors (Column.of over an OutputCol /"
                + " derived / physical); raw construction is for"
                + " origin-preserving rebuilds only");
    }

    /** SQL-IR slice 2 finish (outputs-from-projections): a projection
     * frame's outputs DERIVE from its projections; {@code outputsOf}
     * survives only as the schema DOORS (three physical doors,
     * TDS/VALUES literals, the Pivot source) and the per-projection
     * CONTRACT supplier ({@code paired}/{@code named} attachment at
     * construction). {@code withOutputs} is the star-frame door with
     * ONE production caller (the pivot probe — genuinely external
     * knowledge). A new call site is a new frame ASSERTING outputs the
     * projections should carry — count only SHRINKS. */
    @Test
    void schemaAssertedOutputsOnlyShrink() throws IOException {
        int outputsOf = 0;
        int withOutputs = 0;
        for (Path p : mainSources()) {
            String src = Files.readString(p);
            for (int i = 0; (i = src.indexOf("outputsOf(", i)) >= 0; i++) {
                outputsOf++;
            }
            for (int i = 0; (i = src.indexOf("withOutputs(", i)) >= 0; i++) {
                withOutputs++;
            }
        }
        assertTrue(outputsOf <= 23, "outputsOf call sites grew: "
                + outputsOf + " > 23 — outputs derive from projections;"
                + " attach the contract slot per projection"
                + " (SqlSelect.paired / Fold.named), never assert a"
                + " frame's outputs from the schema");
        assertTrue(withOutputs <= 2, "withOutputs call sites grew: "
                + withOutputs + " > 2 — star frames read their source;"
                + " only externally-discovered outputs (pivot probe) may"
                + " use this door");
    }

    @Test
    void mutableInstanceStateIsExplicit() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            for (String ln : Files.readAllLines(p)) {
                Matcher m = MUTABLE_FIELD.matcher(ln);
                if (!m.find()) {
                    m = NESTED_MUTABLE_FIELD.matcher(ln);
                    if (!m.find()) {
                        continue;
                    }
                }
                // an enum-constant list (EQ, NEQ, GTE;) is commas outside
                // generics — a field's type only holds commas inside <>
                String beforeName = ln.substring(0, m.start(1));
                if (beforeName.contains(",") && !beforeName.contains("<")) {
                    continue;
                }
                if (!MUTABLE_FIELD_ALLOWLIST.contains(cls + "." + m.group(1))) {
                    violations.add(cls + "." + m.group(1)
                            + " is a mutable instance field — make it final,"
                            + " move it into an explicit frame object, or"
                            + " allowlist it WITH the reason");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    @Test
    void noStaticMutableState() throws IOException {
        List<String> violations = new ArrayList<>();
        for (Path p : mainSources()) {
            String cls = p.getFileName().toString().replace(".java", "");
            for (String ln : Files.readAllLines(p)) {
                Matcher m = STATIC_MUTABLE_FIELD.matcher(ln);
                if (m.find()) {
                    violations.add(cls + "." + m.group(1)
                            + " is STATIC MUTABLE state — no allowlist for"
                            + " this one; make it final or design it away");
                }
            }
        }
        assertTrue(violations.isEmpty(), String.join("\n", violations));
    }

    private static int count(String s, char c) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == c) {
                n++;
            }
        }
        return n;
    }
}
