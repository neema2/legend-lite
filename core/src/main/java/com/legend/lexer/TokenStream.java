package com.legend.lexer;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable result of lexing &mdash; an indexable sequence of tokens
 * stored as three parallel {@code int[]} arrays for cache locality and
 * low memory footprint.
 *
 * <p>Constructed only by {@link Lexer#tokenize(String)}.
 *
 * <h2>Storage</h2>
 * The arrays {@code types}, {@code starts}, {@code ends} may hold
 * slack capacity beyond {@link #count()} (the last doubling grow in
 * {@link Lexer} typically over-allocates). Only indices {@code 0 ≤ i &lt; count()}
 * are valid. {@code types[i]} holds the ordinal of a {@link TokenType};
 * {@code starts[i]} and {@code ends[i]} are byte offsets into the
 * original source string. Token text is computed on demand by
 * {@code source.substring(starts[i], ends[i])} &mdash; no {@code String}
 * is allocated up front.
 *
 * <p>The arrays are handed off by reference from {@link Lexer} with no
 * {@code Arrays.copyOf} trim &mdash; matches engine's hot-path-and-tail
 * memory profile.
 *
 * <h2>Access patterns</h2>
 * <ul>
 *   <li><strong>Hot paths</strong> (parser, name resolution): use the
 *       positional accessors {@link #type(int)}, {@link #start(int)},
 *       {@link #end(int)},. Zero
 *       allocation per token read.</li>
 *   <li><strong>Tests, debug, error reporting</strong>: use
 *       {@link #at(int)} to materialize a single {@link Token} record,
 *       or {@link #asList()} for all of them. Allocates.</li>
 * </ul>
 */
public final class TokenStream {

    /** Cached {@code TokenType.values()} to avoid array allocation on every {@link #type(int)} call. */
    private static final TokenType[] TOKEN_TYPES = TokenType.values();

    private final String source;
    private final int count;
    private final int[] types;
    private final int[] starts;
    private final int[] ends;
    private final java.util.List<SkippedSection> skippedSections;
    private final java.util.List<SectionHeader> sectionHeaders;

    /** A section whose CONTENT the lexer raw-skipped (opaque DSL — e.g.
     *  Diagram color literals are unlexable Pure). Recorded, never silent:
     *  the parser surfaces these through the SectionGrammarRegistry as
     *  "no grammar registered" (GRAMMAR_EXTENSIBILITY.md step 1). */
    public record SkippedSection(String name, int nameOffset,
            int startOffset, int endOffset) {
    }

    /**
     * EVERY {@code ###Section} header the lexer saw, skipped or not, in
     * source order — name plus the offset where its CONTENT begins.
     *
     * <p>Recorded because a parser sometimes needs to know which section an
     * element sits in and where that section started: engine anchors
     * AggregationAware sub-parse spans to the section's first content line,
     * so {@code MappingProtocolParser} must be told it. The alternative was
     * scanning raw source for the header, which the drop-in surface rule
     * forbids for exactly the right reason — the lexer already walked this
     * text, so it should be the one to remember it.
     */
    public record SectionHeader(String name, int nameOffset,
            int contentStartOffset,
            @com.legend.base.Nullable String residueToken,
            int residueLine, int residueCol) {

        // residueToken non-null = non-comment residue after the name on
        // the ### line; the reference refuses it (C12 sentinel #42)
    }

    /** Package-private constructor &mdash; only {@link Lexer} creates {@code TokenStream}s. */
    TokenStream(String source, int count, int[] types, int[] starts, int[] ends) {
        this(source, count, types, starts, ends, java.util.List.of(),
                java.util.List.of());
    }

    TokenStream(String source, int count, int[] types, int[] starts, int[] ends,
            java.util.List<SkippedSection> skippedSections) {
        this(source, count, types, starts, ends, skippedSections,
                java.util.List.of());
    }

    TokenStream(String source, int count, int[] types, int[] starts, int[] ends,
            java.util.List<SkippedSection> skippedSections,
            java.util.List<SectionHeader> sectionHeaders) {
        this.source = source;
        this.count = count;
        this.types = types;
        this.starts = starts;
        this.ends = ends;
        this.skippedSections = java.util.List.copyOf(skippedSections);
        this.sectionHeaders = java.util.List.copyOf(sectionHeaders);
    }

    /** The 1-based line on which the enclosing {@code ###name} section's
     *  CONTENT begins, or {@code -1} when {@code offset} precedes every
     *  header of that name. */
    public int sectionContentLine(String name, int offset) {
        int best = -1;
        for (SectionHeader h : sectionHeaders) {
            if (h.name().equals(name) && h.nameOffset() < offset) {
                best = h.contentStartOffset();
            }
        }
        return best < 0 ? -1 : lineOf(best);
    }

    /** EVERY section header seen, in source order (lexable or not). The
     *  parser's registry-routed section dispatch walks these. */
    public java.util.List<SectionHeader> sectionHeaders() {
        return sectionHeaders;
    }

    /** Sections the lexer raw-skipped as opaque, in source order. Empty for
     *  slices — a sub-parse owns no section routing. */
    public java.util.List<SkippedSection> skippedSections() {
        return skippedSections;
    }

    /** Number of tokens in the stream. Arrays may have slack capacity beyond this; do not exceed it. */
    public int count() {
        return count;
    }

    /** The original source string that produced this stream. */
    public String source() {
        return source;
    }

    /** Token type at index {@code i}. Zero allocation. */
    public TokenType type(int i) {
        return TOKEN_TYPES[types[i]];
    }

    /** Source start offset (inclusive) of token at index {@code i}. */
    public int start(int i) {
        return starts[i];
    }

    /** Source end offset (exclusive) of token at index {@code i}. */
    public int end(int i) {
        return ends[i];
    }

    /** Verbatim source slice for token at index {@code i}. Allocates one {@code String}. */
    public String text(int i) {
        return source.substring(starts[i], ends[i]);
    }

    // ---- source positions -------------------------------------------------
    //
    // Built once, lazily, and shared by every slice of the same source. Before this,
    // line/column existed ONLY on the error path and was recomputed by an O(offset) linear
    // rescan per throw (TokenStreamCursor.throwAt). That is fine for errors and useless for
    // emitting protocol source information, which needs a position at every construction site.
    //
    // Engine convention, reproduced exactly: lines and columns are 1-BASED, and an element's
    // END column is INCLUSIVE (endToken.charPositionInLine + endToken.text.length(), with
    // deliberately no +1).

    /**
     * Offsets at which each line starts; {@code lineStarts[0] == 0}. Lazily built, never mutated
     * after. {@code null} until first use — benign racy initialization: two threads may both build
     * it, and both results are identical and immutable.
     */
    private volatile int @com.legend.base.Nullable [] lineStarts;

    private int[] lineStarts() {
        int[] ls = lineStarts;
        if (ls == null) {
            int lines = 1;
            for (int i = 0, n = source.length(); i < n; i++) {
                if (source.charAt(i) == '\n') {
                    lines++;
                }
            }
            ls = new int[lines];
            int k = 1;
            for (int i = 0, n = source.length(); i < n; i++) {
                if (source.charAt(i) == '\n') {
                    ls[k++] = i + 1;
                }
            }
            lineStarts = ls;
        }
        return ls;
    }

    /** 1-based line for a source character offset. Binary search over the line index. */
    public int lineOf(int offset) {
        int[] ls = lineStarts();
        int lo = 0;
        int hi = ls.length - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (ls[mid] <= offset) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        return lo + 1;
    }

    /** 1-based column for a source character offset — counted in CODE
     *  POINTS, matching ANTLR: a non-BMP character (emoji, CJK Ext-B) is
     *  ONE column, not two UTF-16 units (deep audit #2 finding 1c). */
    public int columnOf(int offset) {
        int lineStart = lineStarts()[lineOf(offset) - 1];
        return source.codePointCount(lineStart, offset) + 1;
    }

    /**
     * Per-token line/column caches — spans are computed on nearly every parsed node
     * (the byte-parity feature), and four binary searches per span measured as a
     * uniform ~1.5x parse-time tax on the corpus gate. One monotonic pass builds all
     * four arrays; same benign-racy lazy init as {@link #lineStarts}.
     */
    private volatile int @com.legend.base.Nullable [][] tokenPositions;

    private int[][] tokenPositions() {
        int[][] tp = tokenPositions;
        if (tp == null) {
            int[] ls = lineStarts();
            int n = count();
            int[] sl = new int[n];
            int[] sc = new int[n];
            int[] el = new int[n];
            int[] ec = new int[n];
            // seed by BINARY SEARCH: token starts are file-absolute, so a
            // slice starting at file line L paid L linear iterations here —
            // O(K*L) across K slices (deep-audit H1, the best-ratio fix)
            int line = n > 0 ? lineOf(starts[0]) - 1 : 0;
            for (int i = 0; i < n; i++) {
                int s = starts[i];
                while (line + 1 < ls.length && ls[line + 1] <= s) {
                    line++;
                }
                sl[i] = line + 1;
                sc[i] = s - ls[line] + 1;
                int e = Math.max(s, ends[i] - 1);
                int eline = line;
                while (eline + 1 < ls.length && ls[eline + 1] <= e) {
                    eline++;
                }
                el[i] = eline + 1;
                ec[i] = e - ls[eline] + 1;
            }
            tp = new int[][]{sl, sc, el, ec};
            tokenPositions = tp;
        }
        return tp;
    }

    /** 1-based line of the first character of token {@code i}. */
    public int startLine(int i) {
        return tokenPositions()[0][i];
    }

    /** 1-based column of the first character of token {@code i}. */
    public int startColumn(int i) {
        return tokenPositions()[1][i];
    }

    /** 1-based line of the LAST character of token {@code i} (ends are exclusive, so step back one). */
    public int endLine(int i) {
        return tokenPositions()[2][i];
    }

    /**
     * 1-based, <strong>inclusive</strong> column of the last character of token {@code i} —
     * the engine's convention, not a half-open bound.
     */
    public int endColumn(int i) {
        return tokenPositions()[3][i];
    }

    /** Materialize the token at index {@code i} as a {@link Token} record. */
    public Token at(int i) {
        return new Token(type(i), text(i), starts[i], ends[i]);
    }

    /**
     * Materialize all tokens as a {@code List<Token>}. Allocates one
     * {@link Token} record + one substring per token &mdash; not for hot
     * paths. Use for tests, debugging, and serialization.
     */
    public List<Token> asList() {
        List<Token> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(at(i));
        }
        return list;
    }

    /**
     * Return a new {@code TokenStream} containing the tokens at indices
     * {@code [fromInclusive, toExclusive)} of this stream.
     *
     * <p>The slice shares the original {@link #source()} string &mdash; only
     * the small parallel arrays are copied. Token {@code start}/{@code end}
     * offsets are preserved verbatim so they continue to index correctly into
     * the original source (vital for error reporting that wants to print
     * line/column from the original file).
     *
     * <p>Indices in the returned slice are zero-based: token formerly at
     * index {@code fromInclusive} is now at index {@code 0}.
     *
     * <p>Bounds: {@code 0 <= fromInclusive <= toExclusive <= count()}.
     * Empty slices ({@code fromInclusive == toExclusive}) are permitted and
     * produce a {@code TokenStream} with {@code count() == 0}.
     */
    public TokenStream slice(int fromInclusive, int toExclusive) {
        if (fromInclusive < 0 || toExclusive > count || fromInclusive > toExclusive) {
            throw new IndexOutOfBoundsException(
                    "slice(" + fromInclusive + ", " + toExclusive + ") out of bounds for count=" + count);
        }
        int len = toExclusive - fromInclusive;
        int[] tSlice = new int[len];
        int[] sSlice = new int[len];
        int[] eSlice = new int[len];
        System.arraycopy(types,  fromInclusive, tSlice, 0, len);
        System.arraycopy(starts, fromInclusive, sSlice, 0, len);
        System.arraycopy(ends,   fromInclusive, eSlice, 0, len);
        // the slice is a WINDOW over the same file: section headers and
        // skipped sections are file-absolute facts and travel with it (a
        // slice that forgot its ###section made parseSingle refuse a
        // Mapping that sat inside ###Mapping — ModelOrchestrator, LITE)
        TokenStream s = new TokenStream(source, len, tSlice, sSlice, eSlice,
                skippedSections, sectionHeaders);
        // SHARE the parent's line index: rebuilding it rescans the WHOLE source per
        // slice, and span-everywhere parsing made that quadratic (corpus-gate profile:
        // the lineStarts/tokenPositions BUILD dominated every sample). lineStarts
        // depends only on the shared source, so the reference is safe to share —
        // trigger the parent's lazy build once and hand the array down.
        s.lineStarts = lineStarts();
        return s;
    }
}
