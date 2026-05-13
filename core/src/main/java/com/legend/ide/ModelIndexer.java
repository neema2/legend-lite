package com.legend.ide;

import com.legend.lexer.TokenStream;
import com.legend.lexer.TokenType;
import com.legend.parser.ElementKind;
import com.legend.parser.ElementParser;
import com.legend.parser.ParseException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shallow scanner over a {@link TokenStream} that produces a
 * {@link ModelIndex} mapping every declared FQN to its token range,
 * without parsing element bodies.
 *
 * <p>The output is the foundation of demand-driven parsing
 * ({@link ModelOrchestrator}): an FQN is looked up here in O(1), its
 * token range is sliced out, and the existing element parser is invoked
 * on just that slice. Elements not requested by any caller are never
 * deep-parsed.
 *
 * <h2>Algorithm</h2>
 * Walks the token stream left-to-right, advancing one element at a time:
 * <ol>
 *   <li>Skip any leading {@code import} statements (recorded for the
 *       index but not turned into element entries).</li>
 *   <li>Recognise the element kind by the leading token
 *       (via {@link ElementKind#fromHeaderToken}).</li>
 *   <li>Extract the FQN by skipping past optional decorations
 *       ({@code <<...>>} stereotypes, {@code {tag=...}} tagged-value
 *       block on function / class) and reading the
 *       {@code IDENT (PATH_SEPARATOR IDENT)*} sequence.</li>
 *   <li>Find the element's end by counting balanced delimiters
 *       ({@code () [] {}}) until the next top-level token is either
 *       another element-starting keyword or end-of-stream.</li>
 * </ol>
 *
 * <p>This is a separate concern from {@link ElementParser} on purpose:
 * the indexer needs to be correct without understanding the full
 * grammar inside element bodies. Property-based parity tests assert
 * {@code shallow_scan(src).fqns() == eager_parse(src).elements().map(fqn)}.
 *
 * <h2>Errors</h2>
 * Throws {@link ParseException} on unrecognised top-level tokens or
 * malformed structure (unbalanced delimiters, missing FQN). Mismatches
 * between the shallow scan's expectations and the actual parser would
 * surface at deep-parse time as more specific {@link ParseException}s.
 */
public final class ModelIndexer {

    private ModelIndexer() {}

    /** Scan {@code tokens} into a {@link ModelIndex}. */
    public static ModelIndex scan(TokenStream tokens) {
        Map<String, ModelIndex.Entry> entries = new LinkedHashMap<>();
        List<ModelIndex.ImportEntry> imports = new ArrayList<>();
        int n = tokens.count();
        int pos = 0;

        while (pos < n) {
            TokenType t = tokens.type(pos);

            // Imports: skip to terminating ';'.
            if (t == TokenType.IMPORT) {
                int start = pos;
                pos = skipToSemicolon(tokens, pos);
                imports.add(new ModelIndex.ImportEntry(start, pos));
                continue;
            }

            ElementKind kind = ElementKind.fromHeaderToken(t);
            if (kind == null) {
                ElementParser.throwAt(tokens, pos,
                        "unsupported top-level keyword: " + t + " ('" + safeText(tokens, pos) + "')");
            }

            int elementStart = pos;
            int afterKeyword = pos + 1;
            // 'native' is a one-token prefix on CLASS.
            if (t == TokenType.NATIVE) {
                if (afterKeyword >= n || tokens.type(afterKeyword) != TokenType.CLASS) {
                    ElementParser.throwAt(tokens, afterKeyword,
                            "expected 'Class' after 'native'");
                }
                afterKeyword++;
            }

            int afterDecorations = skipDecorations(tokens, afterKeyword);
            int afterFqn = skipFqn(tokens, afterDecorations);
            String fqn = readFqnText(tokens, afterDecorations, afterFqn);
            int end = findElementEnd(tokens, afterFqn, kind);

            ModelIndex.Entry existing = entries.put(fqn,
                    new ModelIndex.Entry(fqn, kind, elementStart, end));
            if (existing != null) {
                ElementParser.throwAt(tokens, elementStart,
                        "duplicate top-level element '" + fqn + "' (first declared as "
                        + existing.kind() + ")");
            }
            pos = end;
        }

        return new ModelIndex(entries, imports);
    }

    // -------------------------------------------------------------------
    // Decoration skipping
    // -------------------------------------------------------------------

    /**
     * Skip past optional {@code <<stereo>>} and {@code {tag=...}} blocks
     * that may appear between an element's kind keyword and its FQN.
     * Returns the position of the first FQN identifier token.
     */
    private static int skipDecorations(TokenStream tokens, int pos) {
        int n = tokens.count();
        // Stereotypes: '<<' ... '>>'
        if (pos + 1 < n
                && tokens.type(pos) == TokenType.LESS_THAN
                && tokens.type(pos + 1) == TokenType.LESS_THAN) {
            pos += 2;
            int depth = 1;
            while (pos < n && depth > 0) {
                if (pos + 1 < n
                        && tokens.type(pos) == TokenType.LESS_THAN
                        && tokens.type(pos + 1) == TokenType.LESS_THAN) {
                    depth++;
                    pos += 2;
                } else if (pos + 1 < n
                        && tokens.type(pos) == TokenType.GREATER_THAN
                        && tokens.type(pos + 1) == TokenType.GREATER_THAN) {
                    depth--;
                    pos += 2;
                } else {
                    pos++;
                }
            }
        }
        // Tagged-value block: '{' QN '.' IDENT '=' ... '}'.
        // The predicate lives on ElementParser so the scanner and the
        // parser agree on what counts as a tagged-value block. Drift
        // between the two predicates would cause the scanner to slice
        // ranges the parser then refuses to parse.
        if (pos < n && ElementParser.looksLikeTaggedValueBlock(tokens, pos)) {
            pos = skipBalanced(tokens, pos, TokenType.BRACE_OPEN, TokenType.BRACE_CLOSE);
        }
        return pos;
    }

    // -------------------------------------------------------------------
    // FQN extraction
    // -------------------------------------------------------------------

    /**
     * Return the token index one past the end of the FQN starting at
     * {@code pos}. Throws if {@code pos} does not start an identifier.
     * Split from {@link #readFqnText} so the caller can use the end
     * position to drive {@link #findElementEnd} \u2014 the body scan must
     * start <em>after</em> the FQN, not after the kind keyword, otherwise
     * a leading tagged-value decoration would be misread as the body.
     */
    private static int skipFqn(TokenStream tokens, int pos) {
        int n = tokens.count();
        if (pos >= n || !isIdentifierToken(tokens.type(pos))) {
            ElementParser.throwAt(tokens, pos, "expected identifier (FQN start)");
        }
        pos++;
        while (pos + 1 < n
                && tokens.type(pos) == TokenType.PATH_SEPARATOR
                && isIdentifierToken(tokens.type(pos + 1))) {
            pos += 2;
        }
        return pos;
    }

    /** Reconstruct the {@code a::b::c} FQN text from the token range [start, end). */
    private static String readFqnText(TokenStream tokens, int start, int end) {
        StringBuilder sb = new StringBuilder();
        sb.append(tokens.text(start));
        for (int p = start + 2; p < end; p += 2) {
            sb.append("::").append(tokens.text(p));
        }
        return sb.toString();
    }

    // -------------------------------------------------------------------
    // Element-end finder
    // -------------------------------------------------------------------

    /**
     * Find the exclusive end of an element whose header starts at
     * {@code afterFqn}. The element body is exactly one balanced group
     * whose shape is determined by the {@link ElementKind}: parens for
     * {@link ElementKind#DATABASE} and {@link ElementKind#MAPPING},
     * braces for everything else.
     *
     * <p>Steps:
     * <ol>
     *   <li>Skip header tokens (return type, multiplicities, parameter
     *       lists, constraint blocks, {@code extends} clauses) until we
     *       see the body's opening delimiter. Balanced groups in the
     *       header ({@code (...)}, {@code [...]}) are jumped over with
     *       {@link #skipBalanced} so their inner content cannot trigger
     *       false body detection.</li>
     *   <li>Consume the balanced body via {@link #skipBalanced}.</li>
     * </ol>
     *
     * <p>This is stricter than a single depth counter because it
     * enforces that exactly one body group exists and is found in the
     * expected position &mdash; falling out of the header without seeing
     * the body's open delimiter, or finding garbage at depth 0 between
     * header tokens, causes a fail-fast {@link ParseException} rather
     * than silently absorbing tokens into the element's range.
     */
    private static int findElementEnd(TokenStream tokens, int afterFqn, ElementKind kind) {
        TokenType bodyOpen = bodyOpenFor(kind);
        TokenType bodyClose = matchingClose(bodyOpen);
        int n = tokens.count();
        int pos = afterFqn;

        // Phase 1: skip header until body-open. Jump over balanced groups
        // so e.g. function param lists '(p: T[1])' don't confuse the scan.
        while (pos < n) {
            TokenType t = tokens.type(pos);
            if (t == bodyOpen) break;
            switch (t) {
                case PAREN_OPEN ->
                        pos = skipBalanced(tokens, pos, TokenType.PAREN_OPEN, TokenType.PAREN_CLOSE);
                case BRACKET_OPEN ->
                        pos = skipBalanced(tokens, pos, TokenType.BRACKET_OPEN, TokenType.BRACKET_CLOSE);
                case BRACE_OPEN ->
                        // Only reachable for paren-bodied kinds (Database/Mapping):
                        // an unexpected '{' in their header is malformed.
                        ElementParser.throwAt(tokens, pos,
                                "unexpected '{' in header of " + kind + " element");
                case PAREN_CLOSE, BRACKET_CLOSE, BRACE_CLOSE ->
                        ElementParser.throwAt(tokens, pos,
                                "unbalanced closing delimiter " + t + " in element header");
                default -> pos++;
            }
        }
        if (pos >= n) {
            ElementParser.throwAt(tokens, afterFqn,
                    "missing body for " + kind + " element (expected '" + bodyOpen + "')");
        }

        // Phase 2: consume the balanced body.
        return skipBalanced(tokens, pos, bodyOpen, bodyClose);
    }

    private static TokenType bodyOpenFor(ElementKind kind) {
        return switch (kind) {
            case DATABASE, MAPPING -> TokenType.PAREN_OPEN;
            default -> TokenType.BRACE_OPEN;
        };
    }

    private static TokenType matchingClose(TokenType open) {
        return switch (open) {
            case PAREN_OPEN -> TokenType.PAREN_CLOSE;
            case BRACE_OPEN -> TokenType.BRACE_CLOSE;
            case BRACKET_OPEN -> TokenType.BRACKET_CLOSE;
            default -> throw new IllegalArgumentException("not an opening delimiter: " + open);
        };
    }

    // -------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------

    private static int skipToSemicolon(TokenStream tokens, int pos) {
        int n = tokens.count();
        int start = pos;
        while (pos < n && tokens.type(pos) != TokenType.SEMI_COLON) pos++;
        if (pos >= n) {
            ElementParser.throwAt(tokens, start, "import statement missing terminating ';'");
        }
        return pos + 1;
    }

    private static int skipBalanced(TokenStream tokens, int pos,
                                    TokenType open, TokenType close) {
        int n = tokens.count();
        if (tokens.type(pos) != open) return pos;
        int depth = 1;
        pos++;
        while (pos < n && depth > 0) {
            TokenType t = tokens.type(pos);
            if (t == open) depth++;
            else if (t == close) depth--;
            pos++;
        }
        return pos;
    }

    private static boolean isIdentifierToken(TokenType t) {
        return ElementParser.IDENTIFIER_TOKENS.contains(t);
    }

    private static String safeText(TokenStream tokens, int pos) {
        return pos < tokens.count() ? tokens.text(pos) : "<EOF>";
    }
}
