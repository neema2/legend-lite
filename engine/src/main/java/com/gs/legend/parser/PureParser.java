package com.gs.legend.parser;

import com.gs.legend.model.def.*;

import java.util.List;

/**
 * Pure language parser — single entry point for all Pure parsing.
 *
 * <p>Delegates to the hand-rolled {@link PureQueryParser} and
 * {@link PureModelParser}.
 *
 * <p>Entry points:
 * <ul>
 *   <li>{@link #parseQuery(String)}          — query expression → ValueSpecification AST</li>
 *   <li>{@link #parseCodeBlock(String)}      — multi-statement body → List of ValueSpecifications</li>
 *   <li>{@link #parseModel(String)}          — model source → List of PackageableElements</li>
 *   <li>{@link #parseModelWithImports(String)} — model source → definitions + imports</li>
 *   <li>{@link #parseSingle(String, Class)}  — extract the first definition of a given type</li>
 * </ul>
 */
public final class PureParser {

    // Timing instrumentation for query parsing
    static long queryParseTimeNs = 0;
    static int queryParseCount = 0;
    public static void resetQueryStats() { queryParseTimeNs = 0; queryParseCount = 0; }
    public static String queryStats() { return queryParseCount + " queries in " + (queryParseTimeNs / 1_000_000) + " ms (" + (queryParseCount > 0 ? queryParseTimeNs / queryParseCount / 1000 : 0) + " us/query)"; }

    private PureParser() {
    }

    // ==================== Query Parsing ====================

    /**
     * Parses a Pure query expression.
     *
     * Returns a {@link com.gs.legend.ast.ValueSpecification}
     * AST with generic {@link com.gs.legend.ast.AppliedFunction} nodes.
     *
     * @param query The Pure query string
     * @return The parsed ValueSpecification AST
     * @throws PureParseException if parsing fails
     */
    public static com.gs.legend.ast.ValueSpecification parseQuery(String query) {
        long t0 = System.nanoTime();
        var result = PureQueryParser.parseQuery(query);
        queryParseTimeNs += System.nanoTime() - t0;
        queryParseCount++;
        return result;
    }

    /**
     * Parses a Pure code block (one or more semicolon-separated statements).
     * Used for multi-statement function bodies.
     *
     * @param body The Pure body source code
     * @return List of parsed ValueSpecification statements
     * @throws PureParseException if parsing fails
     */
    public static List<com.gs.legend.ast.ValueSpecification> parseCodeBlock(String body) {
        long t0 = System.nanoTime();
        var result = PureQueryParser.parseCodeBlock(body);
        queryParseTimeNs += System.nanoTime() - t0;
        queryParseCount++;
        return result;
    }

    // ==================== Model Parsing ====================

    /**
     * Parses a Pure model source and returns all packageable elements found.
     *
     * Handles section headers (###Pure, ###Relational, ###Mapping),
     * comments, and import statements.
     *
     * @param source The Pure source code
     * @return List of parsed PackageableElements
     * @throws PureParseException if parsing fails
     */
    public static List<PackageableElement> parseModel(String source) {
        return parseModelWithImports(source).definitions();
    }

    /**
     * Parses a Pure model source and returns definitions + imports.
     * The ImportScope contains all wildcard imports from import statements.
     */
    public static ParseResult parseModelWithImports(String source) {
        PureLexer2 lexer = new PureLexer2(source);
        lexer.tokenize();
        return new PureModelParser(lexer).parseDefinition();
    }

    // ==================== Single-Definition Extraction ====================

    /**
     * Parses a model source and returns the first definition of the requested type.
     * Convenience for test cases containing exactly one definition of interest.
     * For multi-definition sources, use {@link #parseModel(String)} directly.
     *
     * @throws PureParseException if no definition of that type is present
     */
    public static <T extends PackageableElement> T parseSingle(String source, Class<T> type) {
        for (PackageableElement el : parseModel(source)) {
            if (type.isInstance(el)) return type.cast(el);
        }
        throw new PureParseException(
                "No " + type.getSimpleName() + " found in source");
    }
}
