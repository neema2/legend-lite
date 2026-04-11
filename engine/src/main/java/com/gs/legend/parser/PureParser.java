package com.gs.legend.parser;

import com.gs.legend.antlr.PackageableElementBuilder;
import com.gs.legend.antlr.PureLexer;
import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.model.def.*;
import org.antlr.v4.runtime.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Pure language parser — single entry point for all Pure parsing.
 *
 * Two top-level entry points:
 * - {@link #parseQuery(String)}  — query expressions → ValueSpecification AST
 * - {@link #parseModel(String)}  — model definitions → List of PackageableElements
 *
 * Plus typed convenience methods for extracting specific definition types:
 * - {@link #parseClassDefinition(String)}
 * - {@link #parseMappingDefinition(String)}
 * - {@link #parseDatabaseDefinition(String)}
 * - etc.
 */
public final class PureParser {

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
        PureLexer lexer = new PureLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        com.gs.legend.antlr.PureParser parser = new com.gs.legend.antlr.PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        com.gs.legend.antlr.PureParser.ProgramLineContext tree = parser.programLine();
        com.gs.legend.antlr.ValueSpecificationBuilder visitor = new com.gs.legend.antlr.ValueSpecificationBuilder();
        visitor.setInputSource(query);
        return visitor.visit(tree);
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
        PureLexer lexer = new PureLexer(CharStreams.fromString(body));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        com.gs.legend.antlr.PureParser parser = new com.gs.legend.antlr.PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        com.gs.legend.antlr.PureParser.CodeBlockContext tree = parser.codeBlock();
        com.gs.legend.antlr.ValueSpecificationBuilder visitor = new com.gs.legend.antlr.ValueSpecificationBuilder();
        visitor.setInputSource(body);

        List<com.gs.legend.antlr.PureParser.ProgramLineContext> lines = tree.programLine();
        if (lines.isEmpty()) {
            throw new PureParseException("Empty function body");
        }
        List<com.gs.legend.ast.ValueSpecification> stmts = new ArrayList<>();
        for (com.gs.legend.antlr.PureParser.ProgramLineContext line : lines) {
            stmts.add(visitor.visit(line));
        }
        return stmts;
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
    public static PackageableElementBuilder.ParseResult parseModelWithImports(String source) {
        return PackageableElementBuilder.extractAllDefinitionsWithImports(antlrParse(source));
    }

    // ==================== Typed Convenience Methods ====================

    /** Parses a single Class definition. */
    public static ClassDefinition parseClassDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstClassDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No class definition found in source"));
    }

    /** Parses a single Profile definition. */
    public static ProfileDefinition parseProfileDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstProfileDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No profile definition found in source"));
    }

    /** Parses a single Function definition. */
    public static FunctionDefinition parseFunctionDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstFunctionDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No function definition found in source"));
    }

    /** Parses a single Connection definition. */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstConnectionDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No connection definition found in source"));
    }

    /** Parses a single Runtime definition. */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstRuntimeDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No runtime definition found in source"));
    }

    /** Parses a single Database definition. */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstDatabaseDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No database definition found in source"));
    }

    /** Parses a single Mapping definition. */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstMappingDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No mapping definition found in source"));
    }

    /** Parses a single Association definition. */
    public static AssociationDefinition parseAssociationDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstAssociationDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No association definition found in source"));
    }

    /** Parses a single Service definition. */
    public static ServiceDefinition parseServiceDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstServiceDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No service definition found in source"));
    }

    /** Parses a single Enum definition. */
    public static EnumDefinition parseEnumDefinition(String pureSource) {
        return PackageableElementBuilder.extractFirstEnumDefinition(antlrParse(pureSource))
                .orElseThrow(() -> new PureParseException("No enum definition found in source"));
    }

    // ==================== Native Function Parsing ====================

    /** Parses a single native function signature into a structured NativeFunctionDef. */
    public static NativeFunctionDef parseNativeFunction(String pureSignature) {
        return PackageableElementBuilder.extractFirstNativeFunctionDefinition(antlrParse(pureSignature))
                .orElseThrow(() -> new PureParseException("No native function definition found in source"));
    }

    // ==================== Chunked Parsing ====================

    /** Threshold in chars above which addSource auto-chunks (~500 KB). */
    public static final int CHUNK_THRESHOLD = 500_000;

    /**
     * Top-level keywords that start a new definition in the grammar.
     * Used by the pre-scanner to find definition boundaries.
     */
    private static final Set<String> DEF_KEYWORDS = Set.of(
            "Class", "Association", "Enum", "Mapping", "Database", "Profile",
            "function", "native", "Service", "Runtime", "SingleConnectionRuntime",
            "RelationalDatabaseConnection", "Measure", "import"
    );

    /**
     * Pre-scans source to find top-level definition boundary positions.
     * Returns a list of char offsets where definitions start.
     * Used by {@link #splitDefinitions} and {@link #parseModelChunked}.
     */
    static List<Integer> findDefinitionStarts(String source) {
        List<Integer> starts = new ArrayList<>();
        int len = source.length();
        int i = 0;
        while (i < len) {
            // Skip whitespace
            while (i < len && Character.isWhitespace(source.charAt(i))) i++;
            if (i >= len) break;

            // Skip string literals like "use strict"
            if (source.charAt(i) == '"') {
                i++;
                while (i < len && source.charAt(i) != '"') i++;
                if (i < len) i++;
                continue;
            }

            // Try to match a definition keyword at this position
            for (String kw : DEF_KEYWORDS) {
                if (i + kw.length() <= len
                        && source.startsWith(kw, i)
                        && (i + kw.length() >= len || !Character.isLetterOrDigit(source.charAt(i + kw.length())))) {
                    starts.add(i);
                    break;
                }
            }

            // Skip to next line
            while (i < len && source.charAt(i) != '\n') i++;
            if (i < len) i++;
        }
        return starts;
    }

    /**
     * Pre-scans source to find top-level definition boundaries.
     * Returns a list of source substrings, each covering one or more
     * complete definitions. Chunks are split at definition boundaries only.
     *
     * @param source The full Pure source
     * @param targetChunkSize Approximate target size per chunk in chars
     * @return List of source substrings, each a valid set of definitions
     */
    public static List<String> splitDefinitions(String source, int targetChunkSize) {
        List<Integer> starts = findDefinitionStarts(source);
        if (starts.isEmpty()) return List.of(source);

        List<String> chunks = new ArrayList<>();
        int chunkStart = 0;
        for (int s = 1; s < starts.size(); s++) {
            int pos = starts.get(s);
            if (pos - chunkStart >= targetChunkSize) {
                chunks.add(source.substring(chunkStart, pos));
                chunkStart = pos;
            }
        }
        chunks.add(source.substring(chunkStart));
        return chunks;
    }

    /**
     * Parses a large Pure source in chunks, returning aggregated results.
     * Each chunk is parsed independently — ANTLR parse tree and token stream
     * are GC-eligible between chunks, keeping peak memory proportional to
     * the largest single chunk rather than the whole source.
     */
    public static PackageableElementBuilder.ParseResult parseModelChunked(String source) {
        List<Integer> starts = findDefinitionStarts(source);
        if (starts.isEmpty()) return parseModelWithImports(source);

        // Compute chunk boundaries (positions into source)
        List<int[]> boundaries = new ArrayList<>();
        int chunkStart = 0;
        for (int s = 1; s < starts.size(); s++) {
            int pos = starts.get(s);
            if (pos - chunkStart >= CHUNK_THRESHOLD) {
                boundaries.add(new int[]{chunkStart, pos});
                chunkStart = pos;
            }
        }
        boundaries.add(new int[]{chunkStart, source.length()});

        List<PackageableElement> allDefs = new ArrayList<>();
        ImportScope mergedImports = new ImportScope();

        for (int[] bounds : boundaries) {
            // Extract chunk substring — only one chunk's substring alive at a time
            String chunk = source.substring(bounds[0], bounds[1]);
            PackageableElementBuilder.ParseResult result = parseModelWithImports(chunk);
            allDefs.addAll(result.definitions());
            for (String pkg : result.imports().getWildcardImports()) {
                mergedImports.addImport(pkg + "::*");
            }
            for (var entry : result.imports().getTypeImports().entrySet()) {
                mergedImports.addImport(entry.getValue());
            }
            // chunk, tokens, parse tree all GC-eligible after this iteration
        }

        return new PackageableElementBuilder.ParseResult(allDefs, mergedImports);
    }

    // ==================== Internal ====================

    /**
     * Runs the ANTLR lexer + parser on Pure source, returning the raw parse tree.
     */
    private static com.gs.legend.antlr.PureParser.DefinitionContext antlrParse(String code) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(code));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        com.gs.legend.antlr.PureParser parser = new com.gs.legend.antlr.PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        return parser.definition();
    }

    private static class ErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                int line, int charPositionInLine, String msg,
                RecognitionException e) {
            throw new PureParseException(msg, line, charPositionInLine);
        }
    }
}
