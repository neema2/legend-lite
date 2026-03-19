package org.finos.legend.pure.dsl;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import org.finos.legend.pure.dsl.antlr.PureLexer;
import org.finos.legend.pure.dsl.antlr.PackageableElementBuilder;
import org.finos.legend.pure.dsl.definition.*;

import java.util.List;

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
     * Returns a {@link org.finos.legend.pure.dsl.ast.ValueSpecification}
     * AST with generic {@link org.finos.legend.pure.dsl.ast.AppliedFunction} nodes.
     *
     * @param query The Pure query string
     * @return The parsed ValueSpecification AST
     * @throws PureParseException if parsing fails
     */
    public static org.finos.legend.pure.dsl.ast.ValueSpecification parseQuery(String query) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        org.finos.legend.pure.dsl.antlr.PureParser parser = new org.finos.legend.pure.dsl.antlr.PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        org.finos.legend.pure.dsl.antlr.PureParser.ProgramLineContext tree = parser.programLine();
        org.finos.legend.pure.dsl.ast.CleanAstBuilder visitor = new org.finos.legend.pure.dsl.ast.CleanAstBuilder();
        visitor.setInputSource(query);
        return visitor.visit(tree);
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
        return PackageableElementBuilder.extractAllDefinitions(antlrParse(source));
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

    // ==================== Internal ====================

    /**
     * Runs the ANTLR lexer + parser on Pure source, returning the raw parse tree.
     */
    private static org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext antlrParse(String code) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(code));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        org.finos.legend.pure.dsl.antlr.PureParser parser = new org.finos.legend.pure.dsl.antlr.PureParser(tokens);
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
