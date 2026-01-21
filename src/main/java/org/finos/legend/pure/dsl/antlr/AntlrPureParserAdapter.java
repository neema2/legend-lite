package org.finos.legend.pure.dsl.antlr;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.finos.legend.pure.dsl.PureExpression;
import org.finos.legend.pure.dsl.PureParseException;

/**
 * ANTLR-based Pure parser using merged PureLexer/PureParser grammar.
 * 
 * Uses PureLexer and PureParser generated from the merged grammar files
 * that combine core, domain, mapping, runtime, connection, and relational
 * grammars from legend-engine.
 */
public final class AntlrPureParserAdapter {

    private AntlrPureParserAdapter() {
        // Static utility class
    }

    /**
     * Parses a Pure query string using the ANTLR-generated parser.
     * 
     * @param query The Pure query string
     * @return The parsed expression AST
     * @throws PureParseException if parsing fails
     */
    public static PureExpression parse(String query) {
        // Create lexer using merged PureLexer
        PureLexer lexer = new PureLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        // Create parser using merged PureParser
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        // Parse as combinedExpression (the main entry point for expressions)
        PureParser.CombinedExpressionContext tree = parser.combinedExpression();
        PureAstBuilder visitor = new PureAstBuilder();
        return visitor.visit(tree);
    }

    /**
     * Parses a Pure definition (Class, Mapping, Database, etc.).
     * 
     * @param code The Pure definition code
     * @return The parsed definition context (for further processing)
     * @throws PureParseException if parsing fails
     */
    public static PureParser.DefinitionContext parseDefinition(String code) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(code));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PureParser parser = new PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        return parser.definition();
    }

    /**
     * Error listener that converts ANTLR errors to PureParseException.
     */
    private static class ErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                int line, int charPositionInLine, String msg,
                RecognitionException e) {
            throw new PureParseException(
                    "Parse error at line " + line + ":" + charPositionInLine + " - " + msg);
        }
    }
}
