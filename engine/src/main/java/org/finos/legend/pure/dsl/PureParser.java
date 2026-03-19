package org.finos.legend.pure.dsl;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import org.finos.legend.pure.dsl.antlr.PureLexer;

/**
 * Pure language parser using ANTLR-generated lexer and parser.
 *
 * Entry points:
 * - {@link #parseClean(String)} — query expressions → ValueSpecification AST
 * - {@link #parseDefinition(String)} — definition blocks (Class, Mapping, etc.)
 */
public final class PureParser {

    private PureParser() {
    }

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
    public static org.finos.legend.pure.dsl.ast.ValueSpecification parseClean(String query) {
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

    /**
     * Parses a Pure definition block (Class, Mapping, Database, etc.).
     *
     * @param code The Pure definition code
     * @return The parsed definition context
     * @throws PureParseException if parsing fails
     */
    public static org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext parseDefinition(String code) {
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
