package org.finos.legend.pure.dsl;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import org.finos.legend.pure.dsl.antlr.PureAstBuilder;
import org.finos.legend.pure.dsl.antlr.PureLexer;

/**
 * Pure language parser using ANTLR-generated lexer and parser.
 * 
 * This is the unified entry point for parsing Pure language constructs:
 * - Query expressions: Person.all()->filter()->project()
 * - Definition blocks: Class, Mapping, Database, etc.
 * 
 * Uses the merged grammar from legend-engine covering:
 * core, domain, mapping, runtime, connection, and relational grammars.
 */
public final class PureParser {

    private PureParser() {
        // Static utility class
    }

    /**
     * Parses a Pure query expression.
     * 
     * Examples:
     * - Person.all()
     * - Person.all()->filter({p | $p.age > 21})->project([{p | $p.firstName}])
     * - #>{store::DB.T_PERSON}->select(~name, ~age)
     * 
     * @param query The Pure query string
     * @return The parsed expression AST
     * @throws PureParseException if parsing fails
     */
    public static PureExpression parse(String query) {
        PureLexer lexer = new PureLexer(CharStreams.fromString(query));
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        org.finos.legend.pure.dsl.antlr.PureParser parser = new org.finos.legend.pure.dsl.antlr.PureParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());

        // Use programLine to support both expressions and let statements
        org.finos.legend.pure.dsl.antlr.PureParser.ProgramLineContext tree = parser.programLine();
        PureAstBuilder visitor = new PureAstBuilder();
        visitor.setInputSource(query); // Enable source text extraction for user function inlining
        return visitor.visit(tree);
    }

    /**
     * Parses a Pure definition block (Class, Mapping, Database, etc.).
     * 
     * Examples:
     * - Class model::Person { firstName: String[1]; }
     * - Database store::MyDB ( Table T_PERSON (...) )
     * - Mapping model::PersonMapping ( ... )
     * 
     * @param code The Pure definition code
     * @return The parsed definition context (for further processing)
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

    /**
     * Error listener that converts ANTLR errors to PureParseException.
     */
    private static class ErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                int line, int charPositionInLine, String msg,
                RecognitionException e) {
            // Use 3-argument constructor to set line/column for IDE integration
            throw new PureParseException(msg, line, charPositionInLine);
        }
    }
}
