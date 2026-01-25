package org.finos.legend.pure.dsl;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;

import org.finos.legend.pure.dsl.antlr.*;
import org.finos.legend.pure.dsl.definition.PureDefinition;
import java.util.ArrayList;
import java.util.List;

/**
 * Pure language parser facade using Legend Engine grammars directly.
 * 
 * Each section type uses its own grammar-specific parser:
 * - ###Pure - DomainParserGrammar (Class, Enum, Function, Association, Profile)
 * - ###Relational - RelationalParserGrammar (Database definitions)
 * - ###Mapping - MappingParserGrammar (Mapping definitions)
 * - ###Connection - ConnectionParserGrammar (Connection definitions)
 * - ###Runtime - RuntimeParserGrammar (Runtime definitions)
 * 
 * This approach uses Legend Engine grammars directly without modifications,
 * avoiding rule conflicts by instantiating the appropriate parser per section.
 */
public final class PureParser {

    private PureParser() {
        // Static utility class
    }

    /**
     * Parses a Pure query expression using DomainParserGrammar.
     * 
     * Examples:
     * - Person.all()
     * - Person.all()->filter({p | $p.age > 21})->project([{p | $p.firstName}])
     * 
     * @param query The Pure query string
     * @return The parsed expression AST
     * @throws PureParseException if parsing fails
     */
    public static PureExpression parse(String query) {
        DomainParserGrammar parser = createDomainParser(query);
        DomainParserGrammar.CombinedExpressionContext tree = parser.combinedExpression();
        PureAstBuilder visitor = new PureAstBuilder();
        return visitor.visit(tree);
    }

    /**
     * Parses Pure source code and returns all definitions.
     * This is the main high-level API that hides grammar internals.
     * 
     * Automatically handles section markers (###Pure, ###Relational, etc.)
     * and dispatches to the appropriate parser for each section.
     * 
     * @param source The Pure source code (with or without section markers)
     * @return List of parsed definitions
     */
    public static List<PureDefinition> parseToDefinitions(String source) {
        return PureDefinitionBuilder.parse(source);
    }

    /**
     * Parses Pure source code and returns all definitions and expressions.
     * Similar to parseToDefinitions() but also includes standalone expressions.
     * 
     * @param source The Pure source code
     * @return ParseResult containing definitions and expressions
     */
    public static PureDefinitionBuilder.ParseResult parseAll(String source) {
        DomainParserGrammar parser = createDomainParser(source);
        DomainParserGrammar.DefinitionContext ctx = parser.definition();
        return PureDefinitionBuilder.extractAllDefinitions(ctx);
    }

    /**
     * Parses Pure source with section markers.
     * Returns parsed contexts for each section.
     * 
     * @param source The complete Pure source with optional section markers
     * @return List of parsed section results
     */
    public static List<ParsedSection> parseSections(String source) {
        List<SectionParser.Section> sections = SectionParser.parse(source);
        List<ParsedSection> results = new ArrayList<>();

        for (SectionParser.Section section : sections) {
            ParserRuleContext ctx = parseSection(section);
            results.add(new ParsedSection(section.type(), ctx, section.startLine()));
        }

        return results;
    }

    /**
     * Parse a single section using its appropriate grammar-specific parser.
     */
    private static ParserRuleContext parseSection(SectionParser.Section section) {
        return switch (section.type()) {
            case PURE -> {
                DomainParserGrammar parser = createDomainParser(section.content());
                yield parser.definition();
            }
            case RELATIONAL -> {
                RelationalParserGrammar parser = createRelationalParser(section.content());
                yield parser.definition();
            }
            case MAPPING -> {
                MappingParserGrammar parser = createMappingParser(section.content());
                yield parser.definition();
            }
            case CONNECTION -> {
                ConnectionParserGrammar parser = createConnectionParser(section.content());
                yield parser.definition();
            }
            case RUNTIME -> {
                RuntimeParserGrammar parser = createRuntimeParser(section.content());
                yield parser.definition();
            }
        };
    }

    // ==================== Parser Factory Methods ====================

    /**
     * Create a DomainParserGrammar for Pure expressions and definitions.
     */
    public static DomainParserGrammar createDomainParser(String code) {
        DomainLexerGrammar lexer = new DomainLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        DomainParserGrammar parser = new DomainParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    /**
     * Create a MappingParserGrammar for Mapping sections.
     */
    public static MappingParserGrammar createMappingParser(String code) {
        MappingLexerGrammar lexer = new MappingLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        MappingParserGrammar parser = new MappingParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    /**
     * Create a RuntimeParserGrammar for Runtime sections.
     */
    public static RuntimeParserGrammar createRuntimeParser(String code) {
        RuntimeLexerGrammar lexer = new RuntimeLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RuntimeParserGrammar parser = new RuntimeParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    /**
     * Create a ConnectionParserGrammar for Connection sections.
     */
    public static ConnectionParserGrammar createConnectionParser(String code) {
        ConnectionLexerGrammar lexer = new ConnectionLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        ConnectionParserGrammar parser = new ConnectionParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    /**
     * Create a RelationalParserGrammar for Relational/Database sections.
     */
    public static RelationalParserGrammar createRelationalParser(String code) {
        RelationalLexerGrammar lexer = new RelationalLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        RelationalParserGrammar parser = new RelationalParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    /**
     * Create a GraphFetchTreeParserGrammar for graph fetch expressions.
     */
    public static GraphFetchTreeParserGrammar createGraphFetchParser(String code) {
        GraphFetchTreeLexerGrammar lexer = new GraphFetchTreeLexerGrammar(CharStreams.fromString(code));
        configureErrorListener(lexer);

        CommonTokenStream tokens = new CommonTokenStream(lexer);
        GraphFetchTreeParserGrammar parser = new GraphFetchTreeParserGrammar(tokens);
        configureErrorListener(parser);

        return parser;
    }

    // ==================== Error Handling ====================

    private static void configureErrorListener(org.antlr.v4.runtime.Lexer lexer) {
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorListener());
    }

    private static void configureErrorListener(org.antlr.v4.runtime.Parser parser) {
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorListener());
    }

    /**
     * Result of parsing a single section.
     */
    public record ParsedSection(
            SectionParser.SectionType type,
            ParserRuleContext context,
            int startLine) {
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
