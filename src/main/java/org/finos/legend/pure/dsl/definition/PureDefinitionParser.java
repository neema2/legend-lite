package org.finos.legend.pure.dsl.definition;

import org.finos.legend.pure.dsl.PureParseException;

import java.util.List;

/**
 * Parser for Pure definition syntax (Class, Database, Mapping).
 *
 * This parses the declarative Pure syntax used to define models and mappings.
 */
public final class PureDefinitionParser {

    /**
     * Parses a Pure source string and returns all definitions found.
     * 
     * Uses ANTLR to parse the source directly. Handles:
     * - Section headers (###Pure, ###Relational, ###Mapping) - skipped by lexer
     * - Comments (line and block) - skipped by lexer
     * - Import statements - parsed by grammar
     * 
     * @param pureSource The Pure source code
     * @return List of parsed definitions
     */
    public static List<PureDefinition> parse(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder.extractAllDefinitions(tree);
    }

    /**
     * Parses a single Class definition using ANTLR.
     */
    public static ClassDefinition parseClassDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstClassDefinition(tree)
                .orElseThrow(() -> new PureParseException("No class definition found in source"));
    }

    /**
     * Parses a single Profile definition using ANTLR.
     */
    public static ProfileDefinition parseProfileDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstProfileDefinition(tree)
                .orElseThrow(() -> new PureParseException("No profile definition found in source"));
    }

    /**
     * Parses a single Function definition using ANTLR.
     */
    public static FunctionDefinition parseFunctionDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstFunctionDefinition(tree)
                .orElseThrow(() -> new PureParseException("No function definition found in source"));
    }

    /**
     * Parses a single Connection definition using ANTLR.
     */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstConnectionDefinition(tree)
                .orElseThrow(() -> new PureParseException("No connection definition found in source"));
    }

    /**
     * Parses a single Runtime definition using ANTLR.
     */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstRuntimeDefinition(tree)
                .orElseThrow(() -> new PureParseException("No runtime definition found in source"));
    }

    /**
     * Parses a single Database definition using ANTLR.
     */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstDatabaseDefinition(tree)
                .orElseThrow(() -> new PureParseException("No database definition found in source"));
    }

    /**
     * Parses a single Mapping definition using ANTLR.
     */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstMappingDefinition(tree)
                .orElseThrow(() -> new PureParseException("No mapping definition found in source"));
    }

    /**
     * Parses a single Association definition using ANTLR.
     */
    public static AssociationDefinition parseAssociationDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstAssociationDefinition(tree)
                .orElseThrow(() -> new PureParseException("No association definition found in source"));
    }

    /**
     * Parses a single Service definition using ANTLR.
     */
    public static ServiceDefinition parseServiceDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstServiceDefinition(tree)
                .orElseThrow(() -> new PureParseException("No service definition found in source"));
    }

    /**
     * Parses a single Enum definition using ANTLR.
     */
    public static EnumDefinition parseEnumDefinition(String pureSource) {
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                .parseDefinition(pureSource);
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                .extractFirstEnumDefinition(tree)
                .orElseThrow(() -> new PureParseException("No enum definition found in source"));
    }
}
