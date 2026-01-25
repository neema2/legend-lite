package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.definition.AssociationDefinition;
import org.finos.legend.pure.dsl.definition.AuthenticationSpec;
import org.finos.legend.pure.dsl.definition.ClassDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.ConstraintDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.DerivedPropertyDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.PropertyDefinition;
import org.finos.legend.pure.dsl.definition.ConnectionDefinition;
import org.finos.legend.pure.dsl.definition.ConnectionSpecification;
import org.finos.legend.pure.dsl.definition.DatabaseDefinition;
import org.finos.legend.pure.dsl.definition.EnumDefinition;
import org.finos.legend.pure.dsl.definition.FunctionDefinition;
import org.finos.legend.pure.dsl.definition.MappingDefinition;
import org.finos.legend.pure.dsl.definition.ProfileDefinition;
import org.finos.legend.pure.dsl.definition.PureDefinition;
import org.finos.legend.pure.dsl.definition.RuntimeDefinition;
import org.finos.legend.pure.dsl.definition.ServiceDefinition;
import org.finos.legend.pure.dsl.definition.StereotypeApplication;
import org.finos.legend.pure.dsl.definition.TaggedValue;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * ANTLR visitor that builds ClassDefinition objects from the parse tree.
 * 
 * This visitor processes the class definition grammar rule, extracting:
 * - Qualified class name
 * - Superclass list (from extends clause)
 * - Properties (simple and derived)
 * - Constraints, stereotypes, and tagged values
 */
public class PureDefinitionBuilder extends DomainParserGrammarBaseVisitor<Object> {

    /**
     * Visits a class definition parse tree node and returns a ClassDefinition.
     * 
     * Grammar rule:
     * classDefinition: CLASS stereotypes? taggedValues? qualifiedName typeParams?
     * (
     * (PROJECTS projection)
     * |
     * (
     * (EXTENDS type (COMMA type)*)?
     * constraints?
     * classBody
     * )
     * )
     */
    @Override
    public ClassDefinition visitClassDefinition(DomainParserGrammar.ClassDefinitionContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract superclasses from EXTENDS clause
        List<String> superClasses = new ArrayList<>();
        List<DomainParserGrammar.TypeContext> typeContexts = ctx.type();
        if (typeContexts != null) {
            for (DomainParserGrammar.TypeContext typeCtx : typeContexts) {
                // type rule: qualifiedName (LESS_THAN typeArguments? ...)? typeVariableValues?
                // We just need the qualifiedName part for the superclass reference
                if (typeCtx.qualifiedName() != null) {
                    superClasses.add(typeCtx.qualifiedName().getText());
                }
            }
        }

        // Extract properties from classBody
        List<PropertyDefinition> properties = new ArrayList<>();
        List<DerivedPropertyDefinition> derivedProperties = new ArrayList<>();

        DomainParserGrammar.ClassBodyContext bodyCtx = ctx.classBody();
        if (bodyCtx != null && bodyCtx.properties() != null) {
            // Process simple properties
            for (DomainParserGrammar.PropertyContext propCtx : bodyCtx.properties().property()) {
                PropertyDefinition prop = visitProperty(propCtx);
                if (prop != null) {
                    properties.add(prop);
                }
            }
            // Process qualified/derived properties
            for (DomainParserGrammar.QualifiedPropertyContext qualPropCtx : bodyCtx.properties().qualifiedProperty()) {
                DerivedPropertyDefinition derivedProp = visitQualifiedProperty(qualPropCtx);
                if (derivedProp != null) {
                    derivedProperties.add(derivedProp);
                }
            }
        }

        // Extract constraints
        List<ConstraintDefinition> constraints = new ArrayList<>();
        if (ctx.constraints() != null) {
            for (DomainParserGrammar.ConstraintContext constraintCtx : ctx.constraints().constraint()) {
                ConstraintDefinition constraint = visitConstraint(constraintCtx);
                if (constraint != null) {
                    constraints.add(constraint);
                }
            }
        }

        // Extract stereotypes
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        if (ctx.stereotypes() != null) {
            for (DomainParserGrammar.StereotypeContext stereotypeCtx : ctx.stereotypes().stereotype()) {
                StereotypeApplication stereotype = visitStereotype(stereotypeCtx);
                if (stereotype != null) {
                    stereotypes.add(stereotype);
                }
            }
        }

        // Extract tagged values
        List<TaggedValue> taggedValues = new ArrayList<>();
        if (ctx.taggedValues() != null) {
            for (DomainParserGrammar.TaggedValueContext tvCtx : ctx.taggedValues().taggedValue()) {
                TaggedValue tv = visitTaggedValue(tvCtx);
                if (tv != null) {
                    taggedValues.add(tv);
                }
            }
        }

        return new ClassDefinition(
                qualifiedName,
                superClasses,
                properties,
                derivedProperties,
                constraints,
                stereotypes,
                taggedValues);
    }

    /**
     * Visits a property definition.
     * 
     * Grammar: property: stereotypes? taggedValues? aggregation? identifier COLON
     * propertyReturnType defaultValue? SEMI_COLON
     */
    @Override
    public PropertyDefinition visitProperty(DomainParserGrammar.PropertyContext ctx) {
        if (ctx == null) {
            return null;
        }

        String name = ctx.identifier().getText();

        // Extract type and multiplicity from propertyReturnType
        DomainParserGrammar.PropertyReturnTypeContext returnTypeCtx = ctx.propertyReturnType();
        String type = "";
        int lowerBound = 1;
        Integer upperBound = 1;

        if (returnTypeCtx != null && returnTypeCtx.type() != null) {
            // Get the type name
            if (returnTypeCtx.type().qualifiedName() != null) {
                type = returnTypeCtx.type().qualifiedName().getText();
            }
            // Get the multiplicity
            if (returnTypeCtx.multiplicity() != null) {
                int[] bounds = parseMultiplicity(returnTypeCtx.multiplicity().getText());
                lowerBound = bounds[0];
                upperBound = bounds[1] == -1 ? null : bounds[1];
            }
        }

        return new PropertyDefinition(name, type, lowerBound, upperBound);
    }

    /**
     * Visits a derived (qualified) property definition.
     * 
     * Grammar: qualifiedProperty: stereotypes? taggedValues? identifier
     * qualifiedPropertyBody COLON propertyReturnType SEMI_COLON
     * 
     * qualifiedPropertyBody: PAREN_OPEN (functionVariableExpression (COMMA
     * functionVariableExpression)*)? PAREN_CLOSE
     * codeBlock
     */
    @Override
    public DerivedPropertyDefinition visitQualifiedProperty(DomainParserGrammar.QualifiedPropertyContext ctx) {
        if (ctx == null) {
            return null;
        }

        String name = ctx.identifier().getText();

        // Extract parameters from qualifiedPropertyBody
        List<ClassDefinition.ParameterDefinition> parameters = new ArrayList<>();
        String expression = "";

        if (ctx.qualifiedPropertyBody() != null) {
            // Extract parameters from functionVariableExpression list
            for (DomainParserGrammar.FunctionVariableExpressionContext paramCtx : ctx.qualifiedPropertyBody()
                    .functionVariableExpression()) {
                ClassDefinition.ParameterDefinition param = visitFunctionVariableExpression(paramCtx);
                if (param != null) {
                    parameters.add(param);
                }
            }

            // Get expression from codeBlock
            if (ctx.qualifiedPropertyBody().codeBlock() != null) {
                expression = ctx.qualifiedPropertyBody().codeBlock().getText();
            }
        }

        // Get return type
        String returnType = "";
        int lowerBound = 1;
        Integer upperBound = 1;
        if (ctx.propertyReturnType() != null && ctx.propertyReturnType().type() != null) {
            if (ctx.propertyReturnType().type().qualifiedName() != null) {
                returnType = ctx.propertyReturnType().type().qualifiedName().getText();
            }
            if (ctx.propertyReturnType().multiplicity() != null) {
                int[] bounds = parseMultiplicity(ctx.propertyReturnType().multiplicity().getText());
                lowerBound = bounds[0];
                upperBound = bounds[1] == -1 ? null : bounds[1];
            }
        }

        return new DerivedPropertyDefinition(name, parameters, expression, returnType, lowerBound, upperBound);
    }

    /**
     * Visits a function variable expression (parameter definition).
     * 
     * Grammar: functionVariableExpression: identifier COLON type multiplicity
     */
    public ClassDefinition.ParameterDefinition visitFunctionVariableExpression(
            DomainParserGrammar.FunctionVariableExpressionContext ctx) {
        if (ctx == null) {
            return null;
        }

        String paramName = ctx.identifier().getText();

        String paramType = "";
        if (ctx.type() != null && ctx.type().qualifiedName() != null) {
            paramType = ctx.type().qualifiedName().getText();
        }

        int lowerBound = 1;
        Integer upperBound = 1;
        if (ctx.multiplicity() != null) {
            int[] bounds = parseMultiplicity(ctx.multiplicity().getText());
            lowerBound = bounds[0];
            upperBound = bounds[1] == -1 ? null : bounds[1];
        }

        return new ClassDefinition.ParameterDefinition(paramName, paramType, lowerBound, upperBound);
    }

    /**
     * Parses multiplicity string into [lowerBound, upperBound].
     * Returns -1 for upperBound if unbounded (*).
     * 
     * Examples:
     * "[1]" -> [1, 1]
     * "[0..1]" -> [0, 1]
     * "[*]" -> [0, -1]
     * "[1..*]" -> [1, -1]
     */
    private int[] parseMultiplicity(String mult) {
        if (mult == null || mult.isEmpty()) {
            return new int[] { 1, 1 }; // Default [1]
        }
        mult = mult.trim();

        // Strip brackets if present (ANTLR multiplicity rule includes
        // BRACKET_OPEN/CLOSE)
        if (mult.startsWith("[") && mult.endsWith("]")) {
            mult = mult.substring(1, mult.length() - 1);
        }

        if ("*".equals(mult)) {
            return new int[] { 0, -1 };
        }

        if (mult.contains("..")) {
            String[] parts = mult.split("\\.\\.");
            int lower = Integer.parseInt(parts[0]);
            int upper = "*".equals(parts[1]) ? -1 : Integer.parseInt(parts[1]);
            return new int[] { lower, upper };
        }

        // Single number
        int val = Integer.parseInt(mult);
        return new int[] { val, val };
    }

    /**
     * Visits a constraint definition.
     */
    public ConstraintDefinition visitConstraint(DomainParserGrammar.ConstraintContext ctx) {
        if (ctx == null) {
            return null;
        }

        // Get constraint name if present
        String name = "unnamed";
        if (ctx.simpleConstraint() != null && ctx.simpleConstraint().constraintId() != null) {
            name = ctx.simpleConstraint().constraintId().identifier().getText();
        }

        // Get constraint expression
        String expression = "";
        if (ctx.simpleConstraint() != null && ctx.simpleConstraint().combinedExpression() != null) {
            expression = ctx.simpleConstraint().combinedExpression().getText();
        }

        return new ConstraintDefinition(name, expression);
    }

    /**
     * Visits a stereotype application.
     * 
     * Grammar: stereotype: qualifiedName DOT identifier
     */
    public StereotypeApplication visitStereotype(DomainParserGrammar.StereotypeContext ctx) {
        if (ctx == null) {
            return null;
        }

        String profile = ctx.qualifiedName().getText();
        String stereotype = ctx.identifier().getText();
        return new StereotypeApplication(profile, stereotype);
    }

    /**
     * Visits a tagged value.
     */
    public TaggedValue visitTaggedValue(DomainParserGrammar.TaggedValueContext ctx) {
        if (ctx == null) {
            return null;
        }

        String profile = "";
        String tag = "";
        String value = "";

        if (ctx.qualifiedName() != null) {
            profile = ctx.qualifiedName().getText();
        }
        if (ctx.identifier() != null) {
            tag = ctx.identifier().getText();
        }
        // Get the tagged value string
        if (ctx.STRING() != null) {
            value = ctx.STRING().getText();
            // Strip quotes
            if (value.startsWith("'") && value.endsWith("'")) {
                value = value.substring(1, value.length() - 1);
            }
        }

        return new TaggedValue(profile, tag, value);
    }

    /**
     * Extracts all ClassDefinitions from a parsed definition context.
     * 
     * @param definitionCtx The definition context from parsing
     * @return List of ClassDefinitions found in the definition
     */
    public static List<ClassDefinition> extractClassDefinitions(DomainParserGrammar.DefinitionContext definitionCtx) {
        List<ClassDefinition> result = new ArrayList<>();
        if (definitionCtx == null) {
            return result;
        }

        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        // Navigate through elementDefinition* to find classDefinitions
        for (DomainParserGrammar.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.classDefinition() != null) {
                ClassDefinition classDef = builder.visitClassDefinition(elemCtx.classDefinition());
                result.add(classDef);
            }
        }

        return result;
    }

    /**
     * Extracts the first ClassDefinition from a parsed definition context.
     * 
     * @param definitionCtx The definition context from parsing
     * @return Optional ClassDefinition if found
     */
    public static Optional<ClassDefinition> extractFirstClassDefinition(
            DomainParserGrammar.DefinitionContext definitionCtx) {
        List<ClassDefinition> defs = extractClassDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Extract All Definitions ====================

    /**
     * Result of parsing a Pure source file.
     * Contains all definitions and any trailing expressions.
     */
    public record ParseResult(
            List<PureDefinition> definitions,
            List<String> trailingExpressions) {
        public ParseResult {
            definitions = definitions != null ? List.copyOf(definitions) : List.of();
            trailingExpressions = trailingExpressions != null ? List.copyOf(trailingExpressions) : List.of();
        }
    }

    // ==================== Convenience Parse Methods ====================
    // These replace the old PureDefinitionParser convenience methods

    /**
     * Parses Pure source and returns all definitions.
     * Uses section-based parsing to route to appropriate grammar entry points.
     */
    public static List<PureDefinition> parse(String pureSource) {
        List<PureDefinition> definitions = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        // Use section-based parsing
        List<org.finos.legend.pure.dsl.SectionParser.Section> sections = org.finos.legend.pure.dsl.SectionParser
                .parse(pureSource);

        for (org.finos.legend.pure.dsl.SectionParser.Section section : sections) {
            try {
                definitions.addAll(builder.parseSection(section));
            } catch (Exception e) {
                System.err.println("Warning: Failed to parse section: " + e.getMessage());
            }
        }

        return definitions;
    }

    /**
     * Parses a single section using the appropriate grammar-specific parser.
     */
    private List<PureDefinition> parseSection(org.finos.legend.pure.dsl.SectionParser.Section section) {
        List<PureDefinition> definitions = new ArrayList<>();

        switch (section.type()) {
            case PURE -> {
                // Domain elements: Class, Enum, Function, Association, Profile
                DomainParserGrammar parser = org.finos.legend.pure.dsl.PureParser.createDomainParser(section.content());
                DomainParserGrammar.DefinitionContext ctx = parser.definition();
                for (DomainParserGrammar.ElementDefinitionContext elemCtx : ctx.elementDefinition()) {
                    if (elemCtx.classDefinition() != null) {
                        definitions.add(visitClassDefinition(elemCtx.classDefinition()));
                    } else if (elemCtx.association() != null) {
                        definitions.add(visitAssociation(elemCtx.association()));
                    } else if (elemCtx.enumDefinition() != null) {
                        definitions.add(visitEnumDefinition(elemCtx.enumDefinition()));
                    } else if (elemCtx.profile() != null) {
                        definitions.add(visitProfile(elemCtx.profile()));
                    } else if (elemCtx.functionDefinition() != null) {
                        definitions.add(visitFunctionDefinition(elemCtx.functionDefinition()));
                    }
                }
            }
            case RELATIONAL -> {
                // Database definition using RelationalParserGrammar
                RelationalParserGrammar parser = org.finos.legend.pure.dsl.PureParser
                        .createRelationalParser(section.content());
                RelationalParserGrammar.DefinitionContext ctx = parser.definition();
                for (RelationalParserGrammar.DatabaseContext dbCtx : ctx.database()) {
                    definitions.add(visitDatabase(dbCtx));
                }
            }
            case MAPPING -> {
                // Mapping definition using MappingParserGrammar
                // Try full ANTLR parsing first, fall back to regex extraction if parsing fails
                // (the relational mapping body syntax may not be fully supported by the
                // grammar)
                try {
                    MappingParserGrammar parser = org.finos.legend.pure.dsl.PureParser
                            .createMappingParser(section.content());
                    MappingParserGrammar.DefinitionContext ctx = parser.definition();
                    for (MappingParserGrammar.MappingContext mappingCtx : ctx.mapping()) {
                        definitions.add(visitMapping(mappingCtx));
                    }
                } catch (Exception e) {
                    // Fallback: extract mapping name via regex when ANTLR parsing fails
                    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                            "Mapping\\s+([\\w:]+)\\s*\\(");
                    java.util.regex.Matcher matcher = pattern.matcher(section.content());
                    while (matcher.find()) {
                        String qualifiedName = matcher.group(1);
                        definitions.add(new MappingDefinition(qualifiedName, java.util.List.of()));
                    }
                }
            }
            case RUNTIME -> {
                // Runtime definition using RuntimeParserGrammar
                // Try full ANTLR parsing first, fall back to regex extraction if parsing fails
                try {
                    RuntimeParserGrammar parser = org.finos.legend.pure.dsl.PureParser
                            .createRuntimeParser(section.content());
                    RuntimeParserGrammar.DefinitionContext ctx = parser.definition();
                    for (RuntimeParserGrammar.RuntimeContext runtimeCtx : ctx.runtime()) {
                        RuntimeDefinition rtDef = visitRuntime(runtimeCtx);
                        System.out.println("[DEBUG] RUNTIME ANTLR parsed: " + rtDef.qualifiedName()
                                + ", mappings=" + rtDef.mappings().size()
                                + ", connections=" + rtDef.connectionBindings().size());
                        definitions.add(rtDef);
                    }
                } catch (Exception e) {
                    // Fallback: extract runtime name via regex when ANTLR parsing fails
                    System.out.println("[DEBUG] RUNTIME ANTLR parse failed: " + e.getMessage());
                    System.out.println("[DEBUG] RUNTIME section content:\n"
                            + section.content().substring(0, Math.min(200, section.content().length())));
                    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                            "Runtime\\s+([\\w:]+)\\s*\\{");
                    java.util.regex.Matcher matcher = pattern.matcher(section.content());
                    while (matcher.find()) {
                        String qualifiedName = matcher.group(1);
                        System.out.println("[DEBUG] RUNTIME regex fallback created: " + qualifiedName);
                        definitions.add(new RuntimeDefinition(qualifiedName, java.util.List.of(), java.util.Map.of()));
                    }
                }
            }
            case CONNECTION -> {
                // Connection definition using ConnectionParserGrammar
                ConnectionParserGrammar parser = org.finos.legend.pure.dsl.PureParser
                        .createConnectionParser(section.content());
                ConnectionParserGrammar.DefinitionContext ctx = parser.definition();
                for (ConnectionParserGrammar.ConnectionContext connCtx : ctx.connection()) {
                    definitions.add(visitConnection(connCtx));
                }
            }
        }

        return definitions;
    }

    /**
     * Parses a single ClassDefinition from Pure source.
     */
    public static ClassDefinition parseClassDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof ClassDefinition)
                .map(d -> (ClassDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Class definition found in source"));
    }

    /**
     * Parses a single DatabaseDefinition from Pure source.
     */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof DatabaseDefinition)
                .map(d -> (DatabaseDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Database definition found in source"));
    }

    /**
     * Parses a single MappingDefinition from Pure source.
     */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof MappingDefinition)
                .map(d -> (MappingDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Mapping definition found in source"));
    }

    /**
     * Parses a single ConnectionDefinition from Pure source.
     */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof ConnectionDefinition)
                .map(d -> (ConnectionDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Connection definition found in source"));
    }

    /**
     * Parses a single RuntimeDefinition from Pure source.
     */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof RuntimeDefinition)
                .map(d -> (RuntimeDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Runtime definition found in source"));
    }

    /**
     * Parses a single AssociationDefinition from Pure source.
     */
    public static AssociationDefinition parseAssociationDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof AssociationDefinition)
                .map(d -> (AssociationDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Association definition found in source"));
    }

    /**
     * Parses a single EnumDefinition from Pure source.
     */
    public static EnumDefinition parseEnumDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof EnumDefinition)
                .map(d -> (EnumDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Enum definition found in source"));
    }

    /**
     * Parses a single ProfileDefinition from Pure source.
     */
    public static ProfileDefinition parseProfileDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof ProfileDefinition)
                .map(d -> (ProfileDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Profile definition found in source"));
    }

    /**
     * Parses a single FunctionDefinition from Pure source.
     */
    public static FunctionDefinition parseFunctionDefinition(String pureSource) {
        return parse(pureSource).stream()
                .filter(d -> d instanceof FunctionDefinition)
                .map(d -> (FunctionDefinition) d)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No Function definition found in source"));
    }

    /**
     * Parses a single ServiceDefinition from Pure source.
     * Note: ServiceDefinition is not yet implemented in ANTLR builder - returns
     * null for now.
     */
    public static ServiceDefinition parseServiceDefinition(String pureSource) {
        // TODO: Implement ServiceDefinition parsing in ANTLR builder
        // For now, throw since Service isn't yet supported
        throw new UnsupportedOperationException("ServiceDefinition parsing not yet implemented in ANTLR builder");
    }

    /**
     * Extracts ALL definitions from a parsed definition context.
     * Handles: Class, Database, Mapping, Runtime, Connection, Association,
     * Enum, Profile, Function.
     * 
     * @param definitionCtx The definition context from parsing
     * @return ParseResult with all definitions found
     */
    public static ParseResult extractAllDefinitions(DomainParserGrammar.DefinitionContext definitionCtx) {
        List<PureDefinition> definitions = new ArrayList<>();
        List<String> expressions = new ArrayList<>();

        if (definitionCtx == null) {
            return new ParseResult(definitions, expressions);
        }

        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        // Note: In Legend Engine grammar, elementDefinition only contains Domain
        // elements
        // (class, enum, function, association, profile, instance, measure).
        // Database, Mapping, Runtime, Connection are parsed via separate entry points.
        for (DomainParserGrammar.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            try {
                if (elemCtx.classDefinition() != null) {
                    definitions.add(builder.visitClassDefinition(elemCtx.classDefinition()));
                } else if (elemCtx.association() != null) {
                    definitions.add(builder.visitAssociation(elemCtx.association()));
                } else if (elemCtx.enumDefinition() != null) {
                    definitions.add(builder.visitEnumDefinition(elemCtx.enumDefinition()));
                } else if (elemCtx.profile() != null) {
                    definitions.add(builder.visitProfile(elemCtx.profile()));
                } else if (elemCtx.functionDefinition() != null) {
                    definitions.add(builder.visitFunctionDefinition(elemCtx.functionDefinition()));
                }
                // instance, measureDefinition, nativeFunction are less common, skip for now
            } catch (Exception e) {
                // Log parse error but continue with other definitions
                System.err.println("Warning: Failed to parse element: " + e.getMessage());
            }
        }

        return new ParseResult(definitions, expressions);
    }

    // ==================== Database Visitor ====================

    /**
     * Visits a database definition.
     */
    public DatabaseDefinition visitDatabase(RelationalParserGrammar.DatabaseContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();

        // Legend Engine grammar uses table() and join() instead of dbTable()/dbJoin()
        for (RelationalParserGrammar.TableContext tableCtx : ctx.table()) {
            tables.add(buildTableDefinition(tableCtx));
        }

        for (RelationalParserGrammar.JoinContext joinCtx : ctx.join()) {
            joins.add(buildJoinDefinition(joinCtx));
        }

        return new DatabaseDefinition(qualifiedName, tables, joins);
    }

    private DatabaseDefinition.TableDefinition buildTableDefinition(RelationalParserGrammar.TableContext ctx) {
        // Legend Engine grammar: table: TABLE relationalIdentifier PAREN_OPEN
        // columnDefinition* PAREN_CLOSE
        String tableName = ctx.relationalIdentifier().getText();
        List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();

        for (RelationalParserGrammar.ColumnDefinitionContext colCtx : ctx.columnDefinition()) {
            columns.add(buildColumnDefinition(colCtx));
        }

        return new DatabaseDefinition.TableDefinition(tableName, columns);
    }

    private DatabaseDefinition.ColumnDefinition buildColumnDefinition(
            RelationalParserGrammar.ColumnDefinitionContext ctx) {
        String colName = ctx.relationalIdentifier().getText();
        String dataType = ctx.identifier().getText();

        // Check for size specification like VARCHAR(100)
        if (ctx.INTEGER() != null && !ctx.INTEGER().isEmpty()) {
            dataType += "(" + ctx.INTEGER(0).getText();
            if (ctx.INTEGER().size() > 1) {
                dataType += "," + ctx.INTEGER(1).getText();
            }
            dataType += ")";
        }

        boolean primaryKey = ctx.PRIMARY_KEY() != null;
        boolean notNull = ctx.NOT_NULL() != null || primaryKey;

        return new DatabaseDefinition.ColumnDefinition(colName, dataType, primaryKey, notNull);
    }

    private DatabaseDefinition.JoinDefinition buildJoinDefinition(RelationalParserGrammar.JoinContext ctx) {
        // Legend Engine grammar: join: JOIN identifier PAREN_OPEN operation PAREN_CLOSE
        String joinName = ctx.identifier().getText();
        // For now, extract the raw expression - proper parsing would require more work
        String expression = ctx.operation().getText();

        // Try to parse simple "TABLE.COL = TABLE.COL" format
        String[] parts = expression.split("=");
        if (parts.length == 2) {
            String[] left = parts[0].trim().split("\\.");
            String[] right = parts[1].trim().split("\\.");
            if (left.length >= 2 && right.length >= 2) {
                return new DatabaseDefinition.JoinDefinition(
                        joinName,
                        left[0], left[1],
                        right[0], right[1]);
            }
        }
        // Fallback
        return new DatabaseDefinition.JoinDefinition(joinName, "", "", "", "");
    }

    // ==================== Mapping Visitor ====================

    /**
     * Parses a mapping definition from its section content.
     * 
     * Note: In Legend Engine, the `mapping` rule in RelationalParserGrammar
     * (mapping: associationMapping | classMapping)
     * overwrites MappingParserGrammar's full definition. We parse mapping content
     * manually.
     * 
     * Format:
     * Mapping package::MappingName (
     * *package::ClassName: Relational { ... }
     * ...
     * )
     * 
     * @param qualifiedName the qualified name of the mapping
     * @param content       the raw content inside the parentheses
     */
    public MappingDefinition parseMappingContent(String qualifiedName, String content) {
        List<MappingDefinition.ClassMappingDefinition> classMappings = new ArrayList<>();

        // Parse each mapping element - format: [*]ClassName: Type { body }
        // For now, return a minimal mapping
        // TODO: Implement full parsing of mapping elements

        return new MappingDefinition(qualifiedName, classMappings);
    }

    /**
     * Visits a mapping definition using MappingParserGrammar.MappingContext.
     * Uses the grammar-specific parser result directly.
     */
    public MappingDefinition visitMapping(MappingParserGrammar.MappingContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<MappingDefinition.ClassMappingDefinition> classMappings = new ArrayList<>();

        // TODO: Fully implement mapping parsing from ANTLR context
        // The MappingParserGrammar has: classMappings, enumerationMappings,
        // associationMappings
        // For now, create a basic mapping definition

        return new MappingDefinition(qualifiedName, classMappings);
    }

    /**
     * Visits a connection definition using
     * ConnectionParserGrammar.ConnectionContext.
     * Uses the grammar-specific parser result directly.
     */
    public ConnectionDefinition visitConnection(ConnectionParserGrammar.ConnectionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        // TODO: Fully implement connection parsing from ANTLR context
        // The ConnectionParserGrammar defines the connection structure
        // For now, create a basic connection definition
        String connectionTypeName = ctx.connectionType() != null ? ctx.connectionType().getText() : "";

        // Return a basic connection definition
        return new ConnectionDefinition(
                qualifiedName,
                "", // store
                ConnectionDefinition.DatabaseType.valueOf("DuckDB"), // default
                new ConnectionSpecification.InMemory(),
                new AuthenticationSpec.NoAuth());
    }

    // ==================== Runtime Visitor ====================

    /**
     * Visits a runtime definition.
     */
    public RuntimeDefinition visitRuntime(RuntimeParserGrammar.RuntimeContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<String> mappings = new ArrayList<>();
        java.util.Map<String, String> connectionBindings = new java.util.LinkedHashMap<>();

        // Parse mappings list - Legend Engine uses mappings() not runtimeMappings()
        for (RuntimeParserGrammar.MappingsContext mappingCtx : ctx.mappings()) {
            for (RuntimeParserGrammar.QualifiedNameContext qn : mappingCtx.qualifiedName()) {
                mappings.add(qn.getText());
            }
        }

        // Parse connection bindings - Legend Engine uses connections() not
        // runtimeConnections()
        for (RuntimeParserGrammar.ConnectionsContext connCtx : ctx.connections()) {
            for (RuntimeParserGrammar.StoreConnectionsContext storeConn : connCtx.storeConnections()) {
                String storeName = storeConn.qualifiedName().getText();
                for (RuntimeParserGrammar.IdentifiedConnectionContext idConn : storeConn.identifiedConnection()) {
                    if (idConn.packageableElementPointer() != null) {
                        String connName = idConn.packageableElementPointer().getText();
                        connectionBindings.put(storeName, connName);
                    }
                }
            }
        }

        return new RuntimeDefinition(qualifiedName, mappings, connectionBindings);
    }

    /**
     * Visits a single connection runtime.
     */
    public RuntimeDefinition visitSingleConnectionRuntime(RuntimeParserGrammar.SingleConnectionRuntimeContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<String> mappings = new ArrayList<>();
        java.util.Map<String, String> connectionBindings = new java.util.LinkedHashMap<>();

        // Legend Engine uses mappings() not runtimeMappings()
        for (RuntimeParserGrammar.MappingsContext mappingCtx : ctx.mappings()) {
            for (RuntimeParserGrammar.QualifiedNameContext qn : mappingCtx.qualifiedName()) {
                mappings.add(qn.getText());
            }
        }

        for (RuntimeParserGrammar.SingleConnectionContext sc : ctx.singleConnection()) {
            if (sc.packageableElementPointer() != null) {
                // Single connection runtime - the connection applies to all stores
                connectionBindings.put("*", sc.packageableElementPointer().getText());
            }
        }

        return new RuntimeDefinition(qualifiedName, mappings, connectionBindings);
    }

    // ==================== Connection Visitor ====================

    /**
     * Parses a connection definition from its section content.
     * 
     * Note: In Legend Engine, connection definitions are parsed via section-based
     * parsing.
     * The full connection rule (`connectionType qualifiedName connectionValue`)
     * from
     * ConnectionParserGrammar is overwritten by RuntimeParserGrammar's `connection:
     * packageableElementPointer`.
     * 
     * For now, we parse connection content manually. The format is:
     * RelationalDatabaseConnection package::ConnectionName {
     * store: package::DatabaseRef;
     * type: DuckDB;
     * specification: InMemory { };
     * auth: NoAuth { };
     * }
     * 
     * @param qualifiedName the qualified name of the connection
     * @param content       the raw content inside the braces
     */
    public ConnectionDefinition parseConnectionContent(String qualifiedName, String content) {
        String storeRef = "";
        ConnectionDefinition.DatabaseType dbType = ConnectionDefinition.DatabaseType.DuckDB;
        ConnectionSpecification specification = new ConnectionSpecification.InMemory();
        AuthenticationSpec authentication = new AuthenticationSpec.NoAuth();

        // Parse store: package::DbRef;
        storeRef = extractSimpleProperty(content, "store");

        // Parse type: DuckDB;
        String typeStr = extractSimpleProperty(content, "type");
        if (!typeStr.isEmpty()) {
            dbType = switch (typeStr) {
                case "DuckDB" -> ConnectionDefinition.DatabaseType.DuckDB;
                case "H2" -> ConnectionDefinition.DatabaseType.H2;
                case "SQLite" -> ConnectionDefinition.DatabaseType.SQLite;
                case "Postgres" -> ConnectionDefinition.DatabaseType.Postgres;
                case "BigQuery" -> ConnectionDefinition.DatabaseType.BigQuery;
                case "Snowflake" -> ConnectionDefinition.DatabaseType.Snowflake;
                default -> ConnectionDefinition.DatabaseType.DuckDB;
            };
        }

        // Parse specification: Type { ... };
        String specBlock = extractBlockProperty(content, "specification");
        if (!specBlock.isEmpty()) {
            int braceIdx = specBlock.indexOf('{');
            String specType = braceIdx > 0 ? specBlock.substring(0, braceIdx).trim() : specBlock.trim();
            String specBody = braceIdx > 0 ? specBlock.substring(braceIdx + 1) : "";
            if (specBody.endsWith("}"))
                specBody = specBody.substring(0, specBody.length() - 1);

            specification = switch (specType) {
                case "InMemory" -> new ConnectionSpecification.InMemory();
                case "LocalFile" -> {
                    String path = extractStringProperty(specBody, "path");
                    yield new ConnectionSpecification.LocalFile(path);
                }
                case "Static" -> {
                    String host = extractStringProperty(specBody, "host");
                    int port = extractIntProperty(specBody, "port", 5432);
                    String database = extractStringProperty(specBody, "database");
                    yield new ConnectionSpecification.StaticDatasource(host, port, database);
                }
                default -> new ConnectionSpecification.InMemory();
            };
        }

        // Parse auth: Type { ... };
        String authBlock = extractBlockProperty(content, "auth");
        if (!authBlock.isEmpty()) {
            int braceIdx = authBlock.indexOf('{');
            String authType = braceIdx > 0 ? authBlock.substring(0, braceIdx).trim() : authBlock.trim();
            String authBody = braceIdx > 0 ? authBlock.substring(braceIdx + 1) : "";
            if (authBody.endsWith("}"))
                authBody = authBody.substring(0, authBody.length() - 1);

            authentication = switch (authType) {
                case "NoAuth" -> new AuthenticationSpec.NoAuth();
                case "UsernamePassword" -> {
                    String username = extractStringProperty(authBody, "username");
                    String passwordRef = extractStringProperty(authBody, "passwordVaultRef");
                    yield new AuthenticationSpec.UsernamePassword(username, passwordRef);
                }
                default -> new AuthenticationSpec.NoAuth();
            };
        }

        return new ConnectionDefinition(qualifiedName, storeRef, dbType, specification, authentication);
    }

    /**
     * Extracts a simple property value (e.g., "store: package::DbRef;").
     */
    private String extractSimpleProperty(String content, String key) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                key + "\\s*:\\s*([^;{]+);");
        java.util.regex.Matcher matcher = pattern.matcher(content);
        return matcher.find() ? matcher.group(1).trim() : "";
    }

    /**
     * Extracts a block property value (e.g., "specification: InMemory { path: '...'
     * };").
     */
    private String extractBlockProperty(String content, String key) {
        // Find key: followed by optional content and { ... }
        int keyIdx = content.indexOf(key + ":");
        if (keyIdx < 0)
            return "";

        int start = keyIdx + key.length() + 1;
        int depth = 0;
        int braceStart = -1;
        int end = start;

        while (end < content.length()) {
            char c = content.charAt(end);
            if (c == '{') {
                if (depth == 0)
                    braceStart = end;
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    end++;
                    break;
                }
            } else if (c == ';' && depth == 0) {
                break;
            }
            end++;
        }

        return content.substring(start, end).trim();
    }

    /**
     * Extract a string property value from connection body content.
     * E.g., from "host: 'localhost'; port: 5432;" extract "localhost" for key
     * "host"
     */
    private String extractStringProperty(String body, String key) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                key + "\\s*:\\s*'([^']*)'");
        java.util.regex.Matcher matcher = pattern.matcher(body);
        return matcher.find() ? matcher.group(1) : "";
    }

    /**
     * Extract an integer property value from connection body content.
     */
    private int extractIntProperty(String body, String key, int defaultValue) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
                key + "\\s*:\\s*(\\d+)");
        java.util.regex.Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    // ==================== Association Visitor ====================

    /**
     * Visits an association definition.
     */
    public AssociationDefinition visitAssociation(DomainParserGrammar.AssociationContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<PropertyDefinition> properties = new ArrayList<>();

        if (ctx.associationBody() != null && ctx.associationBody().properties() != null) {
            for (DomainParserGrammar.PropertyContext propCtx : ctx.associationBody().properties().property()) {
                PropertyDefinition prop = visitProperty(propCtx);
                if (prop != null) {
                    properties.add(prop);
                }
            }
        }

        // Association needs exactly two ends
        AssociationDefinition.AssociationEndDefinition end1;
        AssociationDefinition.AssociationEndDefinition end2;

        if (properties.size() >= 2) {
            PropertyDefinition p1 = properties.get(0);
            PropertyDefinition p2 = properties.get(1);
            end1 = new AssociationDefinition.AssociationEndDefinition(
                    p1.name(), p1.type(), p1.lowerBound(), p1.upperBound());
            end2 = new AssociationDefinition.AssociationEndDefinition(
                    p2.name(), p2.type(), p2.lowerBound(), p2.upperBound());
        } else if (properties.size() == 1) {
            PropertyDefinition p1 = properties.get(0);
            end1 = new AssociationDefinition.AssociationEndDefinition(
                    p1.name(), p1.type(), p1.lowerBound(), p1.upperBound());
            end2 = new AssociationDefinition.AssociationEndDefinition("unknown", "Unknown", 0, null);
        } else {
            end1 = new AssociationDefinition.AssociationEndDefinition("unknown1", "Unknown", 0, null);
            end2 = new AssociationDefinition.AssociationEndDefinition("unknown2", "Unknown", 0, null);
        }

        return new AssociationDefinition(qualifiedName, end1, end2);
    }

    // ==================== Enum Visitor ====================

    /**
     * Visits an enum definition.
     */
    public EnumDefinition visitEnumDefinition(DomainParserGrammar.EnumDefinitionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<String> values = new ArrayList<>();

        for (DomainParserGrammar.EnumValueContext valCtx : ctx.enumValue()) {
            values.add(valCtx.identifier().getText());
        }

        return new EnumDefinition(qualifiedName, values);
    }

    // ==================== Profile Visitor ====================

    /**
     * Visits a profile definition.
     */
    public ProfileDefinition visitProfile(DomainParserGrammar.ProfileContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();
        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        for (DomainParserGrammar.StereotypeDefinitionsContext stereoCtx : ctx.stereotypeDefinitions()) {
            for (DomainParserGrammar.IdentifierContext id : stereoCtx.identifier()) {
                stereotypes.add(id.getText());
            }
        }

        for (DomainParserGrammar.TagDefinitionsContext tagCtx : ctx.tagDefinitions()) {
            for (DomainParserGrammar.IdentifierContext id : tagCtx.identifier()) {
                tags.add(id.getText());
            }
        }

        return new ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    // ==================== Function Visitor ====================

    /**
     * Visits a function definition.
     */
    public FunctionDefinition visitFunctionDefinition(DomainParserGrammar.FunctionDefinitionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract parameters
        List<FunctionDefinition.ParameterDefinition> parameters = new ArrayList<>();
        if (ctx.functionTypeSignature() != null) {
            for (DomainParserGrammar.FunctionVariableExpressionContext paramCtx : ctx.functionTypeSignature()
                    .functionVariableExpression()) {
                ClassDefinition.ParameterDefinition param = visitFunctionVariableExpression(paramCtx);
                if (param != null) {
                    parameters.add(new FunctionDefinition.ParameterDefinition(
                            param.name(), param.type(), param.lowerBound(), param.upperBound()));
                }
            }
        }

        // Extract return type
        String returnType = "";
        int returnLowerBound = 1;
        Integer returnUpperBound = 1;
        if (ctx.functionTypeSignature() != null && ctx.functionTypeSignature().type() != null) {
            if (ctx.functionTypeSignature().type().qualifiedName() != null) {
                returnType = ctx.functionTypeSignature().type().qualifiedName().getText();
            }
            if (ctx.functionTypeSignature().multiplicity() != null) {
                int[] bounds = parseMultiplicity(ctx.functionTypeSignature().multiplicity().getText());
                returnLowerBound = bounds[0];
                returnUpperBound = bounds[1] == -1 ? null : bounds[1];
            }
        }

        // Get function body
        String body = "";
        if (ctx.codeBlock() != null) {
            body = ctx.codeBlock().getText();
        }

        return new FunctionDefinition(qualifiedName, parameters, returnType, returnLowerBound, returnUpperBound, body);
    }
}
