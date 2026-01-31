package org.finos.legend.pure.dsl.antlr;

import org.finos.legend.pure.dsl.definition.ClassDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.ConstraintDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.DerivedPropertyDefinition;
import org.finos.legend.pure.dsl.definition.ClassDefinition.PropertyDefinition;
import org.finos.legend.pure.dsl.definition.StereotypeApplication;
import org.finos.legend.pure.dsl.definition.TaggedValue;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

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
public class PureDefinitionBuilder extends PureParserBaseVisitor<Object> {

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
    public ClassDefinition visitClassDefinition(PureParser.ClassDefinitionContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract superclasses from EXTENDS clause
        List<String> superClasses = new ArrayList<>();
        List<PureParser.TypeContext> typeContexts = ctx.type();
        if (typeContexts != null) {
            for (PureParser.TypeContext typeCtx : typeContexts) {
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

        PureParser.ClassBodyContext bodyCtx = ctx.classBody();
        if (bodyCtx != null && bodyCtx.properties() != null) {
            // Process simple properties
            for (PureParser.PropertyContext propCtx : bodyCtx.properties().property()) {
                PropertyDefinition prop = visitProperty(propCtx);
                if (prop != null) {
                    properties.add(prop);
                }
            }
            // Process qualified/derived properties
            for (PureParser.QualifiedPropertyContext qualPropCtx : bodyCtx.properties().qualifiedProperty()) {
                DerivedPropertyDefinition derivedProp = visitQualifiedProperty(qualPropCtx);
                if (derivedProp != null) {
                    derivedProperties.add(derivedProp);
                }
            }
        }

        // Extract constraints
        List<ConstraintDefinition> constraints = new ArrayList<>();
        if (ctx.constraints() != null) {
            for (PureParser.ConstraintContext constraintCtx : ctx.constraints().constraint()) {
                ConstraintDefinition constraint = visitConstraint(constraintCtx);
                if (constraint != null) {
                    constraints.add(constraint);
                }
            }
        }

        // Extract stereotypes
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        if (ctx.stereotypes() != null) {
            for (PureParser.StereotypeContext stereotypeCtx : ctx.stereotypes().stereotype()) {
                StereotypeApplication stereotype = visitStereotype(stereotypeCtx);
                if (stereotype != null) {
                    stereotypes.add(stereotype);
                }
            }
        }

        // Extract tagged values
        List<TaggedValue> taggedValues = new ArrayList<>();
        if (ctx.taggedValues() != null) {
            for (PureParser.TaggedValueContext tvCtx : ctx.taggedValues().taggedValue()) {
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
    public PropertyDefinition visitProperty(PureParser.PropertyContext ctx) {
        if (ctx == null) {
            return null;
        }

        String name = ctx.identifier().getText();

        // Extract type and multiplicity from propertyReturnType
        PureParser.PropertyReturnTypeContext returnTypeCtx = ctx.propertyReturnType();
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
    public DerivedPropertyDefinition visitQualifiedProperty(PureParser.QualifiedPropertyContext ctx) {
        if (ctx == null) {
            return null;
        }

        String name = ctx.identifier().getText();

        // Extract parameters from qualifiedPropertyBody
        List<ClassDefinition.ParameterDefinition> parameters = new ArrayList<>();
        String expression = "";

        if (ctx.qualifiedPropertyBody() != null) {
            // Extract parameters from functionVariableExpression list
            for (PureParser.FunctionVariableExpressionContext paramCtx : ctx.qualifiedPropertyBody()
                    .functionVariableExpression()) {
                ClassDefinition.ParameterDefinition param = visitFunctionVariableExpression(paramCtx);
                if (param != null) {
                    parameters.add(param);
                }
            }

            // Get expression from codeBlock - preserve original whitespace
            if (ctx.qualifiedPropertyBody().codeBlock() != null) {
                expression = getOriginalText(ctx.qualifiedPropertyBody().codeBlock());
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
            PureParser.FunctionVariableExpressionContext ctx) {
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
    public ConstraintDefinition visitConstraint(PureParser.ConstraintContext ctx) {
        if (ctx == null) {
            return null;
        }

        // Get constraint name if present
        String name = "unnamed";
        if (ctx.simpleConstraint() != null && ctx.simpleConstraint().constraintId() != null) {
            name = ctx.simpleConstraint().constraintId().identifier().getText();
        }

        // Get constraint expression - preserve original whitespace
        String expression = "";
        if (ctx.simpleConstraint() != null && ctx.simpleConstraint().combinedExpression() != null) {
            expression = getOriginalText(ctx.simpleConstraint().combinedExpression());
        }

        return new ConstraintDefinition(name, expression);
    }

    /**
     * Gets the original text from a parse context, preserving whitespace.
     * Uses the input stream to get the exact text as written.
     */
    private static String getOriginalText(ParserRuleContext ctx) {
        if (ctx == null || ctx.start == null || ctx.stop == null) {
            return "";
        }
        // Get the original text from the character stream
        return ctx.start.getInputStream().getText(
                Interval.of(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    /**
     * Visits a stereotype application.
     * 
     * Grammar: stereotype: qualifiedName DOT identifier
     */
    public StereotypeApplication visitStereotype(PureParser.StereotypeContext ctx) {
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
    public TaggedValue visitTaggedValue(PureParser.TaggedValueContext ctx) {
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
    public static List<ClassDefinition> extractClassDefinitions(PureParser.DefinitionContext definitionCtx) {
        List<ClassDefinition> result = new ArrayList<>();
        if (definitionCtx == null) {
            return result;
        }

        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        // Navigate through elementDefinition* to find classDefinitions
        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
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
    public static Optional<ClassDefinition> extractFirstClassDefinition(PureParser.DefinitionContext definitionCtx) {
        List<ClassDefinition> defs = extractClassDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Enum Extraction ====================

    /**
     * Visits an enum definition parse tree node and returns an EnumDefinition.
     * 
     * Grammar rule:
     * enumDefinition: ENUM stereotypes? taggedValues? qualifiedName
     * BRACE_OPEN (enumValue (COMMA enumValue)*)? BRACE_CLOSE
     * enumValue: stereotypes? taggedValues? identifier
     */
    public org.finos.legend.pure.dsl.definition.EnumDefinition visitEnumDefinition(
            PureParser.EnumDefinitionContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract enum values
        List<String> values = new ArrayList<>();
        for (PureParser.EnumValueContext valueCtx : ctx.enumValue()) {
            // For now, just extract the identifier (ignoring per-value stereotypes/tagged
            // values)
            String valueName = valueCtx.identifier().getText();
            values.add(valueName);
        }

        return org.finos.legend.pure.dsl.definition.EnumDefinition.of(qualifiedName, values);
    }

    /**
     * Extracts all EnumDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.EnumDefinition> extractEnumDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.EnumDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.enumDefinition() != null) {
                result.add(builder.visitEnumDefinition(elemCtx.enumDefinition()));
            }
        }

        return result;
    }

    /**
     * Extracts the first EnumDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.EnumDefinition> extractFirstEnumDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.EnumDefinition> defs = extractEnumDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Profile Extraction ====================

    /**
     * Visits a profile parse tree node and returns a ProfileDefinition.
     * 
     * Grammar rule:
     * profile: PROFILE qualifiedName BRACE_OPEN (stereotypeDefinitions |
     * tagDefinitions)* BRACE_CLOSE
     * stereotypeDefinitions: STEREOTYPES COLON BRACKET_OPEN (identifier (COMMA
     * identifier)*)? BRACKET_CLOSE SEMI_COLON
     * tagDefinitions: TAGS COLON BRACKET_OPEN (identifier (COMMA identifier)*)?
     * BRACKET_CLOSE SEMI_COLON
     */
    public org.finos.legend.pure.dsl.definition.ProfileDefinition visitProfile(PureParser.ProfileContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract stereotypes and tags
        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        for (PureParser.StereotypeDefinitionsContext stereotypeCtx : ctx.stereotypeDefinitions()) {
            for (PureParser.IdentifierContext idCtx : stereotypeCtx.identifier()) {
                stereotypes.add(idCtx.getText());
            }
        }

        for (PureParser.TagDefinitionsContext tagCtx : ctx.tagDefinitions()) {
            for (PureParser.IdentifierContext idCtx : tagCtx.identifier()) {
                tags.add(idCtx.getText());
            }
        }

        return new org.finos.legend.pure.dsl.definition.ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    /**
     * Extracts all ProfileDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.ProfileDefinition> extractProfileDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ProfileDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.profile() != null) {
                result.add(builder.visitProfile(elemCtx.profile()));
            }
        }

        return result;
    }

    /**
     * Extracts the first ProfileDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.ProfileDefinition> extractFirstProfileDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ProfileDefinition> defs = extractProfileDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Association Extraction ====================

    /**
     * Visits an association parse tree node and returns an AssociationDefinition.
     * 
     * Grammar rule:
     * association: ASSOCIATION stereotypes? taggedValues? qualifiedName
     * (associationProjection | associationBody)
     * associationBody: BRACE_OPEN properties BRACE_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.AssociationDefinition visitAssociation(
            PureParser.AssociationContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract properties from associationBody (same structure as class properties)
        List<org.finos.legend.pure.dsl.definition.AssociationDefinition.AssociationEndDefinition> ends = new ArrayList<>();

        if (ctx.associationBody() != null && ctx.associationBody().properties() != null) {
            for (PureParser.PropertyContext propCtx : ctx.associationBody().properties().property()) {
                String propName = propCtx.identifier().getText();
                String propType = propCtx.propertyReturnType().type().getText();

                // Parse multiplicity
                String multText = propCtx.propertyReturnType().multiplicity().getText();
                int[] bounds = parseMultiplicity(multText);

                ends.add(new org.finos.legend.pure.dsl.definition.AssociationDefinition.AssociationEndDefinition(
                        propName, propType, bounds[0], bounds[1] == -1 ? null : bounds[1]));
            }
        }

        if (ends.size() != 2) {
            throw new org.finos.legend.pure.dsl.PureParseException(
                    "Association must have exactly 2 properties, found: " + ends.size());
        }

        return new org.finos.legend.pure.dsl.definition.AssociationDefinition(qualifiedName, ends.get(0), ends.get(1));
    }

    /**
     * Extracts all AssociationDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.AssociationDefinition> extractAssociationDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.AssociationDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.association() != null) {
                result.add(builder.visitAssociation(elemCtx.association()));
            }
        }

        return result;
    }

    /**
     * Extracts the first AssociationDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.AssociationDefinition> extractFirstAssociationDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.AssociationDefinition> defs = extractAssociationDefinitions(
                definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Function Extraction ====================

    /**
     * Visits a function definition parse tree node and returns a
     * FunctionDefinition.
     * 
     * Grammar rule:
     * functionDefinition: FUNCTION stereotypes? taggedValues? qualifiedName
     * typeAndMultiplicityParameters?
     * functionTypeSignature constraints? BRACE_OPEN codeBlock BRACE_CLOSE
     * functionTestSuiteDef?
     * functionTypeSignature: PAREN_OPEN (functionVariableExpression (COMMA
     * functionVariableExpression)*)? PAREN_CLOSE COLON type multiplicity
     */
    public org.finos.legend.pure.dsl.definition.FunctionDefinition visitFunctionDefinition(
            PureParser.FunctionDefinitionContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract stereotypes
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        if (ctx.stereotypes() != null) {
            for (PureParser.StereotypeContext sCtx : ctx.stereotypes().stereotype()) {
                stereotypes.add(visitStereotype(sCtx));
            }
        }

        // Extract tagged values
        List<TaggedValue> taggedValues = new ArrayList<>();
        if (ctx.taggedValues() != null) {
            for (PureParser.TaggedValueContext tvCtx : ctx.taggedValues().taggedValue()) {
                taggedValues.add(visitTaggedValue(tvCtx));
            }
        }

        // Extract parameters from functionTypeSignature
        List<org.finos.legend.pure.dsl.definition.FunctionDefinition.ParameterDefinition> parameters = new ArrayList<>();
        PureParser.FunctionTypeSignatureContext sigCtx = ctx.functionTypeSignature();
        for (PureParser.FunctionVariableExpressionContext paramCtx : sigCtx.functionVariableExpression()) {
            String paramName = paramCtx.identifier().getText();
            String paramType = paramCtx.type().getText();
            String multText = paramCtx.multiplicity().getText();
            int[] bounds = parseMultiplicity(multText);
            parameters.add(new org.finos.legend.pure.dsl.definition.FunctionDefinition.ParameterDefinition(
                    paramName, paramType, bounds[0], bounds[1] == -1 ? null : bounds[1]));
        }

        // Extract return type and multiplicity
        String returnType = sigCtx.type().getText();
        String returnMultText = sigCtx.multiplicity().getText();
        int[] returnBounds = parseMultiplicity(returnMultText);

        // Extract function body from codeBlock
        String body = getOriginalText(ctx.codeBlock());

        return new org.finos.legend.pure.dsl.definition.FunctionDefinition(
                qualifiedName,
                parameters,
                returnType,
                returnBounds[0],
                returnBounds[1] == -1 ? null : returnBounds[1],
                body,
                stereotypes,
                taggedValues);
    }

    /**
     * Extracts all FunctionDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.FunctionDefinition> extractFunctionDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.FunctionDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.functionDefinition() != null) {
                result.add(builder.visitFunctionDefinition(elemCtx.functionDefinition()));
            }
        }

        return result;
    }

    /**
     * Extracts the first FunctionDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.FunctionDefinition> extractFirstFunctionDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.FunctionDefinition> defs = extractFunctionDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Database Extraction ====================

    /**
     * Visits a database parse tree node and returns a DatabaseDefinition.
     * 
     * Grammar rule:
     * database: DATABASE stereotypes? taggedValues? qualifiedName PAREN_OPEN
     * (includeDatabase)* (dbSchema | dbTable | dbView | dbJoin | ...)* PAREN_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.DatabaseDefinition visitDatabase(PureParser.DatabaseContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract tables (both top-level and in schemas)
        List<org.finos.legend.pure.dsl.definition.DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        for (PureParser.DbTableContext tableCtx : ctx.dbTable()) {
            tables.add(extractDbTable(tableCtx));
        }
        // Also extract tables from schemas
        for (PureParser.DbSchemaContext schemaCtx : ctx.dbSchema()) {
            for (PureParser.DbTableContext tableCtx : schemaCtx.dbTable()) {
                tables.add(extractDbTable(tableCtx));
            }
        }

        // Extract joins
        List<org.finos.legend.pure.dsl.definition.DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        for (PureParser.DbJoinContext joinCtx : ctx.dbJoin()) {
            joins.add(extractDbJoin(joinCtx));
        }

        return new org.finos.legend.pure.dsl.definition.DatabaseDefinition(qualifiedName, tables, joins);
    }

    /**
     * Visits a table parse tree node.
     */
    private org.finos.legend.pure.dsl.definition.DatabaseDefinition.TableDefinition extractDbTable(
            PureParser.DbTableContext ctx) {
        String tableName = ctx.relationalIdentifier().getText();

        List<org.finos.legend.pure.dsl.definition.DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
        for (PureParser.ColumnDefinitionContext colCtx : ctx.columnDefinition()) {
            columns.add(extractColumnDefinition(colCtx));
        }

        return new org.finos.legend.pure.dsl.definition.DatabaseDefinition.TableDefinition(tableName, columns);
    }

    /**
     * Visits a column definition parse tree node.
     */
    private org.finos.legend.pure.dsl.definition.DatabaseDefinition.ColumnDefinition extractColumnDefinition(
            PureParser.ColumnDefinitionContext ctx) {
        String columnName = ctx.relationalIdentifier().getText();
        String dataType = ctx.identifier().getText();

        // Check for size specification like VARCHAR(100)
        if (ctx.INTEGER() != null && !ctx.INTEGER().isEmpty()) {
            dataType = dataType + "(" + ctx.INTEGER(0).getText();
            if (ctx.INTEGER().size() > 1) {
                dataType += "," + ctx.INTEGER(1).getText();
            }
            dataType += ")";
        }

        boolean primaryKey = ctx.PRIMARY_KEY() != null;
        boolean notNull = ctx.NOT_NULL() != null || primaryKey;

        return new org.finos.legend.pure.dsl.definition.DatabaseDefinition.ColumnDefinition(
                columnName, dataType, primaryKey, notNull);
    }

    /**
     * Visits a join definition parse tree node.
     * Grammar: dbJoin: JOIN stereotypes? taggedValues? identifier PAREN_OPEN
     * operation PAREN_CLOSE
     */
    private org.finos.legend.pure.dsl.definition.DatabaseDefinition.JoinDefinition extractDbJoin(
            PureParser.DbJoinContext ctx) {
        String joinName = ctx.identifier().getText();

        // The join expression is in the 'operation' rule - we need to parse it
        // For now, extract the text and parse it with regex (join expressions are
        // complex)
        String operationText = getOriginalText(ctx.dbOperation());

        // Simple pattern: TABLE_A.COLUMN_A = TABLE_B.COLUMN_B
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)");
        java.util.regex.Matcher matcher = pattern.matcher(operationText);

        if (matcher.find()) {
            return new org.finos.legend.pure.dsl.definition.DatabaseDefinition.JoinDefinition(
                    joinName, matcher.group(1), matcher.group(2), matcher.group(3), matcher.group(4));
        }

        // Fallback - return placeholder
        return new org.finos.legend.pure.dsl.definition.DatabaseDefinition.JoinDefinition(
                joinName, "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN");
    }

    /**
     * Extracts all DatabaseDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.DatabaseDefinition> extractDatabaseDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.DatabaseDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.database() != null) {
                result.add(builder.visitDatabase(elemCtx.database()));
            }
        }

        return result;
    }

    /**
     * Extracts the first DatabaseDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.DatabaseDefinition> extractFirstDatabaseDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.DatabaseDefinition> defs = extractDatabaseDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Mapping Definition ====================

    /**
     * Visits a mapping parse tree node and returns a MappingDefinition.
     * 
     * Grammar rule:
     * mapping: MAPPING qualifiedName PAREN_OPEN (includeMapping)*
     * (classMappingElement
     * | associationMappingElement | enumerationMappingElement)*
     * (mappingTestableDefinition)? PAREN_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition visitMapping(
            PureParser.MappingContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        List<org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition> classMappings = new ArrayList<>();

        // Process class mappings
        for (PureParser.ClassMappingElementContext classMappingCtx : ctx.classMappingElement()) {
            classMappings.add(visitClassMappingElement(classMappingCtx));
        }

        // Process enumeration mappings
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.EnumerationMappingDefinition> enumerationMappings = new ArrayList<>();
        for (PureParser.EnumerationMappingElementContext enumMappingCtx : ctx.enumerationMappingElement()) {
            enumerationMappings.add(visitEnumerationMappingElement(enumMappingCtx));
        }

        // Process testSuites
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.TestSuiteDefinition> testSuites = new ArrayList<>();
        if (ctx.mappingTestableDefinition() != null) {
            for (PureParser.MappingTestSuiteContext suiteCtx : ctx.mappingTestableDefinition().mappingTestSuite()) {
                testSuites.add(visitMappingTestSuite(suiteCtx));
            }
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition(
                qualifiedName, classMappings, enumerationMappings, testSuites);
    }

    /**
     * Visits an enumeration mapping element.
     * 
     * Grammar: qualifiedName (BRACKET_OPEN mappingElementId BRACKET_CLOSE)? COLON
     * ENUMERATION_MAPPING (mappingElementId)?
     * BRACE_OPEN (enumValueMapping (COMMA enumValueMapping)*)? BRACE_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.EnumerationMappingDefinition visitEnumerationMappingElement(
            PureParser.EnumerationMappingElementContext ctx) {
        String enumType = ctx.qualifiedName().getText();

        // Get optional mapping ID (after ENUMERATION_MAPPING keyword)
        String id = null;
        if (ctx.mappingElementId() != null && !ctx.mappingElementId().isEmpty()) {
            // Use the last mappingElementId (after ENUMERATION_MAPPING, not the bracketed
            // one)
            id = ctx.mappingElementId(ctx.mappingElementId().size() - 1).getText();
        }

        // Parse value mappings: EnumValue: ['dbVal1', 'dbVal2'] or EnumValue: 'dbVal'
        java.util.Map<String, java.util.List<Object>> valueMappings = new java.util.LinkedHashMap<>();
        for (PureParser.EnumValueMappingContext valueMappingCtx : ctx.enumValueMapping()) {
            String enumValue = valueMappingCtx.identifier().getText();
            java.util.List<Object> sourceValues = new ArrayList<>();

            if (valueMappingCtx.enumSourceValueArray() != null) {
                // Multiple source values: ['P', 'PEND']
                for (PureParser.EnumSourceValueContext srcCtx : valueMappingCtx.enumSourceValueArray()
                        .enumSourceValue()) {
                    sourceValues.add(parseEnumSourceValue(srcCtx));
                }
            } else if (valueMappingCtx.enumSourceValue() != null) {
                // Single source value: 'P'
                sourceValues.add(parseEnumSourceValue(valueMappingCtx.enumSourceValue()));
            }

            valueMappings.put(enumValue, sourceValues);
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition.EnumerationMappingDefinition(
                enumType, id, valueMappings);
    }

    /**
     * Parses an enum source value (STRING, INTEGER, or enum reference).
     */
    private Object parseEnumSourceValue(PureParser.EnumSourceValueContext ctx) {
        if (ctx.STRING() != null) {
            return unquoteString(ctx.STRING().getText());
        } else if (ctx.INTEGER() != null) {
            return Integer.parseInt(ctx.INTEGER().getText());
        } else {
            // Enum reference: EnumType.VALUE - return as string
            return ctx.getText();
        }
    }

    /**
     * Visits a mapping test suite.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.TestSuiteDefinition visitMappingTestSuite(
            PureParser.MappingTestSuiteContext ctx) {
        String suiteName = ctx.identifier().getText();
        String functionBody = null;
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.TestDefinition> tests = new ArrayList<>();

        // Extract function
        for (PureParser.MappingTestableFuncContext funcCtx : ctx.mappingTestableFunc()) {
            functionBody = getOriginalText(funcCtx.combinedExpression());
        }

        // Extract tests
        for (PureParser.MappingTestsContext testsCtx : ctx.mappingTests()) {
            for (PureParser.MappingTestContentContext testCtx : testsCtx.mappingTestContent()) {
                tests.add(visitMappingTestContent(testCtx));
            }
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition.TestSuiteDefinition(
                suiteName, functionBody, tests);
    }

    /**
     * Visits a mapping test content.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.TestDefinition visitMappingTestContent(
            PureParser.MappingTestContentContext ctx) {
        String testName = ctx.identifier().getText();
        String documentation = null;
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.TestData> inputData = new ArrayList<>();
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.TestAssertion> asserts = new ArrayList<>();

        // Extract doc
        for (PureParser.MappingTestableDocContext docCtx : ctx.mappingTestableDoc()) {
            documentation = unquoteString(docCtx.STRING().getText());
        }

        // Extract data
        for (PureParser.MappingTestableDataContext dataCtx : ctx.mappingTestableData()) {
            for (PureParser.MappingTestDataContentContext contentCtx : dataCtx.mappingTestDataContent()) {
                inputData.add(visitMappingTestDataContent(contentCtx));
            }
        }

        // Extract asserts
        for (PureParser.MappingTestAssertsContext assertsCtx : ctx.mappingTestAsserts()) {
            for (PureParser.MappingTestAssertContext assertCtx : assertsCtx.mappingTestAssert()) {
                asserts.add(visitMappingTestAssert(assertCtx));
            }
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition.TestDefinition(
                testName, documentation, inputData, asserts);
    }

    /**
     * Visits a mapping test data content.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.TestData visitMappingTestDataContent(
            PureParser.MappingTestDataContentContext ctx) {
        String storeName = ctx.qualifiedName().getText();

        PureParser.EmbeddedDataContext embeddedCtx = ctx.embeddedData();
        String format = embeddedCtx.identifier().getText();

        // Get the raw embedded content
        StringBuilder content = new StringBuilder();
        for (PureParser.EmbeddedDataContentContext contentCtx : embeddedCtx.embeddedDataContent()) {
            content.append(getOriginalText(contentCtx));
        }

        String contentStr = content.toString().trim();

        // Check if this is a Reference
        if ("Reference".equals(format)) {
            // Extract the reference path from the island content
            // Strip any trailing whitespace and island markers like }#
            String refPath = contentStr.trim();
            // Remove trailing }# if present
            if (refPath.endsWith("}#")) {
                refPath = refPath.substring(0, refPath.length() - 2).trim();
            }
            return new org.finos.legend.pure.dsl.definition.MappingDefinition.TestData(
                    storeName, null, refPath, true);
        }

        // Parse ExternalFormat content
        String contentType = null;
        String data = null;

        // Look for contentType: '...' and data: '...' in the content
        java.util.regex.Pattern contentTypePattern = java.util.regex.Pattern.compile("contentType:\\s*'([^']*)'");
        java.util.regex.Pattern dataPattern = java.util.regex.Pattern.compile("data:\\s*'([^']*)'");

        java.util.regex.Matcher contentTypeMatcher = contentTypePattern.matcher(contentStr);
        if (contentTypeMatcher.find()) {
            contentType = contentTypeMatcher.group(1);
        }

        java.util.regex.Matcher dataMatcher = dataPattern.matcher(contentStr);
        if (dataMatcher.find()) {
            data = dataMatcher.group(1);
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition.TestData(
                storeName, contentType, data, false);
    }

    /**
     * Visits a mapping test assert.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.TestAssertion visitMappingTestAssert(
            PureParser.MappingTestAssertContext ctx) {
        String assertName = ctx.identifier().getText();

        PureParser.TestAssertionContext assertionCtx = ctx.testAssertion();
        String assertType = assertionCtx.identifier().getText();

        // Get the raw content
        StringBuilder content = new StringBuilder();
        for (PureParser.TestAssertionContentContext contentCtx : assertionCtx.testAssertionContent()) {
            content.append(getOriginalText(contentCtx));
        }
        String contentStr = content.toString().trim();

        // Extract expected data from EqualToJson island
        String expectedData = null;
        java.util.regex.Pattern dataPattern = java.util.regex.Pattern.compile("data:\\s*'([^']*)'");
        java.util.regex.Matcher dataMatcher = dataPattern.matcher(contentStr);
        if (dataMatcher.find()) {
            expectedData = dataMatcher.group(1);
        }

        return new org.finos.legend.pure.dsl.definition.MappingDefinition.TestAssertion(
                assertName, assertType, null, expectedData);
    }

    /**
     * Visits a class mapping element and returns a ClassMappingDefinition.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition visitClassMappingElement(
            PureParser.ClassMappingElementContext ctx) {
        String qualifiedClassName = ctx.qualifiedName().getText();
        String className = qualifiedClassName.contains("::")
                ? qualifiedClassName.substring(qualifiedClassName.lastIndexOf("::") + 2)
                : qualifiedClassName;

        String mappingType = ctx.classMappingType().getText();

        if ("Pure".equals(mappingType)) {
            return visitPureM2MClassMappingBody(className, ctx.classMappingBody().pureM2MClassMappingBody());
        } else {
            return visitRelationalClassMappingBody(className, ctx.classMappingBody().relationalClassMappingBody());
        }
    }

    /**
     * Visits a relational class mapping body.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition visitRelationalClassMappingBody(
            String className, PureParser.RelationalClassMappingBodyContext ctx) {

        org.finos.legend.pure.dsl.definition.MappingDefinition.TableReference mainTable = null;
        if (ctx.mappingMainTable() != null) {
            mainTable = visitMappingMainTable(ctx.mappingMainTable());
        }

        List<org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition> propertyMappings = new ArrayList<>();
        for (PureParser.RelationalPropertyMappingContext propCtx : ctx.relationalPropertyMapping()) {
            org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition propMapping = visitRelationalPropertyMapping(
                    propCtx);
            if (propMapping != null) {
                propertyMappings.add(propMapping);
            }
        }

        return org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition.relational(
                className, mainTable, propertyMappings);
    }

    /**
     * Visits a mainTable clause.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.TableReference visitMappingMainTable(
            PureParser.MappingMainTableContext ctx) {
        String databaseName = ctx.databasePointer().qualifiedName().getText();
        String tableName = ctx.mappingTableRef().getText();
        return new org.finos.legend.pure.dsl.definition.MappingDefinition.TableReference(databaseName, tableName);
    }

    /**
     * Visits a relational property mapping.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitRelationalPropertyMapping(
            PureParser.RelationalPropertyMappingContext ctx) {
        if (ctx.standardPropertyMapping() != null) {
            return visitStandardPropertyMapping(ctx.standardPropertyMapping());
        } else if (ctx.localMappingProperty() != null) {
            return visitLocalMappingProperty(ctx.localMappingProperty());
        }
        return null;
    }

    /**
     * Visits a standard property mapping.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitStandardPropertyMapping(
            PureParser.StandardPropertyMappingContext ctx) {
        String propertyName = ctx.identifier().getText();
        return visitRelationalPropertyValue(propertyName, ctx.relationalPropertyValue());
    }

    /**
     * Visits a local mapping property.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitLocalMappingProperty(
            PureParser.LocalMappingPropertyContext ctx) {
        String propertyName = ctx.identifier().getText();
        return visitRelationalPropertyValue(propertyName, ctx.relationalPropertyValue());
    }

    /**
     * Visits a relational property value.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitRelationalPropertyValue(
            String propertyName, PureParser.RelationalPropertyValueContext ctx) {

        if (ctx.embeddedPropertyMapping() != null || ctx.inlineEmbeddedPropertyMapping() != null) {
            String expression = getOriginalText(ctx);
            return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.expression(
                    propertyName, expression, null);
        }

        if (ctx.mappingOperation() != null) {
            // Check for enumTransformer: EnumerationMapping id:
            String enumMappingId = null;
            if (ctx.enumTransformer() != null) {
                // Grammar: enumTransformer: ENUMERATION_MAPPING (identifier)? COLON
                if (ctx.enumTransformer().identifier() != null) {
                    enumMappingId = ctx.enumTransformer().identifier().getText();
                } else {
                    // No ID specified - use empty string to indicate "use default enum mapping"
                    enumMappingId = "";
                }
            }
            return visitMappingOperation(propertyName, ctx.mappingOperation(), enumMappingId);
        }

        return null;
    }

    /**
     * Visits a mapping operation.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitMappingOperation(
            String propertyName, PureParser.MappingOperationContext ctx, String enumMappingId) {

        PureParser.MappingAtomicOperationContext atomicCtx = ctx.mappingAtomicOperation();

        // Check for join operation: [DB]@JoinName
        if (atomicCtx.databasePointer() != null && atomicCtx.mappingJoinOperation() != null) {
            return visitMappingJoinOperation(propertyName, atomicCtx);
        }

        // Check for column operation: [DB] TABLE.COLUMN
        if (atomicCtx.mappingColumnOperation() != null) {
            return visitMappingColumnOperation(propertyName, atomicCtx.mappingColumnOperation(), enumMappingId);
        }

        // Fallback: store entire operation as expression
        String expression = getOriginalText(ctx);
        return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.expression(
                propertyName, expression, null);
    }

    /**
     * Visits a mapping column operation: [DB] TABLE.COLUMN
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitMappingColumnOperation(
            String propertyName, PureParser.MappingColumnOperationContext ctx, String enumMappingId) {

        String databaseName = null;
        if (ctx.databasePointer() != null) {
            databaseName = ctx.databasePointer().qualifiedName().getText();
        }

        PureParser.MappingTableColumnRefContext colRef = ctx.mappingTableColumnRef();
        List<String> identifiers = new ArrayList<>();
        identifiers.add(colRef.relationalIdentifier().getText());

        if (colRef.mappingScopeInfo() != null) {
            for (PureParser.RelationalIdentifierContext id : colRef.mappingScopeInfo().relationalIdentifier()) {
                identifiers.add(id.getText());
            }
        }

        if (identifiers.size() >= 2 && databaseName != null) {
            String tableName = unquote(identifiers.get(identifiers.size() - 2));
            String columnName = unquote(identifiers.get(identifiers.size() - 1));

            if (ctx.mappingVariantAccess() != null) {
                String expression = getOriginalText(ctx);
                return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.expression(
                        propertyName, expression, null);
            }

            org.finos.legend.pure.dsl.definition.MappingDefinition.ColumnReference colRefObj = new org.finos.legend.pure.dsl.definition.MappingDefinition.ColumnReference(
                    databaseName, tableName, columnName);

            if (enumMappingId != null) {
                return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition
                        .columnWithEnumMapping(
                                propertyName, colRefObj, enumMappingId);
            } else {
                return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.column(
                        propertyName, colRefObj);
            }
        }

        String expression = getOriginalText(ctx);
        return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.expression(
                propertyName, expression, null);
    }

    private String unquote(String id) {
        if (id.startsWith("\"") && id.endsWith("\"")) {
            return id.substring(1, id.length() - 1);
        }
        return id;
    }

    /**
     * Visits a mapping join operation: [DB]@JoinName
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition visitMappingJoinOperation(
            String propertyName, PureParser.MappingAtomicOperationContext ctx) {

        String databaseName = ctx.databasePointer().qualifiedName().getText();

        PureParser.MappingJoinSequenceContext joinSeqCtx = ctx.mappingJoinOperation().mappingJoinSequence();
        if (joinSeqCtx != null && joinSeqCtx.mappingJoinPointer() != null) {
            String joinName = joinSeqCtx.mappingJoinPointer().identifier().getText();

            return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.join(
                    propertyName,
                    new org.finos.legend.pure.dsl.definition.MappingDefinition.JoinReference(
                            databaseName, joinName));
        }

        String expression = getOriginalText(ctx);
        return org.finos.legend.pure.dsl.definition.MappingDefinition.PropertyMappingDefinition.expression(
                propertyName, expression, null);
    }

    /**
     * Visits a Pure M2M class mapping body.
     */
    public org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition visitPureM2MClassMappingBody(
            String className, PureParser.PureM2MClassMappingBodyContext ctx) {

        String sourceClassName = null;
        String filterExpression = null;
        java.util.Map<String, String> m2mPropertyExpressions = new java.util.LinkedHashMap<>();

        if (ctx.pureM2MSrcClause() != null) {
            sourceClassName = ctx.pureM2MSrcClause().qualifiedName().getText();
        }

        if (ctx.pureM2MFilterClause() != null) {
            filterExpression = getOriginalText(ctx.pureM2MFilterClause().combinedExpression());
        }

        for (PureParser.PureM2MPropertyMappingContext propCtx : ctx.pureM2MPropertyMapping()) {
            String propName = propCtx.identifier().getText();
            String expression = getOriginalText(propCtx.combinedExpression());
            m2mPropertyExpressions.put(propName, expression);
        }

        return org.finos.legend.pure.dsl.definition.MappingDefinition.ClassMappingDefinition.pure(
                className, sourceClassName, filterExpression, m2mPropertyExpressions);
    }

    /**
     * Extracts all MappingDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.MappingDefinition> extractMappingDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.MappingDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.mapping() != null) {
                result.add(builder.visitMapping(elemCtx.mapping()));
            }
        }

        return result;
    }

    /**
     * Extracts the first MappingDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.MappingDefinition> extractFirstMappingDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.MappingDefinition> defs = extractMappingDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Service Definition ====================

    /**
     * Visits a service definition parse tree node and returns a ServiceDefinition.
     * 
     * Grammar rule:
     * serviceDefinition: SERVICE stereotypes? taggedValues? qualifiedName
     * BRACE_OPEN (servicePattern | serviceOwners | serviceDocumentation |
     * serviceAutoActivateUpdates | serviceExec | serviceTestSuites)*
     * BRACE_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.ServiceDefinition visitServiceDefinition(
            PureParser.ServiceDefinitionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        String pattern = null;
        String documentation = null;
        String functionBody = null;

        // Extract pattern
        if (ctx.servicePattern() != null && !ctx.servicePattern().isEmpty()) {
            var patternCtx = ctx.servicePattern().get(0);
            pattern = unquoteString(patternCtx.STRING().getText());
        }

        // Extract documentation
        if (ctx.serviceDocumentation() != null && !ctx.serviceDocumentation().isEmpty()) {
            var docCtx = ctx.serviceDocumentation().get(0);
            documentation = unquoteString(docCtx.STRING().getText());
        }

        // Extract function body from execution
        if (ctx.serviceExec() != null && !ctx.serviceExec().isEmpty()) {
            var execCtx = ctx.serviceExec().get(0);
            if (execCtx.serviceSingleExec() != null) {
                var singleExec = execCtx.serviceSingleExec();
                if (singleExec.serviceQuery() != null && !singleExec.serviceQuery().isEmpty()) {
                    var queryCtx = singleExec.serviceQuery().get(0);
                    // Get the combinedExpression text
                    functionBody = getOriginalText(queryCtx.combinedExpression());
                    // Strip leading '|' if present (lambda prefix)
                    if (functionBody != null && functionBody.startsWith("|")) {
                        functionBody = functionBody.substring(1).trim();
                    }
                }
            }
        }

        // Extract testSuites (reuse existing parsing for now)
        List<org.finos.legend.pure.dsl.definition.MappingDefinition.TestSuiteDefinition> testSuites = List.of();

        return org.finos.legend.pure.dsl.definition.ServiceDefinition.of(
                qualifiedName,
                pattern != null ? pattern : "/",
                functionBody != null ? functionBody : "",
                documentation,
                testSuites);
    }

    /**
     * Removes surrounding quotes from a string literal.
     */
    private String unquoteString(String s) {
        if (s == null || s.length() < 2)
            return s;
        if ((s.startsWith("'") && s.endsWith("'")) ||
                (s.startsWith("\"") && s.endsWith("\""))) {
            return s.substring(1, s.length() - 1);
        }
        return s;
    }

    /**
     * Extracts all ServiceDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.ServiceDefinition> extractServiceDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ServiceDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.serviceDefinition() != null) {
                result.add(builder.visitServiceDefinition(elemCtx.serviceDefinition()));
            }
        }

        return result;
    }

    /**
     * Extracts the first ServiceDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.ServiceDefinition> extractFirstServiceDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ServiceDefinition> defs = extractServiceDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Connection Parsing ====================

    /**
     * Visits a RelationalDatabaseConnection parse tree node and returns a
     * ConnectionDefinition.
     * 
     * Grammar rule:
     * relationalDatabaseConnection: RELATIONAL_DATABASE_CONNECTION qualifiedName
     * BRACE_OPEN (dbConnectionStore | dbConnectionType | dbConnectionSpec |
     * dbConnectionAuth | ...)* BRACE_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.ConnectionDefinition visitRelationalDatabaseConnection(
            PureParser.RelationalDatabaseConnectionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        String storeName = null;
        org.finos.legend.pure.dsl.definition.ConnectionDefinition.DatabaseType dbType = org.finos.legend.pure.dsl.definition.ConnectionDefinition.DatabaseType.DuckDB;
        org.finos.legend.pure.dsl.definition.ConnectionSpecification specification = new org.finos.legend.pure.dsl.definition.ConnectionSpecification.InMemory();
        org.finos.legend.pure.dsl.definition.AuthenticationSpec authentication = new org.finos.legend.pure.dsl.definition.AuthenticationSpec.NoAuth();

        // Extract store
        if (ctx.dbConnectionStore() != null && !ctx.dbConnectionStore().isEmpty()) {
            storeName = ctx.dbConnectionStore().get(0).qualifiedName().getText();
        }

        // Extract type
        if (ctx.dbConnectionType() != null && !ctx.dbConnectionType().isEmpty()) {
            String typeStr = ctx.dbConnectionType().get(0).identifier().getText();
            try {
                dbType = org.finos.legend.pure.dsl.definition.ConnectionDefinition.DatabaseType.valueOf(typeStr);
            } catch (IllegalArgumentException e) {
                // Unknown type - keep default DuckDB
            }
        }

        // Extract specification
        if (ctx.dbConnectionSpec() != null && !ctx.dbConnectionSpec().isEmpty()) {
            var specCtx = ctx.dbConnectionSpec().get(0);
            var specValue = specCtx.dbConnectionSpecValue();
            String specType = specValue.identifier().getText();
            String specBody = specValue.dbConnectionValueBody() != null
                    ? getOriginalText(specValue.dbConnectionValueBody())
                    : "";

            specification = switch (specType) {
                case "InMemory" -> new org.finos.legend.pure.dsl.definition.ConnectionSpecification.InMemory();
                case "LocalFile" -> {
                    String path = extractQuotedProperty(specBody, "path");
                    yield new org.finos.legend.pure.dsl.definition.ConnectionSpecification.LocalFile(path);
                }
                case "Static" -> {
                    String host = extractQuotedProperty(specBody, "host");
                    int port = extractIntProperty(specBody, "port", 0);
                    String database = extractQuotedProperty(specBody, "database");
                    yield new org.finos.legend.pure.dsl.definition.ConnectionSpecification.StaticDatasource(host, port,
                            database);
                }
                default -> new org.finos.legend.pure.dsl.definition.ConnectionSpecification.InMemory();
            };
        }

        // Extract authentication
        if (ctx.dbConnectionAuth() != null && !ctx.dbConnectionAuth().isEmpty()) {
            var authCtx = ctx.dbConnectionAuth().get(0);
            var authValue = authCtx.dbConnectionAuthValue();
            String authType = authValue.identifier().getText();
            String authBody = authValue.dbConnectionValueBody() != null
                    ? getOriginalText(authValue.dbConnectionValueBody())
                    : "";

            authentication = switch (authType) {
                case "NoAuth" -> new org.finos.legend.pure.dsl.definition.AuthenticationSpec.NoAuth();
                case "UsernamePassword" -> {
                    String username = extractQuotedProperty(authBody, "username");
                    String passwordVaultRef = extractQuotedProperty(authBody, "passwordVaultRef");
                    yield new org.finos.legend.pure.dsl.definition.AuthenticationSpec.UsernamePassword(username,
                            passwordVaultRef);
                }
                default -> new org.finos.legend.pure.dsl.definition.AuthenticationSpec.NoAuth();
            };
        }

        return new org.finos.legend.pure.dsl.definition.ConnectionDefinition(
                qualifiedName, storeName, dbType, specification, authentication);
    }

    /**
     * Extracts a quoted string property (e.g., "path: './file.db';") from a body
     * string.
     */
    private String extractQuotedProperty(String body, String propertyName) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern
                .compile(propertyName + "\\s*:\\s*'([^']*)'");
        java.util.regex.Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Extracts an integer property from a body string.
     */
    private int extractIntProperty(String body, String propertyName, int defaultValue) {
        java.util.regex.Pattern pattern = java.util.regex.Pattern
                .compile(propertyName + "\\s*:\\s*(\\d+)");
        java.util.regex.Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return defaultValue;
    }

    /**
     * Extracts all ConnectionDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.ConnectionDefinition> extractConnectionDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ConnectionDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.relationalDatabaseConnection() != null) {
                result.add(builder.visitRelationalDatabaseConnection(elemCtx.relationalDatabaseConnection()));
            }
        }

        return result;
    }

    /**
     * Extracts the first ConnectionDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.ConnectionDefinition> extractFirstConnectionDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.ConnectionDefinition> defs = extractConnectionDefinitions(
                definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Runtime Parsing ====================

    /**
     * Visits a Runtime parse tree node and returns a RuntimeDefinition.
     * 
     * Grammar rule:
     * runtime: RUNTIME qualifiedName BRACE_OPEN (runtimeMappings |
     * runtimeConnections | connectionStoresList)* BRACE_CLOSE
     */
    public org.finos.legend.pure.dsl.definition.RuntimeDefinition visitRuntime(
            PureParser.RuntimeContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        List<String> mappings = new ArrayList<>();
        java.util.Map<String, String> connectionBindings = new java.util.HashMap<>();

        // Extract mappings
        for (var mappingsCtx : ctx.runtimeMappings()) {
            for (var qn : mappingsCtx.qualifiedName()) {
                mappings.add(qn.getText());
            }
        }

        // Extract connections - legend-engine format: connections: [ store: [ id:
        // connRef ] ]
        for (var connectionsCtx : ctx.runtimeConnections()) {
            for (var storeConnsCtx : connectionsCtx.storeConnections()) {
                String storeName = storeConnsCtx.qualifiedName().getText();
                // Get the first identified connection for this store
                for (var identConnCtx : storeConnsCtx.identifiedConnection()) {
                    if (identConnCtx.packageableElementPointer() != null) {
                        String connectionRef = identConnCtx.packageableElementPointer().qualifiedName().getText();
                        connectionBindings.put(storeName, connectionRef);
                        break; // Take first connection for simplified model
                    }
                }
            }
        }

        return new org.finos.legend.pure.dsl.definition.RuntimeDefinition(
                qualifiedName, mappings, connectionBindings);
    }

    /**
     * Extracts all RuntimeDefinitions from a parsed definition context.
     */
    public static List<org.finos.legend.pure.dsl.definition.RuntimeDefinition> extractRuntimeDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.RuntimeDefinition> result = new ArrayList<>();
        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.runtime() != null) {
                result.add(builder.visitRuntime(elemCtx.runtime()));
            }
        }

        return result;
    }

    /**
     * Extracts the first RuntimeDefinition from a parsed definition context.
     */
    public static Optional<org.finos.legend.pure.dsl.definition.RuntimeDefinition> extractFirstRuntimeDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.RuntimeDefinition> defs = extractRuntimeDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Combined Extraction ====================

    /**
     * Extracts ALL definitions from a parsed definition context.
     * Iterates through all elementDefinition nodes and builds the appropriate
     * PureDefinition based on which grammar rule matched.
     *
     * @param definitionCtx The definition context from parsing
     * @return List of all PureDefinitions found
     */
    public static List<org.finos.legend.pure.dsl.definition.PureDefinition> extractAllDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<org.finos.legend.pure.dsl.definition.PureDefinition> result = new ArrayList<>();
        if (definitionCtx == null) {
            return result;
        }

        PureDefinitionBuilder builder = new PureDefinitionBuilder();

        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.classDefinition() != null) {
                result.add(builder.visitClassDefinition(elemCtx.classDefinition()));
            } else if (elemCtx.enumDefinition() != null) {
                result.add(builder.visitEnumDefinition(elemCtx.enumDefinition()));
            } else if (elemCtx.profile() != null) {
                result.add(builder.visitProfile(elemCtx.profile()));
            } else if (elemCtx.association() != null) {
                result.add(builder.visitAssociation(elemCtx.association()));
            } else if (elemCtx.functionDefinition() != null) {
                result.add(builder.visitFunctionDefinition(elemCtx.functionDefinition()));
            } else if (elemCtx.database() != null) {
                result.add(builder.visitDatabase(elemCtx.database()));
            } else if (elemCtx.mapping() != null) {
                result.add(builder.visitMapping(elemCtx.mapping()));
            } else if (elemCtx.serviceDefinition() != null) {
                result.add(builder.visitServiceDefinition(elemCtx.serviceDefinition()));
            } else if (elemCtx.relationalDatabaseConnection() != null) {
                result.add(builder.visitRelationalDatabaseConnection(elemCtx.relationalDatabaseConnection()));
            } else if (elemCtx.runtime() != null) {
                result.add(builder.visitRuntime(elemCtx.runtime()));
            }
            // singleConnectionRuntime, nativeFunction, instance, measureDefinition ignored
            // for now
        }

        return result;
    }
}
