package com.gs.legend.antlr;

import com.gs.legend.compiler.Mult;
import com.gs.legend.compiler.NativeFunctionDef;
import com.gs.legend.compiler.PType;
import com.gs.legend.model.def.ClassDefinition;
import com.gs.legend.model.def.ClassDefinition.ConstraintDefinition;
import com.gs.legend.model.def.ClassDefinition.DerivedPropertyDefinition;
import com.gs.legend.model.def.ClassDefinition.PropertyDefinition;
import com.gs.legend.model.def.ImportScope;
import com.gs.legend.model.def.StereotypeApplication;
import com.gs.legend.model.def.TaggedValue;
import com.gs.legend.model.m3.Multiplicity;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * ANTLR visitor that builds ClassDefinition objects from the parse tree.
 * 
 * This visitor processes the class definition grammar rule, extracting:
 * - Qualified class name
 * - Superclass list (from extends clause)
 * - Properties (simple and derived)
 * - Constraints, stereotypes, and tagged values
 */
public class PackageableElementBuilder extends PureParserBaseVisitor<Object> {

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

        // Extract property-level stereotypes
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        if (ctx.stereotypes() != null) {
            for (PureParser.StereotypeContext stereotypeCtx : ctx.stereotypes().stereotype()) {
                StereotypeApplication stereotype = visitStereotype(stereotypeCtx);
                if (stereotype != null) {
                    stereotypes.add(stereotype);
                }
            }
        }

        // Extract property-level tagged values
        List<TaggedValue> taggedValues = new ArrayList<>();
        if (ctx.taggedValues() != null) {
            for (PureParser.TaggedValueContext tvCtx : ctx.taggedValues().taggedValue()) {
                TaggedValue tv = visitTaggedValue(tvCtx);
                if (tv != null) {
                    taggedValues.add(tv);
                }
            }
        }

        return new PropertyDefinition(name, type, lowerBound, upperBound, stereotypes, taggedValues);
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

        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public com.gs.legend.model.def.EnumDefinition visitEnumDefinition(
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

        return com.gs.legend.model.def.EnumDefinition.of(qualifiedName, values);
    }

    /**
     * Extracts all EnumDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.EnumDefinition> extractEnumDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.EnumDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.EnumDefinition> extractFirstEnumDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.EnumDefinition> defs = extractEnumDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.ProfileDefinition visitProfile(PureParser.ProfileContext ctx) {
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

        return new com.gs.legend.model.def.ProfileDefinition(qualifiedName, stereotypes, tags);
    }

    /**
     * Extracts all ProfileDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.ProfileDefinition> extractProfileDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ProfileDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.ProfileDefinition> extractFirstProfileDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ProfileDefinition> defs = extractProfileDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.AssociationDefinition visitAssociation(
            PureParser.AssociationContext ctx) {
        // Extract qualified name
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract properties from associationBody (same structure as class properties)
        List<com.gs.legend.model.def.AssociationDefinition.AssociationEndDefinition> ends = new ArrayList<>();

        if (ctx.associationBody() != null && ctx.associationBody().properties() != null) {
            for (PureParser.PropertyContext propCtx : ctx.associationBody().properties().property()) {
                String propName = propCtx.identifier().getText();
                String propType = propCtx.propertyReturnType().type().getText();

                // Parse multiplicity
                String multText = propCtx.propertyReturnType().multiplicity().getText();
                int[] bounds = parseMultiplicity(multText);

                ends.add(new com.gs.legend.model.def.AssociationDefinition.AssociationEndDefinition(
                        propName, propType, bounds[0], bounds[1] == -1 ? null : bounds[1]));
            }
        }

        if (ends.size() != 2) {
            throw new com.gs.legend.parser.PureParseException(
                    "Association must have exactly 2 properties, found: " + ends.size());
        }

        return new com.gs.legend.model.def.AssociationDefinition(qualifiedName, ends.get(0), ends.get(1));
    }

    /**
     * Extracts all AssociationDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.AssociationDefinition> extractAssociationDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.AssociationDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.AssociationDefinition> extractFirstAssociationDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.AssociationDefinition> defs = extractAssociationDefinitions(
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
    public com.gs.legend.model.def.FunctionDefinition visitFunctionDefinition(
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
        List<com.gs.legend.model.def.FunctionDefinition.ParameterDefinition> parameters = new ArrayList<>();
        PureParser.FunctionTypeSignatureContext sigCtx = ctx.functionTypeSignature();
        for (PureParser.FunctionVariableExpressionContext paramCtx : sigCtx.functionVariableExpression()) {
            String paramName = paramCtx.identifier().getText();
            String paramType = paramCtx.type().getText();
            String multText = paramCtx.multiplicity().getText();
            int[] bounds = parseMultiplicity(multText);
            // Parse structured function type for Function<{...}> params
            PType parsedType = visitPureType(paramCtx.type());
            PType.FunctionType fnType = (parsedType instanceof PType.FunctionType ft) ? ft
                    : (parsedType instanceof PType.Parameterized p
                            && "Function".equals(p.rawType())
                            && !p.typeArgs().isEmpty()
                            && p.typeArgs().get(0) instanceof PType.FunctionType ft2) ? ft2
                    : null;
            parameters.add(new com.gs.legend.model.def.FunctionDefinition.ParameterDefinition(
                    paramName, paramType, bounds[0], bounds[1] == -1 ? null : bounds[1], fnType));
        }

        // Extract return type and multiplicity
        String returnType = sigCtx.type().getText();
        String returnMultText = sigCtx.multiplicity().getText();
        int[] returnBounds = parseMultiplicity(returnMultText);

        // Extract function body from codeBlock
        String body = getOriginalText(ctx.codeBlock());

        return new com.gs.legend.model.def.FunctionDefinition(
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
    public static List<com.gs.legend.model.def.FunctionDefinition> extractFunctionDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.FunctionDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.FunctionDefinition> extractFirstFunctionDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.FunctionDefinition> defs = extractFunctionDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.DatabaseDefinition visitDatabase(PureParser.DatabaseContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        // Extract includes
        List<String> includes = new ArrayList<>();
        for (PureParser.IncludeDatabaseContext incCtx : ctx.includeDatabase()) {
            includes.add(incCtx.qualifiedName().getText());
        }

        // Extract top-level tables
        List<com.gs.legend.model.def.DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        for (PureParser.DbTableContext tableCtx : ctx.dbTable()) {
            tables.add(extractDbTable(tableCtx));
        }

        // Extract joins
        List<com.gs.legend.model.def.DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();
        for (PureParser.DbJoinContext joinCtx : ctx.dbJoin()) {
            joins.add(extractDbJoin(joinCtx));
        }

        // Extract filters
        List<com.gs.legend.model.def.DatabaseDefinition.FilterDefinition> filters = new ArrayList<>();
        for (PureParser.DbFilterContext filterCtx : ctx.dbFilter()) {
            filters.add(extractDbFilter(filterCtx));
        }

        // Extract multi-grain filters
        List<com.gs.legend.model.def.DatabaseDefinition.FilterDefinition> multiGrainFilters = new ArrayList<>();
        for (PureParser.DbMultiGrainFilterContext mgfCtx : ctx.dbMultiGrainFilter()) {
            String name = mgfCtx.identifier().getText();
            var condition = buildDbOperation(mgfCtx.dbOperation());
            multiGrainFilters.add(new com.gs.legend.model.def.DatabaseDefinition.FilterDefinition(name, condition));
        }

        // Extract schemas (with their tables and views)
        List<com.gs.legend.model.def.DatabaseDefinition.SchemaDefinition> schemas = new ArrayList<>();
        for (PureParser.DbSchemaContext schemaCtx : ctx.dbSchema()) {
            schemas.add(extractDbSchema(schemaCtx));
            // Also add schema tables to top-level list for backward compat
            for (PureParser.DbTableContext tableCtx : schemaCtx.dbTable()) {
                tables.add(extractDbTable(tableCtx));
            }
        }

        // Extract top-level views
        List<com.gs.legend.model.def.DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        for (PureParser.DbViewContext viewCtx : ctx.dbView()) {
            views.add(extractDbView(viewCtx));
        }
        // Also collect views from schemas
        for (PureParser.DbSchemaContext schemaCtx : ctx.dbSchema()) {
            for (PureParser.DbViewContext viewCtx : schemaCtx.dbView()) {
                views.add(extractDbView(viewCtx));
            }
        }

        return new com.gs.legend.model.def.DatabaseDefinition(
                qualifiedName, includes, schemas, tables, views, joins, filters, multiGrainFilters);
    }

    /**
     * Visits a table parse tree node.
     */
    private com.gs.legend.model.def.DatabaseDefinition.TableDefinition extractDbTable(
            PureParser.DbTableContext ctx) {
        String tableName = ctx.relationalIdentifier().getText();

        List<com.gs.legend.model.def.DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();
        for (PureParser.ColumnDefinitionContext colCtx : ctx.columnDefinition()) {
            columns.add(extractColumnDefinition(colCtx));
        }

        return new com.gs.legend.model.def.DatabaseDefinition.TableDefinition(tableName, columns);
    }

    /**
     * Visits a column definition parse tree node.
     */
    private com.gs.legend.model.def.DatabaseDefinition.ColumnDefinition extractColumnDefinition(
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

        return new com.gs.legend.model.def.DatabaseDefinition.ColumnDefinition(
                columnName, dataType, primaryKey, notNull);
    }

    /**
     * Visits a filter definition: Filter identifier PAREN_OPEN dbOperation PAREN_CLOSE
     */
    private com.gs.legend.model.def.DatabaseDefinition.FilterDefinition extractDbFilter(
            PureParser.DbFilterContext ctx) {
        String filterName = ctx.identifier().getText();
        var condition = buildDbOperation(ctx.dbOperation());
        return new com.gs.legend.model.def.DatabaseDefinition.FilterDefinition(filterName, condition);
    }

    /**
     * Visits a schema definition:
     * Schema schemaIdentifier PAREN_OPEN (dbTable | dbView | dbTabularFunction)* PAREN_CLOSE
     */
    private com.gs.legend.model.def.DatabaseDefinition.SchemaDefinition extractDbSchema(
            PureParser.DbSchemaContext ctx) {
        String schemaName = ctx.schemaIdentifier().getText();

        List<com.gs.legend.model.def.DatabaseDefinition.TableDefinition> tables = new ArrayList<>();
        for (PureParser.DbTableContext tableCtx : ctx.dbTable()) {
            tables.add(extractDbTable(tableCtx));
        }

        List<com.gs.legend.model.def.DatabaseDefinition.ViewDefinition> views = new ArrayList<>();
        for (PureParser.DbViewContext viewCtx : ctx.dbView()) {
            views.add(extractDbView(viewCtx));
        }

        return new com.gs.legend.model.def.DatabaseDefinition.SchemaDefinition(schemaName, tables, views);
    }

    /**
     * Visits a view definition:
     * View relationalIdentifier PAREN_OPEN (viewFilterMapping)? (viewGroupBy)? (DISTINCT_CMD)?
     *     (viewColumnMapping (COMMA viewColumnMapping)*)? PAREN_CLOSE
     */
    private com.gs.legend.model.def.DatabaseDefinition.ViewDefinition extractDbView(
            PureParser.DbViewContext ctx) {
        String viewName = ctx.relationalIdentifier().getText();

        // Optional filter mapping — stored as raw text for now (complex: join chains + filter ref)
        com.gs.legend.model.def.RelationalOperation filterMapping = null;
        if (ctx.viewFilterMapping() != null) {
            // viewFilterMapping is a reference to a named filter, not an inline expression.
            // Store as a Literal containing the filter identifier for now.
            String filterRef = ctx.viewFilterMapping().identifier().getText();
            filterMapping = com.gs.legend.model.def.RelationalOperation.Literal.string("~filter:" + filterRef);
        }

        // Optional groupBy
        List<com.gs.legend.model.def.RelationalOperation> groupBy = new ArrayList<>();
        if (ctx.viewGroupBy() != null) {
            for (PureParser.DbOperationContext opCtx : ctx.viewGroupBy().dbOperation()) {
                groupBy.add(buildDbOperation(opCtx));
            }
        }

        // Distinct flag
        boolean distinct = ctx.DISTINCT_CMD() != null;

        // Column mappings
        List<com.gs.legend.model.def.DatabaseDefinition.ViewDefinition.ViewColumnMapping> columnMappings = new ArrayList<>();
        for (PureParser.ViewColumnMappingContext colCtx : ctx.viewColumnMapping()) {
            String colName = colCtx.identifier(0).getText();
            // Optional target set ID: [id]
            String targetSetId = colCtx.identifier().size() > 1 ? colCtx.identifier(1).getText() : null;
            var expression = buildDbOperation(colCtx.dbOperation());
            boolean pk = colCtx.PRIMARY_KEY() != null;
            columnMappings.add(new com.gs.legend.model.def.DatabaseDefinition.ViewDefinition.ViewColumnMapping(
                    colName, targetSetId, expression, pk));
        }

        return new com.gs.legend.model.def.DatabaseDefinition.ViewDefinition(
                viewName, filterMapping, groupBy, distinct, columnMappings);
    }

    /**
     * Visits a join definition parse tree node.
     * Grammar: dbJoin: JOIN identifier PAREN_OPEN dbOperation PAREN_CLOSE
     */
    private com.gs.legend.model.def.DatabaseDefinition.JoinDefinition extractDbJoin(
            PureParser.DbJoinContext ctx) {
        String joinName = ctx.identifier().getText();
        com.gs.legend.model.def.RelationalOperation operation = buildDbOperation(ctx.dbOperation());
        return new com.gs.legend.model.def.DatabaseDefinition.JoinDefinition(joinName, operation);
    }

    // ==================== dbOperation AST Walker ====================

    /**
     * Visits a dbOperation: dbBooleanOperation | dbJoinOperation
     */
    private com.gs.legend.model.def.RelationalOperation buildDbOperation(
            PureParser.DbOperationContext ctx) {
        if (ctx.dbJoinOperation() != null) {
            return buildDbJoinOperation(ctx.dbJoinOperation());
        }
        return buildDbBooleanOperation(ctx.dbBooleanOperation());
    }

    /**
     * Visits a dbBooleanOperation: dbAtomicOperation dbBooleanOperationRight?
     */
    private com.gs.legend.model.def.RelationalOperation buildDbBooleanOperation(
            PureParser.DbBooleanOperationContext ctx) {
        var left = buildDbAtomicOperation(ctx.dbAtomicOperation());
        if (ctx.dbBooleanOperationRight() != null) {
            String op = ctx.dbBooleanOperationRight().dbBooleanOperator().getText(); // "and" or "or"
            var right = buildDbOperation(ctx.dbBooleanOperationRight().dbOperation());
            return new com.gs.legend.model.def.RelationalOperation.BooleanOp(left, op, right);
        }
        return left;
    }

    /**
     * Visits a dbAtomicOperation: (group | func | column | join | constant) dbAtomicOperationRight?
     */
    private com.gs.legend.model.def.RelationalOperation buildDbAtomicOperation(
            PureParser.DbAtomicOperationContext ctx) {
        com.gs.legend.model.def.RelationalOperation expr;

        if (ctx.dbGroupOperation() != null) {
            expr = new com.gs.legend.model.def.RelationalOperation.Group(
                    buildDbOperation(ctx.dbGroupOperation().dbOperation()));
        } else if (ctx.dbFunctionOperation() != null) {
            String dbName = ctx.databasePointer() != null
                    ? ctx.databasePointer().qualifiedName().getText() : null;
            expr = buildDbFunctionOperation(ctx.dbFunctionOperation(), dbName);
        } else if (ctx.dbColumnOperation() != null) {
            expr = buildDbColumnOperation(ctx.dbColumnOperation());
        } else if (ctx.dbJoinOperation() != null) {
            expr = buildDbJoinOperation(ctx.dbJoinOperation());
        } else if (ctx.dbConstant() != null) {
            expr = buildDbConstant(ctx.dbConstant());
        } else {
            throw new IllegalStateException("Unknown dbAtomicOperation alternative: " + ctx.getText());
        }

        // Handle optional right side: comparison or self-operator
        if (ctx.dbAtomicOperationRight() != null) {
            var rightCtx = ctx.dbAtomicOperationRight();
            if (rightCtx.dbAtomicSelfOperator() != null) {
                String selfOp = rightCtx.dbAtomicSelfOperator().getText(); // "is null" or "is not null"
                if (selfOp.contains("not")) {
                    expr = new com.gs.legend.model.def.RelationalOperation.IsNotNull(expr);
                } else {
                    expr = new com.gs.legend.model.def.RelationalOperation.IsNull(expr);
                }
            } else {
                String op = rightCtx.dbAtomicOperator().getText(); // =, !=, <>, >, <, >=, <=
                var rightExpr = buildDbAtomicOperation(rightCtx.dbAtomicOperation());
                expr = new com.gs.legend.model.def.RelationalOperation.Comparison(expr, op, rightExpr);
            }
        }

        return expr;
    }

    /**
     * Visits a dbConstant: STRING | INTEGER | FLOAT
     */
    private com.gs.legend.model.def.RelationalOperation.Literal buildDbConstant(
            PureParser.DbConstantContext ctx) {
        if (ctx.STRING() != null) {
            return com.gs.legend.model.def.RelationalOperation.Literal.string(
                    unquoteString(ctx.STRING().getText()));
        } else if (ctx.INTEGER() != null) {
            return com.gs.legend.model.def.RelationalOperation.Literal.integer(
                    Long.parseLong(ctx.INTEGER().getText()));
        } else {
            return com.gs.legend.model.def.RelationalOperation.Literal.decimal(
                    Double.parseDouble(ctx.FLOAT().getText()));
        }
    }

    /**
     * Visits a dbFunctionOperation: identifier ( args... )
     */
    private com.gs.legend.model.def.RelationalOperation.FunctionCall buildDbFunctionOperation(
            PureParser.DbFunctionOperationContext ctx, String databaseName) {
        String funcName = ctx.identifier().getText();
        List<com.gs.legend.model.def.RelationalOperation> args = new ArrayList<>();
        for (var argCtx : ctx.dbFunctionOperationArgument()) {
            if (argCtx.dbFunctionOperationArgumentArray() != null) {
                args.add(buildDbFunctionOperationArray(argCtx.dbFunctionOperationArgumentArray()));
            } else {
                args.add(buildDbOperation(argCtx.dbOperation()));
            }
        }
        return new com.gs.legend.model.def.RelationalOperation.FunctionCall(funcName, args);
    }

    /**
     * Visits a dbFunctionOperationArgumentArray: [ args... ]
     */
    private com.gs.legend.model.def.RelationalOperation.ArrayLiteral buildDbFunctionOperationArray(
            PureParser.DbFunctionOperationArgumentArrayContext ctx) {
        List<com.gs.legend.model.def.RelationalOperation> elements = new ArrayList<>();
        for (var argCtx : ctx.dbFunctionOperationArgument()) {
            if (argCtx.dbFunctionOperationArgumentArray() != null) {
                elements.add(buildDbFunctionOperationArray(argCtx.dbFunctionOperationArgumentArray()));
            } else {
                elements.add(buildDbOperation(argCtx.dbOperation()));
            }
        }
        return new com.gs.legend.model.def.RelationalOperation.ArrayLiteral(elements);
    }

    /**
     * Visits a dbColumnOperation: databasePointer? dbTableAliasColumnOperation
     */
    private com.gs.legend.model.def.RelationalOperation buildDbColumnOperation(
            PureParser.DbColumnOperationContext ctx) {
        String dbName = ctx.databasePointer() != null
                ? ctx.databasePointer().qualifiedName().getText() : null;
        return buildDbTableAliasColumnOperation(ctx.dbTableAliasColumnOperation(), dbName);
    }

    /**
     * Visits dbTableAliasColumnOperation: target ref or scope info ref
     */
    private com.gs.legend.model.def.RelationalOperation buildDbTableAliasColumnOperation(
            PureParser.DbTableAliasColumnOperationContext ctx, String dbName) {
        if (ctx.dbTableAliasColumnOperationWithTarget() != null) {
            String col = extractRelationalIdentifier(
                    ctx.dbTableAliasColumnOperationWithTarget().relationalIdentifier());
            return new com.gs.legend.model.def.RelationalOperation.TargetColumnRef(col);
        }
        var scopeCtx = ctx.dbTableAliasColumnOperationWithScopeInfo();
        String firstId = extractRelationalIdentifier(scopeCtx.relationalIdentifier());
        if (scopeCtx.scopeInfo() != null) {
            var scopeIds = scopeCtx.scopeInfo().relationalIdentifier();
            if (scopeIds.size() == 1) {
                // TABLE.COLUMN
                return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(
                        dbName, firstId, extractRelationalIdentifier(scopeIds.get(0)));
            } else {
                // SCHEMA.TABLE.COLUMN
                String table = firstId + "." + extractRelationalIdentifier(scopeIds.get(0));
                String column = extractRelationalIdentifier(scopeIds.get(1));
                return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, table, column);
            }
        }
        // Bare identifier (no table prefix) — used in function args with implicit context
        return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, firstId, firstId);
    }

    /**
     * Visits a dbJoinOperation: databasePointer? joinSequence (PIPE terminal)?
     */
    private com.gs.legend.model.def.RelationalOperation.JoinNavigation buildDbJoinOperation(
            PureParser.DbJoinOperationContext ctx) {
        String dbName = ctx.databasePointer() != null
                ? ctx.databasePointer().qualifiedName().getText() : null;

        var joinSeqCtx = ctx.joinSequence();
        List<com.gs.legend.model.def.JoinChainElement> chain = new ArrayList<>();

        // First join: optional (joinType) before @joinName
        String firstJoinType = null;
        if (joinSeqCtx.PAREN_OPEN() != null && joinSeqCtx.identifier() != null) {
            firstJoinType = joinSeqCtx.identifier().getText();
        }
        String firstJoinName = joinSeqCtx.joinPointer().identifier().getText();
        chain.add(new com.gs.legend.model.def.JoinChainElement(firstJoinName, firstJoinType, null, false));

        // Subsequent joins: > (joinType)? databasePointer? @joinName
        for (var fullCtx : joinSeqCtx.joinPointerFull()) {
            String joinType = null;
            if (fullCtx.PAREN_OPEN() != null && fullCtx.identifier() != null) {
                joinType = fullCtx.identifier().getText();
            }
            String hopDb = fullCtx.databasePointer() != null
                    ? fullCtx.databasePointer().qualifiedName().getText() : null;
            String joinName = fullCtx.joinPointer().identifier().getText();
            chain.add(new com.gs.legend.model.def.JoinChainElement(joinName, joinType, hopDb, false));
        }

        // Optional terminal after PIPE
        com.gs.legend.model.def.RelationalOperation terminal = null;
        if (ctx.PIPE() != null) {
            if (ctx.dbBooleanOperation() != null) {
                terminal = buildDbBooleanOperation(ctx.dbBooleanOperation());
            } else if (ctx.dbTableAliasColumnOperation() != null) {
                terminal = buildDbTableAliasColumnOperation(ctx.dbTableAliasColumnOperation(), null);
            }
        }

        return new com.gs.legend.model.def.RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    /**
     * Extracts text from a relationalIdentifier: identifier | QUOTED_STRING
     */
    private String extractRelationalIdentifier(PureParser.RelationalIdentifierContext ctx) {
        if (ctx.QUOTED_STRING() != null) {
            String text = ctx.QUOTED_STRING().getText();
            // Strip surrounding quotes
            if (text.length() >= 2) {
                return text.substring(1, text.length() - 1);
            }
            return text;
        }
        return ctx.identifier().getText();
    }

    /**
     * Extracts all DatabaseDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.DatabaseDefinition> extractDatabaseDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.DatabaseDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.DatabaseDefinition> extractFirstDatabaseDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.DatabaseDefinition> defs = extractDatabaseDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.MappingDefinition visitMapping(
            PureParser.MappingContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        // Process includes
        List<com.gs.legend.model.def.MappingInclude> includes = new ArrayList<>();
        for (PureParser.IncludeMappingContext incCtx : ctx.includeMapping()) {
            includes.add(extractMappingInclude(incCtx));
        }

        // Process class mappings
        List<com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition> classMappings = new ArrayList<>();
        for (PureParser.ClassMappingElementContext classMappingCtx : ctx.classMappingElement()) {
            classMappings.add(visitClassMappingElement(classMappingCtx));
        }

        // Process association mappings
        List<com.gs.legend.model.def.AssociationMappingDefinition> associationMappings = new ArrayList<>();
        for (PureParser.AssociationMappingElementContext assocCtx : ctx.associationMappingElement()) {
            associationMappings.add(extractAssociationMapping(assocCtx));
        }

        // Process enumeration mappings
        List<com.gs.legend.model.def.MappingDefinition.EnumerationMappingDefinition> enumerationMappings = new ArrayList<>();
        for (PureParser.EnumerationMappingElementContext enumMappingCtx : ctx.enumerationMappingElement()) {
            enumerationMappings.add(visitEnumerationMappingElement(enumMappingCtx));
        }

        // Process testSuites
        List<com.gs.legend.model.def.MappingDefinition.TestSuiteDefinition> testSuites = new ArrayList<>();
        if (ctx.mappingTestableDefinition() != null) {
            for (PureParser.MappingTestSuiteContext suiteCtx : ctx.mappingTestableDefinition().mappingTestSuite()) {
                testSuites.add(visitMappingTestSuite(suiteCtx));
            }
        }

        return new com.gs.legend.model.def.MappingDefinition(
                qualifiedName, includes, classMappings, associationMappings, enumerationMappings, testSuites);
    }

    /**
     * Visits an enumeration mapping element.
     * 
     * Grammar: qualifiedName (BRACKET_OPEN mappingElementId BRACKET_CLOSE)? COLON
     * ENUMERATION_MAPPING (mappingElementId)?
     * BRACE_OPEN (enumValueMapping (COMMA enumValueMapping)*)? BRACE_CLOSE
     */
    public com.gs.legend.model.def.MappingDefinition.EnumerationMappingDefinition visitEnumerationMappingElement(
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

        return new com.gs.legend.model.def.MappingDefinition.EnumerationMappingDefinition(
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

    // ==================== Include + Association Mapping Extraction ====================

    /**
     * Extracts a mapping include.
     * Grammar: includeMapping: INCLUDE qualifiedName (BRACKET_OPEN (storeSubPath (COMMA storeSubPath)*)? BRACKET_CLOSE)?
     */
    private com.gs.legend.model.def.MappingInclude extractMappingInclude(
            PureParser.IncludeMappingContext ctx) {
        String includedPath = ctx.qualifiedName().getText();
        List<com.gs.legend.model.def.MappingInclude.StoreSubstitution> subs = new ArrayList<>();
        for (PureParser.StoreSubPathContext subCtx : ctx.storeSubPath()) {
            String source = subCtx.sourceStore().qualifiedName().getText();
            String target = subCtx.targetStore().qualifiedName().getText();
            subs.add(new com.gs.legend.model.def.MappingInclude.StoreSubstitution(source, target));
        }
        return new com.gs.legend.model.def.MappingInclude(includedPath, subs);
    }

    /**
     * Visits an association mapping element.
     * Grammar: associationMappingElement: qualifiedName COLON ASSOCIATION_MAPPING
     *     PAREN_OPEN (associationPropertyMapping (COMMA associationPropertyMapping)*)? PAREN_CLOSE
     */
    private com.gs.legend.model.def.AssociationMappingDefinition extractAssociationMapping(
            PureParser.AssociationMappingElementContext ctx) {
        String associationName = ctx.qualifiedName().getText();

        List<com.gs.legend.model.def.AssociationMappingDefinition.AssociationPropertyMapping> properties = new ArrayList<>();
        for (PureParser.AssociationPropertyMappingContext propCtx : ctx.associationPropertyMapping()) {
            String propertyName = propCtx.identifier().getText();

            // Optional [source, target] set IDs
            String sourceSetId = null;
            String targetSetId = null;
            if (propCtx.sourceAndTargetMappingId() != null) {
                var ids = propCtx.sourceAndTargetMappingId().identifier();
                sourceSetId = ids.get(0).getText();
                if (ids.size() > 1) {
                    targetSetId = ids.get(1).getText();
                }
            }

            // Optional database pointer
            String dbName = propCtx.databasePointer() != null
                    ? propCtx.databasePointer().qualifiedName().getText() : null;

            // Join chain
            List<com.gs.legend.model.def.JoinChainElement> joinChain = new ArrayList<>();
            if (propCtx.mappingJoinSequence() != null) {
                joinChain = extractMappingJoinChain(propCtx.mappingJoinSequence());
                // Propagate database name to first element if not set
                if (dbName != null && !joinChain.isEmpty() && joinChain.get(0).databaseName() == null) {
                    var first = joinChain.get(0);
                    joinChain.set(0, new com.gs.legend.model.def.JoinChainElement(
                            first.joinName(), first.joinType(), dbName, first.strict()));
                }
            }

            properties.add(new com.gs.legend.model.def.AssociationMappingDefinition.AssociationPropertyMapping(
                    propertyName, sourceSetId, targetSetId, joinChain, null));
        }

        return new com.gs.legend.model.def.AssociationMappingDefinition(
                associationName, "Relational", properties);
    }

    /**
     * Visits a mapping test suite.
     */
    public com.gs.legend.model.def.MappingDefinition.TestSuiteDefinition visitMappingTestSuite(
            PureParser.MappingTestSuiteContext ctx) {
        String suiteName = ctx.identifier().getText();
        String functionBody = null;
        List<com.gs.legend.model.def.MappingDefinition.TestDefinition> tests = new ArrayList<>();

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

        return new com.gs.legend.model.def.MappingDefinition.TestSuiteDefinition(
                suiteName, functionBody, tests);
    }

    /**
     * Visits a mapping test content.
     */
    public com.gs.legend.model.def.MappingDefinition.TestDefinition visitMappingTestContent(
            PureParser.MappingTestContentContext ctx) {
        String testName = ctx.identifier().getText();
        String documentation = null;
        List<com.gs.legend.model.def.MappingDefinition.TestData> inputData = new ArrayList<>();
        List<com.gs.legend.model.def.MappingDefinition.TestAssertion> asserts = new ArrayList<>();

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

        return new com.gs.legend.model.def.MappingDefinition.TestDefinition(
                testName, documentation, inputData, asserts);
    }

    /**
     * Visits a mapping test data content.
     */
    public com.gs.legend.model.def.MappingDefinition.TestData visitMappingTestDataContent(
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
            return new com.gs.legend.model.def.MappingDefinition.TestData(
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

        return new com.gs.legend.model.def.MappingDefinition.TestData(
                storeName, contentType, data, false);
    }

    /**
     * Visits a mapping test assert.
     */
    public com.gs.legend.model.def.MappingDefinition.TestAssertion visitMappingTestAssert(
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

        return new com.gs.legend.model.def.MappingDefinition.TestAssertion(
                assertName, assertType, null, expectedData);
    }

    /**
     * Visits a class mapping element and returns a ClassMappingDefinition.
     */
    public com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition visitClassMappingElement(
            PureParser.ClassMappingElementContext ctx) {
        String className = ctx.qualifiedName().getText();

        // Extract isRoot (*), setId ([id]), extendsSetId (extends [id])
        boolean isRoot = ctx.STAR() != null;
        String setId = ctx.mappingElementId() != null ? ctx.mappingElementId().getText() : null;
        String extendsSetId = ctx.superClassMappingId() != null ? ctx.superClassMappingId().getText() : null;

        String mappingType = ctx.classMappingType().getText();

        if ("Pure".equals(mappingType)) {
            return visitPureM2MClassMappingBody(className, ctx.classMappingBody().pureM2MClassMappingBody());
        } else {
            return visitRelationalClassMappingBody(className, setId, isRoot, extendsSetId,
                    ctx.classMappingBody().relationalClassMappingBody());
        }
    }

    /**
     * Visits a relational class mapping body.
     */
    public com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition visitRelationalClassMappingBody(
            String className, String setId, boolean isRoot, String extendsSetId,
            PureParser.RelationalClassMappingBodyContext ctx) {

        // ~filter
        com.gs.legend.model.def.MappingDefinition.MappingFilter filter = null;
        if (ctx.mappingFilter() != null) {
            filter = extractMappingFilter(ctx.mappingFilter());
        }

        // ~distinct
        boolean distinct = ctx.mappingDistinct() != null;

        // ~groupBy
        List<com.gs.legend.model.def.RelationalOperation> groupBy = new ArrayList<>();
        if (ctx.mappingGroupBy() != null) {
            for (PureParser.MappingOperationContext opCtx : ctx.mappingGroupBy().mappingOperation()) {
                groupBy.add(buildMappingOperation(opCtx));
            }
        }

        // ~primaryKey
        List<com.gs.legend.model.def.RelationalOperation> primaryKey = new ArrayList<>();
        if (ctx.mappingPrimaryKey() != null) {
            for (PureParser.MappingOperationContext opCtx : ctx.mappingPrimaryKey().mappingOperation()) {
                primaryKey.add(buildMappingOperation(opCtx));
            }
        }

        // ~mainTable
        com.gs.legend.model.def.MappingDefinition.TableReference mainTable = null;
        if (ctx.mappingMainTable() != null) {
            mainTable = visitMappingMainTable(ctx.mappingMainTable());
        }

        // Property mappings
        List<com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition> propertyMappings = new ArrayList<>();
        for (PureParser.RelationalPropertyMappingContext propCtx : ctx.relationalPropertyMapping()) {
            com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition propMapping = visitRelationalPropertyMapping(
                    propCtx);
            if (propMapping != null) {
                propertyMappings.add(propMapping);
            }
        }

        return new com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition(
                className, "Relational", setId, isRoot, extendsSetId,
                mainTable, filter, distinct, groupBy, primaryKey,
                propertyMappings, null, null, null);
    }

    /**
     * Visits a mainTable clause.
     */
    public com.gs.legend.model.def.MappingDefinition.TableReference visitMappingMainTable(
            PureParser.MappingMainTableContext ctx) {
        String databaseName = ctx.databasePointer().qualifiedName().getText();
        String tableName = ctx.mappingTableRef().getText();
        return new com.gs.legend.model.def.MappingDefinition.TableReference(databaseName, tableName);
    }

    /**
     * Visits a relational property mapping.
     */
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitRelationalPropertyMapping(
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
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitStandardPropertyMapping(
            PureParser.StandardPropertyMappingContext ctx) {
        String propertyName = ctx.identifier().getText();
        return visitRelationalPropertyValue(propertyName, ctx.relationalPropertyValue());
    }

    /**
     * Visits a local mapping property.
     */
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitLocalMappingProperty(
            PureParser.LocalMappingPropertyContext ctx) {
        String propertyName = ctx.identifier().getText();
        return visitRelationalPropertyValue(propertyName, ctx.relationalPropertyValue());
    }

    /**
     * Visits a relational property value.
     */
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitRelationalPropertyValue(
            String propertyName, PureParser.RelationalPropertyValueContext ctx) {

        if (ctx.embeddedPropertyMapping() != null) {
            var embCtx = ctx.embeddedPropertyMapping();
            var subMappings = new java.util.ArrayList<com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition>();
            for (var rpm : embCtx.relationalPropertyMapping()) {
                var sub = visitRelationalPropertyMapping(rpm);
                if (sub != null) subMappings.add(sub);
            }
            // Check for Otherwise clause: Otherwise([setId]: [DB]@JoinName)
            if (embCtx.otherwiseEmbeddedPropertyMapping() != null) {
                var owCtx = embCtx.otherwiseEmbeddedPropertyMapping().otherwisePropertyMapping();
                String fallbackSetId = owCtx.identifier().getText();
                String dbName = owCtx.databasePointer() != null
                        ? owCtx.databasePointer().qualifiedName().getText() : null;
                var joinChain = extractMappingJoinChain(owCtx.mappingJoinSequence());
                var fallbackJoin = new com.gs.legend.model.def.PropertyMappingValue.JoinMapping(
                        dbName, joinChain, null);
                return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.otherwise(
                        propertyName, subMappings, fallbackSetId, fallbackJoin);
            }
            return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.embedded(
                    propertyName, subMappings);
        }
        if (ctx.inlineEmbeddedPropertyMapping() != null) {
            String targetSetId = ctx.inlineEmbeddedPropertyMapping().identifier().getText();
            return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.inline(
                    propertyName, targetSetId);
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
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitMappingOperation(
            String propertyName, PureParser.MappingOperationContext ctx, String enumMappingId) {

        PureParser.MappingAtomicOperationContext atomicCtx = ctx.mappingAtomicOperation();

        // Check for join operation: [DB]@JoinName or @JoinName (databasePointer is optional per grammar)
        if (atomicCtx.mappingJoinOperation() != null) {
            return visitMappingJoinOperation(propertyName, atomicCtx);
        }

        // Check for column operation: [DB] TABLE.COLUMN
        if (atomicCtx.mappingColumnOperation() != null) {
            return visitMappingColumnOperation(propertyName, atomicCtx.mappingColumnOperation(), enumMappingId);
        }

        // Check for function operation: funcName(args...)
        // Also handle boolean/group operations — buildMappingOperation covers the full tree
        if (atomicCtx.mappingFunctionOperation() != null
                || atomicCtx.mappingGroupOperation() != null
                || ctx.mappingOperationRight() != null
                || atomicCtx.mappingConstant() != null) {
            var expr = buildMappingOperation(ctx);
            return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.mappingExpression(
                    propertyName, expr);
        }

        // Fallback: store entire operation as expression
        String expression = getOriginalText(ctx);
        return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.expression(
                propertyName, expression, null);
    }

    /**
     * Visits a mapping column operation: [DB] TABLE.COLUMN
     */
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitMappingColumnOperation(
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
                return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.expression(
                        propertyName, expression, null);
            }

            com.gs.legend.model.def.MappingDefinition.ColumnReference colRefObj = new com.gs.legend.model.def.MappingDefinition.ColumnReference(
                    databaseName, tableName, columnName);

            if (enumMappingId != null) {
                return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition
                        .columnWithEnumMapping(
                                propertyName, colRefObj, enumMappingId);
            } else {
                return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.column(
                        propertyName, colRefObj);
            }
        }

        String expression = getOriginalText(ctx);
        return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.expression(
                propertyName, expression, null);
    }

    private String unquote(String id) {
        if (id.startsWith("\"") && id.endsWith("\"")) {
            return id.substring(1, id.length() - 1);
        }
        return id;
    }

    // ==================== Mapping Filter Extraction ====================

    /**
     * Extracts a ~filter clause from a relational class mapping.
     * Grammar: mappingFilter: FILTER_CMD databasePointer? (mappingJoinSequence PIPE databasePointer)? identifier
     */
    private com.gs.legend.model.def.MappingDefinition.MappingFilter extractMappingFilter(
            PureParser.MappingFilterContext ctx) {
        String filterName = ctx.identifier().getText();

        // Database name: from direct databasePointer, or from the one after PIPE
        String databaseName = null;
        if (ctx.databasePointer() != null && !ctx.databasePointer().isEmpty()) {
            // Use the last databasePointer (after PIPE if present, otherwise the direct one)
            databaseName = ctx.databasePointer(ctx.databasePointer().size() - 1).qualifiedName().getText();
        }

        // Optional join path before PIPE
        List<com.gs.legend.model.def.JoinChainElement> joinPath = new ArrayList<>();
        if (ctx.mappingJoinSequence() != null) {
            joinPath = extractMappingJoinChain(ctx.mappingJoinSequence());
        }

        return new com.gs.legend.model.def.MappingDefinition.MappingFilter(databaseName, joinPath, filterName);
    }

    /**
     * Extracts a join chain from a mappingJoinSequence.
     * Grammar: mappingJoinSequence: (PAREN_OPEN identifier PAREN_CLOSE)? mappingJoinPointer (GREATER_THAN mappingJoinPointerFull)*
     */
    private List<com.gs.legend.model.def.JoinChainElement> extractMappingJoinChain(
            PureParser.MappingJoinSequenceContext ctx) {
        List<com.gs.legend.model.def.JoinChainElement> chain = new ArrayList<>();

        // First join: optional (joinType) before @joinName
        String firstJoinType = null;
        if (ctx.PAREN_OPEN() != null && ctx.identifier() != null) {
            firstJoinType = ctx.identifier().getText();
        }
        String firstJoinName = ctx.mappingJoinPointer().identifier().getText();
        chain.add(new com.gs.legend.model.def.JoinChainElement(firstJoinName, firstJoinType, null, false));

        // Subsequent joins: > (joinType)? databasePointer? @joinName
        for (PureParser.MappingJoinPointerFullContext fullCtx : ctx.mappingJoinPointerFull()) {
            String joinType = null;
            if (fullCtx.PAREN_OPEN() != null && fullCtx.identifier() != null) {
                joinType = fullCtx.identifier().getText();
            }
            String hopDb = fullCtx.databasePointer() != null
                    ? fullCtx.databasePointer().qualifiedName().getText() : null;
            String joinName = fullCtx.mappingJoinPointer().identifier().getText();
            chain.add(new com.gs.legend.model.def.JoinChainElement(joinName, joinType, hopDb, false));
        }

        return chain;
    }

    // ==================== Mapping Operation AST Walker ====================

    /**
     * Builds a RelationalOperation from a mapping operation parse tree.
     * Grammar: mappingOperation: mappingAtomicOperation (mappingOperationRight)?
     */
    private com.gs.legend.model.def.RelationalOperation buildMappingOperation(
            PureParser.MappingOperationContext ctx) {
        var left = buildMappingAtomicOperation(ctx.mappingAtomicOperation());
        if (ctx.mappingOperationRight() != null) {
            String op = ctx.mappingOperationRight().mappingBooleanOperator().getText();
            var right = buildMappingOperation(ctx.mappingOperationRight().mappingOperation());
            return new com.gs.legend.model.def.RelationalOperation.BooleanOp(left, op, right);
        }
        return left;
    }

    /**
     * Builds a RelationalOperation from a mapping atomic operation.
     */
    private com.gs.legend.model.def.RelationalOperation buildMappingAtomicOperation(
            PureParser.MappingAtomicOperationContext ctx) {
        com.gs.legend.model.def.RelationalOperation expr;

        if (ctx.mappingGroupOperation() != null) {
            expr = new com.gs.legend.model.def.RelationalOperation.Group(
                    buildMappingOperation(ctx.mappingGroupOperation().mappingOperation()));
        } else if (ctx.mappingFunctionOperation() != null) {
            String dbName = ctx.databasePointer() != null
                    ? ctx.databasePointer().qualifiedName().getText() : null;
            expr = buildMappingFunctionOperation(ctx.mappingFunctionOperation(), dbName);
        } else if (ctx.mappingColumnOperation() != null) {
            expr = buildMappingColumnOperation(ctx.mappingColumnOperation());
        } else if (ctx.mappingJoinOperation() != null) {
            String dbName = ctx.databasePointer() != null
                    ? ctx.databasePointer().qualifiedName().getText() : null;
            expr = buildMappingJoinOp(ctx.mappingJoinOperation(), dbName);
        } else if (ctx.mappingConstant() != null) {
            expr = buildMappingConstant(ctx.mappingConstant());
        } else {
            throw new IllegalStateException("Unknown mappingAtomicOperation alternative: " + ctx.getText());
        }

        return expr;
    }

    private com.gs.legend.model.def.RelationalOperation buildMappingColumnOperation(
            PureParser.MappingColumnOperationContext ctx) {
        String dbName = ctx.databasePointer() != null
                ? ctx.databasePointer().qualifiedName().getText() : null;

        PureParser.MappingTableColumnRefContext colRef = ctx.mappingTableColumnRef();
        String firstId = colRef.relationalIdentifier().getText();

        if (colRef.mappingScopeInfo() != null) {
            var scopeIds = colRef.mappingScopeInfo().relationalIdentifier();
            if (scopeIds.size() == 1) {
                // TABLE.COLUMN
                String column = scopeIds.get(0).getText();
                var result = com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, firstId, column);
                return wrapWithMappingRightSide(result, ctx.mappingAtomicOperationRight());
            } else {
                // SCHEMA.TABLE.COLUMN
                String table = firstId + "." + scopeIds.get(0).getText();
                String column = scopeIds.get(1).getText();
                var result = com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, table, column);
                return wrapWithMappingRightSide(result, ctx.mappingAtomicOperationRight());
            }
        }

        // Bare quoted string without scope → string literal (grammar ambiguity:
        // relationalIdentifier matches QUOTED_STRING, which shadows mappingConstant)
        if (dbName == null && colRef.mappingScopeInfo() == null
                && ctx.mappingAtomicOperationRight() == null
                && firstId.startsWith("'") && firstId.endsWith("'")) {
            return com.gs.legend.model.def.RelationalOperation.Literal.string(
                    firstId.substring(1, firstId.length() - 1));
        }

        // Bare identifier
        var result = com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, firstId, firstId);
        return wrapWithMappingRightSide(result, ctx.mappingAtomicOperationRight());
    }

    private com.gs.legend.model.def.RelationalOperation wrapWithMappingRightSide(
            com.gs.legend.model.def.RelationalOperation expr,
            PureParser.MappingAtomicOperationRightContext rightCtx) {
        if (rightCtx == null) return expr;
        if (rightCtx.mappingAtomicSelfOperator() != null) {
            String selfOp = rightCtx.mappingAtomicSelfOperator().getText();
            if (selfOp.contains("not")) {
                return new com.gs.legend.model.def.RelationalOperation.IsNotNull(expr);
            } else {
                return new com.gs.legend.model.def.RelationalOperation.IsNull(expr);
            }
        }
        String op = rightCtx.mappingComparisonOperator().getText();
        var rightExpr = buildMappingAtomicOperation(rightCtx.mappingAtomicOperation());
        return new com.gs.legend.model.def.RelationalOperation.Comparison(expr, op, rightExpr);
    }

    private com.gs.legend.model.def.RelationalOperation.FunctionCall buildMappingFunctionOperation(
            PureParser.MappingFunctionOperationContext ctx, String databaseName) {
        String funcName = ctx.identifier().getText();
        List<com.gs.legend.model.def.RelationalOperation> args = new ArrayList<>();
        for (PureParser.MappingOperationContext argCtx : ctx.mappingOperation()) {
            args.add(buildMappingOperation(argCtx));
        }
        return new com.gs.legend.model.def.RelationalOperation.FunctionCall(funcName, args);
    }

    private com.gs.legend.model.def.RelationalOperation.JoinNavigation buildMappingJoinOp(
            PureParser.MappingJoinOperationContext ctx, String dbName) {
        List<com.gs.legend.model.def.JoinChainElement> chain = extractMappingJoinChain(ctx.mappingJoinSequence());

        com.gs.legend.model.def.RelationalOperation terminal = null;
        if (ctx.PIPE() != null) {
            if (ctx.mappingAtomicOperation() != null) {
                terminal = buildMappingAtomicOperation(ctx.mappingAtomicOperation());
            } else if (ctx.mappingTableColumnRef() != null) {
                terminal = buildMappingTableColumnRef(ctx.mappingTableColumnRef(), null);
            }
        }

        return new com.gs.legend.model.def.RelationalOperation.JoinNavigation(dbName, chain, terminal);
    }

    private com.gs.legend.model.def.RelationalOperation buildMappingTableColumnRef(
            PureParser.MappingTableColumnRefContext ctx, String dbName) {
        String firstId = ctx.relationalIdentifier().getText();
        if (ctx.mappingScopeInfo() != null) {
            var scopeIds = ctx.mappingScopeInfo().relationalIdentifier();
            if (scopeIds.size() == 1) {
                return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, firstId, scopeIds.get(0).getText());
            } else {
                String table = firstId + "." + scopeIds.get(0).getText();
                return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, table, scopeIds.get(1).getText());
            }
        }
        return com.gs.legend.model.def.RelationalOperation.ColumnRef.of(dbName, firstId, firstId);
    }

    private com.gs.legend.model.def.RelationalOperation.Literal buildMappingConstant(
            PureParser.MappingConstantContext ctx) {
        if (ctx.STRING() != null) {
            return com.gs.legend.model.def.RelationalOperation.Literal.string(
                    unquoteString(ctx.STRING().getText()));
        } else if (ctx.INTEGER() != null) {
            return com.gs.legend.model.def.RelationalOperation.Literal.integer(
                    Long.parseLong(ctx.INTEGER().getText()));
        } else {
            return com.gs.legend.model.def.RelationalOperation.Literal.decimal(
                    Double.parseDouble(ctx.FLOAT().getText()));
        }
    }

    /**
     * Visits a mapping join operation: [DB]@JoinName or [DB]@J1 > @J2 | TABLE.COL
     */
    public com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition visitMappingJoinOperation(
            String propertyName, PureParser.MappingAtomicOperationContext ctx) {

        String databaseName = ctx.databasePointer() != null
                ? ctx.databasePointer().qualifiedName().getText() : null;

        PureParser.MappingJoinOperationContext joinOpCtx = ctx.mappingJoinOperation();
        if (joinOpCtx == null || joinOpCtx.mappingJoinSequence() == null) {
            String expression = getOriginalText(ctx);
            return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.expression(
                    propertyName, expression, null);
        }

        List<com.gs.legend.model.def.JoinChainElement> chain = extractMappingJoinChain(joinOpCtx.mappingJoinSequence());
        // Propagate database name to first element if not set
        if (databaseName != null && !chain.isEmpty() && chain.get(0).databaseName() == null) {
            var first = chain.get(0);
            chain.set(0, new com.gs.legend.model.def.JoinChainElement(
                    first.joinName(), first.joinType(), databaseName, first.strict()));
        }

        // Optional terminal column after PIPE
        com.gs.legend.model.def.RelationalOperation terminal = null;
        if (joinOpCtx.PIPE() != null) {
            if (joinOpCtx.mappingAtomicOperation() != null) {
                terminal = buildMappingAtomicOperation(joinOpCtx.mappingAtomicOperation());
            } else if (joinOpCtx.mappingTableColumnRef() != null) {
                terminal = buildMappingTableColumnRef(joinOpCtx.mappingTableColumnRef(), null);
            }
        }

        return com.gs.legend.model.def.MappingDefinition.PropertyMappingDefinition.join(
                propertyName,
                new com.gs.legend.model.def.MappingDefinition.JoinReference(
                        databaseName, chain, terminal));
    }

    /**
     * Visits a Pure M2M class mapping body.
     */
    public com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition visitPureM2MClassMappingBody(
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

        return com.gs.legend.model.def.MappingDefinition.ClassMappingDefinition.pure(
                className, sourceClassName, filterExpression, m2mPropertyExpressions);
    }

    /**
     * Extracts all MappingDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.MappingDefinition> extractMappingDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.MappingDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.MappingDefinition> extractFirstMappingDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.MappingDefinition> defs = extractMappingDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.ServiceDefinition visitServiceDefinition(
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

        // Extract function body, mapping, and runtime from execution
        String mappingRef = null;
        String runtimeRef = null;
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
                if (singleExec.serviceMappingRef() != null && !singleExec.serviceMappingRef().isEmpty()) {
                    mappingRef = singleExec.serviceMappingRef().get(0).qualifiedName().getText();
                }
                if (singleExec.serviceRuntimeRef() != null && !singleExec.serviceRuntimeRef().isEmpty()) {
                    runtimeRef = singleExec.serviceRuntimeRef().get(0).qualifiedName().getText();
                }
            }
        }

        // Extract testSuites (reuse existing parsing for now)
        List<com.gs.legend.model.def.MappingDefinition.TestSuiteDefinition> testSuites = List.of();

        return com.gs.legend.model.def.ServiceDefinition.of(
                qualifiedName,
                pattern != null ? pattern : "/",
                functionBody != null ? functionBody : "",
                documentation,
                mappingRef, runtimeRef,
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
    public static List<com.gs.legend.model.def.ServiceDefinition> extractServiceDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ServiceDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.ServiceDefinition> extractFirstServiceDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ServiceDefinition> defs = extractServiceDefinitions(definitionCtx);
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
    public com.gs.legend.model.def.ConnectionDefinition visitRelationalDatabaseConnection(
            PureParser.RelationalDatabaseConnectionContext ctx) {
        String qualifiedName = ctx.qualifiedName().getText();

        String storeName = null;
        com.gs.legend.model.def.ConnectionDefinition.DatabaseType dbType = com.gs.legend.model.def.ConnectionDefinition.DatabaseType.DuckDB;
        com.gs.legend.model.def.ConnectionSpecification specification = new com.gs.legend.model.def.ConnectionSpecification.InMemory();
        com.gs.legend.model.def.AuthenticationSpec authentication = new com.gs.legend.model.def.AuthenticationSpec.NoAuth();

        // Extract store
        if (ctx.dbConnectionStore() != null && !ctx.dbConnectionStore().isEmpty()) {
            storeName = ctx.dbConnectionStore().get(0).qualifiedName().getText();
        }

        // Extract type
        if (ctx.dbConnectionType() != null && !ctx.dbConnectionType().isEmpty()) {
            String typeStr = ctx.dbConnectionType().get(0).identifier().getText();
            try {
                dbType = com.gs.legend.model.def.ConnectionDefinition.DatabaseType.valueOf(typeStr);
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
                case "InMemory" -> new com.gs.legend.model.def.ConnectionSpecification.InMemory();
                case "LocalFile" -> {
                    String path = extractQuotedProperty(specBody, "path");
                    yield new com.gs.legend.model.def.ConnectionSpecification.LocalFile(path);
                }
                case "Static" -> {
                    String host = extractQuotedProperty(specBody, "host");
                    int port = extractIntProperty(specBody, "port", 0);
                    String database = extractQuotedProperty(specBody, "database");
                    yield new com.gs.legend.model.def.ConnectionSpecification.StaticDatasource(host, port,
                            database);
                }
                default -> new com.gs.legend.model.def.ConnectionSpecification.InMemory();
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
                case "NoAuth" -> new com.gs.legend.model.def.AuthenticationSpec.NoAuth();
                case "UsernamePassword" -> {
                    String username = extractQuotedProperty(authBody, "username");
                    String passwordVaultRef = extractQuotedProperty(authBody, "passwordVaultRef");
                    yield new com.gs.legend.model.def.AuthenticationSpec.UsernamePassword(username,
                            passwordVaultRef);
                }
                default -> new com.gs.legend.model.def.AuthenticationSpec.NoAuth();
            };
        }

        return new com.gs.legend.model.def.ConnectionDefinition(
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
    public static List<com.gs.legend.model.def.ConnectionDefinition> extractConnectionDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ConnectionDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.ConnectionDefinition> extractFirstConnectionDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.ConnectionDefinition> defs = extractConnectionDefinitions(
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
    public com.gs.legend.model.def.RuntimeDefinition visitRuntime(
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

        List<com.gs.legend.model.def.JsonModelConnection> jsonConnections = new ArrayList<>();

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
                    // Handle embedded connections: #{ JsonModelConnection { class: ...; url: '...'; } }#
                    if (identConnCtx.embeddedConnection() != null) {
                        var jmc = parseEmbeddedJsonModelConnection(identConnCtx.embeddedConnection());
                        if (jmc != null) {
                            jsonConnections.add(jmc);
                        }
                    }
                }
            }
        }

        return new com.gs.legend.model.def.RuntimeDefinition(
                qualifiedName, mappings, connectionBindings, jsonConnections);
    }

    /**
     * Extracts all RuntimeDefinitions from a parsed definition context.
     */
    public static List<com.gs.legend.model.def.RuntimeDefinition> extractRuntimeDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.RuntimeDefinition> result = new ArrayList<>();
        PackageableElementBuilder builder = new PackageableElementBuilder();

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
    public static Optional<com.gs.legend.model.def.RuntimeDefinition> extractFirstRuntimeDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.RuntimeDefinition> defs = extractRuntimeDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Embedded Connection Parsing ====================

    private static final java.util.regex.Pattern JMC_CLASS_PATTERN =
            java.util.regex.Pattern.compile("class\\s*:\\s*([\\w:]+)\\s*;");
    private static final java.util.regex.Pattern JMC_URL_PATTERN =
            java.util.regex.Pattern.compile("url\\s*:\\s*'([^']*)'\\s*;");

    /**
     * Parses an embedded connection island into a JsonModelConnection.
     * Extracts class name and URL from the raw text content.
     * Returns null if the content is not a JsonModelConnection.
     */
    private com.gs.legend.model.def.JsonModelConnection parseEmbeddedJsonModelConnection(
            PureParser.EmbeddedConnectionContext ctx) {
        // Reconstruct raw text from island tokens
        var sb = new StringBuilder();
        for (var content : ctx.embeddedConnectionContent()) {
            sb.append(content.getText());
        }
        String raw = sb.toString().trim();
        // Must start with "JsonModelConnection"
        if (!raw.startsWith("JsonModelConnection")) return null;

        var classMatcher = JMC_CLASS_PATTERN.matcher(raw);
        var urlMatcher = JMC_URL_PATTERN.matcher(raw);
        if (!classMatcher.find() || !urlMatcher.find()) return null;

        String qualifiedClass = classMatcher.group(1);
        return new com.gs.legend.model.def.JsonModelConnection(
                qualifiedClass, urlMatcher.group(1));
    }

    // ==================== Native Function Parsing ====================

    /** Known type parameter names (declared in <T,V,K,...>). Set during visitNativeFunction. */
    private Set<String> currentTypeParams = Set.of();

    /**
     * Visits a native function definition and returns a NativeFunctionDef.
     *
     * Grammar rule:
     * nativeFunction: NATIVE FUNCTION qualifiedName typeAndMultiplicityParameters?
     *                 functionTypeSignature SEMI_COLON
     * functionTypeSignature: PAREN_OPEN (functionVariableExpression (COMMA functionVariableExpression)*)?
     *                       PAREN_CLOSE COLON type multiplicity
     */
    public NativeFunctionDef visitNativeFunction(PureParser.NativeFunctionContext ctx) {
        // Extract simple name
        String qualifiedName = ctx.qualifiedName().getText();
        String name = qualifiedName.contains("::")
                ? qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2)
                : qualifiedName;

        // Extract type params and multiplicity params from <T,V|m>
        List<String> typeParams = new ArrayList<>();
        List<String> multParams = new ArrayList<>();
        if (ctx.typeAndMultiplicityParameters() != null) {
            var tmp = ctx.typeAndMultiplicityParameters();
            if (tmp.typeParameters() != null) {
                for (var tp : tmp.typeParameters().typeParameter()) {
                    typeParams.add(tp.getText());
                }
            }
            if (tmp.multiplictyParameters() != null) {
                for (var id : tmp.multiplictyParameters().identifier()) {
                    multParams.add(id.getText());
                }
            }
        }

        // Set context for type resolution (so T is recognized as TypeVar)
        currentTypeParams = Set.copyOf(typeParams);

        // Extract parameters from functionTypeSignature
        var sigCtx = ctx.functionTypeSignature();
        List<PType.Param> params = new ArrayList<>();
        if (sigCtx.functionVariableExpression() != null) {
            for (var varCtx : sigCtx.functionVariableExpression()) {
                String paramName = varCtx.identifier().getText();
                PType paramType = visitPureType(varCtx.type());
                Mult paramMult = visitPureMult(varCtx.multiplicity());
                params.add(new PType.Param(paramName, paramType, paramMult));
            }
        }

        // Extract return type and multiplicity
        PType returnType = visitPureType(sigCtx.type());
        Mult returnMult = visitPureMult(sigCtx.multiplicity());

        // Raw signature text for display/debugging
        String rawSignature = getOriginalText(ctx);

        currentTypeParams = Set.of();

        return new NativeFunctionDef(
                name, typeParams, multParams, params, returnType, returnMult, rawSignature);
    }

    /**
     * Visits a Pure type node and produces a PType.
     *
     * Grammar rule:
     * type: (qualifiedName (<typeArguments (| multArgs)?>)?) typeVariableValues?
     *     | ({functionTypePureType (,...)* -> type multiplicity})
     *     | relationType
     *     | unitName
     */
    public PType visitPureType(PureParser.TypeContext ctx) {
        // Case 1: Function type — {T[1]->Boolean[1]}
        if (ctx.BRACE_OPEN() != null) {
            List<PType.Param> fnParams = new ArrayList<>();
            for (var ftp : ctx.functionTypePureType()) {
                PType pType = visitPureType(ftp.type());
                Mult pMult = visitPureMult(ftp.multiplicity());
                fnParams.add(new PType.Param("", pType, pMult));
            }
            // The return type and multiplicity after the arrow
            PType retType = visitPureType(ctx.type());
            Mult retMult = visitPureMult(ctx.multiplicity());
            return new PType.FunctionType(fnParams, retType, retMult);
        }

        // Case 2: Relation type — (name:String[1], age:Integer[1])
        if (ctx.relationType() != null) {
            List<PType.RelationTypeVar.Column> columns = new ArrayList<>();
            for (var colCtx : ctx.relationType().columnInfo()) {
                // mayColumnName: QUESTION | columnName
                String colName = colCtx.mayColumnName().columnName() != null
                        ? colCtx.mayColumnName().columnName().getText() : "?";
                // mayColumnType: QUESTION | type
                PType colType = colCtx.mayColumnType().type() != null
                        ? visitPureType(colCtx.mayColumnType().type()) : new PType.Concrete("Any");
                Mult colMult = colCtx.multiplicity() != null
                        ? visitPureMult(colCtx.multiplicity()) : Mult.ONE;
                columns.add(new PType.RelationTypeVar.Column(colName, colType, colMult));
            }
            return new PType.RelationTypeVar(columns);
        }

        // Case 3: Named type — qualifiedName with optional type arguments
        if (ctx.qualifiedName() != null) {
            String rawName = ctx.qualifiedName().getText();
            // Simple name for type-var check (T, V, K are never qualified)
            String simpleName = rawName.contains("::")
                    ? rawName.substring(rawName.lastIndexOf("::") + 2) : rawName;

            // Check for type arguments: Relation<T>, Function<{...}>, ColSpec<Z⊆T>
            if (ctx.LESS_THAN() != null && ctx.typeArguments() != null) {
                List<PType> typeArgs = new ArrayList<>();

                for (var typeWithOpCtx : ctx.typeArguments().typeWithOperation()) {
                    PType arg = visitTypeWithOperation(typeWithOpCtx);
                    typeArgs.add(arg);
                }

                return new PType.Parameterized(simpleName, typeArgs);
            }

            // No type args — concrete type or type variable
            if (currentTypeParams.contains(simpleName)) {
                return new PType.TypeVar(simpleName);
            }
            return new PType.Concrete(rawName);
        }

        // Case 4: Unit name
        if (ctx.unitName() != null) {
            return new PType.Concrete(ctx.unitName().getText());
        }

        // Fallback
        return new PType.Concrete(ctx.getText());
    }

    /**
     * Visits a typeWithOperation node and produces a PType, potentially
     * wrapping in SchemaAlgebra nodes for type algebra.
     *
     * <p>Grammar rule:
     * <pre>typeWithOperation: type equalType? (typeAddSubOperation)* subsetType?</pre>
     *
     * <p>Examples:
     * <ul>
     *   <li>{@code T} → {@code TypeVar("T")}</li>
     *   <li>{@code T-Z+V} → {@code Union(Difference(TypeVar("T"), TypeVar("Z")), TypeVar("V"))}</li>
     *   <li>{@code Z⊆T} → {@code Subset(TypeVar("Z"), TypeVar("T"))}</li>
     *   <li>{@code Z=K⊆T} → {@code Subset(Equal(TypeVar("Z"), TypeVar("K")), TypeVar("T"))}</li>
     * </ul>
     */
    public PType visitTypeWithOperation(PureParser.TypeWithOperationContext ctx) {
        PType result = visitPureType(ctx.type());

        // Handle equalType: Z=K → Equal(Z, K)
        if (ctx.equalType() != null) {
            PType eqType = visitPureType(ctx.equalType().type());
            result = new PType.SchemaAlgebra(result, eqType, PType.SchemaAlgebra.OpType.Equal);
        }

        // Handle add/sub operations: T-Z+V → Union(Difference(T, Z), V)
        for (var op : ctx.typeAddSubOperation()) {
            if (op.typeAdd() != null) {
                PType right = visitPureType(op.typeAdd().type());
                result = new PType.SchemaAlgebra(result, right, PType.SchemaAlgebra.OpType.Union);
            } else {
                PType right = visitPureType(op.typeSubtract().type());
                result = new PType.SchemaAlgebra(result, right, PType.SchemaAlgebra.OpType.Difference);
            }
        }

        // Handle subsetType: Z⊆T → Subset(Z, T)
        if (ctx.subsetType() != null) {
            PType superSet = visitPureType(ctx.subsetType().type());
            result = new PType.SchemaAlgebra(result, superSet, PType.SchemaAlgebra.OpType.Subset);
        }

        return result;
    }

    /**
     * Visits a multiplicity node and produces a Mult.
     *
     * Grammar rule:
     * multiplicity: BRACKET_OPEN multiplicityArgument BRACKET_CLOSE
     * multiplicityArgument: identifier | ((fromMultiplicity DOT_DOT)? toMultiplicity)
     */
    public Mult visitPureMult(PureParser.MultiplicityContext ctx) {
        if (ctx == null) return Mult.ONE;
        var arg = ctx.multiplicityArgument();

        // Multiplicity variable: [m]
        if (arg.identifier() != null) {
            return new Mult.Var(arg.identifier().getText());
        }

        // Range: [0..1], [1..*], [1], [*]
        if (arg.fromMultiplicity() != null) {
            int from = Integer.parseInt(arg.fromMultiplicity().getText());
            var toCtx = arg.toMultiplicity();
            Integer to = toCtx.STAR() != null ? null : Integer.parseInt(toCtx.getText());
            return new Mult.Fixed(new Multiplicity(from, to));
        }

        // Just toMultiplicity: [1], [*]
        if (arg.toMultiplicity() != null) {
            var toCtx = arg.toMultiplicity();
            if (toCtx.STAR() != null) {
                return Mult.ZERO_MANY;
            }
            int val = Integer.parseInt(toCtx.getText());
            if (val == 1) return Mult.ONE;
            return new Mult.Fixed(new Multiplicity(val, val));
        }

        return Mult.ONE;
    }

    // ==================== Native Function Extraction ====================

    /**
     * Extracts all NativeFunctionDefs from a parsed definition context.
     */
    public static List<NativeFunctionDef> extractNativeFunctionDefinitions(
            PureParser.DefinitionContext definitionCtx) {
        List<NativeFunctionDef> result = new ArrayList<>();
        if (definitionCtx == null) return result;
        PackageableElementBuilder builder = new PackageableElementBuilder();
        for (PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
            if (elemCtx.nativeFunction() != null) {
                result.add(builder.visitNativeFunction(elemCtx.nativeFunction()));
            }
        }
        return result;
    }

    /**
     * Extracts the first NativeFunctionDef from a parsed definition context.
     */
    public static Optional<NativeFunctionDef> extractFirstNativeFunctionDefinition(
            PureParser.DefinitionContext definitionCtx) {
        List<NativeFunctionDef> defs = extractNativeFunctionDefinitions(definitionCtx);
        return defs.isEmpty() ? Optional.empty() : Optional.of(defs.get(0));
    }

    // ==================== Combined Extraction ====================

    /**
     * Extracts ALL definitions from a parsed definition context.
     * Iterates through all elementDefinition nodes and builds the appropriate
     * PackageableElement based on which grammar rule matched.
     *
     * @param definitionCtx The definition context from parsing
     * @return List of all PackageableElements found
     */
    public static List<com.gs.legend.model.def.PackageableElement> extractAllDefinitions(
            com.gs.legend.antlr.PureParser.DefinitionContext definitionCtx) {
        return extractAllDefinitionsWithImports(definitionCtx).definitions();
    }

    /**
     * Extracts all definitions AND import statements from a parsed source.
     * Import statements are collected into an ImportScope for name resolution.
     */
    /** Result of parsing: definitions + imports from import statements. */
    public record ParseResult(List<com.gs.legend.model.def.PackageableElement> definitions, ImportScope imports) {}

    public static ParseResult extractAllDefinitionsWithImports(
            com.gs.legend.antlr.PureParser.DefinitionContext definitionCtx) {
        List<com.gs.legend.model.def.PackageableElement> result = new ArrayList<>();
        ImportScope imports = new ImportScope();
        if (definitionCtx == null) {
            return new ParseResult(result, imports);
        }

        // Collect import statements
        for (com.gs.legend.antlr.PureParser.ImportStatementContext importCtx : definitionCtx.importStatement()) {
            // Grammar: IMPORT packagePath PATH_SEPARATOR STAR SEMI_COLON
            // packagePath.getText() = "simple::model", so import = "simple::model::*"
            String pkg = importCtx.packagePath().getText();
            imports.addImport(pkg + "::*");
        }

        PackageableElementBuilder builder = new PackageableElementBuilder();

        for (com.gs.legend.antlr.PureParser.ElementDefinitionContext elemCtx : definitionCtx.elementDefinition()) {
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
            // singleConnectionRuntime, instance, measureDefinition ignored for now
            // nativeFunction handled via extractNativeFunctionDefinitions()
        }

        return new ParseResult(result, imports);
    }
}
