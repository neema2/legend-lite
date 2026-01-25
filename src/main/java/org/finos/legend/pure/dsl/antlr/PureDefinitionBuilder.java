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
}
