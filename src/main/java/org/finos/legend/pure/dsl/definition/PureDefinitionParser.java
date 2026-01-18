package org.finos.legend.pure.dsl.definition;

import org.finos.legend.pure.dsl.PureParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Pure definition syntax (Class, Database, Mapping).
 * 
 * This parses the declarative Pure syntax used to define models and mappings.
 */
public final class PureDefinitionParser {

    /**
     * Parses a Pure source string and returns all definitions found.
     * 
     * @param pureSource The Pure source code
     * @return List of parsed definitions
     */
    public static List<PureDefinition> parse(String pureSource) {
        List<PureDefinition> definitions = new ArrayList<>();
        String remaining = pureSource.trim();

        while (!remaining.isEmpty()) {
            remaining = remaining.trim();
            if (remaining.isEmpty())
                break;

            if (remaining.startsWith("Class ") || remaining.startsWith("<<")) {
                // Both regular classes and annotated classes (<<profile.stereotype>> Class ...)
                var result = parseClass(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Database ")) {
                var result = parseDatabase(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Mapping ")) {
                var result = parseMapping(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Association ")) {
                var result = parseAssociation(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Service ")) {
                var result = parseService(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Enum ")) {
                var result = parseEnum(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Profile ")) {
                var result = parseProfile(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("function ")) {
                var result = parseFunction(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else {
                throw new PureParseException("Unknown definition starting with: " +
                        remaining.substring(0, Math.min(50, remaining.length())));
            }
        }

        return definitions;
    }

    /**
     * Parses a single Class definition.
     */
    public static ClassDefinition parseClassDefinition(String pureSource) {
        var result = parseClass(pureSource.trim());
        return result.definition;
    }

    /**
     * Parses a single Profile definition.
     */
    public static ProfileDefinition parseProfileDefinition(String pureSource) {
        var result = parseProfile(pureSource.trim());
        return result.definition;
    }

    /**
     * Parses a single Function definition.
     */
    public static FunctionDefinition parseFunctionDefinition(String pureSource) {
        var result = parseFunction(pureSource.trim());
        return result.definition;
    }

    /**
     * Parses a single Database definition.
     */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource) {
        var result = parseDatabase(pureSource.trim());
        return result.definition;
    }

    /**
     * Parses a single Mapping definition.
     */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        var result = parseMapping(pureSource.trim());
        return result.definition;
    }

    // ==================== Class Parsing ====================

    private static ParseResult<ClassDefinition> parseClass(String source) {
        // Parse stereotypes and tagged values before Class keyword
        // Pattern: <<profile::Name.stereotype>> or <<profile::Name.tag: 'value'>>
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        List<TaggedValue> taggedValues = new ArrayList<>();

        String remaining = source.trim();
        while (remaining.startsWith("<<")) {
            int closeIdx = remaining.indexOf(">>");
            if (closeIdx < 0)
                break;
            String annotation = remaining.substring(2, closeIdx).trim();
            remaining = remaining.substring(closeIdx + 2).trim();

            // Check if it's a tagged value (has colon AFTER the dot separator)
            // e.g., profile::Name.tag: 'value' - colon comes after .tag
            int dotIdx = annotation.lastIndexOf('.');
            int colonIdx = annotation.indexOf(':', dotIdx > 0 ? dotIdx : 0);
            if (colonIdx > dotIdx && dotIdx > 0) {
                // Tagged value: profile::Name.tag: 'value'
                String reference = annotation.substring(0, colonIdx).trim();
                String value = annotation.substring(colonIdx + 1).trim();
                // Remove quotes
                if ((value.startsWith("'") && value.endsWith("'")) ||
                        (value.startsWith("\"") && value.endsWith("\""))) {
                    value = value.substring(1, value.length() - 1);
                }
                int refDotIdx = reference.lastIndexOf('.');
                if (refDotIdx > 0) {
                    taggedValues.add(new TaggedValue(
                            reference.substring(0, refDotIdx),
                            reference.substring(refDotIdx + 1),
                            value));
                }
            } else {
                // Stereotype: profile::Name.stereotype
                stereotypes.add(StereotypeApplication.parse(annotation));
            }
        }

        // Pattern: Class qualified::Name { ... } [constraints]?
        Pattern headerPattern = Pattern.compile("Class\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(remaining);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Class definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(remaining, bodyStart - 1);

        String body = remaining.substring(bodyStart, bodyEnd);

        List<ClassDefinition.PropertyDefinition> properties = parseProperties(body);
        List<ClassDefinition.DerivedPropertyDefinition> derivedProperties = parseDerivedProperties(body);

        // Check for constraints block after class body: [ constraint1: expr,
        // constraint2: expr ]
        remaining = remaining.substring(bodyEnd + 1).trim();
        List<ClassDefinition.ConstraintDefinition> constraints = new ArrayList<>();

        if (remaining.startsWith("[")) {
            int constraintEnd = findMatchingBracket(remaining, 0);
            String constraintBody = remaining.substring(1, constraintEnd);
            constraints = parseConstraints(constraintBody);
            remaining = remaining.substring(constraintEnd + 1);
        }

        return new ParseResult<>(
                new ClassDefinition(qualifiedName, properties, derivedProperties, constraints, stereotypes,
                        taggedValues),
                remaining);
    }

    // ==================== Profile Parsing ====================

    private static ParseResult<ProfileDefinition> parseProfile(String source) {
        // Pattern: Profile qualified::Name { stereotypes: [...]; tags: [...]; }
        Pattern headerPattern = Pattern.compile("Profile\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Profile definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd);
        String remaining = source.substring(bodyEnd + 1).trim();

        List<String> stereotypes = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        // Parse stereotypes: [name1, name2, ...]
        Pattern stereotypesPattern = Pattern.compile("stereotypes\\s*:\\s*\\[([^\\]]+)]");
        Matcher stereotypesMatcher = stereotypesPattern.matcher(body);
        if (stereotypesMatcher.find()) {
            String[] names = stereotypesMatcher.group(1).split(",");
            for (String name : names) {
                name = name.trim();
                if (!name.isEmpty()) {
                    stereotypes.add(name);
                }
            }
        }

        // Parse tags: [name1, name2, ...]
        Pattern tagsPattern = Pattern.compile("tags\\s*:\\s*\\[([^\\]]+)]");
        Matcher tagsMatcher = tagsPattern.matcher(body);
        if (tagsMatcher.find()) {
            String[] names = tagsMatcher.group(1).split(",");
            for (String name : names) {
                name = name.trim();
                if (!name.isEmpty()) {
                    tags.add(name);
                }
            }
        }

        return new ParseResult<>(
                new ProfileDefinition(qualifiedName, stereotypes, tags),
                remaining);
    }

    // ==================== Function Parsing ====================

    private static ParseResult<FunctionDefinition> parseFunction(String source) {
        // Parse stereotypes and tagged values before function keyword
        List<StereotypeApplication> stereotypes = new ArrayList<>();
        List<TaggedValue> taggedValues = new ArrayList<>();

        String remaining = source.trim();
        while (remaining.startsWith("<<")) {
            int closeIdx = remaining.indexOf(">>");
            if (closeIdx < 0)
                break;
            String annotation = remaining.substring(2, closeIdx).trim();
            remaining = remaining.substring(closeIdx + 2).trim();

            int dotIdx = annotation.lastIndexOf('.');
            int colonIdx = annotation.indexOf(':', dotIdx > 0 ? dotIdx : 0);
            if (colonIdx > dotIdx && dotIdx > 0) {
                String reference = annotation.substring(0, colonIdx).trim();
                String value = annotation.substring(colonIdx + 1).trim();
                if ((value.startsWith("'") && value.endsWith("'")) ||
                        (value.startsWith("\"") && value.endsWith("\""))) {
                    value = value.substring(1, value.length() - 1);
                }
                int refDotIdx = reference.lastIndexOf('.');
                if (refDotIdx > 0) {
                    taggedValues.add(new TaggedValue(
                            reference.substring(0, refDotIdx),
                            reference.substring(refDotIdx + 1),
                            value));
                }
            } else {
                stereotypes.add(StereotypeApplication.parse(annotation));
            }
        }

        // Pattern: function qualified::name(params): ReturnType[mult] { body }
        Pattern headerPattern = Pattern.compile(
                "function\\s+([\\w:]+)\\s*\\(([^)]*)\\)\\s*:\\s*([\\w:]+)\\s*\\[([^\\]]+)]\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(remaining);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Function definition: " +
                    remaining.substring(0, Math.min(100, remaining.length())));
        }

        String qualifiedName = headerMatcher.group(1);
        String paramsStr = headerMatcher.group(2).trim();
        String returnType = headerMatcher.group(3);
        String returnMult = headerMatcher.group(4);

        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(remaining, bodyStart - 1);

        String body = remaining.substring(bodyStart, bodyEnd).trim();
        remaining = remaining.substring(bodyEnd + 1).trim();

        // Parse parameters
        List<FunctionDefinition.ParameterDefinition> parameters = new ArrayList<>();
        if (!paramsStr.isEmpty()) {
            // Split by comma but be careful of nested generics
            String[] paramParts = paramsStr.split(",");
            for (String paramPart : paramParts) {
                paramPart = paramPart.trim();
                if (paramPart.isEmpty())
                    continue;

                // Pattern: paramName: Type[mult]
                Pattern paramPattern = Pattern.compile("(\\w+)\\s*:\\s*([\\w:]+)\\s*\\[([^\\]]+)]");
                Matcher paramMatcher = paramPattern.matcher(paramPart);
                if (paramMatcher.matches()) {
                    String paramName = paramMatcher.group(1);
                    String paramType = paramMatcher.group(2);
                    String paramMult = paramMatcher.group(3);
                    Integer[] bounds = parseMultiplicity(paramMult);
                    parameters.add(new FunctionDefinition.ParameterDefinition(
                            paramName, paramType, bounds[0], bounds[1]));
                }
            }
        }

        // Parse return multiplicity
        Integer[] returnBounds = parseMultiplicity(returnMult);

        return new ParseResult<>(
                new FunctionDefinition(
                        qualifiedName,
                        parameters,
                        returnType,
                        returnBounds[0],
                        returnBounds[1],
                        body,
                        stereotypes,
                        taggedValues),
                remaining);
    }

    /**
     * Parses constraints from a constraint block body.
     * Format: name1: expression1, name2: expression2
     */
    private static List<ClassDefinition.ConstraintDefinition> parseConstraints(String body) {
        List<ClassDefinition.ConstraintDefinition> constraints = new ArrayList<>();

        // Split by comma (but be careful of commas in expressions)
        // Pattern: constraintName: $this.property > value
        Pattern pattern = Pattern.compile("(\\w+)\\s*:\\s*([^,]+)");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String name = matcher.group(1).trim();
            String expression = matcher.group(2).trim();
            constraints.add(new ClassDefinition.ConstraintDefinition(name, expression));
        }

        return constraints;
    }

    /**
     * Finds the matching closing bracket for an opening bracket.
     */
    private static int findMatchingBracket(String source, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new PureParseException("Unmatched bracket starting at position " + openPos);
    }

    private static List<ClassDefinition.PropertyDefinition> parseProperties(String body) {
        List<ClassDefinition.PropertyDefinition> properties = new ArrayList<>();

        // Pattern: propertyName: Type[multiplicity];
        // Must NOT be followed by () which would indicate a derived property
        Pattern pattern = Pattern.compile(
                "(\\w+)\\s*:\\s*(\\w+)\\s*\\[([^\\]]+)\\]\\s*;?");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String name = matcher.group(1);
            String type = matcher.group(2);
            String multiplicity = matcher.group(3);

            // Skip derived properties (they have () after name)
            int start = matcher.start();
            if (start > 0) {
                // Check if there's a () before the :
                String before = body.substring(Math.max(0, start - 10), start);
                if (before.contains("()")) {
                    continue; // This is part of a derived property expression
                }
            }

            var bounds = parseMultiplicity(multiplicity);
            properties.add(new ClassDefinition.PropertyDefinition(name, type, bounds[0], bounds[1]));
        }

        return properties;
    }

    private static List<ClassDefinition.DerivedPropertyDefinition> parseDerivedProperties(String body) {
        List<ClassDefinition.DerivedPropertyDefinition> derivedProperties = new ArrayList<>();

        // Pattern: name() {expression}: Type[multiplicity];
        // Example: fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
        Pattern pattern = Pattern.compile(
                "(\\w+)\\s*\\(\\s*\\)\\s*\\{([^}]+)\\}\\s*:\\s*(\\w+)\\s*\\[([^\\]]+)\\]\\s*;?");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String name = matcher.group(1);
            String expression = matcher.group(2).trim();
            String type = matcher.group(3);
            String multiplicity = matcher.group(4);

            var bounds = parseMultiplicity(multiplicity);
            derivedProperties.add(new ClassDefinition.DerivedPropertyDefinition(
                    name, expression, type, bounds[0], bounds[1]));
        }

        return derivedProperties;
    }

    private static Integer[] parseMultiplicity(String mult) {
        mult = mult.trim();
        if (mult.equals("*")) {
            return new Integer[] { 0, null };
        }
        if (mult.equals("1")) {
            return new Integer[] { 1, 1 };
        }
        if (mult.contains("..")) {
            String[] parts = mult.split("\\.\\.");
            int lower = Integer.parseInt(parts[0].trim());
            Integer upper = parts[1].trim().equals("*") ? null : Integer.parseInt(parts[1].trim());
            return new Integer[] { lower, upper };
        }
        int val = Integer.parseInt(mult);
        return new Integer[] { val, val };
    }

    // ==================== Database Parsing ====================

    private static ParseResult<DatabaseDefinition> parseDatabase(String source) {
        // Pattern: Database qualified::Name ( ... )
        // Find the opening paren
        Pattern headerPattern = Pattern.compile("Database\\s+([\\w:]+)\\s*\\(");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Database definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingParen(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd);
        List<DatabaseDefinition.TableDefinition> tables = parseTables(body);
        List<DatabaseDefinition.JoinDefinition> joins = parseJoins(body);

        return new ParseResult<>(
                new DatabaseDefinition(qualifiedName, tables, joins),
                source.substring(bodyEnd + 1));
    }

    private static List<DatabaseDefinition.JoinDefinition> parseJoins(String body) {
        List<DatabaseDefinition.JoinDefinition> joins = new ArrayList<>();

        // Pattern: Join JoinName(TABLE_A.COLUMN_A = TABLE_B.COLUMN_B)
        Pattern pattern = Pattern.compile(
                "Join\\s+(\\w+)\\s*\\(\\s*(\\w+)\\.(\\w+)\\s*=\\s*(\\w+)\\.(\\w+)\\s*\\)",
                Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String name = matcher.group(1);
            String leftTable = matcher.group(2);
            String leftColumn = matcher.group(3);
            String rightTable = matcher.group(4);
            String rightColumn = matcher.group(5);

            joins.add(new DatabaseDefinition.JoinDefinition(
                    name, leftTable, leftColumn, rightTable, rightColumn));
        }

        return joins;
    }

    private static List<DatabaseDefinition.TableDefinition> parseTables(String body) {
        List<DatabaseDefinition.TableDefinition> tables = new ArrayList<>();

        // Pattern: Table TABLE_NAME ( ... )
        Pattern pattern = Pattern.compile("Table\\s+(\\w+)\\s*\\(");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String tableName = matcher.group(1);
            int columnsStart = matcher.end();
            int columnsEnd = findMatchingParen(body, columnsStart - 1);

            String columnsBody = body.substring(columnsStart, columnsEnd);
            List<DatabaseDefinition.ColumnDefinition> columns = parseColumns(columnsBody);

            tables.add(new DatabaseDefinition.TableDefinition(tableName, columns));

            // Continue searching after this table
            matcher.region(columnsEnd + 1, body.length());
        }

        return tables;
    }

    private static List<DatabaseDefinition.ColumnDefinition> parseColumns(String body) {
        List<DatabaseDefinition.ColumnDefinition> columns = new ArrayList<>();

        // Split by comma
        String[] parts = body.split(",");
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty())
                continue;

            // Pattern: COLUMN_NAME DATA_TYPE [(size)] [PRIMARY KEY] [NOT NULL]
            Pattern pattern = Pattern.compile(
                    "(\\w+)\\s+(\\w+(?:\\s*\\([^)]+\\))?)\\s*(PRIMARY\\s+KEY)?\\s*(NOT\\s+NULL)?",
                    Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(part);

            if (matcher.find()) {
                String name = matcher.group(1);
                String dataType = matcher.group(2).replaceAll("\\s+", "");
                boolean primaryKey = matcher.group(3) != null;
                boolean notNull = matcher.group(4) != null || primaryKey;

                columns.add(new DatabaseDefinition.ColumnDefinition(name, dataType, primaryKey, notNull));
            }
        }

        return columns;
    }

    // ==================== Mapping Parsing ====================

    private static ParseResult<MappingDefinition> parseMapping(String source) {
        // Pattern: Mapping qualified::Name ( ... )
        Pattern headerPattern = Pattern.compile("Mapping\\s+([\\w:]+)\\s*\\(");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Mapping definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingParen(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd);
        List<MappingDefinition.ClassMappingDefinition> classMappings = parseClassMappings(body);

        return new ParseResult<>(
                new MappingDefinition(qualifiedName, classMappings),
                source.substring(bodyEnd + 1));
    }

    private static List<MappingDefinition.ClassMappingDefinition> parseClassMappings(String body) {
        List<MappingDefinition.ClassMappingDefinition> mappings = new ArrayList<>();

        // Pattern: ClassName: MappingType { ... }
        Pattern pattern = Pattern.compile("(\\w+)\\s*:\\s*(\\w+)\\s*\\{");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String className = matcher.group(1);
            String mappingType = matcher.group(2);
            int mappingBodyStart = matcher.end();
            int mappingBodyEnd = findMatchingBrace(body, mappingBodyStart - 1);

            String mappingBody = body.substring(mappingBodyStart, mappingBodyEnd);

            // Parse ~mainTable
            MappingDefinition.TableReference mainTable = parseMainTable(mappingBody);

            // Parse property mappings
            List<MappingDefinition.PropertyMappingDefinition> propertyMappings = parsePropertyMappings(mappingBody);

            mappings.add(new MappingDefinition.ClassMappingDefinition(
                    className, mappingType, mainTable, propertyMappings));

            // Continue searching
            matcher.region(mappingBodyEnd + 1, body.length());
        }

        return mappings;
    }

    private static MappingDefinition.TableReference parseMainTable(String body) {
        // Pattern: ~mainTable [DatabaseName] TABLE_NAME
        Pattern pattern = Pattern.compile("~mainTable\\s+\\[(\\w+)\\]\\s+(\\w+)");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return new MappingDefinition.TableReference(matcher.group(1), matcher.group(2));
        }
        return null;
    }

    private static List<MappingDefinition.PropertyMappingDefinition> parsePropertyMappings(String body) {
        List<MappingDefinition.PropertyMappingDefinition> mappings = new ArrayList<>();

        // Pattern: propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME
        Pattern pattern = Pattern.compile("(\\w+)\\s*:\\s*\\[(\\w+)\\]\\s+(\\w+)\\.(\\w+)");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String propertyName = matcher.group(1);
            String databaseName = matcher.group(2);
            String tableName = matcher.group(3);
            String columnName = matcher.group(4);

            mappings.add(new MappingDefinition.PropertyMappingDefinition(
                    propertyName,
                    new MappingDefinition.ColumnReference(databaseName, tableName, columnName)));
        }

        return mappings;
    }

    // ==================== M2M Mapping Parsing ====================

    /**
     * Parses a single M2M Mapping definition (mappings with ": Pure" type).
     */
    public static M2MMappingDefinition parseM2MMappingDefinition(String pureSource) {
        var result = parseM2MMapping(pureSource.trim());
        return result.definition;
    }

    private static ParseResult<M2MMappingDefinition> parseM2MMapping(String source) {
        // Pattern: Mapping qualified::Name ( ... )
        Pattern headerPattern = Pattern.compile("Mapping\\s+([\\w:]+)\\s*\\(");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid M2M Mapping definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingParen(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd);
        List<M2MMappingDefinition.M2MClassMappingDefinition> classMappings = parseM2MClassMappings(body);

        return new ParseResult<>(
                new M2MMappingDefinition(qualifiedName, classMappings),
                source.substring(bodyEnd + 1));
    }

    private static List<M2MMappingDefinition.M2MClassMappingDefinition> parseM2MClassMappings(String body) {
        List<M2MMappingDefinition.M2MClassMappingDefinition> mappings = new ArrayList<>();

        // Pattern: ClassName: Pure { ... }
        Pattern pattern = Pattern.compile("(\\w+)\\s*:\\s*Pure\\s*\\{");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String targetClassName = matcher.group(1);
            int mappingBodyStart = matcher.end();
            int mappingBodyEnd = findMatchingBrace(body, mappingBodyStart - 1);

            String mappingBody = body.substring(mappingBodyStart, mappingBodyEnd);

            // Parse ~src SourceClass
            String sourceClassName = parseM2MSource(mappingBody);

            // Parse ~filter expression (optional)
            String filterExpression = parseM2MFilter(mappingBody);

            // Parse property mappings (propertyName: expression)
            List<M2MMappingDefinition.M2MPropertyMappingDefinition> propertyMappings = parseM2MPropertyMappings(
                    mappingBody);

            mappings.add(new M2MMappingDefinition.M2MClassMappingDefinition(
                    targetClassName, sourceClassName, filterExpression, propertyMappings));

            // Continue searching
            matcher.region(mappingBodyEnd + 1, body.length());
        }

        return mappings;
    }

    private static String parseM2MSource(String body) {
        // Pattern: ~src SourceClass
        Pattern pattern = Pattern.compile("~src\\s+(\\w+)");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new PureParseException("M2M mapping must have ~src directive");
    }

    private static String parseM2MFilter(String body) {
        // Pattern: ~filter expression (until end of line or comma)
        Pattern pattern = Pattern.compile("~filter\\s+(.+?)(?:,\\s*\\n|\\n|$)");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null; // Filter is optional
    }

    private static List<M2MMappingDefinition.M2MPropertyMappingDefinition> parseM2MPropertyMappings(String body) {
        List<M2MMappingDefinition.M2MPropertyMappingDefinition> mappings = new ArrayList<>();

        // Split body into lines and look for property: expression patterns
        // Skip lines starting with ~ (directives)
        String[] lines = body.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("~")) {
                continue;
            }

            // Pattern: propertyName: expression (may end with comma)
            // Expression can contain $src.prop, functions, operators, etc.
            Pattern pattern = Pattern.compile("^(\\w+)\\s*:\\s*(.+?)\\s*,?\\s*$");
            Matcher matcher = pattern.matcher(line);

            if (matcher.find()) {
                String propertyName = matcher.group(1);
                String expression = matcher.group(2).trim();
                // Remove trailing comma if present
                if (expression.endsWith(",")) {
                    expression = expression.substring(0, expression.length() - 1).trim();
                }

                mappings.add(new M2MMappingDefinition.M2MPropertyMappingDefinition(
                        propertyName, expression));
            }
        }

        return mappings;
    }

    // ==================== Association Parsing ====================

    /**
     * Parses a single Association definition.
     */
    public static AssociationDefinition parseAssociationDefinition(String pureSource) {
        var result = parseAssociation(pureSource.trim());
        return result.definition;
    }

    private static ParseResult<AssociationDefinition> parseAssociation(String source) {
        // Pattern: Association qualified::Name { ... }
        Pattern pattern = Pattern.compile(
                "Association\\s+([\\w:]+)\\s*\\{([^}]*)\\}",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(source);

        if (!matcher.find()) {
            throw new PureParseException("Invalid Association definition");
        }

        String qualifiedName = matcher.group(1);
        String body = matcher.group(2);

        // Parse the two association ends (properties)
        List<AssociationDefinition.AssociationEndDefinition> ends = parseAssociationEnds(body);

        if (ends.size() != 2) {
            throw new PureParseException("Association must have exactly 2 properties, found: " + ends.size());
        }

        return new ParseResult<>(
                new AssociationDefinition(qualifiedName, ends.get(0), ends.get(1)),
                source.substring(matcher.end()));
    }

    private static List<AssociationDefinition.AssociationEndDefinition> parseAssociationEnds(String body) {
        List<AssociationDefinition.AssociationEndDefinition> ends = new ArrayList<>();

        // Pattern: propertyName: ClassName[multiplicity];
        Pattern pattern = Pattern.compile(
                "(\\w+)\\s*:\\s*(\\w+)\\s*\\[([^\\]]+)\\]\\s*;?");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String propertyName = matcher.group(1);
            String targetClass = matcher.group(2);
            String multiplicity = matcher.group(3);

            var bounds = parseMultiplicity(multiplicity);
            ends.add(new AssociationDefinition.AssociationEndDefinition(
                    propertyName, targetClass, bounds[0], bounds[1]));
        }

        return ends;
    }

    // ==================== Helper Methods ====================

    private static int findMatchingParen(String source, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '(')
                depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0)
                    return i;
            }
        }
        throw new PureParseException("Unmatched parenthesis");
    }

    private static int findMatchingBrace(String source, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{')
                depth++;
            else if (c == '}') {
                depth--;
                if (depth == 0)
                    return i;
            }
        }
        throw new PureParseException("Unmatched brace");
    }

    private record ParseResult<T>(T definition, String remaining) {
    }

    // ==================== Service Parsing ====================

    /**
     * Parses a single Service definition.
     */
    public static ServiceDefinition parseServiceDefinition(String pureSource) {
        var result = parseService(pureSource.trim());
        return result.definition;
    }

    private static ParseResult<ServiceDefinition> parseService(String source) {
        // Pattern: Service qualified::Name { ... }
        Pattern headerPattern = Pattern.compile("Service\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Service definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd);

        // Parse pattern
        String pattern = parseServicePattern(body);

        // Parse function body
        String functionBody = parseServiceFunction(body);

        // Parse documentation (optional)
        String documentation = parseServiceDocumentation(body);

        return new ParseResult<>(
                ServiceDefinition.of(qualifiedName, pattern, functionBody, documentation),
                source.substring(bodyEnd + 1));
    }

    private static String parseServicePattern(String body) {
        // Pattern: pattern: '/api/path/{param}';
        Pattern pattern = Pattern.compile("pattern\\s*:\\s*'([^']+)'\\s*;?");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new PureParseException("Service must have a pattern");
    }

    private static String parseServiceFunction(String body) {
        // Pattern: function: |expr;
        // The function body is a lambda expression starting with |
        Pattern pattern = Pattern.compile("function\\s*:\\s*\\|([^;]+);?");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        throw new PureParseException("Service must have a function");
    }

    private static String parseServiceDocumentation(String body) {
        // Pattern: documentation: 'description';
        Pattern pattern = Pattern.compile("documentation\\s*:\\s*'([^']*)'\\s*;?");
        Matcher matcher = pattern.matcher(body);

        if (matcher.find()) {
            return matcher.group(1);
        }
        return null; // Documentation is optional
    }

    // ==================== Enum Parsing ====================

    /**
     * Parses a single Enum definition.
     * 
     * Syntax: Enum package::Name { VALUE1, VALUE2, VALUE3 }
     */
    public static EnumDefinition parseEnumDefinition(String pureSource) {
        var result = parseEnum(pureSource.trim());
        return result.definition;
    }

    private static ParseResult<EnumDefinition> parseEnum(String source) {
        // Pattern: Enum qualified::Name { VALUE1, VALUE2, ... }
        Pattern headerPattern = Pattern.compile("Enum\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Enum definition");
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd).trim();

        // Parse values: VALUE1, VALUE2, VALUE3
        List<String> values = new ArrayList<>();
        for (String value : body.split(",")) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                values.add(trimmed);
            }
        }

        if (values.isEmpty()) {
            throw new PureParseException("Enum must have at least one value");
        }

        return new ParseResult<>(
                EnumDefinition.of(qualifiedName, values),
                source.substring(bodyEnd + 1));
    }
}
