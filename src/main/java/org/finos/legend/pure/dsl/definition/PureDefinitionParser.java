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
     * Handles legend-engine format with section headers (###Pure, ###Relational,
     * ###Mapping)
     * and import statements (import package::name::*).
     * 
     * @param pureSource The Pure source code
     * @return List of parsed definitions
     */
    public static List<PureDefinition> parse(String pureSource) {
        // Preprocess: strip section headers and parse imports
        PreprocessResult preprocessed = preprocess(pureSource);

        List<PureDefinition> definitions = new ArrayList<>();
        String remaining = preprocessed.cleanedSource.trim();

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
            } else if (remaining.startsWith("RelationalDatabaseConnection ")) {
                var result = parseConnection(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
            } else if (remaining.startsWith("Runtime ")) {
                var result = parseRuntimeDef(remaining);
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
     * Preprocesses Pure source to handle legend-engine format.
     * 
     * Handles:
     * - Line comments (// ...)
     * - Block comments (slash-star ... star-slash)
     * - Section headers (###Pure, ###Relational, ###Mapping)
     * - Import statements parsed into ImportScope for name resolution
     * 
     * @param source The raw Pure source
     * @return Preprocessed result with cleaned source and import scope
     */
    private static PreprocessResult preprocess(String source) {
        // First, strip block comments /* ... */
        String withoutBlockComments = source.replaceAll("/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/", "");

        ImportScope imports = new ImportScope();
        StringBuilder cleaned = new StringBuilder();

        for (String line : withoutBlockComments.split("\n")) {
            String trimmed = line.trim();

            // Strip line comments: // ...
            int commentIdx = line.indexOf("//");
            if (commentIdx >= 0) {
                // Check it's not inside a string (simplified check)
                String beforeComment = line.substring(0, commentIdx);
                long quoteCount = beforeComment.chars().filter(c -> c == '\'').count();
                if (quoteCount % 2 == 0) {
                    // Not inside a string, strip comment
                    line = line.substring(0, commentIdx);
                    trimmed = line.trim();
                }
            }

            // Skip empty lines after comment stripping
            if (trimmed.isEmpty()) {
                cleaned.append("\n");
                continue;
            }

            // Skip section headers: ###Pure, ###Relational, ###Mapping, etc.
            if (trimmed.startsWith("###")) {
                continue;
            }

            // Parse import statements: import package::name::*; or import package::Type;
            if (trimmed.startsWith("import ") && trimmed.contains("::")) {
                String importPath = trimmed.substring(7).replace(";", "").trim();
                imports.addImport(importPath);
                continue;
            }

            cleaned.append(line).append("\n");
        }

        return new PreprocessResult(cleaned.toString(), imports);
    }

    /**
     * Result of preprocessing Pure source.
     */
    private record PreprocessResult(String cleanedSource, ImportScope imports) {
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
     * Parses a single Connection definition.
     */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource) {
        var result = parseConnection(pureSource.trim());
        return result.definition;
    }

    /**
     * Parses a single Runtime definition.
     */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource) {
        var result = parseRuntimeDef(pureSource.trim());
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

        // Pattern: Class qualified::Name [extends superclass1, superclass2]? { ... }
        // [constraints]?
        // Group 1: qualified class name
        // Group 2: extends clause (optional, including "extends" keyword)
        Pattern headerPattern = Pattern.compile("Class\\s+([\\w:]+)(\\s+extends\\s+[\\w:,\\s]+)?\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(remaining);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Class definition");
        }

        String qualifiedName = headerMatcher.group(1);

        // Parse superclasses from extends clause
        List<String> superClasses = new ArrayList<>();
        String extendsClause = headerMatcher.group(2);
        if (extendsClause != null && !extendsClause.isEmpty()) {
            // Remove "extends" keyword and parse comma-separated class names
            String classListStr = extendsClause.trim().substring("extends".length()).trim();
            String[] classNames = classListStr.split(",");
            for (String className : classNames) {
                String trimmed = className.trim();
                if (!trimmed.isEmpty()) {
                    superClasses.add(trimmed);
                }
            }
        }

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
                new ClassDefinition(qualifiedName, superClasses, properties, derivedProperties, constraints,
                        stereotypes,
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

    // ==================== Connection Parsing ====================

    private static ParseResult<ConnectionDefinition> parseConnection(String source) {
        // Pattern: RelationalDatabaseConnection qualified::name { store: ...; type:
        // ...; specification: ...; auth: ...; }
        Pattern headerPattern = Pattern.compile(
                "RelationalDatabaseConnection\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Connection definition: " +
                    source.substring(0, Math.min(100, source.length())));
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd).trim();
        String remaining = source.substring(bodyEnd + 1).trim();

        // Parse store: storeName;
        String storeName = null;
        Pattern storePattern = Pattern.compile("store:\\s*([\\w:]+)\\s*;");
        Matcher storeMatcher = storePattern.matcher(body);
        if (storeMatcher.find()) {
            storeName = storeMatcher.group(1);
        }

        // Parse type: DuckDB|SQLite|H2|etc;
        ConnectionDefinition.DatabaseType databaseType = ConnectionDefinition.DatabaseType.DuckDB;
        Pattern typePattern = Pattern.compile("type:\\s*(\\w+)\\s*;");
        Matcher typeMatcher = typePattern.matcher(body);
        if (typeMatcher.find()) {
            String typeStr = typeMatcher.group(1);
            try {
                databaseType = ConnectionDefinition.DatabaseType.valueOf(typeStr);
            } catch (IllegalArgumentException e) {
                throw new PureParseException("Unknown database type: " + typeStr);
            }
        }

        // Parse specification: InMemory {} | LocalFile { path: '...'; } | Static {
        // host: ...; port: ...; database: ...; }
        ConnectionSpecification specification = new ConnectionSpecification.InMemory();
        Pattern specPattern = Pattern.compile("specification:\\s*(\\w+)\\s*\\{([^}]*)}");
        Matcher specMatcher = specPattern.matcher(body);
        if (specMatcher.find()) {
            String specType = specMatcher.group(1);
            String specBody = specMatcher.group(2).trim();

            specification = switch (specType) {
                case "InMemory" -> new ConnectionSpecification.InMemory();
                case "LocalFile" -> {
                    String path = extractStringProperty(specBody, "path");
                    yield new ConnectionSpecification.LocalFile(path);
                }
                case "Static" -> {
                    String host = extractStringProperty(specBody, "host");
                    int port = extractIntProperty(specBody, "port", 0);
                    String database = extractStringProperty(specBody, "database");
                    yield new ConnectionSpecification.StaticDatasource(host, port, database);
                }
                default -> new ConnectionSpecification.InMemory();
            };
        }

        // Parse auth: NoAuth {} | UsernamePassword { username: ...; passwordVaultRef:
        // ...; }
        AuthenticationSpec authentication = new AuthenticationSpec.NoAuth();
        Pattern authPattern = Pattern.compile("auth:\\s*(\\w+)\\s*\\{([^}]*)}");
        Matcher authMatcher = authPattern.matcher(body);
        if (authMatcher.find()) {
            String authType = authMatcher.group(1);
            String authBody = authMatcher.group(2).trim();

            authentication = switch (authType) {
                case "NoAuth" -> new AuthenticationSpec.NoAuth();
                case "UsernamePassword" -> {
                    String username = extractStringProperty(authBody, "username");
                    String passwordVaultRef = extractStringProperty(authBody, "passwordVaultRef");
                    yield new AuthenticationSpec.UsernamePassword(username, passwordVaultRef);
                }
                default -> new AuthenticationSpec.NoAuth();
            };
        }

        return new ParseResult<>(
                new ConnectionDefinition(qualifiedName, storeName, databaseType, specification, authentication),
                remaining);
    }

    private static String extractStringProperty(String body, String propertyName) {
        Pattern pattern = Pattern.compile(propertyName + ":\\s*'([^']*)'");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private static int extractIntProperty(String body, String propertyName, int defaultValue) {
        Pattern pattern = Pattern.compile(propertyName + ":\\s*(\\d+)");
        Matcher matcher = pattern.matcher(body);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        }
        return defaultValue;
    }

    // ==================== Runtime Parsing ====================

    private static ParseResult<RuntimeDefinition> parseRuntimeDef(String source) {
        // Pattern: Runtime qualified::name { mappings: [...]; connections: [...]; }
        Pattern headerPattern = Pattern.compile("Runtime\\s+([\\w:]+)\\s*\\{");
        Matcher headerMatcher = headerPattern.matcher(source);

        if (!headerMatcher.find()) {
            throw new PureParseException("Invalid Runtime definition: " +
                    source.substring(0, Math.min(100, source.length())));
        }

        String qualifiedName = headerMatcher.group(1);
        int bodyStart = headerMatcher.end();
        int bodyEnd = findMatchingBrace(source, bodyStart - 1);

        String body = source.substring(bodyStart, bodyEnd).trim();
        String remaining = source.substring(bodyEnd + 1).trim();

        // Parse mappings: [ mapping1, mapping2 ];
        List<String> mappings = new ArrayList<>();
        Pattern mappingsPattern = Pattern.compile("mappings:\\s*\\[([^\\]]*)]");
        Matcher mappingsMatcher = mappingsPattern.matcher(body);
        if (mappingsMatcher.find()) {
            String mappingsStr = mappingsMatcher.group(1).trim();
            if (!mappingsStr.isEmpty()) {
                for (String mapping : mappingsStr.split(",")) {
                    mappings.add(mapping.trim());
                }
            }
        }

        // Parse connections: [ store1: connection1, store2: connection2 ];
        java.util.Map<String, String> connectionBindings = new java.util.HashMap<>();
        Pattern connectionsPattern = Pattern.compile("connections:\\s*\\[([^\\]]*)]");
        Matcher connectionsMatcher = connectionsPattern.matcher(body);
        if (connectionsMatcher.find()) {
            String connectionsStr = connectionsMatcher.group(1).trim();
            if (!connectionsStr.isEmpty()) {
                for (String binding : connectionsStr.split(",")) {
                    // Use regex to match qualified names: "store::Name: conn::Name"
                    // The pattern matches two qualified names separated by ": "
                    binding = binding.trim();
                    Pattern bindingPattern = Pattern.compile("([\\w:]+):\\s*([\\w:]+)$");
                    Matcher bindingMatcher = bindingPattern.matcher(binding);
                    if (bindingMatcher.find()) {
                        connectionBindings.put(bindingMatcher.group(1).trim(), bindingMatcher.group(2).trim());
                    }
                }
            }
        }

        return new ParseResult<>(
                new RuntimeDefinition(qualifiedName, mappings, connectionBindings),
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

        // Pattern: name(params) {expression}: Type[multiplicity];
        // Examples:
        // fullName() {$this.firstName + ' ' + $this.lastName}: String[1];
        // firmByName(name: String[1]) {$this.firms->filter(f|$f.name == $name)}:
        // Firm[0..1];
        // firmByName(name: String[1]) {...}: package::Firm[0..1]; (qualified type)
        Pattern pattern = Pattern.compile(
                "(\\w+)\\s*\\(([^)]*)\\)\\s*\\{([^}]+)\\}\\s*:\\s*([\\w:]+)\\s*\\[([^\\]]+)\\]\\s*;?");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String name = matcher.group(1);
            String paramsStr = matcher.group(2).trim();
            String expression = matcher.group(3).trim();
            String type = matcher.group(4);
            String multiplicity = matcher.group(5);

            // Parse parameters
            List<ClassDefinition.ParameterDefinition> params = parseParameters(paramsStr);

            var bounds = parseMultiplicity(multiplicity);
            derivedProperties.add(new ClassDefinition.DerivedPropertyDefinition(
                    name, params, expression, type, bounds[0], bounds[1]));
        }

        return derivedProperties;
    }

    /**
     * Parses parameter definitions from a comma-separated string.
     * Format: paramName: Type[mult], paramName2: Type2[mult2]
     */
    private static List<ClassDefinition.ParameterDefinition> parseParameters(String paramsStr) {
        List<ClassDefinition.ParameterDefinition> params = new ArrayList<>();
        if (paramsStr == null || paramsStr.isEmpty()) {
            return params;
        }

        // Pattern: paramName: Type[mult]
        Pattern paramPattern = Pattern.compile("(\\w+)\\s*:\\s*(\\w+)\\s*\\[([^\\]]+)\\]");
        Matcher matcher = paramPattern.matcher(paramsStr);

        while (matcher.find()) {
            String paramName = matcher.group(1);
            String paramType = matcher.group(2);
            String mult = matcher.group(3);
            var bounds = parseMultiplicity(mult);
            params.add(new ClassDefinition.ParameterDefinition(paramName, paramType, bounds[0], bounds[1]));
        }

        return params;
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

        // Separate classMappings from testSuites
        // testSuites: [ ... ] appears at the end of the body
        List<MappingDefinition.ClassMappingDefinition> classMappings;
        List<MappingDefinition.TestSuiteDefinition> testSuites = List.of();

        int testSuitesIdx = body.indexOf("testSuites:");
        if (testSuitesIdx >= 0) {
            String mappingsBody = body.substring(0, testSuitesIdx).trim();
            String testSuitesBody = body.substring(testSuitesIdx);
            classMappings = parseClassMappings(mappingsBody);
            testSuites = parseTestSuites(testSuitesBody);
        } else {
            classMappings = parseClassMappings(body);
        }

        return new ParseResult<>(
                new MappingDefinition(qualifiedName, classMappings, testSuites),
                source.substring(bodyEnd + 1));
    }

    private static List<MappingDefinition.ClassMappingDefinition> parseClassMappings(String body) {
        List<MappingDefinition.ClassMappingDefinition> mappings = new ArrayList<>();

        // Pattern: ClassName: MappingType { ... }
        // This matches simple class names (no :: package prefix) for class mappings
        // Association mappings use qualified names with :: (e.g., package::AssocName)
        Pattern pattern = Pattern.compile("(?<!:)(\\w+)\\s*:\\s*(\\w+)\\s*\\{");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String className = matcher.group(1);
            String mappingType = matcher.group(2);
            int mappingBodyStart = matcher.end();
            int mappingBodyEnd = findMatchingBrace(body, mappingBodyStart - 1);

            String mappingBody = body.substring(mappingBodyStart, mappingBodyEnd);

            if ("Pure".equals(mappingType)) {
                // Parse as M2M mapping
                String sourceClassName = parseM2MSource(mappingBody);
                String filterExpression = parseM2MFilter(mappingBody);
                java.util.Map<String, String> m2mPropertyExpressions = parseM2MPropertyExpressionsMap(mappingBody);

                mappings.add(MappingDefinition.ClassMappingDefinition.pure(
                        className, sourceClassName, filterExpression, m2mPropertyExpressions));
            } else {
                // Parse as Relational mapping
                MappingDefinition.TableReference mainTable = parseMainTable(mappingBody);
                List<MappingDefinition.PropertyMappingDefinition> propertyMappings = parsePropertyMappings(mappingBody);

                mappings.add(MappingDefinition.ClassMappingDefinition.relational(
                        className, mainTable, propertyMappings));
            }

            // Continue searching
            matcher.region(mappingBodyEnd + 1, body.length());
        }

        return mappings;
    }

    /**
     * Parses test suites from a mapping body.
     * 
     * Expected format:
     * testSuites:
     * [
     * SuiteName: { function: ...; tests: [...]; }
     * ]
     */
    private static List<MappingDefinition.TestSuiteDefinition> parseTestSuites(String source) {
        List<MappingDefinition.TestSuiteDefinition> suites = new ArrayList<>();

        // Find the opening bracket after 'testSuites:'
        int bracketStart = source.indexOf('[');
        if (bracketStart < 0) {
            return suites;
        }

        int bracketEnd = findMatchingBracket(source, bracketStart);
        String suitesContent = source.substring(bracketStart + 1, bracketEnd).trim();

        // Pattern: SuiteName: { ... }
        Pattern suitePattern = Pattern.compile("(\\w+)\\s*:\\s*\\{");
        Matcher suiteMatcher = suitePattern.matcher(suitesContent);

        while (suiteMatcher.find()) {
            String suiteName = suiteMatcher.group(1);
            int suiteBodyStart = suiteMatcher.end();
            int suiteBodyEnd = findMatchingBrace(suitesContent, suiteBodyStart - 1);

            String suiteBody = suitesContent.substring(suiteBodyStart, suiteBodyEnd);

            // Extract function body (|...;)
            String functionBody = extractFunctionBody(suiteBody);

            // Extract tests
            List<MappingDefinition.TestDefinition> tests = parseTestDefinitions(suiteBody);

            suites.add(new MappingDefinition.TestSuiteDefinition(suiteName, functionBody, tests));

            suiteMatcher.region(suiteBodyEnd + 1, suitesContent.length());
        }

        return suites;
    }

    private static String extractFunctionBody(String suiteBody) {
        // Pattern: function: |...;
        int funcIdx = suiteBody.indexOf("function:");
        if (funcIdx < 0) {
            return null;
        }

        int start = suiteBody.indexOf('|', funcIdx);
        if (start < 0) {
            return null;
        }

        // Find the end of function (semicolon before 'tests:')
        int testsIdx = suiteBody.indexOf("tests:", start);
        if (testsIdx < 0) {
            testsIdx = suiteBody.length();
        }

        String funcBody = suiteBody.substring(start, testsIdx).trim();
        // Remove trailing semicolon
        if (funcBody.endsWith(";")) {
            funcBody = funcBody.substring(0, funcBody.length() - 1).trim();
        }

        return funcBody;
    }

    private static List<MappingDefinition.TestDefinition> parseTestDefinitions(String suiteBody) {
        List<MappingDefinition.TestDefinition> tests = new ArrayList<>();

        // Find tests: [ ... ]
        int testsIdx = suiteBody.indexOf("tests:");
        if (testsIdx < 0) {
            return tests;
        }

        int bracketStart = suiteBody.indexOf('[', testsIdx);
        if (bracketStart < 0) {
            return tests;
        }

        int bracketEnd = findMatchingBracket(suiteBody, bracketStart);
        String testsContent = suiteBody.substring(bracketStart + 1, bracketEnd).trim();

        // Pattern: TestName: { ... }
        Pattern testPattern = Pattern.compile("(\\w+)\\s*:\\s*\\{");
        Matcher testMatcher = testPattern.matcher(testsContent);

        while (testMatcher.find()) {
            String testName = testMatcher.group(1);
            int testBodyStart = testMatcher.end();
            int testBodyEnd = findMatchingBrace(testsContent, testBodyStart - 1);

            String testBody = testsContent.substring(testBodyStart, testBodyEnd);

            // Extract documentation if present
            String doc = extractDocumentation(testBody);

            // Extract input data
            List<MappingDefinition.TestData> inputData = parseTestData(testBody);

            // Extract asserts
            List<MappingDefinition.TestAssertion> asserts = parseTestAsserts(testBody);

            tests.add(new MappingDefinition.TestDefinition(testName, doc, inputData, asserts));

            testMatcher.region(testBodyEnd + 1, testsContent.length());
        }

        return tests;
    }

    private static String extractDocumentation(String testBody) {
        // Pattern: doc: 'text';
        Pattern docPattern = Pattern.compile("doc:\\s*'([^']*)'");
        Matcher docMatcher = docPattern.matcher(testBody);
        if (docMatcher.find()) {
            return docMatcher.group(1);
        }
        return null;
    }

    private static List<MappingDefinition.TestData> parseTestData(String testBody) {
        List<MappingDefinition.TestData> dataList = new ArrayList<>();

        // Find data: [ ... ]
        int dataIdx = testBody.indexOf("data:");
        if (dataIdx < 0) {
            return dataList;
        }

        int bracketStart = testBody.indexOf('[', dataIdx);
        if (bracketStart < 0) {
            return dataList;
        }

        int bracketEnd = findMatchingBracket(testBody, bracketStart);
        String dataContent = testBody.substring(bracketStart + 1, bracketEnd).trim();

        // Look for inline ExternalFormat or Reference
        // For now, simplified parsing - extract contentType and data from
        // ExternalFormat
        Pattern extFormatPattern = Pattern.compile("contentType:\\s*'([^']+)'[^}]*data:\\s*'([^']*)'");
        Matcher extFormatMatcher = extFormatPattern.matcher(dataContent);

        if (extFormatMatcher.find()) {
            String contentType = extFormatMatcher.group(1);
            String data = extFormatMatcher.group(2);
            // Unescape JSON newlines
            data = data.replace("\\n", "\n").replace("\\r", "\r");
            dataList.add(MappingDefinition.TestData.inline("ModelStore", contentType, data));
        }

        // Check for Reference
        Pattern refPattern = Pattern.compile("Reference\\s*#\\{\\s*([\\w:]+)\\s*}#");
        Matcher refMatcher = refPattern.matcher(dataContent);
        if (refMatcher.find()) {
            String refPath = refMatcher.group(1);
            dataList.add(MappingDefinition.TestData.reference("ModelStore", refPath));
        }

        return dataList;
    }

    private static List<MappingDefinition.TestAssertion> parseTestAsserts(String testBody) {
        List<MappingDefinition.TestAssertion> asserts = new ArrayList<>();

        // Find asserts: [ ... ]
        int assertsIdx = testBody.indexOf("asserts:");
        if (assertsIdx < 0) {
            return asserts;
        }

        int bracketStart = testBody.indexOf('[', assertsIdx);
        if (bracketStart < 0) {
            return asserts;
        }

        int bracketEnd = findMatchingBracket(testBody, bracketStart);
        String assertsContent = testBody.substring(bracketStart + 1, bracketEnd).trim();

        // Pattern: assertionName: EqualToJson #{ ... }#
        Pattern assertPattern = Pattern.compile("(\\w+):\\s*EqualToJson\\s*#\\{");
        Matcher assertMatcher = assertPattern.matcher(assertsContent);

        if (assertMatcher.find()) {
            String assertName = assertMatcher.group(1);
            int assertBodyStart = assertMatcher.end();

            // Extract expected data from ExternalFormat
            String assertBody = assertsContent.substring(assertBodyStart);
            Pattern expectedPattern = Pattern.compile("contentType:\\s*'([^']+)'[^}]*data:\\s*'([^']*)'");
            Matcher expectedMatcher = expectedPattern.matcher(assertBody);

            if (expectedMatcher.find()) {
                String expectedData = expectedMatcher.group(2);
                // Unescape JSON newlines
                expectedData = expectedData.replace("\\n", "\n").replace("\\r", "\r");
                asserts.add(MappingDefinition.TestAssertion.equalToJson(assertName, expectedData));
            }
        }

        return asserts;
    }

    /**
     * Parses M2M property expressions into a Map (propertyName -> expression
     * string).
     */
    private static java.util.Map<String, String> parseM2MPropertyExpressionsMap(String body) {
        java.util.Map<String, String> expressions = new java.util.LinkedHashMap<>();

        // Skip ~src and ~filter lines, then parse propertyName: expression lines
        String[] lines = body.split("\n");
        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith("~src") || line.startsWith("~filter")) {
                continue;
            }

            // Pattern: propertyName: expression[,]
            int colonIdx = line.indexOf(':');
            if (colonIdx > 0) {
                String propertyName = line.substring(0, colonIdx).trim();
                String expression = line.substring(colonIdx + 1).trim();
                // Remove trailing comma if present
                if (expression.endsWith(",")) {
                    expression = expression.substring(0, expression.length() - 1).trim();
                }
                if (!propertyName.isEmpty() && !expression.isEmpty()) {
                    expressions.put(propertyName, expression);
                }
            }
        }

        return expressions;
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

        // First, try to parse expression-based mappings (with ->get)
        // Pattern: propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME->get('key',
        // @Type)
        Pattern getPattern = Pattern.compile(
                "(\\w+)\\s*:\\s*\\[(\\w+)\\]\\s+(\\w+)\\.(\\w+)\\s*->\\s*get\\s*\\(\\s*'([^']+)'\\s*,\\s*@(\\w+)\\s*\\)");
        Matcher getMatcher = getPattern.matcher(body);

        java.util.Set<Integer> getMatchEnds = new java.util.HashSet<>();
        while (getMatcher.find()) {
            String propertyName = getMatcher.group(1);
            String databaseName = getMatcher.group(2);
            String tableName = getMatcher.group(3);
            String columnName = getMatcher.group(4);
            String jsonKey = getMatcher.group(5);
            String typeName = getMatcher.group(6);

            // Store the full expression for compilation
            String expression = "[" + databaseName + "] " + tableName + "." + columnName
                    + "->get('" + jsonKey + "', @" + typeName + ")";

            mappings.add(MappingDefinition.PropertyMappingDefinition.expression(
                    propertyName, expression, null)); // No embedded class, just expression
            getMatchEnds.add(getMatcher.end());
        }

        // Then parse simple column references
        // Pattern: propertyName: [DatabaseName] TABLE_NAME.COLUMN_NAME
        Pattern simplePattern = Pattern.compile("(\\w+)\\s*:\\s*\\[(\\w+)\\]\\s+(\\w+)\\.(\\w+)");
        Matcher simpleMatcher = simplePattern.matcher(body);

        while (simpleMatcher.find()) {
            // Skip if this was already matched as part of a get expression
            // Check if there's a "->" after this match
            int matchEnd = simpleMatcher.end();
            String afterMatch = body.substring(matchEnd).trim();
            if (afterMatch.startsWith("->") || afterMatch.startsWith("-> ")) {
                continue; // This is part of an expression, already handled
            }

            String propertyName = simpleMatcher.group(1);
            String databaseName = simpleMatcher.group(2);
            String tableName = simpleMatcher.group(3);
            String columnName = simpleMatcher.group(4);

            mappings.add(MappingDefinition.PropertyMappingDefinition.column(
                    propertyName,
                    new MappingDefinition.ColumnReference(databaseName, tableName, columnName)));
        }

        // Parse join references for association properties
        // Pattern: propertyName: [DatabaseName]@JoinName
        Pattern joinPattern = Pattern.compile("(\\w+)\\s*:\\s*\\[(\\w+)]\\s*@\\s*(\\w+)");
        Matcher joinMatcher = joinPattern.matcher(body);

        while (joinMatcher.find()) {
            String propertyName = joinMatcher.group(1);
            String databaseName = joinMatcher.group(2);
            String joinName = joinMatcher.group(3);

            mappings.add(MappingDefinition.PropertyMappingDefinition.join(
                    propertyName,
                    new MappingDefinition.JoinReference(databaseName, joinName)));
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

        // Parse testSuites (optional) - reuse the same format as mappings
        List<MappingDefinition.TestSuiteDefinition> testSuites = List.of();
        int testSuitesIdx = body.indexOf("testSuites:");
        if (testSuitesIdx >= 0) {
            String testSuitesBody = body.substring(testSuitesIdx);
            testSuites = parseTestSuites(testSuitesBody);
        }

        return new ParseResult<>(
                ServiceDefinition.of(qualifiedName, pattern, functionBody, documentation, testSuites),
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
