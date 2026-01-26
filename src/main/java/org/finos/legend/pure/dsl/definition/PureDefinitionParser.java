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
        String remaining = preprocessed.cleanedSource;

        // Track position in original source for error reporting
        // Count leading whitespace removed by any trim operations
        int leadingNewlines = 0;
        for (int i = 0; i < remaining.length() && Character.isWhitespace(remaining.charAt(i)); i++) {
            if (remaining.charAt(i) == '\n')
                leadingNewlines++;
        }
        remaining = remaining.trim();
        int lineOffset = leadingNewlines; // Lines consumed from start

        while (!remaining.isEmpty()) {
            // Track newlines consumed by trimming leading whitespace between definitions
            int leadingNl = countLeadingNewlines(remaining);
            lineOffset += leadingNl;
            remaining = remaining.trim();
            if (remaining.isEmpty())
                break;

            if (remaining.startsWith("Class ")) {
                // Find the class body brace - it's the first '{' not preceded by '>>' (tagged
                // values)
                int bodyStart = findClassBodyBrace(remaining);
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Class definition - missing class body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String classSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                ClassDefinition classDef = parseClassDefinition(classSource, lineOffset);
                definitions.add(classDef);

                // Calculate new line offset by counting newlines consumed
                lineOffset += countNewlines(classSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("Database ")) {
                // Database uses parentheses, not braces: Database name ( ... )
                int bodyStart = remaining.indexOf('(');
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Database definition - missing body", lineOffset + 1, 0);
                }

                int parenEnd = findMatchingParen(remaining, bodyStart);
                String dbSource = remaining.substring(0, parenEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                DatabaseDefinition dbDef = parseDatabaseDefinition(dbSource, lineOffset);
                definitions.add(dbDef);

                lineOffset += countNewlines(dbSource);
                remaining = remaining.substring(parenEnd + 1);
            } else if (remaining.startsWith("Mapping ")) {
                // Find the mapping body paren - Mapping uses ( ... ) not { ... }
                int parenStart = remaining.indexOf('(');
                if (parenStart < 0) {
                    throw new PureParseException("Invalid Mapping definition - missing body", lineOffset + 1, 0);
                }

                int parenEnd = findMatchingParen(remaining, parenStart);
                String mappingSource = remaining.substring(0, parenEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                MappingDefinition mappingDef = parseMappingDefinition(mappingSource);
                definitions.add(mappingDef);

                lineOffset += countNewlines(mappingSource);
                remaining = remaining.substring(parenEnd + 1);
            } else if (remaining.startsWith("Association ")) {
                // Find the association body brace - same as Class (first '{' not preceded by
                // '>>')
                int bodyStart = findClassBodyBrace(remaining);
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Association definition - missing body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String assocSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                AssociationDefinition assocDef = parseAssociationDefinition(assocSource, lineOffset);
                definitions.add(assocDef);

                lineOffset += countNewlines(assocSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("Service ")) {
                // Find the service body brace - first '{' (Service has no stereotypes on itself
                // typically)
                int bodyStart = remaining.indexOf('{');
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Service definition - missing body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String serviceSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                ServiceDefinition serviceDef = parseServiceDefinition(serviceSource, lineOffset);
                definitions.add(serviceDef);

                lineOffset += countNewlines(serviceSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("Enum ")) {
                // Find the enum body brace - similar to Class, first '{' not preceded by '>>'
                int bodyStart = findClassBodyBrace(remaining); // Same logic works for Enum
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Enum definition - missing body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String enumSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                EnumDefinition enumDef = parseEnumDefinition(enumSource, lineOffset);
                definitions.add(enumDef);

                lineOffset += countNewlines(enumSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("Profile ")) {
                // Find the profile body brace - first '{' (Profile has no stereotypes on
                // itself)
                int bodyStart = remaining.indexOf('{');
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Profile definition - missing body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String profileSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                ProfileDefinition profileDef = parseProfileDefinition(profileSource, lineOffset);
                definitions.add(profileDef);

                lineOffset += countNewlines(profileSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("function ")) {
                // Find the function body brace - first '{' after the signature
                int bodyStart = remaining.indexOf('{');
                if (bodyStart < 0) {
                    throw new PureParseException("Invalid Function definition - missing body", lineOffset + 1, 0);
                }

                int braceEnd = findMatchingBrace(remaining, bodyStart, lineOffset);
                String funcSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                FunctionDefinition funcDef = parseFunctionDefinition(funcSource, lineOffset);
                definitions.add(funcDef);

                lineOffset += countNewlines(funcSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("RelationalDatabaseConnection ")) {
                // Find the end of the definition
                int braceStart = remaining.indexOf('{');
                int braceEnd = findMatchingBrace(remaining, braceStart);
                String connectionSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                ConnectionDefinition connDef = parseConnectionDefinition(connectionSource, lineOffset);
                definitions.add(connDef);

                lineOffset += countNewlines(connectionSource);
                remaining = remaining.substring(braceEnd + 1);
            } else if (remaining.startsWith("Runtime ")) {
                // Find the end of the definition
                int braceStart = remaining.indexOf('{');
                int braceEnd = findMatchingBrace(remaining, braceStart);
                String runtimeSource = remaining.substring(0, braceEnd + 1);

                // Parse using ANTLR with line offset for accurate error reporting
                RuntimeDefinition runtimeDef = parseRuntimeDefinition(runtimeSource, lineOffset);
                definitions.add(runtimeDef);

                lineOffset += countNewlines(runtimeSource);
                remaining = remaining.substring(braceEnd + 1);
            } else {
                // Unknown definition - report error at current position with line offset
                // lineOffset is 0-based (lines consumed), so add 1 for 1-based line number
                throw new PureParseException(
                        "Unknown definition starting with: " + remaining.substring(0, Math.min(50, remaining.length())),
                        lineOffset + 1, 0);
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
        // First, strip block comments /* ... */ but preserve newlines for line number
        // mapping
        // Replace each block comment with the same number of newlines it contained
        StringBuilder withoutBlockComments = new StringBuilder();
        int i = 0;
        while (i < source.length()) {
            if (i + 1 < source.length() && source.charAt(i) == '/' && source.charAt(i + 1) == '*') {
                // Found start of block comment, find the end
                int end = source.indexOf("*/", i + 2);
                if (end < 0) {
                    end = source.length(); // Unclosed comment, go to end
                } else {
                    end += 2; // Include */
                }
                // Count newlines in the comment and preserve them
                String comment = source.substring(i, end);
                for (char c : comment.toCharArray()) {
                    if (c == '\n') {
                        withoutBlockComments.append('\n');
                    }
                }
                i = end;
            } else {
                withoutBlockComments.append(source.charAt(i));
                i++;
            }
        }

        ImportScope imports = new ImportScope();
        StringBuilder cleaned = new StringBuilder();

        for (String line : withoutBlockComments.toString().split("\n")) {
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

            // Skip section headers but preserve newline for line number mapping
            if (trimmed.startsWith("###")) {
                cleaned.append("\n");
                continue;
            }

            // Parse import statements but preserve newline for line number mapping
            if (trimmed.startsWith("import ") && trimmed.contains("::")) {
                String importPath = trimmed.substring(7).replace(";", "").trim();
                imports.addImport(importPath);
                cleaned.append("\n");
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
     * Parses a single Class definition using ANTLR.
     */
    public static ClassDefinition parseClassDefinition(String pureSource) {
        return parseClassDefinition(pureSource, 0);
    }

    /**
     * Parses a single Class definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static ClassDefinition parseClassDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstClassDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No class definition found in source"));
        } catch (PureParseException e) {
            if (lineOffset > 0 && e.hasLocation()) {
                // Adjust line number for the offset within the original source
                throw new PureParseException(
                        e.getMessage().replace("line " + e.getLine(), "line " + (e.getLine() + lineOffset)),
                        e.getLine() + lineOffset, e.getColumn());
            }
            throw e;
        }
    }

    /**
     * Parses a single Profile definition using ANTLR.
     */
    public static ProfileDefinition parseProfileDefinition(String pureSource) {
        return parseProfileDefinition(pureSource, 0);
    }

    /**
     * Parses a single Profile definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static ProfileDefinition parseProfileDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstProfileDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No profile definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    /**
     * Parses a single Function definition using ANTLR.
     */
    public static FunctionDefinition parseFunctionDefinition(String pureSource) {
        return parseFunctionDefinition(pureSource, 0);
    }

    /**
     * Parses a single Function definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static FunctionDefinition parseFunctionDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstFunctionDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No function definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    /**
     * Parses a single Connection definition using ANTLR.
     */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource) {
        return parseConnectionDefinition(pureSource, 0);
    }

    /**
     * Parses a single Connection definition using ANTLR with line offset.
     */
    public static ConnectionDefinition parseConnectionDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstConnectionDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No connection definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    /**
     * Parses a single Runtime definition using ANTLR.
     */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource) {
        return parseRuntimeDefinition(pureSource, 0);
    }

    /**
     * Parses a single Runtime definition using ANTLR with line offset.
     */
    public static RuntimeDefinition parseRuntimeDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstRuntimeDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No runtime definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    /**
     * Parses a single Database definition using ANTLR.
     */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource) {
        return parseDatabaseDefinition(pureSource, 0);
    }

    /**
     * Parses a single Database definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static DatabaseDefinition parseDatabaseDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstDatabaseDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No database definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    /**
     * Adjusts the line number in a parse exception by adding the given offset.
     * This is used when parsing a substring of a larger source to report
     * accurate line numbers for IDE integration.
     */
    private static PureParseException adjustErrorLineOffset(PureParseException e, int lineOffset) {
        if (lineOffset > 0 && e.hasLocation()) {
            return new PureParseException(
                    e.getMessage().replace("line " + e.getLine(), "line " + (e.getLine() + lineOffset)),
                    e.getLine() + lineOffset, e.getColumn());
        }
        return e;
    }

    /**
     * Parses a single Mapping definition using ANTLR.
     */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        // Use ANTLR parser
        org.antlr.v4.runtime.CharStream input = org.antlr.v4.runtime.CharStreams.fromString(pureSource);
        org.finos.legend.pure.dsl.antlr.PureLexer lexer = new org.finos.legend.pure.dsl.antlr.PureLexer(input);
        org.antlr.v4.runtime.CommonTokenStream tokens = new org.antlr.v4.runtime.CommonTokenStream(lexer);
        org.finos.legend.pure.dsl.antlr.PureParser parser = new org.finos.legend.pure.dsl.antlr.PureParser(tokens);

        // Parse the definition
        org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext ctx = parser.definition();

        // Extract mapping definition
        return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder.extractFirstMappingDefinition(ctx)
                .orElseThrow(() -> new PureParseException("Invalid Mapping definition"));
    }

    /**
     * Count the number of newlines in a string.
     */
    private static int countNewlines(String s) {
        int count = 0;
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) == '\n')
                count++;
        }
        return count;
    }

    /**
     * Count newlines in leading whitespace only.
     */
    private static int countLeadingNewlines(String s) {
        int count = 0;
        for (int i = 0; i < s.length() && Character.isWhitespace(s.charAt(i)); i++) {
            if (s.charAt(i) == '\n')
                count++;
        }
        return count;
    }

    /**
     * Finds the opening brace of a Class body.
     * 
     * Class syntax: Class [<<...>>]* [{taggedValues}]? Name [extends...]?
     * [constraints]? { body }
     * 
     * The class body { is the first one NOT immediately preceded by '>>' (which
     * would be tagged values).
     * 
     * @param source Source starting with "Class "
     * @return Position of the class body opening brace, or -1 if not found
     */
    private static int findClassBodyBrace(String source) {
        int pos = 0;
        while (pos < source.length()) {
            int bracePos = source.indexOf('{', pos);
            if (bracePos < 0)
                return -1;

            // Check if preceded by '>>' (tagged values brace)
            int checkPos = bracePos - 1;
            while (checkPos >= 0 && Character.isWhitespace(source.charAt(checkPos)))
                checkPos--;

            if (checkPos >= 1 && source.charAt(checkPos) == '>' && source.charAt(checkPos - 1) == '>') {
                // This is a tagged values brace, skip past it
                pos = findMatchingBrace(source, bracePos) + 1;
            } else {
                // This is the class body brace
                return bracePos;
            }
        }
        return -1;
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
     * Parses a single Association definition using ANTLR.
     */
    public static AssociationDefinition parseAssociationDefinition(String pureSource) {
        return parseAssociationDefinition(pureSource, 0);
    }

    /**
     * Parses a single Association definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static AssociationDefinition parseAssociationDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstAssociationDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No association definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
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
        // Calculate line number from source up to openPos
        int line = 1 + countNewlines(source.substring(0, Math.min(openPos, source.length())));
        throw new PureParseException("Unmatched parenthesis at line " + line, line, 0);
    }

    private static int findMatchingBrace(String source, int openPos) {
        return findMatchingBrace(source, openPos, 0);
    }

    private static int findMatchingBrace(String source, int openPos, int baseLineOffset) {
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
        // Point to the opening brace - more actionable for the user
        int line = baseLineOffset + 1 + countNewlines(source.substring(0, openPos));
        throw new PureParseException("Unmatched brace - missing closing '}'", line, 0);
    }

    private record ParseResult<T>(T definition, String remaining) {
    }

    // ==================== Service Parsing ====================

    /**
     * Parses a single Service definition using ANTLR.
     */
    public static ServiceDefinition parseServiceDefinition(String pureSource) {
        return parseServiceDefinition(pureSource, 0);
    }

    /**
     * Parses a single Service definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static ServiceDefinition parseServiceDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstServiceDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No service definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }

    // ==================== Enum Parsing ====================

    /**
     * Parses a single Enum definition using ANTLR.
     * 
     * Syntax: Enum [<<stereotypes>>] [taggedValues] package::Name { VALUE1, VALUE2,
     * VALUE3 }
     */
    public static EnumDefinition parseEnumDefinition(String pureSource) {
        return parseEnumDefinition(pureSource, 0);
    }

    /**
     * Parses a single Enum definition using ANTLR with line offset.
     * Line offset is added to ANTLR-reported line numbers for accurate error
     * location.
     */
    public static EnumDefinition parseEnumDefinition(String pureSource, int lineOffset) {
        try {
            org.finos.legend.pure.dsl.antlr.PureParser.DefinitionContext tree = org.finos.legend.pure.dsl.PureParser
                    .parseDefinition(pureSource);
            return org.finos.legend.pure.dsl.antlr.PureDefinitionBuilder
                    .extractFirstEnumDefinition(tree)
                    .orElseThrow(() -> new PureParseException("No enum definition found in source"));
        } catch (PureParseException e) {
            throw adjustErrorLineOffset(e, lineOffset);
        }
    }
}
