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
                var result = parseMapping(remaining);
                definitions.add(result.definition);
                remaining = result.remaining;
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
     * Parses a single Mapping definition.
     */
    public static MappingDefinition parseMappingDefinition(String pureSource) {
        var result = parseMapping(pureSource.trim());
        return result.definition;
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

        // Pattern: [package::]ClassName: MappingType { ... }
        // Captures full qualified name (e.g., "model::Person" or just "Person"), then
        // mapping type
        // Changed from negative lookbehind (which incorrectly matched "erson") to
        // capturing qualified names
        Pattern pattern = Pattern.compile("([\\w:]+)\\s*:\\s*(Relational|Pure)\\s*\\{");
        Matcher matcher = pattern.matcher(body);

        while (matcher.find()) {
            String qualifiedClassName = matcher.group(1);
            // Extract simple name (after last ::)
            String className = qualifiedClassName.contains("::")
                    ? qualifiedClassName.substring(qualifiedClassName.lastIndexOf("::") + 2)
                    : qualifiedClassName;
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
