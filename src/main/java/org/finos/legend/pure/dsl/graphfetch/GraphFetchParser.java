package org.finos.legend.pure.dsl.graphfetch;

import org.finos.legend.pure.dsl.PureParseException;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for graphFetch tree syntax.
 * 
 * Parses: #{ ClassName { prop1, prop2, nested { subProp } } }#
 * 
 * The parser handles:
 * - Simple properties: firstName, lastName
 * - Nested properties: addresses { street, city }
 * - Whitespace and newlines between elements
 */
public final class GraphFetchParser {

    // Pattern to extract content between #{ and }#
    private static final Pattern TREE_PATTERN = Pattern.compile(
            "#\\{\\s*(.+?)\\s*}#",
            Pattern.DOTALL);

    // Pattern to match class name at start: "ClassName {"
    private static final Pattern CLASS_START_PATTERN = Pattern.compile(
            "^\\s*(\\w+(?:::\\w+)*)\\s*\\{",
            Pattern.DOTALL);

    /**
     * Parses a graphFetch tree string.
     * 
     * @param treeString The tree string including #{ }# delimiters
     * @return The parsed GraphFetchTree
     * @throws PureParseException if parsing fails
     */
    public static GraphFetchTree parse(String treeString) {
        // Extract content between #{ and }#
        Matcher matcher = TREE_PATTERN.matcher(treeString.trim());
        if (!matcher.matches()) {
            throw new PureParseException("Invalid graphFetch tree syntax: " + treeString);
        }

        String content = matcher.group(1).trim();
        return parseTreeContent(content);
    }

    /**
     * Parses tree content without the #{ }# delimiters.
     */
    private static GraphFetchTree parseTreeContent(String content) {
        // Extract class name
        Matcher classMatch = CLASS_START_PATTERN.matcher(content);
        if (!classMatch.find()) {
            throw new PureParseException("Expected 'ClassName {' at start of graphFetch tree: " + content);
        }

        String className = classMatch.group(1);

        // Find the matching closing brace
        int braceStart = classMatch.end() - 1; // Position of opening {
        int braceEnd = findMatchingBrace(content, braceStart);

        // Extract properties content
        String propsContent = content.substring(braceStart + 1, braceEnd).trim();

        List<GraphFetchTree.PropertyFetch> properties = parseProperties(propsContent);

        return new GraphFetchTree(className, properties);
    }

    /**
     * Parses comma-separated properties, handling nested structures.
     */
    private static List<GraphFetchTree.PropertyFetch> parseProperties(String propsContent) {
        List<GraphFetchTree.PropertyFetch> properties = new ArrayList<>();

        if (propsContent.isEmpty()) {
            return properties;
        }

        // Split by comma, but respect nested braces
        List<String> parts = splitByComma(propsContent);

        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty())
                continue;

            // Check if this is a nested property: "name { ... }"
            int bracePos = part.indexOf('{');
            if (bracePos > 0) {
                // Nested property
                String propName = part.substring(0, bracePos).trim();

                // Find matching brace
                int braceEnd = findMatchingBrace(part, bracePos);
                String nestedContent = part.substring(bracePos + 1, braceEnd).trim();

                // Recursively parse nested properties
                List<GraphFetchTree.PropertyFetch> nestedProps = parseProperties(nestedContent);
                GraphFetchTree subTree = new GraphFetchTree(propName, nestedProps);

                properties.add(GraphFetchTree.PropertyFetch.nested(propName, subTree));
            } else {
                // Simple property
                properties.add(GraphFetchTree.PropertyFetch.simple(part));
            }
        }

        return properties;
    }

    /**
     * Splits string by comma, respecting nested braces.
     */
    private static List<String> splitByComma(String content) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(content.substring(start, i).trim());
                start = i + 1;
            }
        }

        // Add last part
        if (start < content.length()) {
            parts.add(content.substring(start).trim());
        }

        return parts;
    }

    /**
     * Finds the matching closing brace for an opening brace.
     */
    private static int findMatchingBrace(String content, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < content.length(); i++) {
            char c = content.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        throw new PureParseException("Unmatched brace in graphFetch tree");
    }
}
