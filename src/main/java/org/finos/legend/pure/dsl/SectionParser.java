package org.finos.legend.pure.dsl;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses Pure source files into sections based on ### directives.
 * 
 * Legend Pure files use section markers like ###Pure, ###Relational,
 * ###Mapping, etc.
 * to indicate which DSL grammar should parse each section.
 */
public class SectionParser {

    /**
     * Supported section types, matching Legend Engine's DSLs.
     */
    public enum SectionType {
        PURE, // Default - Domain elements (Class, Enum, Function, Association, Profile)
        RELATIONAL, // Database definitions
        MAPPING, // Mapping definitions
        CONNECTION, // Connection definitions (RelationalDatabaseConnection)
        RUNTIME // Runtime definitions
    }

    /**
     * Represents a parsed section of a Pure file.
     */
    public record Section(
            SectionType type,
            String content,
            int startLine,
            int endLine) {
    }

    // Pattern to match section markers: ###SectionName (with optional leading
    // whitespace)
    private static final Pattern SECTION_MARKER = Pattern.compile("^\\s*###(\\w+)\\s*$", Pattern.MULTILINE);

    /**
     * Parse a Pure source file into sections.
     * Content before any section marker is treated as ###Pure by default.
     */
    public static List<Section> parse(String source) {
        List<Section> sections = new ArrayList<>();
        Matcher matcher = SECTION_MARKER.matcher(source);

        int lastEnd = 0;
        SectionType currentType = SectionType.PURE; // Default section type
        int currentStartLine = 1;

        while (matcher.find()) {
            // Content before this marker belongs to current section
            if (matcher.start() > lastEnd) {
                String content = source.substring(lastEnd, matcher.start()).trim();
                if (!content.isEmpty()) {
                    int endLine = countLines(source, 0, matcher.start());
                    sections.add(new Section(currentType, content, currentStartLine, endLine));
                }
            }

            // Parse the new section type
            String sectionName = matcher.group(1);
            currentType = parseSectionType(sectionName);
            currentStartLine = countLines(source, 0, matcher.end()) + 1;
            lastEnd = matcher.end();
        }

        // Handle remaining content after last marker
        if (lastEnd < source.length()) {
            String content = source.substring(lastEnd).trim();
            if (!content.isEmpty()) {
                int endLine = countLines(source, 0, source.length());
                sections.add(new Section(currentType, content, currentStartLine, endLine));
            }
        }

        return sections;
    }

    /**
     * Parse section type from marker name.
     * Returns PURE for unknown section types.
     */
    private static SectionType parseSectionType(String name) {
        return switch (name.toLowerCase()) {
            case "pure" -> SectionType.PURE;
            case "relational" -> SectionType.RELATIONAL;
            case "mapping" -> SectionType.MAPPING;
            case "connection" -> SectionType.CONNECTION;
            case "runtime" -> SectionType.RUNTIME;
            default -> SectionType.PURE; // Default to Pure for unknown sections
        };
    }

    /**
     * Count lines in a substring.
     */
    private static int countLines(String source, int start, int end) {
        int lines = 1;
        for (int i = start; i < end && i < source.length(); i++) {
            if (source.charAt(i) == '\n') {
                lines++;
            }
        }
        return lines;
    }

    /**
     * Convenience method to detect section type from a simple content string
     * that may not have section markers. Uses keyword detection.
     */
    public static SectionType detectSectionType(String content) {
        String trimmed = content.trim();

        // Check for section marker first
        if (trimmed.startsWith("###")) {
            Matcher m = SECTION_MARKER.matcher(trimmed);
            if (m.find()) {
                return parseSectionType(m.group(1));
            }
        }

        // Detect by first keyword
        if (trimmed.startsWith("Database "))
            return SectionType.RELATIONAL;
        if (trimmed.startsWith("Mapping "))
            return SectionType.MAPPING;
        if (trimmed.startsWith("RelationalDatabaseConnection "))
            return SectionType.CONNECTION;
        if (trimmed.startsWith("Runtime "))
            return SectionType.RUNTIME;
        if (trimmed.startsWith("SingleConnectionRuntime "))
            return SectionType.RUNTIME;

        // Default to Pure (Class, Enum, function, etc.)
        return SectionType.PURE;
    }
}
