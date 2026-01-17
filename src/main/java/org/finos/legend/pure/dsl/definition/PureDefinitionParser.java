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
            if (remaining.isEmpty()) break;
            
            if (remaining.startsWith("Class ")) {
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
        // Pattern: Class qualified::Name { ... }
        Pattern pattern = Pattern.compile(
                "Class\\s+([\\w:]+)\\s*\\{([^}]*)\\}",
                Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(source);
        
        if (!matcher.find()) {
            throw new PureParseException("Invalid Class definition");
        }
        
        String qualifiedName = matcher.group(1);
        String body = matcher.group(2);
        
        List<ClassDefinition.PropertyDefinition> properties = parseProperties(body);
        
        return new ParseResult<>(
                new ClassDefinition(qualifiedName, properties),
                source.substring(matcher.end())
        );
    }
    
    private static List<ClassDefinition.PropertyDefinition> parseProperties(String body) {
        List<ClassDefinition.PropertyDefinition> properties = new ArrayList<>();
        
        // Pattern: propertyName: Type[multiplicity];
        Pattern pattern = Pattern.compile(
                "(\\w+)\\s*:\\s*(\\w+)\\s*\\[([^\\]]+)\\]\\s*;?"
        );
        Matcher matcher = pattern.matcher(body);
        
        while (matcher.find()) {
            String name = matcher.group(1);
            String type = matcher.group(2);
            String multiplicity = matcher.group(3);
            
            var bounds = parseMultiplicity(multiplicity);
            properties.add(new ClassDefinition.PropertyDefinition(name, type, bounds[0], bounds[1]));
        }
        
        return properties;
    }
    
    private static Integer[] parseMultiplicity(String mult) {
        mult = mult.trim();
        if (mult.equals("*")) {
            return new Integer[]{0, null};
        }
        if (mult.equals("1")) {
            return new Integer[]{1, 1};
        }
        if (mult.contains("..")) {
            String[] parts = mult.split("\\.\\.");
            int lower = Integer.parseInt(parts[0].trim());
            Integer upper = parts[1].trim().equals("*") ? null : Integer.parseInt(parts[1].trim());
            return new Integer[]{lower, upper};
        }
        int val = Integer.parseInt(mult);
        return new Integer[]{val, val};
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
        
        return new ParseResult<>(
                new DatabaseDefinition(qualifiedName, tables),
                source.substring(bodyEnd + 1)
        );
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
            if (part.isEmpty()) continue;
            
            // Pattern: COLUMN_NAME DATA_TYPE [(size)] [PRIMARY KEY] [NOT NULL]
            Pattern pattern = Pattern.compile(
                    "(\\w+)\\s+(\\w+(?:\\s*\\([^)]+\\))?)\\s*(PRIMARY\\s+KEY)?\\s*(NOT\\s+NULL)?",
                    Pattern.CASE_INSENSITIVE
            );
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
                source.substring(bodyEnd + 1)
        );
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
            List<MappingDefinition.PropertyMappingDefinition> propertyMappings = 
                    parsePropertyMappings(mappingBody);
            
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
                    new MappingDefinition.ColumnReference(databaseName, tableName, columnName)
            ));
        }
        
        return mappings;
    }
    
    // ==================== Helper Methods ====================
    
    private static int findMatchingParen(String source, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new PureParseException("Unmatched parenthesis");
    }
    
    private static int findMatchingBrace(String source, int openPos) {
        int depth = 1;
        for (int i = openPos + 1; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new PureParseException("Unmatched brace");
    }
    
    private record ParseResult<T>(T definition, String remaining) {}
}
