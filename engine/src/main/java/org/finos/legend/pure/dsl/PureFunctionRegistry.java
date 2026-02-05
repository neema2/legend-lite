package org.finos.legend.pure.dsl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Registry of Pure function definitions that can be inlined.
 * 
 * Functions are stored with their body as source text. When called,
 * parameters are textually substituted with argument source text,
 * and the result is re-parsed for correct semantic analysis.
 * 
 * This is Tier 2 of the 3-tier function resolution:
 * - Tier 1: Built-in SQL generators (filter, select, groupBy)
 * - Tier 2: Pure function inlining (this registry)
 * - Tier 3: Passthrough to SQL functions
 */
public class PureFunctionRegistry {

    /**
     * Represents a registered Pure function.
     */
    public record FunctionEntry(
            String qualifiedName,      // Full package path
            String simpleName,         // Just the function name
            List<String> paramNames,   // Parameter names for substitution
            String bodySource          // Function body as Pure source text
    ) {}

    private final Map<String, FunctionEntry> functions = new HashMap<>();

    /**
     * Register a function from its full Pure source code.
     * 
     * Example:
     * registry.registerPure("""
     *     function meta::pure::test::myFunc(x: Integer[1]):Boolean[1]
     *     {
     *         $x > 0
     *     }
     *     """);
     */
    public void registerPure(String pureSource) {
        FunctionEntry entry = parseFunctionSource(pureSource);
        functions.put(entry.qualifiedName(), entry);
        functions.put(entry.simpleName(), entry);
    }

    /**
     * Look up a function by name (qualified or simple).
     */
    public Optional<FunctionEntry> getFunction(String name) {
        return Optional.ofNullable(functions.get(name));
    }

    /**
     * Check if a function is registered.
     */
    public boolean hasFunction(String name) {
        return functions.containsKey(name);
    }

    /**
     * Create a registry with built-in Pure functions.
     */
    public static PureFunctionRegistry withBuiltins() {
        PureFunctionRegistry registry = new PureFunctionRegistry();
        registerBuiltins(registry);
        return registry;
    }

    /**
     * Parse a Pure function definition to extract metadata.
     */
    private FunctionEntry parseFunctionSource(String source) {
        // Find "function" keyword and extract name
        int funcIndex = source.indexOf("function");
        if (funcIndex < 0) {
            throw new IllegalArgumentException("Cannot find 'function' keyword in: " + source);
        }
        
        // Find opening paren for parameters
        int parenStart = source.indexOf('(', funcIndex);
        if (parenStart < 0) {
            throw new IllegalArgumentException("Cannot find '(' in function definition");
        }
        
        // Extract qualified name (between "function" and "(")
        String qualifiedName = source.substring(funcIndex + 8, parenStart).trim();
        // Remove generic type parameters like <T|m>
        int genericStart = qualifiedName.indexOf('<');
        if (genericStart > 0) {
            qualifiedName = qualifiedName.substring(0, genericStart);
        }
        
        // Extract simple name (after last ::)
        int lastSep = qualifiedName.lastIndexOf("::");
        String simpleName = lastSep >= 0 ? qualifiedName.substring(lastSep + 2) : qualifiedName;
        
        // Extract parameters
        int parenEnd = findMatchingParen(source, parenStart);
        String paramsSection = source.substring(parenStart + 1, parenEnd).trim();
        List<String> paramNames = extractParamNames(paramsSection);
        
        // Extract body (between { and })
        int bodyStart = source.indexOf('{', parenEnd);
        int bodyEnd = findMatchingBrace(source, bodyStart);
        String bodySource = source.substring(bodyStart + 1, bodyEnd).trim();
        
        // Remove trailing semicolon from body
        if (bodySource.endsWith(";")) {
            bodySource = bodySource.substring(0, bodySource.length() - 1).trim();
        }
        
        return new FunctionEntry(qualifiedName, simpleName, paramNames, bodySource);
    }

    private List<String> extractParamNames(String paramsSection) {
        List<String> names = new ArrayList<>();
        if (paramsSection.isEmpty()) {
            return names;
        }
        // Split by comma, extract name before colon
        for (String param : paramsSection.split(",")) {
            param = param.trim();
            int colonPos = param.indexOf(':');
            if (colonPos > 0) {
                names.add(param.substring(0, colonPos).trim());
            }
        }
        return names;
    }

    private int findMatchingParen(String s, int openPos) {
        int depth = 0;
        for (int i = openPos; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new IllegalArgumentException("No matching ) found");
    }

    private int findMatchingBrace(String s, int openPos) {
        int depth = 0;
        for (int i = openPos; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '{') depth++;
            else if (c == '}') {
                depth--;
                if (depth == 0) return i;
            }
        }
        throw new IllegalArgumentException("No matching } found");
    }

    /**
     * Register built-in Pure function definitions.
     */
    private static void registerBuiltins(PureFunctionRegistry registry) {
        // PCT test helper: testVariantColumn_functionComposition_filterValues
        registry.registerPure("""
            function meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition_filterValues(val: Integer[*]):Boolean[1]
            {
                $val->filter(y | $y->mod(2) == 0)->size() == 2
            }
            """);
    }
}
