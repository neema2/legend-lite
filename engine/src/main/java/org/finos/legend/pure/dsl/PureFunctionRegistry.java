package org.finos.legend.pure.dsl;

import org.finos.legend.pure.dsl.definition.FunctionDefinition;
import org.finos.legend.pure.dsl.definition.PureDefinitionParser;

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
            String qualifiedName,
            String simpleName,
            List<String> paramNames,
            String bodySource
    ) {}

    private final Map<String, FunctionEntry> functions = new HashMap<>();

    /**
     * Register a function from its full Pure source code.
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
     * Parse a Pure function definition using the ANTLR parser.
     */
    private FunctionEntry parseFunctionSource(String pureSource) {
        FunctionDefinition def = PureDefinitionParser.parseFunctionDefinition(pureSource);
        
        List<String> paramNames = def.parameters().stream()
                .map(FunctionDefinition.ParameterDefinition::name)
                .toList();
        
        return new FunctionEntry(
                def.qualifiedName(),
                def.simpleName(),
                paramNames,
                def.body()
        );
    }

    /**
     * Register built-in Pure function definitions.
     */
    private static void registerBuiltins(PureFunctionRegistry registry) {
        registry.registerPure("""
            function meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition_filterValues(val: Integer[*]):Boolean[1]
            {
                $val->filter(y | $y->mod(2) == 0)->size() == 2
            }
            """);
    }
}
