package com.gs.legend.ast;

import java.util.List;
import java.util.Map;

/**
 * Data holder for instance expressions ({@code ^ClassName(prop=value)}).
 *
 * <p>Carried as the value of a {@link ClassInstance} with type {@code "instance"} inside
 * the {@code new(PE(className), ClassInstance("instance", data))} form that parsers emit
 * for struct literals.
 *
 * <p>Lives in {@code com.gs.legend.ast} so downstream layers (compiler, plangen)
 * consume a parser-neutral shape instead of importing from the parser package.
 *
 * @param className     Raw class name as written in source (may be simple or qualified)
 * @param properties    Property name → expression AST, in source order (LinkedHashMap)
 * @param typeArguments Generic type arguments for parameterized classes (e.g., {@code Pair<Integer, String>})
 */
public record InstanceData(
        String className,
        Map<String, ValueSpecification> properties,
        List<String> typeArguments) {

    public InstanceData(String className, Map<String, ValueSpecification> properties) {
        this(className, properties, List.of());
    }
}
