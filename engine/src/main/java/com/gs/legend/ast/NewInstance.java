package com.gs.legend.ast;

import java.util.List;
import java.util.Map;

/**
 * AST node for {@code new(PackageableElementPtr(className), NewInstance(...))} —
 * the desugared form of struct literal syntax {@code ^ClassName(prop=value)}.
 *
 * <p>Sibling of {@link ColSpec} / {@link ColSpecArray}: a parser-emitted DSL literal
 * that denotes an instance of a user class without runtime evaluation.
 *
 * @param className     Raw class name as written in source (may be simple or qualified)
 * @param properties    Property name → expression AST, in source order (LinkedHashMap)
 * @param typeArguments Generic type arguments for parameterized classes (e.g., {@code Pair<Integer, String>})
 */
public record NewInstance(
        String className,
        Map<String, ValueSpecification> properties,
        List<String> typeArguments) implements ValueSpecification {

    public NewInstance(String className, Map<String, ValueSpecification> properties) {
        this(className, properties, List.of());
    }
}
