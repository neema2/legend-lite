package com.gs.legend.parser;

import com.gs.legend.model.PureModelBuilder;
import com.gs.legend.model.m3.PureClass;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Dedicated regression test for the phase-2.5e fix in {@code NameResolver.resolveClass}:
 * when {@code resolveClass} rebuilds a {@code ClassDefinition} because some property type
 * or stereotype was canonicalized, it must preserve {@code typeParams} and {@code isNative}.
 *
 * <p>The original bug used the backward-compat 7-arg {@code ClassDefinition} constructor in
 * the rebuild path, which silently defaulted {@code typeParams = []} and {@code isNative = false}.
 * The symptom was non-local: a generic class like {@code Class Foo<T> { x: T[1]; y: String[1]; }}
 * would lose its type-params whenever <em>any</em> property on the class triggered an
 * import-scope rewrite (e.g., {@code String} → {@code meta::pure::metamodel::type::String}).
 *
 * <p>Without a direct test a future refactor could regress this and only fail with a
 * confusing downstream symptom ("property T unknown") instead of the real cause.
 */
class NameResolverGenericsTest {

    @Test
    void resolveClassPreservesTypeParamsWhenPropertyTypesTriggerRebuild() {
        // Choose property types that FORCE NameResolver to rebuild the ClassDefinition:
        //   - String / Integer are primitives; import resolution canonicalises them to their
        //     FQN form, so resolveProperty produces a new PropertyDefinition and the
        //     resolveClass short-circuit (line 106-112) does NOT fire.
        //
        // This reproduces the exact failure mode of the original bug.
        String source = """
                Class model::Generic<T, U>
                {
                    id: T[1];
                    label: String[1];
                    count: Integer[1];
                    extra: U[0..1];
                }
                """;
        PureClass generic = new PureModelBuilder().addSource(source)
                .findClass("model::Generic").orElseThrow();

        assertEquals(List.of("T", "U"), generic.typeParams(),
                "Type parameters [T, U] must survive NameResolver's rebuild pass");
        assertEquals(4, generic.properties().size(),
                "All four properties must be preserved through the rebuild");
    }

    @Test
    void resolveClassPreservesIsNativeFlagWhenPropertyTypesTriggerRebuild() {
        // Same rebuild-trigger pattern, but on a native Class declaration. If the rebuild
        // path dropped isNative, the seeded symbol would look like an ordinary user class.
        String source = """
                native Class model::NativeGeneric<T>
                {
                    placeholder: String[1];
                }
                """;
        PureClass nativeGeneric = new PureModelBuilder().addSource(source)
                .findClass("model::NativeGeneric").orElseThrow();

        assertTrue(nativeGeneric.isNative(),
                "native Class flag must survive NameResolver's rebuild pass");
        assertEquals(List.of("T"), nativeGeneric.typeParams(),
                "Type parameters on a native generic must also be preserved");
    }
}
