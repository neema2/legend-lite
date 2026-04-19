package com.gs.legend.compiled;

import com.gs.legend.model.m3.Multiplicity;
import com.gs.legend.model.m3.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Contract test for the sealed {@link CompiledElement} hierarchy.
 *
 * <p>Exists to make adding a new {@code PackageableElement} kind without
 * updating the switch a compile-time error rather than a silent drift.
 * Every {@code switch (e)} over {@link CompiledElement} anywhere in the
 * codebase (codecs, back-ref derivation, etc.) inherits that same
 * exhaustiveness guarantee as long as this test keeps compiling.
 */
class CompiledElementSealedDispatchTest {

    @Test
    void exhaustiveSwitchCoversEverySubtype() {
        for (CompiledElement e : sampleOfEverySubtype()) {
            assertNotNull(kindOf(e), () -> "kind dispatch missed " + e.getClass().getSimpleName());
            assertNotNull(e.qualifiedName());
            assertNotNull(e.sourceLocation());
        }
    }

    @Test
    void hierarchyCoversAllTenKinds() {
        assertEquals(10, sampleOfEverySubtype().size(),
                "If the hierarchy changes, update sampleOfEverySubtype() and this count deliberately.");
    }

    /** Exhaustive sealed switch. A new permitted subtype forces a compile error here. */
    private static String kindOf(CompiledElement e) {
        return switch (e) {
            case CompiledClass       ignored -> "class";
            case CompiledAssociation ignored -> "association";
            case CompiledMapping     ignored -> "mapping";
            case CompiledFunction    ignored -> "function";
            case CompiledService     ignored -> "service";
            case CompiledEnum        ignored -> "enum";
            case CompiledProfile     ignored -> "profile";
            case CompiledDatabase    ignored -> "database";
            case CompiledConnection  ignored -> "connection";
            case CompiledRuntime     ignored -> "runtime";
        };
    }

    private static List<CompiledElement> sampleOfEverySubtype() {
        var loc = SourceLocation.UNKNOWN;
        return List.of(
                new CompiledClass("m::C", List.of(), List.of(), List.of(), List.of(), List.of(), Map.of(), loc),
                new CompiledAssociation("m::A", List.of(), List.of(), loc),
                new CompiledMapping("m::M", List.of(), loc),
                new CompiledFunction("m::f__", List.of(), Type.Primitive.STRING, Multiplicity.ONE, null, loc),
                new CompiledService("m::s", List.of(), null, loc),
                new CompiledEnum("m::E", List.of(), List.of(), Map.of(), loc),
                new CompiledProfile("m::P", List.of(), List.of(), loc),
                new CompiledDatabase("m::DB", List.of(), List.of(), loc),
                new CompiledConnection("m::Conn", "m::DB", ConnectionKind.RELATIONAL, Map.of(), loc),
                new CompiledRuntime("m::R", List.of(), List.of(), loc));
    }
}
