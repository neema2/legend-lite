package com.legend.resolver;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.PureModelContext;
import com.legend.compiler.spec.PhaseHCensusTest;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.error.MappingResolutionException;
import com.legend.normalizer.ModelNormalizer;
import com.legend.parser.ElementParser;
import com.legend.model.NormalizedModel;
import com.legend.model.MappingDefinition;
import com.legend.model.PackageableElement;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * M-H2a — {@link ClassSource} extraction (docs/PHASE_H2_H3_RESOLVER_PLAN.md).
 * Every RELATIONAL/PURE class binding in the census battery must extract
 * (pipeline + rowVar + binding table) or fail loud-by-design; misses are
 * {@link MappingResolutionException}s naming class and mapping; contract
 * violations are {@code IllegalStateException}s.
 */
class ClassSourceTest {

    private record Ctx(PureModelContext model, ClassSources sources) {}

    private static Ctx load(String source) {
        NormalizedModel normalized = ModelNormalizer.normalize(
                NameResolver.resolve(ElementParser.parse(source)));
        PureModelContext ctx = PureModelContext.from(normalized);
        return new Ctx(ctx, new ClassSources(ctx, new SpecCompiler(ctx)));
    }

    private static List<MappingDefinition> mappingsOf(NormalizedModel m) {
        return m.elements().stream()
                .filter(e -> e instanceof MappingDefinition)
                .map(e -> (MappingDefinition) e)
                .toList();
    }

    @Test
    @DisplayName("every class binding in the census battery extracts")
    void censusBatteryExtracts() {
        int extracted = 0;
        for (var fx : PhaseHCensusTest.FIXTURES.entrySet()) {
            NormalizedModel normalized = ModelNormalizer.normalize(
                    NameResolver.resolve(ElementParser.parse(fx.getValue())));
            PureModelContext ctx = PureModelContext.from(normalized);
            ClassSources sources = new ClassSources(ctx, new SpecCompiler(ctx));
            for (MappingDefinition md : mappingsOf(normalized)) {
                for (MappingDefinition.ClassBinding cb : md.classBindings()) {
                    ClassSource cs;
                    try {
                        cs = sources.get(md.qualifiedName(), cb.classFqn());
                    } catch (RuntimeException e) {
                        fail(fx.getKey() + " / " + cb.classFqn() + ": " + e.getMessage());
                        return;
                    }
                    assertNotNull(cs.pipeline(), fx.getKey());
                    assertFalse(cs.rowVar().isEmpty(), fx.getKey());
                    assertFalse(cs.bindings().isEmpty(),
                            fx.getKey() + " / " + cb.classFqn() + ": empty binding table");
                    assertFalse(cs.rowType().columns().isEmpty(),
                            fx.getKey() + " / " + cb.classFqn() + ": empty row type");
                    extracted++;
                }
            }
        }
        // The battery currently carries 12 class bindings across its 10
        // fixtures (assoc bindings and prop-hats are not class bindings).
        // A drop means extraction silently skipped a binding kind.
        assertTrue(extracted >= 12, "expected >= 12 extractions, got " + extracted);
    }

    @Test
    @DisplayName("extraction: pipeline is the body minus the map terminal; bindings are the ^Class fields")
    void extractionSplitsTheTerminal() {
        Ctx c = load("""
            Class m::Person { name: String[1]; age: Integer[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), AGE INTEGER) )
            Mapping m::M ( *m::Person: Relational { ~mainTable [s::DB] T
              name: T.NAME, age: T.AGE } )
            """);
        ClassSource cs = c.sources().get("m::M", "m::Person");
        assertEquals("m::M", cs.mappingFqn());
        assertEquals("m::Person", cs.classFqn());
        assertEquals("row", cs.rowVar());
        assertEquals(List.of("name", "age"), List.copyOf(cs.bindings().keySet()),
                "binding order follows the ^Class declaration order");
        // The terminal itself must NOT be in the extraction output.
        for (TypedSpec b : cs.bindings().values()) {
            assertFalse(b instanceof TypedNewInstance,
                    "scalar bindings are expressions, not constructors");
        }
        assertEquals(List.of("NAME", "AGE"),
                cs.rowType().columns().stream().map(col -> col.name()).toList(),
                "row type is the main table's schema");
    }

    @Test
    @DisplayName("memo: same (mapping, class) returns the same instance")
    void memoized() {
        Ctx c = load("""
            Class m::P { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T name: T.NAME } )
            """);
        assertSame(c.sources().get("m::M", "m::P"), c.sources().get("m::M", "m::P"));
    }

    @Test
    @DisplayName("unmapped class is a loud user-facing error naming class and mapping")
    void unmappedClassIsLoud() {
        Ctx c = load("""
            Class m::P { name: String[1]; }
            Class m::Q { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T name: T.NAME } )
            """);
        MappingResolutionException e = assertThrows(MappingResolutionException.class,
                () -> c.sources().get("m::M", "m::Q"));
        assertTrue(e.getMessage().contains("m::Q"), e.getMessage());
        assertTrue(e.getMessage().contains("m::M"), e.getMessage());
    }

    @Test
    @DisplayName("unknown mapping is a loud user-facing error")
    void unknownMappingIsLoud() {
        Ctx c = load("""
            Class m::P { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50)) )
            Mapping m::M ( *m::P: Relational { ~mainTable [s::DB] T name: T.NAME } )
            """);
        assertThrows(MappingResolutionException.class,
                () -> c.sources().get("m::NOPE", "m::P"));
    }

    @Test
    @DisplayName("mapping ~filter stays in the pipeline (below any user op by construction)")
    void mappingFilterRidesThePipeline() {
        Ctx c = load("""
            Class m::P { name: String[1]; }
            Database s::DB ( Table T (NAME VARCHAR(50), ACTIVE INTEGER)
              Filter ActiveF ( T.ACTIVE = 1 ) )
            Mapping m::M ( *m::P: Relational { ~filter [s::DB] ActiveF
              ~mainTable [s::DB] T name: T.NAME } )
            """);
        ClassSource cs = c.sources().get("m::M", "m::P");
        assertTrue(cs.pipeline() instanceof com.legend.compiler.spec.typed.TypedFilter,
                "the mapping ~filter is the pipeline's top node, got "
                        + cs.pipeline().getClass().getSimpleName());
    }
}
