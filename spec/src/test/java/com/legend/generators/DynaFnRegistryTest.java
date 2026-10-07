package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.builtin.DynaFn;
import com.legend.builtin.EngineHandlers;
import com.legend.builtin.Pure;
import com.legend.normalizer.DynaFnArms;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DynaFn} is GENERATED whole from the pinned legend-engine checkout's two
 * dynafunction registries by {@code DynaFnGenerator} (its diff test keeps the
 * committed file current); the platform's decisions are {@code DynaFnDecisions}',
 * by hand, and are VERIFIED here: a PURE name resolves in the catalog, a SHIM names
 * a Lite native, TRANSLATED names have arms (and armed names are TRANSLATED or
 * PURE), the residue's names are still absent from the engine surface,
 * UNSUPPORTED only shrinks, and so do the engine handler ids the platform declares
 * nowhere. The translator's declared arm set is derived from its SOURCE, and
 * {@code Pure.ENGINE_VOCAB_SHIMS} from the registry.
 */
class DynaFnRegistryTest {

    private static final Path TRANSLATOR = CoreTree.main("com/legend/normalizer/RelOpTranslator.java");
    /** Shrink-only: engine operators the platform handles by nothing yet.
     *  37 → 42 at the 4.145.0 bump (batch 8): the engine ADDED five
     *  dynafunctions — allOf, anyOf (quantified comparisons), nullSafeEqual,
     *  nullSafeNotEqual (the #4900 null-safe equality the platform lowers
     *  as IS [NOT] DISTINCT FROM, but the engine's new operator NAMES are
     *  not yet claimed) and split — the denominator grew, the platform's
     *  side did not move; each is a leg, not a ledger row. */
    static final int UNSUPPORTED_MAX = 41;   // 42 -> 41 (2026-09-17: isAlphaNumeric is OURS — ledger F-X)

    static Path engineRoot() {
        return com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
    }

    @Test
    @DisplayName("every resolution holds: PURE in the catalog, SHIM a Lite constant, TRANSLATED armed, UNSUPPORTED shrink-only")
    void resolutionsHold() {
        List<String> bad = new ArrayList<>();
        for (DynaFn d : DynaFn.values()) {
            switch (d.resolution()) {
                case PURE -> {
                    // the declarations are the engine surface's FQNs for the
                    // name (or the declared residue), joined when the classes
                    // load; each a catalog native
                    if (d.fqns().isEmpty()) {
                        bad.add(d.dynaName() + ": PURE but neither the engine surface nor DynaFnDecisions.RESIDUE"
                                + " declares what it resolves to");
                    }
                    for (String fqn : d.fqns()) {
                        if (Pure.nativeFunctionsAt(fqn).isEmpty()) {
                            bad.add(d.dynaName() + ": PURE names " + fqn + ", not a catalog native");
                        }
                    }
                }
                case SHIM -> {
                    String fqn = d.liteFqn();
                    if (!fqn.startsWith(Pure.Lite.PKG) || Pure.nativeFunctionsAt(fqn).isEmpty()) {
                        bad.add(d.dynaName() + ": SHIM but " + fqn + " is not a registered Lite native");
                    }
                }
                case TRANSLATED, UNSUPPORTED -> { }
            }
        }
        assertTrue(bad.isEmpty(), String.join("\n", bad));
        for (DynaFn d : DynaFn.withResolution(DynaFn.Resolution.TRANSLATED)) {
            assertTrue(DynaFnArms.ARMS.contains(d), d.dynaName() + ": TRANSLATED but the translator declares no arm for it");
        }
        for (DynaFn d : DynaFnArms.ARMS) {
            assertTrue(d.resolution() == DynaFn.Resolution.TRANSLATED || d.resolution() == DynaFn.Resolution.PURE,
                    d.dynaName() + ": has a translator arm but is " + d.resolution()
                    + " — an armed name is TRANSLATED (nothing passes through) or PURE (pure's own shape passes through)");
        }
        int unsupported = DynaFn.withResolution(DynaFn.Resolution.UNSUPPORTED).size();
        assertTrue(unsupported <= UNSUPPORTED_MAX, "UNSUPPORTED dynafunctions grew: " + unsupported + " > " + UNSUPPORTED_MAX);
        // the measured count is ratchets.tsv's (generated, diff-tested); UNSUPPORTED_MAX stays the ceiling (D9), and a
        // ceiling with headroom is no ceiling: it comes down with the count
        assertEquals(UNSUPPORTED_MAX, unsupported, "UNSUPPORTED dynafunctions shrank to " + unsupported
                + " -- lower UNSUPPORTED_MAX with the reason (headroom is not a pin)");
        assertEquals(SpecRatchets.measured("dynafn.unsupported"), unsupported, "UNSUPPORTED dynafunctions moved"
                + " -- bazel run //spec:update_ratchets (and lower UNSUPPORTED_MAX when it shrank: headroom is not a pin)");
    }

    /** The engine handler ids the platform declares nowhere: shrink-only. 162 when EngineHandlers began joining
     *  them at load (2026-10-06, the build rebuild's Phase 2): the hand pin's history (169 -> 168 -> 162) ended when
     *  engine-handlers.tsv carried each id's FQN, and that column went with the join. 162 -> 142 (2026-10-06, Phase 3):
     *  the 26 versions that got rows declare 20 engine handler ids (isEmpty[0..1], average/median on Integer and Float,
     *  the [1..*] date max/min, the Boolean comparisons, ...). */
    static final int UNDECLARED_ENGINE_IDS_MAX = 142;

    @Test
    @DisplayName("the engine handler ids the platform declares nowhere only shrink; the residue is still needed")
    void undeclaredEngineIdsShrinkAndTheResidueHolds() {
        int undeclared = EngineHandlers.undeclaredIds().size();
        assertEquals(UNDECLARED_ENGINE_IDS_MAX, undeclared, "engine handler ids the platform declares nowhere: "
                + undeclared + " (shrink-only; lower UNDECLARED_ENGINE_IDS_MAX with the reason, never raise it)");
        assertEquals(SpecRatchets.measured("engine.handlers.undeclared"), undeclared, "the undeclared engine ids moved"
                + " -- bazel run //spec:update_ratchets");
        // sqlNull, sqlTrue and sqlFalse resolve through DynaFnDecisions.RESIDUE only because no engine handler names
        // them; the day one does, the residue row is stale
        for (DynaFn d : List.of(DynaFn.SQL_NULL, DynaFn.SQL_TRUE, DynaFn.SQL_FALSE)) {
            assertEquals(List.of(), EngineHandlers.fqnsOf(d.dynaName()), d.dynaName() + " is on the engine surface now:"
                    + " drop its DynaFnDecisions.RESIDUE row");
            assertTrue(!d.fqns().isEmpty(), d.dynaName() + " has no declarations");
        }
    }

    @Test
    @DisplayName("the declared arm set IS the translator's: every DynaFn member its source names")
    void armsAreDerivedFromTheTranslatorSource() throws IOException {
        String src = Files.readString(TRANSLATOR, StandardCharsets.UTF_8);
        Set<DynaFn> named = new TreeSet<>();
        Matcher m = Pattern.compile("DynaFn\\.([A-Z][A-Z_0-9]+)\\b").matcher(src);
        while (m.find()) {
            named.add(DynaFn.valueOf(m.group(1)));
        }
        assertEquals(new TreeSet<>(named), new TreeSet<>(DynaFnArms.ARMS),
                "DynaFnArms.ARMS must equal the DynaFn members RelOpTranslator's source names");
    }

    @Test
    @DisplayName("Pure.ENGINE_VOCAB_SHIMS IS the registry's SHIM rows plus the translator's declared landings")
    void engineVocabShimsAreDerivedFromTheRegistry() {
        TreeSet<String> derived = new TreeSet<>();
        for (DynaFn d : DynaFn.withResolution(DynaFn.Resolution.SHIM)) {
            derived.add(d.liteFqn().substring(Pure.Lite.PKG.length()));
        }
        for (String fqn : DynaFnArms.LANDINGS.values()) {
            derived.add(fqn.substring(Pure.Lite.PKG.length()));
        }
        assertEquals(derived, new TreeSet<>(Pure.ENGINE_VOCAB_SHIMS),
                "the engine-vocabulary shim set must be exactly what the registry and the arms land on");
    }


}
