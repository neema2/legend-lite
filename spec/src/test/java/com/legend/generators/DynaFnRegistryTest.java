package com.legend.generators;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.legend.builtin.DynaFn;
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
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link DynaFn} is GENERATED from the pinned legend-engine checkout's two
 * dynafunction registries and VERIFIED here: every {@code dynaFnToSql('name', …)}
 * in {@code extensionDefaults.pure} and the dialect extensions, and every name in
 * the relational type-inference map ({@code getDynaFunctionTypeInferenceMap},
 * relationalExtension.pure), is a member (and only those); each member carries
 * exactly the dialects that register it and whether the inference map knows it;
 * and each member's resolution holds — a PURE name resolves in the catalog, a
 * SHIM names a Lite constant, TRANSLATED names have arms (and armed names are
 * TRANSLATED or PURE), UNSUPPORTED only shrinks. The translator's declared arm
 * set is derived from its SOURCE, and {@code Pure.ENGINE_VOCAB_SHIMS} from the
 * registry. {@link DynaFnGenerator} writes the members (existing resolutions
 * kept; a new engine name lands UNSUPPORTED): {@code bazel run //:update_generated}.
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
        return com.legend.testing.Upstream.engine();
    }

    @Test
    @DisplayName("the registry IS the engine's: every dynaFnToSql and type-inference name, with its dialects, and nothing else")
    void registryMatchesTheCheckout() throws IOException {
        Assumptions.assumeTrue(Files.isDirectory(engineRoot()), "legend-engine checkout not present");
        TreeMap<String, DynaFnGenerator.Upstream> up = DynaFnGenerator.upstream(engineRoot());
        TreeMap<String, DynaFnGenerator.Upstream> ours = new TreeMap<>();
        for (DynaFn d : DynaFn.values()) {
            TreeSet<String> ds = new TreeSet<>();
            d.dialects().forEach(x -> ds.add(x.name()));
            ours.put(d.dynaName(), new DynaFnGenerator.Upstream(ds, d.inference() == DynaFn.Inference.MAPPED));
        }
        assertEquals(up, ours, "DynaFn drifted from the checkout's registries — regenerate: bazel run //:update_generated");
    }

    @Test
    @DisplayName("every resolution holds: PURE in the catalog, SHIM a Lite constant, TRANSLATED armed, UNSUPPORTED shrink-only")
    void resolutionsHold() {
        List<String> bad = new ArrayList<>();
        for (DynaFn d : DynaFn.values()) {
            switch (d.resolution()) {
                case PURE -> {
                    if (Pure.nativeFunctionsAt(d.dynaName()).isEmpty()) {
                        bad.add(d.dynaName() + ": PURE but no catalog native of that name");
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
        assertTrue(unsupported == UNSUPPORTED_MAX, "UNSUPPORTED shrank to " + unsupported + " — re-pin UNSUPPORTED_MAX (headroom is not a pin)");
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
