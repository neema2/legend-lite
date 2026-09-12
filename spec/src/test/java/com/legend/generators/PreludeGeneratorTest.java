// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.Compiler;
import com.legend.lexer.TokenType;
import com.legend.protocol.Protocol;
import com.legend.compiler.NameResolver;
import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.model.ClassDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.ImportScope;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.parser.Dialect;
import com.legend.parser.ElementParser;
import com.legend.protocol.DerivedPropertyDefinition;
import com.legend.protocol.ParameterDefinition;
import com.legend.protocol.TypeExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PRELUDE GENERATOR — the prelude is a MODULE
 * (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §10, docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md):
 * the library shapes a program may name are DATA, generated from the spec —
 * never hand-typed. This tool reads the engine and legend-pure checkouts
 * (spec), finds every class/enum the corpus and the platform's Java name
 * (plus the closure of what those declarations name — the module is a
 * CLOSED library the boot layer checks, T5), parses their files with OUR
 * parser, and writes each declaration VERBATIM — constraints, stereotypes,
 * tagged values, derived properties, defaults, exactly as the spec spells
 * it — under its spec file's imports, into
 * {@code core/src/main/resources/com/legend/builtin/prelude.pure}. The
 * compiler resolves and normalizes that module ONCE per process as the
 * boot layer ({@code Compiler.bootLayer}), so a derived property lifts
 * like a user class's and nothing is re-printed.
 *
 * <p>Shapes {@code Pure.java} still declares by hand and the system
 * metamodel's own elements are skipped (phase 2 migrates the hand shapes).
 * A graph class the corpus tree ALSO declares is listed at the foot of the
 * module — the T4 receipt list phase 3 burns.
 *
 * <p>Modes: {@code -Dprelude.generate=1} WRITES the file; {@code
 * -Dprelude.census=1} also writes one row per declaration to
 * {@code target/prelude-census.tsv} (HOMEWORK §4); otherwise the test
 * regenerates in memory and asserts the committed file is current (the
 * parity guard — the spec moved, or someone edited by hand).
 */
class PreludeGeneratorTest {

    private static final Path OUT = CoreTree.resource("com/legend/builtin/prelude.pure");

    /** Packages whose shapes are not (yet) generated — each line a decision. */
    private static final List<String> EXCLUDED_PACKAGE_PREFIXES = List.of(
            // VERSIONED protocol payload classes (nine copies of the same
            // shapes, meta::protocols::pure::v1_2x_0::…). The one TEMPLATE copy
            // the engine's own programs name — meta::protocols::pure::vX_X_X::
            // metamodel::m3 (the relational extension's tdsToRelation adapter
            // types its transfers over the template AppliedFunction) — is
            // admitted; see excludedByDecision() (Phase 5 batch 147, strict first)
            "meta::protocols::",
            // m3 path classes: `Path<-U,V|m> extends Function<{U[1]->V[m]}>`
            // generalizes with a NON-identity argument, which the kernel's
            // positional-pairing rule refuses (NativeFunctionTest.
            // parameterizedGeneralizationsAreIdentityArgument) — a kernel
            // gap to lift before these shapes can be data
            "meta::pure::metamodel::path::");
    /** Individual declarations left out, each with its reason. */
    private static final Map<String, String> EXCLUDED_CLASSES = Map.of();
    /** The protocol TEMPLATE package admitted out of the versioned exclusion. */
    private static final String PROTOCOL_TEMPLATE_M3 = "meta::protocols::pure::vX_X_X::metamodel::m3::";


    @Test
    @DisplayName("prelude.pure is the generator's current output (regenerate with -Dprelude.generate=1)")
    void preludeIsCurrent() throws Exception {
        String generated = generate();
        if ("1".equals(System.getProperty("prelude.generate"))) {
            Files.createDirectories(OUT.getParent());
            Files.writeString(OUT, generated, StandardCharsets.UTF_8);
            System.out.println("[prelude] wrote " + OUT + " (" + generated.lines().count() + " lines)");
            return;
        }
        assertTrue(Files.exists(OUT), "prelude.pure missing — run with -Dprelude.generate=1");
        assertEquals(generated, Files.readString(OUT, StandardCharsets.UTF_8),
                "prelude.pure is stale: the spec moved or the file was edited by hand —"
                        + " regenerate with -Dprelude.generate=1");
    }

    // ------------------------------------------------------------------
    // the generator
    // ------------------------------------------------------------------

    /** The engine-checkout roots the generator indexes (ENGINE_ROOT-relative)
     *  — the spec's declaration files. Public for the upstream path manifest
     *  ({@code UpstreamPathManifestTest}); the pure checkout is indexed WHOLE. */
    public static final List<String> ENGINE_SPEC_ROOTS = List.of(
            "legend-engine-xts-relationalStore",
            "legend-engine-core/legend-engine-core-pure",
            // the service metamodel (core_service): ^Service(...) in the
            // execution-strategy tests (batch 57)
            "legend-engine-xts-service/legend-engine-language-pure-dsl-service-pure/"
                    + "src/main/resources/core_service");
    /** The relational corpus root the generator scans for DEMAND — the same
     *  directory as {@code Corpus.RELATIONAL}, declared separately on purpose
     *  (the two can go stale independently). */
    public static final String CORPUS_ROOT =
            "legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
            + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
            + "src/main/resources/core_relational/relational";
    /** m3.pure — the one spec file in the M3 instance syntax (PURE_ROOT-relative). */
    public static final String M3_PURE =
            "legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/grammar/m3.pure";

    static Path engineRoot() {
        return Path.of(System.getProperty("legend.engine.root",
                System.getProperty("user.home") + "/legend/legend-engine"));
    }

    static Path pureRoot() {
        return Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
    }

    static String generate() throws IOException {
        // the roots arrive as system properties from the root pom (surefire
        // forwards -D / LEGEND_*_ROOT / the ${user.home} default); the
        // literal here is only the IDE fallback
        Path engine = engineRoot();
        Path pure = pureRoot();
        List<Path> roots = new ArrayList<>();
        for (String r : ENGINE_SPEC_ROOTS) {
            roots.add(engine.resolve(r));
        }
        roots.add(pure);
        Path corpus = engine.resolve(CORPUS_ROOT);

        // 1. the spec index: every Class/Enum FQN -> its defining file
        Map<String, Path> index = new TreeMap<>();
        for (Path root : roots) {
            if (!Files.isDirectory(root)) {
                throw new IllegalStateException("spec root missing (upstream moved it?): " + root);
            }
            try (Stream<Path> s = Files.walk(root)) {
                for (Path f : s.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    Matcher m = DECL_HEADER.matcher(Files.readString(f, StandardCharsets.UTF_8));
                    while (m.find()) {
                        index.putIfAbsent(m.group(2), f);
                    }
                }
            }
        }

        // 1a. m3.pure — the one spec file in the M3 instance syntax — indexes
        // through the reader: its classes and enumerations are spec
        // declarations like any other (T1: legend-pure's platform packages
        // whole), printed from the graph, never copied as text
        Path m3File = pure.resolve(M3_PURE);
        Map<String, String> m3Decls = m3Declarations(Files.readString(m3File, StandardCharsets.UTF_8));
        for (String fqn : m3Decls.keySet()) {
            index.putIfAbsent(fqn, m3File);
        }

        // 1b. PROVENANCE of every hand declaration: a class Pure.java still
        // declares by hand must be a spec shape (indexed — a Java-referenced
        // definition or a carrier awaiting migration), an m3 BOOTSTRAP shape
        // (legend-pure's m3.pure graph, unreadable by this generator), or an
        // allowlisted platform carrier with its reason — a GUESSED shape
        // (the ::metamodel::DateLiteral of 2026-09-04) is a generator error
        Set<String> m3 = new LinkedHashSet<>();
        Matcher m3h = M3_HEADER.matcher(Files.readString(pure.resolve(
                "legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/grammar/m3.pure"),
                StandardCharsets.UTF_8));
        while (m3h.find()) {
            String at = m3h.group(3);
            // m3.pure's UN-ANNOTATED bootstrap declarations (no @package):
            // the root package's Package class and the primitive types
            String pkg = at == null
                    ? ("Class".equals(m3h.group(1)) ? "meta::pure::metamodel" : "meta::pure::metamodel::type")
                    : String.join("::", at.replace("Root.children[", "").replace("].children[", "::")
                            .replace("].children", "").replace("]", "").split("::"));
            m3.add(pkg + "::" + m3h.group(2));
        }
        List<String> unprovenanced = new ArrayList<>();
        for (String fqn : handDeclaredFqns()) {
            if (!index.containsKey(fqn) && !m3.contains(fqn) && !HAND_CARRIERS.containsKey(fqn)) {
                unprovenanced.add(fqn);
            }
        }
        if (!unprovenanced.isEmpty()) {
            throw new IllegalStateException("prelude generator: hand-declared classes with no"
                    + " provenance (not in the spec index, not in m3.pure, not an allowlisted"
                    + " carrier): " + unprovenanced);
        }

        // 2. what the GRAPH declares itself (the corpus tree, the program
        // libraries, the shape files) — for the T4 receipts only: a vocabulary
        // class the graph also declares is listed at the foot of the module
        Set<String> corpusDefined = new LinkedHashSet<>();
        Set<String> javaDemand = new LinkedHashSet<>();
        List<Path> scanned = new ArrayList<>();
        try (Stream<Path> s = Files.walk(corpus)) {
            scanned.addAll(s.filter(p -> p.toString().endsWith(".pure")).sorted().toList());
        }
        scanned.addAll(com.legend.rcorpus.Corpus.LIBRARY_FILES);
        scanned.addAll(com.legend.rcorpus.Corpus.SHAPE_FILES);
        for (Path f : scanned) {
            if (!Files.isRegularFile(f)) {
                // LOUD (batch 2): a named LIBRARY/SHAPE file that is gone is a
                // moved upstream path, and a demand scan that skips it under-
                // generates the prelude without a word
                throw new IllegalStateException("demand input missing (upstream moved it?): " + f);
            }
            for (String line : Files.readString(f, StandardCharsets.UTF_8).split("\n")) {
                Matcher d = DECL.matcher(line);
                if (d.find()) {
                    corpusDefined.add(d.group(2));
                }
            }
        }
        // the SYSTEM LAYER's own Pure text (SystemMetamodel: the metamodel
        // store's classes, mappings and views) names vocabulary bare through
        // its imports (SQLExecutionNode.resultColumns: SQLResultColumn[*])
        List<String> texts = new ArrayList<>();
        String systemText = com.legend.builtin.SystemMetamodel.source();
        texts.add(systemText);
        Set<String> systemDemand = new LinkedHashSet<>();   // what the system metamodel's source names
        {
            for (String src : texts) {
                Set<String> sink = systemDemand;
                List<String> imports = new ArrayList<>();
                for (String line : src.split("\n")) {
                    Matcher d = DECL.matcher(line);
                    if (d.find()) {
                        corpusDefined.add(d.group(2));
                    }
                    Matcher im = IMPORT.matcher(line.strip());
                    if (im.matches()) {
                        imports.add(im.group(1));
                    }
                }
                // every FULLY-QUALIFIED name the source spells, in any
                // position (dynamicNew(meta::…::LateralJoin, …) names its
                // class as an ARGUMENT)
                Matcher fq = FQN_TOKEN.matcher(src);
                while (fq.find()) {
                    if (index.containsKey(fq.group())) {
                        sink.add(fq.group());
                    }
                }
                // SUPERTYPES of the corpus's own declarations (Class X extends
                // A, B<T>): the corpus class needs them to resolve (TableTDS
                // extends TabularDataSetImplementation — its `store` end)
                List<String> bareRefs = new ArrayList<>();
                Matcher ex = EXTENDS_CLAUSE.matcher(src);
                while (ex.find()) {
                    for (String part : ex.group(1).split(",")) {
                        String nm = part.trim().replaceAll("<.*$", "").trim();
                        if (!nm.isEmpty()) {
                            bareRefs.add(nm);
                        }
                    }
                }
                Matcher r = TYPE_REF.matcher(src);
                while (r.find()) {
                    bareRefs.add(r.group(1) != null ? r.group(1) : r.group(2));
                }
                for (String n : bareRefs) {
                    if (index.containsKey(n)) {
                        sink.add(n);
                        continue;
                    }
                    List<String> scope = new ArrayList<>(imports);
                    scope.addAll(NameResolver.CORE_IMPORTS);   // real pure's implicit imports
                    for (String imp : scope) {
                        if (index.containsKey(imp + "::" + n)) {
                            sink.add(imp + "::" + n);
                            break;
                        }
                    }
                }
            }
        }
        // JAVA demand = the platform's VOCABULARY, by the MECHANICAL rule (USER
        // 2026-09-08, after phase 3: "just take everything instead of the
        // declared-vs-used whitelist"): every spec class or enum the platform's
        // own Java NAMES in code — a native signature, a constructed instance, a
        // dispatch constant alike; comment lines never count. If our Java has
        // the name in it, the platform depends on the class existing; the
        // receipt is a grep. (A curated "constructed" list lived one batch, 154.)
        try (Stream<Path> s = Files.walk(CoreTree.CORE.resolve("src/main/java"))) {
            for (Path f : s.filter(p -> p.toString().endsWith(".java")
                    && !p.getFileName().toString().equals("Prelude.java")).sorted().toList()) {
                for (String line : Files.readString(f, StandardCharsets.UTF_8).split("\n")) {
                    String code = line.strip();
                    if (code.startsWith("//") || code.startsWith("*") || code.startsWith("/*")) {
                        continue;
                    }
                    Matcher r = FQN_TOKEN.matcher(code);
                    while (r.find()) {
                        // a spec TEST MODEL (…::tests::Person) named in a harness
                        // fixture is corpus input, never a platform shape
                        if (index.containsKey(r.group()) && (!r.group().matches(".*::tests?::.*")
                                || r.group().startsWith("meta::pure::test::"))) {
                            javaDemand.add(r.group());
                        }
                    }
                }
            }
        }
        javaDemand.addAll(systemDemand);
        // owned = the HAND-declared natives (read from Pure.java's SOURCE, so
        // the generator never depends on the module it writes), the system
        // layer and the corpus's own definitions
        Set<String> platformOwned = new LinkedHashSet<>(handDeclaredFqns());
        platformOwned.addAll(com.legend.builtin.SystemMetamodel.elementFqns());
        Set<String> owned = new LinkedHashSet<>(platformOwned);
        owned.addAll(corpusDefined);

        // 3. parse + resolve the defining files, closing over referenced types
        // (Spec.close: the closure walk, reusable — the T1 demand below runs it too)
        Set<String> knownFqns = new LinkedHashSet<>(index.keySet());
        knownFqns.addAll(owned);
        Spec spec = new Spec(index, platformOwned, corpusDefined, knownFqns, m3File, m3Decls);
        // THE DEMAND IS T1 (PRELUDE_MODULE_HOMEWORK §2, PHASE3_DEMAND_CUT_HOMEWORK;
        // batch 155 = phase 3b-2): (1) legend-pure's platform packages WHOLE —
        // every class and enum under the nine platform roots, minus the decided
        // exclusions and the spec's test packages, demanded or not; (2) the
        // platform's VOCABULARY — every spec class or enum its Java NAMES in
        // code, plus what the system metamodel's source names; (3) the closure of those
        // declarations. "The corpus names it" is no reason (T2): an engine
        // class a program needs enters that program's graph by file
        // (Corpus.SHAPE_FILES). A vocabulary class the corpus tree also
        // declares is generated all the same — the graph's copy yields (T4).
        Set<String> todaySeed = new LinkedHashSet<>();
        for (String fqn : javaDemand) {
            if (!handDeclaredFqns().contains(fqn) && !excluded(fqn)
                    && !com.legend.builtin.SystemMetamodel.elementFqns().contains(fqn)) {
                todaySeed.add(fqn);
            }
        }
        List<Path> platformRoots = new ArrayList<>();
        for (String r : SpecBodyCensusTest.PLATFORM_ROOTS) {
            platformRoots.add(pure.resolve(r));
        }
        for (Map.Entry<String, Path> e : index.entrySet()) {
            boolean platform = platformRoots.stream().anyMatch(e.getValue()::startsWith);
            if (platform && !owned.contains(e.getKey()) && !excluded(e.getKey())) {
                todaySeed.add(e.getKey());
            }
        }
        // ENGINE-DECLARED NATIVES demand the types their signatures name
        // (compileJava(...):JavaSource[1] — an engine class no platform seed
        // carries): resolved through the declaring file's imports against the
        // spec index and seeded BEFORE the closure, so the prelude is closed
        // over them like every other wanted shape (batch 4 §6.2, 2026-09-11)
        List<String> engineNativeWalls = new ArrayList<>();
        List<PlatformFunction> engineNativeDecls = engineNatives(engine, engineNativeWalls);
        Map<String, List<String>> byBare = new HashMap<>();
        for (String fqn : index.keySet()) {
            byBare.computeIfAbsent(fqn.substring(fqn.lastIndexOf(':') + 1), k -> new ArrayList<>()).add(fqn);
        }
        // an engine native whose signature names a type the platform DECIDED
        // not to carry cannot be declared — listed in the module's header,
        // never dropped silently
        Map<String, String> engineNativesNotCarried = new TreeMap<>();
        for (PlatformFunction pf : engineNativeDecls) {
            String ownPkg = pf.fqn().substring(0, pf.fqn().lastIndexOf("::"));
            for (String name : referencedTypeNames(pf.text())) {
                String resolved0 = name.contains("::") ? name : null;
                if (resolved0 == null) {
                    // real pure's precedence: the declaring package, then the
                    // file's imports, then the core imports — one candidate per tier
                    for (int tier = 0; tier < 3 && resolved0 == null; tier++) {
                        List<String> cands = new ArrayList<>();
                        for (String c : byBare.getOrDefault(name, List.of())) {
                            String pkg = c.substring(0, c.lastIndexOf("::"));
                            boolean inTier = tier == 0 ? pkg.equals(ownPkg)
                                    : tier == 1 ? pf.wildcards().contains(pkg)
                                    : NameResolver.CORE_IMPORTS.contains(pkg);
                            if (inTier) {
                                cands.add(c);
                            }
                        }
                        if (cands.size() == 1) {
                            resolved0 = cands.get(0);
                        }
                    }
                }
                if (resolved0 != null && index.containsKey(resolved0)) {
                    if (owned.contains(resolved0) || excluded(resolved0)) {
                        // a platform-OWNED name is reserved, not necessarily declared
                        // (meta::json::JSONDeserializationConfig is owned and absent)
                        engineNativesNotCarried.putIfAbsent(pf.fqn(), resolved0
                                + (owned.contains(resolved0) ? " (platform-owned type)" : " (excluded type)"));
                    } else {
                        todaySeed.add(resolved0);
                    }
                }
            }
        }
        Closure today = spec.close(todaySeed);
        Set<String> want = today.want();
        Set<String> pulledFromCorpus = today.pulledFromCorpus();
        Map<String, PackageableElement> resolved = spec.resolved;
        Map<String, String> declText = spec.declText;
        Map<String, String> fileOf = spec.fileOf;
        // THE PLATFORM LIBRARY'S FUNCTIONS (COMPILE_EVERYTHING_HOMEWORK §10,
        // USER 2026-09-09: legend-pure's platform packages WHOLE — classes AND
        // functions; the eager corpus compile found the platform's own bodied
        // functions existing in no runtime world): every bodied, non-test
        // function of the platform roots, VERBATIM under its section's
        // imports. Natives stay the registry's (`native function` skipped);
        // test functions are the PCT lane's programs, not the library.
        List<String> functionWalls = new ArrayList<>();
        List<PlatformFunction> platformFunctions = new ArrayList<>();
        // the system metamodel's own row-reading versions of a library
        // function (classMappingById, allPropertyMappings, mainTable, …) are
        // the platform's implementation of that NAME, every overload: the
        // library's copies stay out, listed at the module's foot
        Set<String> systemOwnedFunctions = new TreeSet<>();
        // a NAME the platform implements — a registered native (isEmpty, sort,
        // contains, …) or an operator special form (join, filter, project,
        // …) — is the platform's definition outright (batch 147 row 19: the
        // native IS the definition; the operator forms have no signature to
        // tie-break on): the library's bodies under that name stay out,
        // else a bare call in a program resolves to the library's FQN through
        // the core imports and bypasses the platform's form (batch 169:
        // legend-pure's meta::pure::tds::join captured the corpus's bare
        // join calls from the built-in join)
        Set<String> platformOwnedNames = new TreeSet<>();
        // THE EXCLUSION RULE KEYS ON CLAIMS (upstream boundary batch 4, D3):
        // the bare names the registry CLAIMS (com.legend.claims.Claims — the
        // four lowering registries, CoreFn, the walls, the NativeFn family
        // enums) plus the CoreFn forms — not "a signature exists in
        // Pure.java". A Pure.java entry nothing implements no longer
        // suppresses upstream's working body: it left Pure.java, or it is
        // UNCLAIMED and the ledger says so. An upstream NATIVE is owned by the
        // platform only when Pure.java declares that exact FQN (a respelled
        // twin of a catalog native would be an ambiguous 2-candidate tie).
        Set<String> claimedNames = com.legend.claims.Claims.claimedBareNames();
        Set<String> respelledNatives = new TreeSet<>();
        for (PlatformFunction pf : platformFunctions(platformRoots, functionWalls)) {
            if (com.legend.builtin.SystemMetamodel.elementFqns().contains(pf.fqn())) {
                systemOwnedFunctions.add(pf.fqn());
                continue;
            }
            String simple = pf.fqn().substring(pf.fqn().lastIndexOf(':') + 1);
            if (pf.nativeDecl()) {
                // a native is platform-owned when Pure.java declares that exact
                // FQN, OR when its NAME is a claimed one or a CoreFn language
                // form (`new`, `cast`, `copy`… have no Pure.java signature: the
                // Typer owns them as forms — first landing of batch 4 carried
                // upstream's `native function new` and it captured every
                // ^Class(...) in the corpus: 2,436 tests red)
                if (!com.legend.builtin.Pure.nativeFunctionsAt(pf.fqn()).isEmpty()
                        || claimedNames.contains(simple)
                        || com.legend.compiler.spec.CoreFn.of(simple).isPresent()) {
                    platformOwnedNames.add(pf.fqn());
                    continue;
                }
                respelledNatives.add(pf.fqn());
                platformFunctions.add(pf);
                continue;
            }
            if (claimedNames.contains(simple)
                    || com.legend.compiler.spec.CoreFn.of(simple).isPresent()) {
                platformOwnedNames.add(pf.fqn());
                continue;
            }
            platformFunctions.add(pf);
        }
        // ENGINE-DECLARED NATIVES, the same ownership rule (batch 4 §6.2)
        for (PlatformFunction pf : engineNativeDecls) {
            if (engineNativesNotCarried.containsKey(pf.fqn())) {
                continue;
            }
            if (com.legend.builtin.SystemMetamodel.elementFqns().contains(pf.fqn())) {
                systemOwnedFunctions.add(pf.fqn());
                continue;
            }
            String simple = pf.fqn().substring(pf.fqn().lastIndexOf(':') + 1);
            if (!com.legend.builtin.Pure.nativeFunctionsAt(pf.fqn()).isEmpty()
                    || claimedNames.contains(simple)
                    || com.legend.compiler.spec.CoreFn.of(simple).isPresent()) {
                platformOwnedNames.add(pf.fqn());
                continue;
            }
            if (respelledNatives.add(pf.fqn())) {
                platformFunctions.add(pf);
            }
        }
        functionWalls.addAll(engineNativeWalls);
        for (PlatformFunction pf : platformFunctions) {
            declText.put(pf.key(), pf.text());
            fileOf.put(pf.key(), pf.file());
            spec.offsetOf.put(pf.key(), pf.offset());
            spec.scopeOf.put(pf.key(), new ImportScope(pf.wildcards()));
        }
        Map<String, Integer> offsetOf = spec.offsetOf;
        Map<String, ImportScope> scopeOf = spec.scopeOf;
        for (String fqn : want) {
            if (!resolved.containsKey(fqn)) {
                throw new IllegalStateException("prelude generator: '" + fqn
                        + "' is indexed at " + index.get(fqn) + " but did not parse as a class/enum");
            }
            if (!declText.containsKey(fqn)) {
                throw new IllegalStateException("prelude generator: no declaration text for '" + fqn + "'");
            }
        }
        checkClosed(want, resolved, platformOwned, corpusDefined, index);

        // THE CENSUS (-Dprelude.census=1, HOMEWORK §4): one row per wanted
        // declaration — where it comes from, who demands it, what the module
        // must carry. Snapshot: docs/PRELUDE_MODULE_CENSUS_2026_09_08.tsv
        if ("1".equals(System.getProperty("prelude.census"))) {
            List<String> rows = new ArrayList<>();
            rows.add("fqn\tsource\tdemand\tcorpusDefined\tconstraints\tderived\tstereotypes\ttaggedValues\tdefaults\tescapes\tfile");
            for (String fqn : new TreeSet<>(want)) {
                PackageableElement el = resolved.get(fqn);
                String file = relative(fileOf.get(fqn), engine, pure);
                String source = file.startsWith("legend-pure/") ? "legend-pure"
                        : file.startsWith("legend-engine/") ? "legend-engine" : "?";
                String dem = javaDemand.contains(fqn) ? "java" : todaySeed.contains(fqn) ? "platform" : "closure";
                if (el instanceof ClassDefinition cd) {
                    long defaults = cd.properties().stream().filter(ClassDefinition.PropertyDefinition::hasDefault).count();
                    rows.add(String.join("\t", fqn, source, dem, String.valueOf(corpusDefined.contains(fqn)),
                            String.valueOf(cd.constraints().size()), String.valueOf(cd.derivedProperties().size()),
                            String.valueOf(cd.stereotypes().size()), String.valueOf(cd.taggedValues().size()),
                            String.valueOf(defaults), "", file));
                } else {
                    rows.add(String.join("\t", fqn, source, dem, String.valueOf(corpusDefined.contains(fqn)),
                            "enum", "", "", "", "", "", file));
                }
            }
            Files.createDirectories(Path.of("target"));
            Files.write(Path.of("target/prelude-census.tsv"), rows);
            System.out.println("[prelude-census] " + (rows.size() - 1) + " rows -> target/prelude-census.tsv");
        }

        // 4. EMIT: one ###Pure section per (spec file, import scope), the
        // scope's imports, then each declaration VERBATIM in source order
        StringBuilder sb = new StringBuilder();
        sb.append("// Copyright 2026 Legend Contributors\n");
        sb.append("// SPDX-License-Identifier: Apache-2.0\n");
        sb.append("//\n");
        sb.append("// GENERATED — do not edit (com.legend.generators.PreludeGeneratorTest, -Dprelude.generate=1).\n");
        sb.append("// THE PRELUDE AS A MODULE (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §10,\n");
        sb.append("// docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md): the library shapes the corpus and the platform's Java\n");
        sb.append("// name, copied VERBATIM from the legend-pure / legend-engine spec — one ###Pure section per spec file\n");
        sb.append("// and import scope, each declaration exactly as the spec spells it (constraints, stereotypes, tagged\n");
        sb.append("// values, derived properties, defaults). Compiled through the user pipeline as the boot layer beside\n");
        sb.append("// the system metamodel (Compiler.bootLayer): resolved under these imports, normalized, cached once.\n");
        sb.append("// Shapes Pure.java still declares by hand are skipped here until their hand copy is deleted.\n");
        // MODULE ORDER IS A RULE (HOMEWORK §9.12): legend-pure's sections
        // before legend-engine's, each tier by spec path, declarations in
        // source order — the resolver's bare-name fallback reads the module
        // in this order (first claimant wins after the catalog). Section
        // key: tier, relative file, then the scope (a file may open several
        // ###Pure sections with different imports; each element keeps its own)
        Map<String, List<String>> bySection = new TreeMap<>();
        Map<String, ImportScope> sectionScope = new LinkedHashMap<>();
        List<String> emitted = new ArrayList<>(want);
        for (PlatformFunction pf : platformFunctions) {
            emitted.add(pf.key());
        }
        for (String fqn : emitted) {
            String file = relative(fileOf.get(fqn), engine, pure);
            ImportScope scope = scopeOf.get(fqn);
            String tier = file.startsWith("legend-pure/") ? "0" : "1";
            String key = tier + "\t" + file + "\t" + String.join(",", scope.wildcards());
            bySection.computeIfAbsent(key, k -> new ArrayList<>()).add(fqn);
            sectionScope.putIfAbsent(key, scope);
        }
        int classes = 0;
        int enums = 0;
        int functions = 0;
        for (Map.Entry<String, List<String>> section : bySection.entrySet()) {
            String file = section.getKey().split("\t")[1];
            sb.append("\n###Pure\n// ").append(file);
            if (file.endsWith("/grammar/m3.pure")) {
                sb.append(" — PRINTED from the M3 instance graph by the generator's reader (no class syntax to copy):"
                        + " stored properties, supertypes, type and multiplicity parameters; m3's qualified properties are not carried");
            }
            sb.append('\n');
            for (String pkg : sectionScope.get(section.getKey()).wildcards()) {
                sb.append("import ").append(pkg).append("::*;\n");
            }
            List<String> inOrder = new ArrayList<>(section.getValue());
            inOrder.sort(java.util.Comparator.comparingInt(offsetOf::get));
            for (String fqn : inOrder) {
                if (fqn.indexOf('#') >= 0) {
                    functions++;
                } else if (resolved.get(fqn) instanceof ClassDefinition) {
                    classes++;
                } else {
                    enums++;
                }
                sb.append(declText.get(fqn)).append("\n\n");
            }
        }
        sb.append("// ").append(classes).append(" classes, ").append(enums).append(" enums, ")
                .append(functions).append(" functions (legend-pure's platform library, bodied and non-test;")
                .append(functionWalls.size()).append(" files unparsed, the census's load walls).\n");
        sb.append("// RESPELLED NATIVES — ").append(respelledNatives.size())
                .append(" upstream `native function` declarations the platform does not implement (carried so they\n")
                .append("// resolve and type-check; a call fails at lowering as 'not implemented', never 'unknown function').\n");
        if (!engineNativesNotCarried.isEmpty()) {
            sb.append("// ENGINE NATIVES NOT CARRIED — the signature names a type the platform does not carry\n");
            sb.append("// (a decided exclusion or a platform-owned name); a call is 'unknown function':\n");
            for (Map.Entry<String, String> e : engineNativesNotCarried.entrySet()) {
                sb.append("//   ").append(e.getKey()).append(" — ").append(e.getValue()).append('\n');
            }
        }
        if (!platformOwnedNames.isEmpty()) {
            sb.append("// PLATFORM-OWNED NAMES — library functions whose name the platform CLAIMS (native-claims.tsv:\n");
            sb.append("// a registered lowering, a family enum, a wall) or an operator special form; not carried:\n");
            for (String f : platformOwnedNames) {
                sb.append("//   ").append(f).append('\n');
            }
        }
        if (!systemOwnedFunctions.isEmpty()) {
            sb.append("// SYSTEM-OWNED FUNCTIONS — library functions the system metamodel implements over its\n");
            sb.append("// rows (SystemMetamodel.java); the platform's versions stand, the spec's bodies are not carried:\n");
            for (String f : systemOwnedFunctions) {
                sb.append("//   ").append(f).append('\n');
            }
        }
        if (!pulledFromCorpus.isEmpty()) {
            sb.append("// T4 RECEIPTS — declared by the corpus tree too; the prelude wins, the graph's copy yields\n");
            sb.append("// (Compiler.withoutPreludeShadows); this list burns to zero in phase 3:\n");
            for (String c : new TreeSet<>(pulledFromCorpus)) {
                sb.append("//   ").append(c).append('\n');
            }
        }
        String module = sb.toString();
        // the whole module parses as ONE model, sections and imports included,
        // to exactly the wanted declarations
        ParsedModel whole = ElementParser.parse(module, Dialect.LEGEND_PLATFORM);
        Set<String> parsedFqns = new TreeSet<>();
        int parsedFunctions = 0;
        for (PackageableElement e : whole.elements()) {
            if (e instanceof com.legend.model.FunctionDefinition
                    || e instanceof com.legend.model.NativeFunctionDefinition) {
                parsedFunctions++;   // overloads are distinct elements; respelled natives are functions too (batch 4)
            } else {
                parsedFqns.add(e.qualifiedName());
            }
        }
        // and RESOLVES, as the boot layer will resolve it — beside the system
        // metamodel's names, strictly: a function whose signature or body
        // names something outside the module is a generator error here,
        // never a boot failure later (COMPILE_EVERYTHING: strict at boot)
        Map<String, String> resolveWalls = new LinkedHashMap<>();
        NameResolver.resolveAlongside(whole,
                com.legend.builtin.SystemMetamodel.elementFqns(), resolveWalls);
        if (!resolveWalls.isEmpty()) {
            throw new IllegalStateException("prelude generator: the module does not resolve"
                    + " against the boot names (" + resolveWalls.size() + "): " + resolveWalls);
        }
        if (whole.elements().size() - parsedFunctions != classes + enums
                || !parsedFqns.equals(new TreeSet<>(want)) || parsedFunctions != functions) {
            Set<String> missing = new TreeSet<>(want);
            missing.removeAll(parsedFqns);
            Set<String> extra = new TreeSet<>(parsedFqns);
            extra.removeAll(want);
            throw new IllegalStateException("prelude generator: the module parses to "
                    + (whole.elements().size() - parsedFunctions) + " declarations + "
                    + parsedFunctions + " functions, expected " + (classes + enums) + " + " + functions
                    + "; missing " + missing + ", extra " + extra);
        }
        return module;
    }

    /** One closure walk's result. */
    record Closure(Set<String> want, Set<String> pulledFromCorpus) {
    }

    /**
     * The spec as parsed on demand: files parsed once and cached across
     * closure walks; {@link #close} takes a SEED of wanted FQNs and returns
     * it closed over every type the wanted declarations name (HOMEWORK
     * §9.9 — declarations only, never bodies, §9a).
     */
    static final class Spec {
        final Map<String, Path> index;
        final Set<String> platformOwned;
        final Set<String> corpusDefined;
        final Set<String> knownFqns;
        final Map<String, PackageableElement> resolved = new LinkedHashMap<>();
        final Map<String, String> declText = new LinkedHashMap<>();
        final Map<String, String> fileOf = new LinkedHashMap<>();
        final Map<String, Integer> offsetOf = new LinkedHashMap<>();
        final Map<String, ImportScope> scopeOf = new LinkedHashMap<>();
        final Set<Path> parsedFiles = new LinkedHashSet<>();
        final Map<String, TokenStream> tokensOf = new LinkedHashMap<>();
        final Path m3File;
        final Map<String, String> m3Decls;

        Spec(Map<String, Path> index, Set<String> platformOwned, Set<String> corpusDefined,
                Set<String> knownFqns, Path m3File, Map<String, String> m3Decls) {
            this.index = index;
            this.platformOwned = platformOwned;
            this.corpusDefined = corpusDefined;
            this.knownFqns = knownFqns;
            this.m3File = m3File;
            this.m3Decls = m3Decls;
        }

        /** An m3 declaration enters the caches from the READER's print. */
        private void admitM3(String fqn) {
            if (resolved.containsKey(fqn)) {
                return;
            }
            String text = m3Decls.get(fqn);
            ParsedModel one = ElementParser.parse(text, Dialect.LEGEND_PLATFORM);
            if (one.elements().size() != 1) {
                throw new IllegalStateException("m3 reader: '" + fqn + "' printed as "
                        + one.elements().size() + " elements: " + text);
            }
            resolved.put(fqn, one.elements().get(0));
            declText.put(fqn, text);
            fileOf.put(fqn, m3File.toString());
            offsetOf.put(fqn, new ArrayList<>(m3Decls.keySet()).indexOf(fqn));
            scopeOf.put(fqn, ImportScope.empty());
        }

        Closure close(Set<String> seed) throws IOException {
            Set<String> want = new LinkedHashSet<>(seed);
            Set<String> pulledFromCorpus = new LinkedHashSet<>();
            for (String fqn : seed) {
                if (corpusDefined.contains(fqn)) {
                    pulledFromCorpus.add(fqn);
                }
            }
            boolean grew = true;
            while (grew) {
                grew = false;
                List<Compiler.ModelSource> sources = new ArrayList<>();
                for (String fqn : new ArrayList<>(want)) {
                    Path f = index.get(fqn);
                    if (f != null && f.equals(m3File)) {
                        admitM3(fqn);
                        continue;
                    }
                    if (f != null && parsedFiles.add(f)) {
                        sources.add(new Compiler.ModelSource(f.toString(),
                                Files.readString(f, StandardCharsets.UTF_8)));
                    }
                }
                if (!sources.isEmpty()) {
                    List<String> parseWalls = new ArrayList<>();
                    ParsedModel parsed = Compiler.parseSources(sources,
                            (name, err) -> parseWalls.add(name + " => " + err),
                            Dialect.LEGEND_PLATFORM).model();
                    if (!parseWalls.isEmpty()) {
                        throw new IllegalStateException("prelude generator: spec files that do not"
                                + " parse (a parser gap to fix, never a hand copy): " + parseWalls);
                    }
                    Map<String, String> walls = new LinkedHashMap<>();
                    ParsedModel r = NameResolver.resolveAlongside(parsed, knownFqns, walls);
                    // a pulled FILE also carries functions (never emitted): only a
                    // wanted class/enum that fails to resolve is a generator error
                    Map<String, String> shapeWalls = new LinkedHashMap<>();
                    walls.forEach((fqn, msg) -> {
                        if (want.contains(fqn)) {
                            shapeWalls.put(fqn, msg);
                        }
                    });
                    if (!shapeWalls.isEmpty()) {
                        throw new IllegalStateException("prelude generator: unresolved names in"
                                + " wanted declarations: " + shapeWalls);
                    }
                    for (PackageableElement el : r.elements()) {
                        if (el instanceof ClassDefinition || el instanceof EnumDefinition) {
                            resolved.putIfAbsent(el.qualifiedName(), el);
                            String srcName = parsed.elementSources().get(el.qualifiedName());
                            Integer off = parsed.elementOffsets().get(el.qualifiedName());
                            if (srcName != null && off != null && !declText.containsKey(el.qualifiedName())) {
                                for (Compiler.ModelSource ms : sources) {
                                    if (ms.name().equals(srcName)) {
                                        TokenStream ts = tokensOf.computeIfAbsent(srcName,
                                                k -> Lexer.tokenize(ms.text()));
                                        declText.put(el.qualifiedName(), declarationText(ts, ms.text(), off,
                                                el instanceof EnumDefinition));
                                        fileOf.put(el.qualifiedName(), srcName);
                                        offsetOf.put(el.qualifiedName(), off);
                                        scopeOf.put(el.qualifiedName(), parsed.elementImports()
                                                .getOrDefault(el.qualifiedName(), ImportScope.empty()));
                                    }
                                }
                            }
                        }
                    }
                }
                // CLOSURE (HOMEWORK §9.9): every type a wanted declaration names —
                // supertypes, stored and derived property types, derived parameter
                // types — is part of that shape's graph and is admitted when the
                // spec declares it and it is not a DECIDED exclusion; the
                // spec-test-package rule governs DEMAND only (the engine's
                // SqlFunction.tests : SqlFunctionTest[*] names a tests:: class).
                // PLATFORM ownership (hand + system) stops the walk; a corpus-tree
                // class is admitted and listed (T4 receipt — the graph's copy yields)
                for (String fqn : new ArrayList<>(want)) {
                    PackageableElement el = resolved.get(fqn);
                    if (el instanceof ClassDefinition cd) {
                        for (String ref : referencedFqns(cd)) {
                            if (!platformOwned.contains(ref) && !excludedByDecision(ref)
                                    && index.containsKey(ref) && want.add(ref)) {
                                if (corpusDefined.contains(ref)) {
                                    pulledFromCorpus.add(ref);
                                }
                                grew = true;
                            }
                        }
                    }
                }
            }
            return new Closure(want, pulledFromCorpus);
        }
    }

    /** CLOSURE COMPLETENESS (T5 — the module is a closed library the boot
     * layer checks eagerly): every type a generated declaration names must
     * be owned by the platform, generated, a primitive, or one of the
     * class's own type parameters — a bare or dangling name here is a
     * generator gap or an exclusion to widen, never an omitted class. */
    static void checkClosed(Set<String> want, Map<String, PackageableElement> resolved,
            Set<String> platformOwned, Set<String> corpusDefined, Map<String, Path> index) {
        java.util.SortedMap<String, String> dangling = new TreeMap<>();
        for (String fqn : want) {
            if (resolved.get(fqn) instanceof ClassDefinition cd) {
                Set<String> names = new LinkedHashSet<>();
                for (TypeExpression t : cd.superClasses()) {
                    collectAll(t, names);
                }
                for (ClassDefinition.PropertyDefinition p : cd.properties()) {
                    collectAll(p.type(), names);
                }
                for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
                    collectAll(dp.type(), names);
                    for (ParameterDefinition pd : dp.parameters()) {
                        collectAll(pd.type(), names);
                    }
                }
                for (String n : names) {
                    boolean ok = cd.typeParams().contains(n) || n.equals("?")
                            || n.startsWith("meta::pure::metamodel::type::")
                            || platformOwned.contains(n) || want.contains(n);
                    if (!ok) {
                        dangling.put(fqn + " -> " + n, excludedByDecision(n) ? "excluded package"
                                : index.containsKey(n) ? "indexed but not closed"
                                : corpusDefined.contains(n) ? "graph-owned, not indexed"
                                : "unresolved/bare name");
                    }
                }
            }
        }
        if (!dangling.isEmpty()) {
            throw new IllegalStateException("prelude generator: dangling type references in"
                    + " generated declarations (widen the closure, lift an exclusion, or exclude the"
                    + " referencing class):\n  " + dangling.entrySet().stream()
                            .map(e -> e.getKey() + " [" + e.getValue() + "]")
                            .collect(java.util.stream.Collectors.joining("\n  ")));
        }
    }

    // ------------------------------------------------------------------
    // the m3 reader: legend-pure's m3.pure is the ONE spec file in the M3
    // instance (graph) syntax — no `Class` text to copy — so its classes are
    // read structurally and PRINTED as declarations: stored properties,
    // supertypes, type and multiplicity parameters, fully qualified. What
    // it does not carry (receipt): m3's qualified properties, whose bodies
    // are graph-encoded expression sequences (the Typer serves the two the
    // platform needs, classifierGenericType / elementOverride, by hand).
    // A shape it cannot read fails LOUDLY — never a guessed declaration.
    // ------------------------------------------------------------------

    /** A parsed M3 value: a path, an instance ({@code ^classifier [name] { entries }}),
     * a list, a string or a number. */
    sealed interface M3 permits M3.Path, M3.Instance, M3.Items, M3.Str, M3.Num {
        record Path(String text) implements M3 {
            /** {@code Root.children[a].children[b]…} → {@code a::b::…}; a bare
             * name (a primitive: {@code Integer}) stays bare. */
            String fqn() {
                Matcher m = Pattern.compile("children\\[([A-Za-z0-9_]+)\\]").matcher(text);
                List<String> parts = new ArrayList<>();
                while (m.find()) {
                    parts.add(m.group(1));
                }
                if (parts.isEmpty()) {
                    // m3's ROOT-declared names (no @package): the primitive types
                    // live in meta::pure::metamodel::type; Package at the
                    // metamodel root — the same rule the provenance check uses
                    return HAND_CARRIERS.containsKey("meta::pure::metamodel::type::" + text)
                            ? "meta::pure::metamodel::type::" + text
                            : "meta::pure::metamodel::" + text;
                }
                return String.join("::", parts);
            }
            String last() {
                int i = text.lastIndexOf('[');
                return i < 0 ? text : text.substring(i + 1, text.length() - 1);
            }
        }
        record Instance(String classifier, @com.legend.Nullable String name,
                @com.legend.Nullable String at, Map<String, M3> entries) implements M3 {
            /** The entry whose key ends with {@code properties[<key>]}. */
            @com.legend.Nullable M3 entry(String key) {
                for (Map.Entry<String, M3> e : entries.entrySet()) {
                    if (e.getKey().endsWith("properties[" + key + "]")) {
                        return e.getValue();
                    }
                }
                return null;
            }
        }
        record Items(List<M3> items) implements M3 {
        }
        record Str(String value) implements M3 {
        }
        record Num(String value) implements M3 {
        }
    }

    /** A recursive-descent reader of the m3 instance syntax. */
    static final class M3Reader {
        private final String src;
        private int i;

        M3Reader(String src) {
            this.src = src;
        }

        /** Every TOP-LEVEL named instance of the file, in order. */
        static List<M3.Instance> topLevel(String src) {
            M3Reader r = new M3Reader(src);
            List<M3.Instance> out = new ArrayList<>();
            while (true) {
                r.ws();
                if (r.i >= src.length()) {
                    break;
                }
                if (r.peek() == '^') {
                    out.add(r.instance());
                } else {
                    // a non-instance top-level line (imports, section markers,
                    // functions in class syntax): skip the line
                    int nl = src.indexOf('\n', r.i);
                    r.i = nl < 0 ? src.length() : nl + 1;
                }
            }
            return out;
        }

        private char peek() {
            return src.charAt(i);
        }

        private void ws() {
            while (i < src.length()) {
                char c = src.charAt(i);
                if (Character.isWhitespace(c)) {
                    i++;
                } else if (c == '/' && i + 1 < src.length() && src.charAt(i + 1) == '/') {
                    int nl = src.indexOf('\n', i);
                    i = nl < 0 ? src.length() : nl;
                } else {
                    break;
                }
            }
        }

        /** {@code Root.children[a].children[b]…}, optionally ending in a bare
         * segment ({@code …children[relationship].children} — a package path). */
        private static final Pattern PATH = Pattern.compile(
                "[A-Za-z_][A-Za-z0-9_]*(?:\\.[A-Za-z_]+(?:\\[[A-Za-z0-9_]+\\])?)*");

        private String path() {
            Matcher m = PATH.matcher(src);
            if (!m.find(i) || m.start() != i) {
                throw new IllegalStateException("m3 reader: expected a path at " + where());
            }
            i = m.end();
            return m.group();
        }

        private String where() {
            int line = 1;
            for (int k = 0; k < i && k < src.length(); k++) {
                if (src.charAt(k) == '\n') {
                    line++;
                }
            }
            return "line " + line + ": '" + src.substring(i, Math.min(src.length(), i + 60)).replace('\n', ' ') + "'";
        }

        private void expect(char c) {
            ws();
            if (i >= src.length() || src.charAt(i) != c) {
                throw new IllegalStateException("m3 reader: expected '" + c + "' at " + where());
            }
            i++;
        }

        /** {@code ^classifier [name] [@at] { key : value, … }} */
        private M3.Instance instance() {
            expect('^');
            ws();
            String classifier = path();
            ws();
            String name = null;
            String at = null;
            if (i < src.length() && Character.isLetter(peek())) {
                name = path();
                ws();
            }
            if (i < src.length() && peek() == '@') {
                i++;
                at = path();
                ws();
            }
            Map<String, M3> entries = new LinkedHashMap<>();
            expect('{');
            while (true) {
                ws();
                if (peek() == '}') {
                    i++;
                    break;
                }
                String key = path();
                expect(':');
                ws();
                entries.put(key, value());
                ws();
                if (peek() == ',') {
                    i++;
                }
            }
            return new M3.Instance(classifier, name, at, entries);
        }

        private M3 value() {
            ws();
            char c = peek();
            if (c == '^') {
                return instance();
            }
            if (c == '[') {
                i++;
                List<M3> items = new ArrayList<>();
                while (true) {
                    ws();
                    if (peek() == ']') {
                        i++;
                        break;
                    }
                    items.add(value());
                    ws();
                    if (peek() == ',') {
                        i++;
                    }
                }
                return new M3.Items(items);
            }
            if (c == '\'') {
                int start = ++i;
                while (src.charAt(i) != '\'') {
                    if (src.charAt(i) == '\\') {
                        i++;
                    }
                    i++;
                }
                return new M3.Str(src.substring(start, i++));
            }
            if (Character.isDigit(c) || c == '-') {
                int start = i++;
                while (i < src.length() && (Character.isDigit(peek()) || peek() == '.')) {
                    i++;
                }
                return new M3.Num(src.substring(start, i));
            }
            return new M3.Path(path());
        }
    }

    /** The m3 classes as printed declarations, keyed by FQN. */
    static Map<String, String> m3Declarations(String m3Source) {
        Map<String, String> out = new LinkedHashMap<>();
        for (M3.Instance inst : M3Reader.topLevel(m3Source)) {
            boolean cls = inst.classifier().endsWith("children[Class]");
            boolean enm = inst.classifier().endsWith("children[Enumeration]");
            if ((!cls && !enm) || inst.name() == null) {
                continue;
            }
            String pkg = inst.at() == null ? null : new M3.Path(inst.at()).fqn();
            // m3 declares Package (no @package) at the metamodel root
            String fqn = (pkg == null || pkg.isEmpty() ? "meta::pure::metamodel" : pkg) + "::" + inst.name();
            if (cls) {
                out.put(fqn, m3Class(fqn, inst));
            } else {
                List<String> values = new ArrayList<>();
                for (M3 v : items(inst.entry("values"))) {
                    M3.Instance vi = (M3.Instance) v;
                    values.add(vi.name() != null ? vi.name() : str(vi.entry("name")));
                }
                out.put(fqn, "Enum " + fqn + " { " + String.join(", ", values) + " }");
            }
        }
        return out;
    }

    private static String m3Class(String fqn, M3.Instance cls) {
        StringBuilder sb = new StringBuilder("Class ").append(fqn);
        List<String> typeParams = new ArrayList<>();
        for (M3 tp : items(cls.entry("typeParameters"))) {
            typeParams.add(str(((M3.Instance) tp).entry("name")));
        }
        List<String> multParams = new ArrayList<>();
        for (M3 mp : items(cls.entry("multiplicityParameters"))) {
            // spelled as an InstanceValue whose values are the parameter names
            if (mp instanceof M3.Str s) {
                multParams.add(s.value());
            } else if (mp instanceof M3.Instance iv && iv.entry("values") != null) {
                for (M3 v : items(iv.entry("values"))) {
                    multParams.add(str(v));
                }
            } else {
                throw new IllegalStateException("m3 reader: cannot read the multiplicity parameters of " + fqn + ": " + mp);
            }
        }
        if (!typeParams.isEmpty() || !multParams.isEmpty()) {
            sb.append('<').append(String.join(",", typeParams));
            if (!multParams.isEmpty()) {
                sb.append('|').append(String.join(",", multParams));
            }
            sb.append('>');
        }
        List<String> supers = new ArrayList<>();
        for (M3 g : items(cls.entry("generalizations"))) {
            supers.add(genericType(((M3.Instance) g).entry("general")));
        }
        if (!supers.isEmpty()) {
            sb.append(" extends ").append(String.join(", ", supers));
        }
        sb.append(" {");
        for (M3 p : items(cls.entry("properties"))) {
            M3.Instance prop = (M3.Instance) p;
            String name = prop.name() != null ? prop.name() : str(prop.entry("name"));
            sb.append(' ').append(name).append(": ")
                    .append(genericType(prop.entry("genericType")))
                    .append(multiplicity(prop.entry("multiplicity"))).append(';');
        }
        return sb.append(" }").toString();
    }

    private static List<M3> items(@com.legend.Nullable M3 v) {
        return v == null ? List.of() : v instanceof M3.Items it ? it.items() : List.of(v);
    }

    private static String str(@com.legend.Nullable M3 v) {
        if (v instanceof M3.Str s) {
            return s.value();
        }
        throw new IllegalStateException("m3 reader: expected a string, got " + v);
    }

    /** A GenericType instance → its type spelling: a raw type with arguments,
     * a type parameter, or a function type. */
    private static String genericType(@com.legend.Nullable M3 v) {
        if (!(v instanceof M3.Instance g)) {
            throw new IllegalStateException("m3 reader: expected a GenericType instance, got " + v);
        }
        M3 tp = g.entry("typeParameter");
        if (tp instanceof M3.Instance tpi) {
            return str(tpi.entry("name"));
        }
        M3 raw = g.entry("rawType");
        if (raw instanceof M3.Path p) {
            String base = p.fqn();
            List<String> args = new ArrayList<>();
            for (M3 a : items(g.entry("typeArguments"))) {
                args.add(genericType(a));
            }
            List<String> mults = new ArrayList<>();
            for (M3 m : items(g.entry("multiplicityArguments"))) {
                mults.add(multiplicity(m).replaceAll("^\\[|\\]$", ""));
            }
            if (args.isEmpty() && mults.isEmpty()) {
                return base;
            }
            return base + "<" + String.join(",", args) + (mults.isEmpty() ? "" : "|" + String.join(",", mults)) + ">";
        }
        if (raw instanceof M3.Instance ft && ft.classifier().endsWith("children[FunctionType]")) {
            List<String> ps = new ArrayList<>();
            for (M3 pv : items(ft.entry("parameters"))) {
                M3.Instance pi = (M3.Instance) pv;
                ps.add(genericType(pi.entry("genericType")) + multiplicity(pi.entry("multiplicity")));
            }
            return "{" + String.join(", ", ps) + "->" + genericType(ft.entry("returnType"))
                    + multiplicity(ft.entry("returnMultiplicity")) + "}";
        }
        throw new IllegalStateException("m3 reader: cannot spell the generic type " + g);
    }

    private static String multiplicity(@com.legend.Nullable M3 v) {
        if (v instanceof M3.Path p) {
            return switch (p.last()) {
                case "PureOne" -> "[1]";
                case "ZeroOne" -> "[0..1]";
                case "ZeroMany" -> "[*]";
                case "OneMany" -> "[1..*]";
                case "PureZero" -> "[0]";
                default -> throw new IllegalStateException("m3 reader: unknown packageable multiplicity " + p.text());
            };
        }
        if (v instanceof M3.Instance m) {
            M3 param = m.entry("multiplicityParameter");
            if (param != null) {
                return "[" + str(param) + "]";
            }
            String lo = m.entry("lowerBound") instanceof M3.Instance lb && lb.entry("value") instanceof M3.Num n
                    ? n.value() : null;
            String hi = m.entry("upperBound") instanceof M3.Instance ub && ub.entry("value") instanceof M3.Num n
                    ? n.value() : null;
            if (lo == null) {
                throw new IllegalStateException("m3 reader: a multiplicity without a lower bound: " + m);
            }
            return hi == null ? (lo.equals("0") ? "[*]" : "[" + lo + "..*]")
                    : lo.equals(hi) ? "[" + lo + "]" : "[" + lo + ".." + hi + "]";
        }
        throw new IllegalStateException("m3 reader: cannot spell the multiplicity " + v);
    }

    @Test
    @DisplayName("m3 reader: every class of m3.pure prints as a declaration (-Dprelude.m3=1 lists them)")
    void m3ReaderPrintsEveryClass() throws IOException {
        Path pure = Path.of(System.getProperty("legend.pure.root",
                System.getProperty("user.home") + "/legend/legend-pure"));
        Path m3 = pure.resolve(M3_PURE);
        // never an assumption-skip (SkipCensusTest): the reference checkout is
        // this test class's hard default, exactly as preludeIsCurrent's
        assertTrue(Files.isRegularFile(m3), "m3.pure missing at " + m3);
        Map<String, String> decls = m3Declarations(Files.readString(m3, StandardCharsets.UTF_8));
        for (Map.Entry<String, String> e : decls.entrySet()) {
            // every printed declaration parses through the platform's own door
            ParsedModel parsed = ElementParser.parse(e.getValue(), Dialect.LEGEND_PLATFORM);
            assertEquals(1, parsed.elements().size(), e.getKey());
            if ("1".equals(System.getProperty("prelude.m3"))) {
                System.out.println(e.getValue());
            }
        }
        assertTrue(decls.size() >= 85, "m3.pure declares 85 classes; read " + decls.size());
    }

    /** One platform-library function: its module key ({@code fqn#offset} —
     * overloads share an FQN), its verbatim text, its spec file and offset,
     * and the wildcard imports of the section it sits in. */
    record PlatformFunction(String key, String fqn, String file, int offset, String text,
                            List<String> wildcards, boolean nativeDecl) {
    }

    private static final Pattern IMPORT_LINE = Pattern.compile("(?m)^\\s*import\\s+([A-Za-z0-9_:]+)::\\*;");

    /** Every bodied, non-test {@code function} declaration in the platform
     * roots, sliced VERBATIM by the parser ({@code parseFunctionProtocol}
     * from the {@code function} token to the end of its body); a file that
     * does not parse whole is a wall (the census's load walls), never a
     * partial copy. Test functions ({@code test.*}, {@code PCT.test}) are the
     * PCT lane's programs and stay out; {@code PCT.function} marks a library
     * function under conformance and stays in. */
    static List<PlatformFunction> platformFunctions(List<Path> platformRoots, List<String> walls)
            throws IOException {
        List<Path> files = new ArrayList<>();
        for (Path root : platformRoots) {
            if (!Files.isDirectory(root)) {
                // LOUD (batch 2): a missing platform root shrinks the function
                // census and every rule keyed on it — never silently
                throw new IllegalStateException("platform root missing (upstream moved it?): " + root);
            }
            try (Stream<Path> walk = Files.walk(root)) {
                files.addAll(walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList());
            }
        }
        return functionsIn(files, walls);
    }

    /** The engine's {@code native function} declarations: only the files
     *  under {@link #ENGINE_SPEC_ROOTS} that declare one are parsed (105 at
     *  4.138.2 — one per file in core_functions_unclassified, json.pure, the
     *  compilers), and only their natives are carried (the engine's BODIED
     *  functions are not the platform library: legend-pure's is, USER
     *  2026-09-09). Batch 4 §6.2 completed by the batches 1–5 audit
     *  (2026-09-11): 39 upstream natives had been in neither Pure.java nor
     *  the prelude — "unknown function" where "not implemented" is the truth. */
    private static final Pattern ENGINE_NATIVE = Pattern.compile("(?m)^\\s*native\\s+function\\s");

    static List<PlatformFunction> engineNatives(Path engine, List<String> walls) throws IOException {
        List<Path> files = new ArrayList<>();
        for (String r : ENGINE_SPEC_ROOTS) {
            Path root = engine.resolve(r);
            if (!Files.isDirectory(root)) {
                throw new IllegalStateException("engine spec root missing (upstream moved it?): " + root);
            }
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    if (ENGINE_NATIVE.matcher(Files.readString(f, StandardCharsets.UTF_8)).find()) {
                        files.add(f);
                    }
                }
            }
        }
        List<PlatformFunction> out = new ArrayList<>();
        for (PlatformFunction pf : functionsIn(files, walls)) {
            if (pf.nativeDecl()) {
                out.add(pf);
            }
        }
        return out;
    }

    /** The type names a declaration's SIGNATURE spells — after a parameter's
     *  or the return's colon, or as a generic argument — minus the primitives
     *  and the metamodel roots the prelude always carries. */
    static Set<String> referencedTypeNames(String declText) {
        Set<String> out = new LinkedHashSet<>();
        // an identifier or an FQN (double colons only — a single colon is the
        // parameter separator, `config:Type`)
        Matcher m = Pattern.compile("(?<!:)[:<,](?!:)\\s*([A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*)\\s*(?=[<\\[,>])").matcher(declText);
        while (m.find()) {
            String n = m.group(1);
            if (!PRIMITIVE_OR_ROOT.contains(n)) {
                out.add(n);
            }
        }
        return out;
    }

    private static final Set<String> PRIMITIVE_OR_ROOT = Set.of("String", "Integer", "Float", "Decimal",
            "Boolean", "Date", "StrictDate", "DateTime", "Number", "Any", "Nil", "Variant", "Byte",
            "LatestDate", "StrictTime", "Function", "T", "U", "V", "K", "Z", "X", "Y", "R", "P");

    static List<PlatformFunction> functionsIn(List<Path> files, List<String> walls) throws IOException {
        List<PlatformFunction> out = new ArrayList<>();
        {
            for (Path f : files) {
                String text = Files.readString(f, StandardCharsets.UTF_8);
                List<String> parseWalls = new ArrayList<>();
                Compiler.parseSources(List.of(new Compiler.ModelSource(f.toString(), text)),
                        (n, e) -> parseWalls.add(n + ": " + e), Dialect.LEGEND_PLATFORM);
                if (!parseWalls.isEmpty()) {
                    walls.add(parseWalls.get(0));
                    continue;
                }
                TokenStream ts = Lexer.tokenize(text);
                int depth = 0;
                for (int k = 0; k < ts.count(); k++) {
                    if (ts.type(k) == TokenType.BRACE_OPEN) {
                        depth++;
                    } else if (ts.type(k) == TokenType.BRACE_CLOSE) {
                        depth--;
                    }
                    // a DECLARATION-position `function`: at brace depth 0 (a
                    // class's property named `function` sits at depth 1, so
                    // does a Profile's stereotype list), the first token or
                    // right after a previous declaration's `;` or `}` (section
                    // headers lex to nothing; `native function` follows NATIVE,
                    // the keyword in `<<PCT.function>>` follows a DOT).
                    if (depth != 0) {
                        continue;
                    }
                    // a DOCUMENTATION literal (5.99.0: `documentation? function
                    // ...`) occupies declaration position and passes it on;
                    // the declaration's slice then starts AT the literal, so
                    // the prelude carries the doc tagged value the parser
                    // makes of it — verbatim, as everything else here
                    boolean docBefore = k > 0 && ts.type(k - 1) == TokenType.DOC_STRING;
                    int declTok = docBefore ? k - 1 : k;
                    boolean atStart = declTok == 0 || ts.type(declTok - 1) == TokenType.SEMI_COLON
                            || ts.type(declTok - 1) == TokenType.BRACE_CLOSE;
                    // UPSTREAM NATIVES ARE CARRIED, RESPELLED (upstream boundary
                    // batch 4, program §0 right column): a `native function`
                    // declaration at declaration position is sliced from
                    // `native` to its `;` and parsed like the catalog parses its
                    // own signatures — so a function upstream declares in Java
                    // resolves and type-checks here, and fails at LOWERING with
                    // "not implemented", never as "unknown function". Until
                    // 2026-09-10 the `native` token in front of `function` made
                    // the position test fail and every native was dropped.
                    if (ts.type(k) == TokenType.NATIVE && atStart && k + 1 < ts.count()
                            && ts.type(k + 1) == TokenType.FUNCTION) {
                        int semi = k + 1;
                        while (semi < ts.count() && ts.type(semi) != TokenType.SEMI_COLON) {
                            semi++;
                        }
                        if (semi >= ts.count()) {
                            throw new IllegalStateException("prelude generator: native declaration"
                                    + " without ';' at " + f + " token " + k);
                        }
                        String slice = text.substring(ts.start(declTok), ts.end(semi));
                        com.legend.model.ParsedModel one;
                        try {
                            one = ElementParser.parse(slice, Dialect.LEGEND_PLATFORM);
                        } catch (com.legend.parser.ParseException e) {
                            throw new IllegalStateException("prelude generator: native slice at "
                                    + f + " token " + k + ": " + e.getMessage(), e);
                        }
                        if (one.elements().size() != 1
                                || !(one.elements().get(0) instanceof com.legend.model.NativeFunctionDefinition nfd)) {
                            throw new IllegalStateException("prelude generator: native slice at " + f
                                    + " token " + k + " did not parse to one native function");
                        }
                        String nfqn = nfd.qualifiedName();
                        if (!nfqn.contains("::tests::")) {
                            int nstart = ts.start(declTok);
                            int nsectionStart = Math.max(0, text.lastIndexOf("###Pure", nstart));
                            List<String> nwildcards = new ArrayList<>();
                            Matcher nim = IMPORT_LINE.matcher(text.substring(nsectionStart, nstart));
                            while (nim.find()) {
                                nwildcards.add(nim.group(1));
                            }
                            out.add(new PlatformFunction(nfqn + "#" + nstart, nfqn, f.toString(), nstart,
                                    slice, nwildcards, true));
                        }
                        k = semi;
                        continue;
                    }
                    if (ts.type(k) != TokenType.FUNCTION || !atStart) {
                        continue;
                    }
                    ElementParser p = ElementParser.at(ts, declTok, Dialect.LEGEND_PLATFORM);
                    Protocol.PFunction fn;
                    try {
                        fn = p.parseFunctionProtocol();
                    } catch (com.legend.parser.ParseException e) {
                        throw new IllegalStateException("prelude generator: function slice at "
                                + f + " token " + k + " (" + ts.text(Math.min(k + 1, ts.count() - 1))
                                + "…): " + e.getMessage(), e);
                    }
                    boolean test = fn.stereotypes().stream().anyMatch(st ->
                            st.profile().endsWith("test") && !st.profile().endsWith("PCT")
                                    || (st.profile().endsWith("PCT") && st.value().equals("test")));
                    int start = ts.start(declTok);
                    String fqn = fn.pkg().isEmpty() ? fn.name() : fn.pkg() + "::" + fn.name();
                    // a `tests` package is test SUPPORT (fixture models, helpers
                    // over them — the equality test model's ClassWithoutEquality):
                    // the PCT lane's world, not the library's
                    if (test || fqn.contains("::tests::")) {
                        continue;
                    }
                    int sectionStart = Math.max(0, text.lastIndexOf("###Pure", start));
                    List<String> wildcards = new ArrayList<>();
                    Matcher im = IMPORT_LINE.matcher(text.substring(sectionStart, start));
                    while (im.find()) {
                        wildcards.add(im.group(1));
                    }
                    out.add(new PlatformFunction(fqn + "#" + start, fqn, f.toString(), start,
                            text.substring(start, ts.end(p.pos() - 1)), wildcards, false));
                }
            }
        }
        return out;
    }

    /**
     * The declaration's VERBATIM text, delimited by THE PARSER (HOMEWORK
     * §9.7): from the element's first token (the parser's own element offset)
     * to where {@code parseClassDefinition} / {@code parseEnumDefinition}
     * leaves the cursor. A header tagged-value block, a constraint block, a
     * brace inside a string literal — the parser that will read the module
     * decides, never a regex or a brace count.
     */
    static String declarationText(TokenStream tokens, String source, int offset, boolean isEnum) {
        int i = -1;
        for (int k = 0; k < tokens.count(); k++) {
            if (tokens.start(k) == offset) {
                i = k;
                break;
            }
        }
        if (i < 0) {
            throw new IllegalStateException("prelude generator: no token starts at offset " + offset);
        }
        ElementParser p = ElementParser.at(tokens, i, Dialect.LEGEND_PLATFORM);
        if (isEnum) {
            p.parseEnumDefinition();
        } else {
            p.parseClassDefinition(false);
        }
        return source.substring(tokens.start(i), tokens.end(p.pos() - 1));
    }

    /** The spec file's path relative to its checkout root — the module is a
     * committed resource and carries no machine's absolute paths. */
    static String relative(String absolute, Path engine, Path pure) {
        Path f = Path.of(absolute);
        if (f.startsWith(engine)) {
            return "legend-engine/" + engine.relativize(f).toString().replace(java.io.File.separatorChar, '/');
        }
        if (f.startsWith(pure)) {
            return "legend-pure/" + pure.relativize(f).toString().replace(java.io.File.separatorChar, '/');
        }
        throw new IllegalStateException("prelude generator: " + absolute + " is under neither checkout root");
    }

    /** The FQNs {@code Pure.java} declares by hand ({@code native Class …}
     * and {@code Enum …} text), read from the source file. */
    static Set<String> handDeclaredFqns() throws IOException {
        String src = Files.readString(CoreTree.main("com/legend/builtin/Pure.java"),
                StandardCharsets.UTF_8);
        Set<String> out = new LinkedHashSet<>();
        Matcher c = Pattern.compile("native Class ([A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+)").matcher(src);
        while (c.find()) {
            out.add(c.group(1));
        }
        Matcher e = Pattern.compile("\\bEnum (meta::[A-Za-z0-9_:]+)").matcher(src);
        while (e.find()) {
            out.add(e.group(1));
        }
        return out;
    }

    /** The exclusions that are DECISIONS (named classes, the versioned
     * protocol packages, m3 paths) — what the CLOSURE honours. */
    private static boolean excludedByDecision(String fqn) {
        return EXCLUDED_CLASSES.containsKey(fqn)
                || (EXCLUDED_PACKAGE_PREFIXES.stream().anyMatch(fqn::startsWith)
                        && !fqn.startsWith(PROTOCOL_TEMPLATE_M3));
    }

    /** What DEMAND honours: the decisions plus the spec-test-package rule. */
    private static boolean excluded(String fqn) {
        // a spec TEST MODEL (…::tests::Person, …::test::shared::dest::Person)
        // is corpus/library input, never a platform shape — the platform's
        // own test-support namespace (meta::pure::functions::test) stays
        return excludedByDecision(fqn)
                || (fqn.matches(".*::tests?::.*") && !fqn.startsWith("meta::pure::functions::test::")
                        // the spec's PCT harness (meta::pure::test::pct / ::surveyor) — the natives
                        // executeTest/executePCTTest/loadPCTManifest name its shapes (batch 150)
                        && !fqn.startsWith("meta::pure::test::"));
    }

    /** A declaration header, stereotypes/tags and line breaks tolerated
     * ({@code Class <<typemodifiers.abstract>>\n  meta::…::RoutedValueSpecification}). */
    static final Pattern DECL_HEADER = Pattern.compile(
            "(?m)^(Class|Enum)\\s+(?:<<[^>]*>>\\s*)*(?:\\{[^}]*\\}\\s*)?([A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+)");
    private static final Pattern DECL = Pattern.compile(
            "^(Class|Enum)\\s+(?:<<[^>]*>>\\s*)*(?:\\{[^}]*\\}\\s*)?([A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+)");
    /** An m3.pure bootstrap header: {@code ^Root.…children[Class] Name @Root.…children[pkg].children}. */
    private static final Pattern M3_HEADER = Pattern.compile(
            "(?m)^\\^Root\\.[^ ]*children\\[(Class|PrimitiveType|Enumeration)\\] ([A-Za-z_][A-Za-z0-9_]*)(?: @(Root\\.[^ \\n]*))?$");
    /** Platform carriers declared by hand with no spec/m3 counterpart, each with its reason. */
    private static final Map<String, String> HAND_CARRIERS = Map.ofEntries(
            // the PRIMITIVE types: m3 PrimitiveType instances (bootstrap), the
            // language's own value kinds — the compiler's SQL type wall keys on them
            Map.entry("meta::pure::metamodel::type::Number", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Integer", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Float", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Decimal", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::String", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Boolean", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Byte", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::Date", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::StrictDate", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::DateTime", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::LatestDate", "m3 primitive"),
            Map.entry("meta::pure::metamodel::type::StrictTime", "m3 primitive"));
    private static final Pattern EXTENDS_CLAUSE = Pattern.compile(
            "(?m)^(?:Class|Association)\\b[^\\n{]*?\\bextends\\s+([^\\n{\\[]+)");
    private static final Pattern FQN_TOKEN = Pattern.compile("meta::[A-Za-z0-9_]+(?:::[A-Za-z0-9_]+)+");
    private static final Pattern IMPORT = Pattern.compile("^import\\s+([A-Za-z0-9_:]+)::\\*;");
    private static final Pattern TYPE_REF = Pattern.compile(
            "(?:@|\\^|instanceOf\\(|:\\s*)((?:[A-Za-z0-9_]+::)*[A-Z][A-Za-z0-9_]*)"
            // an ENUM VALUE reference (TemporalUnit.YEAR, DurationUnit.YEARS)
            // names its enumeration too — the value's owner is demand
            + "|(?<![\\w$.])((?:[A-Za-z0-9_]+::)*[A-Z][A-Za-z0-9_]*)\\.[A-Z][A-Z0-9_]*\\b");

    // ------------------------------------------------------------------
    // what a declaration names (the closure walks these)
    // ------------------------------------------------------------------

    /** Every QUALIFIED type a class declaration names: supertypes, stored
     * and derived property types, derived parameter types. */
    static Set<String> referencedFqns(ClassDefinition cd) {
        Set<String> out = new LinkedHashSet<>();
        for (TypeExpression t : cd.superClasses()) {
            collectAll(t, out);
        }
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            collectAll(p.type(), out);
        }
        for (DerivedPropertyDefinition dp : cd.derivedProperties()) {
            collectAll(dp.type(), out);
            for (ParameterDefinition pd : dp.parameters()) {
                collectAll(pd.type(), out);
            }
        }
        out.removeIf(n -> !n.contains("::"));
        return out;
    }

    /** Every name a type expression mentions, bare names included. */
    private static void collectAll(TypeExpression t, Set<String> out) {
        switch (t) {
            case TypeExpression.NameRef nr -> out.add(nr.name());
            case TypeExpression.Generic g -> {
                out.add(g.name());
                g.arguments().forEach(a -> collectAll(a, out));
            }
            case TypeExpression.FunctionType ft -> {
                ft.parameters().forEach(p -> collectAll(p.type(), out));
                collectAll(ft.result().type(), out);
            }
            case TypeExpression.RelationType rt -> rt.columns().forEach(c -> collectAll(c.type(), out));
            case TypeExpression.SchemaAlgebra sa -> {
                collectAll(sa.left(), out);
                collectAll(sa.right(), out);
            }
        }
    }
}
