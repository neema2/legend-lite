// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools;

import com.legend.Compiler;
import com.legend.compiler.NameResolver;
import com.legend.model.ClassDefinition;
import com.legend.model.EnumDefinition;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;
import com.legend.protocol.Multiplicity;
import com.legend.protocol.TypeExpression;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE PRELUDE GENERATOR (docs/DECLARATIONS_HOMEWORK_2026_09_04.md, option
 * S, user-ratified 2026-09-04): the library SHAPES a program may name are
 * DATA, generated from the spec — never hand-typed. This tool reads the
 * engine and legend-pure checkouts (spec), finds every class/enum the
 * corpus references (plus the transitive closure of the types those
 * declarations name — the model integrity pass is eager), parses their
 * files with OUR parser, resolves names with OUR resolver, and prints each
 * declaration in the prelude's {@code native Class …} form with fully
 * qualified names into {@code core/src/main/java/com/legend/builtin/Prelude.java}.
 *
 * <p>Declarations ONLY (WORLD_MAP §3): stored properties with their
 * {@code <<equality.Key>>} and default marker, supertypes, type
 * parameters; derived properties, constraints, tagged values and every
 * function are NOT emitted. Shapes already owned by {@code Pure.java}
 * natives, the system metamodel, or a corpus source are skipped (natives
 * win at lookup; corpus duplicates would refuse the build).
 *
 * <p>Modes: {@code -Dprelude.generate=1} WRITES the file; otherwise the
 * test regenerates in memory and asserts the committed file is current
 * (the parity guard — the spec moved, or someone edited by hand).
 */
class PreludeGeneratorTest {

    private static final Path OUT = Path.of(
            "src/main/resources/com/legend/builtin/prelude.pure");

    /** Packages whose shapes are not (yet) generated — each line a decision. */
    private static final List<String> EXCLUDED_PACKAGE_PREFIXES = List.of(
            // VERSIONED protocol payload classes (nine copies of the same
            // shapes, meta::protocols::pure::v1_2x_0::…). The one TEMPLATE copy
            // the engine's own programs name — meta::protocols::pure::vX_X_X::
            // metamodel::m3 (the relational extension's tdsToRelation adapter
            // types its transfers over the template AppliedFunction) — is
            // admitted; see excluded() (Phase 5 batch 147, strict first)
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

    static String generate() throws IOException {
        Path engine = Path.of(System.getProperty("legend.engine.root",
                "/Users/neemsandv/legend/legend-engine"));
        Path pure = Path.of(System.getProperty("legend.pure.root",
                "/Users/neemsandv/legend/legend-pure"));
        List<Path> roots = List.of(
                engine.resolve("legend-engine-xts-relationalStore"),
                engine.resolve("legend-engine-core/legend-engine-core-pure"),
                // the service metamodel (core_service): ^Service(...) in the
                // execution-strategy tests (batch 57)
                engine.resolve("legend-engine-xts-service/legend-engine-language-pure-dsl-service-pure/"
                        + "src/main/resources/core_service"),
                pure);
        Path corpus = engine.resolve("legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/"
                + "legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/"
                + "src/main/resources/core_relational/relational");

        // 1. the spec index: every Class/Enum FQN -> its defining file
        Map<String, Path> index = new TreeMap<>();
        for (Path root : roots) {
            try (Stream<Path> s = Files.walk(root)) {
                for (Path f : s.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    Matcher m = DECL_HEADER.matcher(Files.readString(f, StandardCharsets.UTF_8));
                    while (m.find()) {
                        index.putIfAbsent(m.group(2), f);
                    }
                }
            }
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

        // 2. what the corpus defines itself, and what it names
        Set<String> corpusDefined = new LinkedHashSet<>();
        Set<String> demand = new LinkedHashSet<>();
        Set<String> javaDemand = new LinkedHashSet<>();
        List<Path> scanned = new ArrayList<>();
        try (Stream<Path> s = Files.walk(corpus)) {
            scanned.addAll(s.filter(p -> p.toString().endsWith(".pure")).sorted().toList());
        }
        // the admitted PROGRAM libraries (Corpus.LIBRARY_FILES) are corpus
        // input too: their signatures are eager, so their shapes are demand
        scanned.addAll(com.legend.rcorpus.Corpus.LIBRARY_FILES);
        // the SYSTEM LAYER's own Pure text (SystemMetamodel: the metamodel
        // store's classes, mappings and views) names library shapes bare
        // through its imports (SQLExecutionNode.resultColumns:
        // SQLResultColumn[*]) — scanned like a corpus file
        List<String> texts = new ArrayList<>();
        for (Path f : scanned) {
            texts.add(Files.readString(f, StandardCharsets.UTF_8));
        }
        String systemText = com.legend.builtin.SystemMetamodel.source();
        texts.add(systemText);
        {
            for (String src : texts) {
                // the system layer is PLATFORM demand: what it names must exist
                // without the corpus (a corpus-defined shape is generated too)
                Set<String> sink = src == systemText ? javaDemand : demand;
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
        // JAVA demand: every spec FQN the platform's own sources name — the
        // native SIGNATURES and remaining hand declarations in Pure.java, and
        // the FQN literals the compiler/resolver/lowering code dispatches on
        // (Prelude.java itself excluded: generated output is never demand)
        try (Stream<Path> s = Files.walk(Path.of("src/main/java"))) {
            for (Path f : s.filter(p -> p.toString().endsWith(".java")
                    && !p.getFileName().toString().equals("Prelude.java")).sorted().toList()) {
                Matcher r = FQN_TOKEN.matcher(Files.readString(f, StandardCharsets.UTF_8));
                while (r.find()) {
                    // a spec TEST MODEL (…::tests::Person) named in a harness
                    // comment or fixture is corpus input, never a platform shape
                    if (index.containsKey(r.group()) && (!r.group().matches(".*::tests?::.*")
                            || r.group().startsWith("meta::pure::test::"))) {
                        javaDemand.add(r.group());
                    }
                }
            }
        }
        // owned = the HAND-declared natives (read from Pure.java's SOURCE, so
        // the generator never depends on the previous Prelude.java loading),
        // the system layer and the corpus's own definitions
        Set<String> platformOwned = new LinkedHashSet<>(handDeclaredFqns());
        platformOwned.addAll(com.legend.builtin.SystemMetamodel.elementFqns());
        Set<String> owned = new LinkedHashSet<>(platformOwned);
        owned.addAll(corpusDefined);

        // 3. parse + resolve the defining files, closing over referenced types
        Map<String, PackageableElement> resolved = new LinkedHashMap<>();
        Map<String, String> declText = new LinkedHashMap<>();   // fqn -> the declaration's source text
        Map<String, String> fileOf = new LinkedHashMap<>();     // fqn -> the spec file (its section's imports)
        Set<String> pulledFromCorpus = new LinkedHashSet<>();   // corpus-tree classes the closure needed
        Map<String, List<String>> fileImports = new LinkedHashMap<>();   // spec file -> its import statements
        Set<Path> parsedFiles = new LinkedHashSet<>();
        Set<String> want = new LinkedHashSet<>();
        for (String fqn : demand) {
            if (!owned.contains(fqn) && !excluded(fqn)) {
                want.add(fqn);
            }
        }
        // what the PLATFORM names must exist without the corpus: a library
        // class that happens to be defined inside the corpus tree
        // (scanRelations::RelationTree, TestDataGenResult) is generated all
        // the same — the corpus loader's own copy is shadowed by the native
        for (String fqn : javaDemand) {
            if (!handDeclaredFqns().contains(fqn) && !excluded(fqn)
                    && !com.legend.builtin.SystemMetamodel.elementFqns().contains(fqn)) {
                want.add(fqn);
            }
        }
        Set<String> knownFqns = new LinkedHashSet<>(index.keySet());
        knownFqns.addAll(owned);
        boolean grew = true;
        while (grew) {
            grew = false;
            List<Compiler.ModelSource> sources = new ArrayList<>();
            for (String fqn : new ArrayList<>(want)) {
                Path f = index.get(fqn);
                if (f != null && parsedFiles.add(f)) {
                    sources.add(new Compiler.ModelSource(f.toString(),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
            if (!sources.isEmpty()) {
                List<String> parseWalls = new ArrayList<>();
                ParsedModel parsed = Compiler.parseSources(sources,
                        (name, err) -> parseWalls.add(name + " => " + err),
                        com.legend.parser.Dialect.LEGEND_PLATFORM).model();
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
                        if (srcName != null && off != null) {
                            for (Compiler.ModelSource ms : sources) {
                                if (ms.name().equals(srcName)) {
                                    declText.putIfAbsent(el.qualifiedName(), declarationText(ms.text(), off));
                                    fileOf.putIfAbsent(el.qualifiedName(), srcName);
                                    fileImports.computeIfAbsent(srcName, k -> importsOf(ms.text()));
                                }
                            }
                        }
                    }
                }
            }
            // closure: every type a wanted declaration names — PLATFORM
            // ownership only (hand + system): a corpus-defined shape a
            // generated declaration names is generated too, or the platform
            // would not stand without the corpus (SQLExecutionNode.resultColumns)
            for (String fqn : new ArrayList<>(want)) {
                PackageableElement el = resolved.get(fqn);
                if (el instanceof ClassDefinition cd) {
                    for (String ref : referencedFqns(cd)) {
                        // a type a WANTED declaration names is part of that shape's
                        // graph: admitted even from a spec tests:: package (the
                        // engine's SqlFunction.tests : SqlFunctionTest[*]) — only a
                        // DECIDED exclusion or a graph-owned class stays out (§10:
                        // the module is a closed library the boot layer checks)
                        if (!platformOwned.contains(ref) && !excludedByDecision(ref)
                                && index.containsKey(ref) && want.add(ref)) {
                            // a corpus-tree class pulled in this way: the graph's own
                            // copy yields to the prelude's (Compiler.withoutPreludeShadows)
                            if (corpusDefined.contains(ref)) {
                                pulledFromCorpus.add(ref);
                            }
                            grew = true;
                        }
                    }
                }
            }
        }
        for (String fqn : want) {
            if (!resolved.containsKey(fqn)) {
                throw new IllegalStateException("prelude generator: '" + fqn
                        + "' is indexed at " + index.get(fqn) + " but did not parse as a class/enum");
            }
        }
        // closure completeness (the model integrity pass is eager): every
        // type a generated declaration names must be owned, generated, a
        // primitive, or one of the class's own type parameters — a bare or
        // dangling name here is a generator gap or an exclusion to widen
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
                for (String n : names) {
                    boolean ok = cd.typeParams().contains(n) || n.equals("?")
                            || n.startsWith("meta::pure::metamodel::type::")
                            || owned.contains(n) || want.contains(n);
                    if (!ok) {
                        dangling.put(fqn + " -> " + n, excluded(n) ? "excluded package"
                                : index.containsKey(n) ? "indexed but not closed" : "unresolved/bare name");
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

        // 4. print
        // THE MODULE IS A CLOSED LIBRARY (§10): the boot layer checks every
        // referenced type. A class whose declaration names something outside
        // the platform universe (an excluded spec TEST model, a corpus class
        // the graph owns) is test scaffolding — omitted and recorded, unless
        // Java itself demands it (then it is a generator error to fix).
        Set<String> universe = new LinkedHashSet<>(platformOwned);
        universe.addAll(want);
        List<String> omitted = new ArrayList<>();
        boolean dropped = true;
        while (dropped) {
            dropped = false;
            for (String fqn : new ArrayList<>(want)) {
                if (!(resolved.get(fqn) instanceof ClassDefinition cd)) {
                    continue;
                }
                String escape = escapingReference(cd, universe);
                if (escape != null) {
                    if (javaDemand.contains(fqn)) {
                        throw new IllegalStateException("prelude generator: Java demands " + fqn
                                + " but its declaration names " + escape + ", which is outside the module");
                    }
                    want.remove(fqn);
                    universe.remove(fqn);
                    omitted.add(fqn + " names " + escape);
                    dropped = true;
                }
            }
        }
        // THE CENSUS (-Dprelude.census=1, docs/PRELUDE_MODULE_HOMEWORK_2026_09_08.md): one row per
        // wanted declaration — where it comes from, who demands it, and what the module must carry
        if ("1".equals(System.getProperty("prelude.census"))) {
            List<String> rows = new ArrayList<>();
            rows.add("fqn\tsource\tdemand\tcorpusDefined\tconstraints\tderived\tstereotypes\ttaggedValues\tdefaults\tescapes\tfile");
            for (String fqn : new java.util.TreeSet<>(want)) {
                PackageableElement el = resolved.get(fqn);
                String file = fileOf.getOrDefault(fqn, "?");
                String source = file.contains("legend-pure") ? "legend-pure" : file.contains("legend-engine") ? "legend-engine" : "?";
                String dem = javaDemand.contains(fqn) ? "java" : demand.contains(fqn) ? "corpus" : "closure";
                if (el instanceof ClassDefinition cd) {
                    long defaults = cd.properties().stream().filter(ClassDefinition.PropertyDefinition::hasDefault).count();
                    String esc = escapingReference(cd, universe);
                    rows.add(String.join("\t", fqn, source, dem, String.valueOf(corpusDefined.contains(fqn)),
                            String.valueOf(cd.constraints().size()), String.valueOf(cd.derivedProperties().size()),
                            String.valueOf(cd.stereotypes().size()), String.valueOf(cd.taggedValues().size()),
                            String.valueOf(defaults), esc == null ? "" : esc, file));
                } else {
                    rows.add(String.join("\t", fqn, source, dem, String.valueOf(corpusDefined.contains(fqn)),
                            "enum", "", "", "", "", "", file));
                }
            }
            for (String o : omitted) {
                rows.add("OMITTED\t" + o);
            }
            Files.createDirectories(Path.of("target"));
            Files.write(Path.of("target/prelude-census.tsv"), rows);
            System.out.println("[prelude-census] " + (rows.size() - 1) + " rows -> target/prelude-census.tsv");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("// Copyright 2026 Legend Contributors\n");
        sb.append("// SPDX-License-Identifier: Apache-2.0\n");
        sb.append("//\n");
        sb.append("// GENERATED — do not edit (com.legend.tools.PreludeGeneratorTest, -Dprelude.generate=1).\n");
        sb.append("// THE PRELUDE AS A MODULE (docs/SYSTEM_PRELUDE_DESIGN_2026_09_08.md §10): the library shapes the\n");
        sb.append("// corpus and the platform's Java name, copied from the legend-pure / legend-engine spec — one\n");
        sb.append("// ###Pure section per spec file with that file's imports, each class with its stored properties\n");
        sb.append("// (FQN-qualified) and its DERIVED properties verbatim. Compiled through the user pipeline as the\n");
        sb.append("// boot layer beside the system metamodel (Compiler.bootLayer): resolved, normalized, cached once.\n");
        sb.append("// Shapes Pure.java still declares by hand are skipped here until their hand copy is deleted.\n");
        int classes = 0;
        int enums = 0;
        java.util.TreeMap<String, List<String>> byFile = new java.util.TreeMap<>();
        for (String fqn : new java.util.TreeSet<>(want)) {
            if (resolved.get(fqn) instanceof ClassDefinition || resolved.get(fqn) instanceof EnumDefinition) {
                byFile.computeIfAbsent(fileOf.getOrDefault(fqn, "?"), k -> new ArrayList<>()).add(fqn);
            }
        }
        for (Map.Entry<String, List<String>> section : byFile.entrySet()) {
            sb.append("\n###Pure\n// ").append(section.getKey()).append('\n');
            for (String imp : fileImports.getOrDefault(section.getKey(), List.of())) {
                sb.append(imp).append('\n');
            }
            for (String fqn : section.getValue()) {
                PackageableElement el = resolved.get(fqn);
                String text;
                if (el instanceof ClassDefinition cd) {
                    text = printClass(cd, declText.getOrDefault(fqn, ""));
                    classes++;
                } else {
                    text = printEnum((EnumDefinition) el);
                    enums++;
                }
                roundTrip(fqn, text);
                sb.append(text).append('\n');
            }
        }
        sb.append("\n// ").append(classes).append(" classes, ").append(enums).append(" enums.\n");
        if (!pulledFromCorpus.isEmpty()) {
            sb.append("// PULLED FROM THE CORPUS TREE (a platform shape's declaration names them; the graph's copy yields):\n");
            for (String c : new java.util.TreeSet<>(pulledFromCorpus)) {
                sb.append("//   ").append(c).append('\n');
            }
        }
        if (!omitted.isEmpty()) {
            sb.append("// OMITTED (declaration names a type outside the module — spec test scaffolding):\n");
            for (String o : omitted) {
                sb.append("//   ").append(o).append('\n');
            }
        }
        String module = sb.toString();
        // the whole module parses as ONE model, sections and imports included
        var whole = com.legend.parser.ElementParser.parse(module, com.legend.parser.Dialect.LEGEND_PLATFORM);
        if (whole.elements().size() != classes + enums) {
            throw new IllegalStateException("prelude generator: the module parses to "
                    + whole.elements().size() + " elements, expected " + (classes + enums));
        }
        return module;
    }

    /** The first type a class declaration names that is not a primitive, a
     * type parameter, or a member of {@code universe}; null when closed. */
    static @com.legend.Nullable String escapingReference(ClassDefinition cd, Set<String> universe) {
        Set<String> params = new java.util.HashSet<>(cd.typeParams());
        List<TypeExpression> refs = new ArrayList<>(cd.superClasses());
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            refs.add(p.type());
        }
        for (com.legend.protocol.DerivedPropertyDefinition dp : cd.derivedProperties()) {
            refs.add(dp.type());
            for (com.legend.protocol.ParameterDefinition pd : dp.parameters()) {
                refs.add(pd.type());
            }
        }
        for (TypeExpression t : refs) {
            String e = escapingName(t, params, universe);
            if (e != null) {
                return e;
            }
        }
        return null;
    }

    private static @com.legend.Nullable String escapingName(TypeExpression t, Set<String> params, Set<String> universe) {
        switch (t) {
            case TypeExpression.NameRef nr -> {
                String n = nr.name();
                return params.contains(n) || universe.contains(n)
                        || com.legend.compiler.element.type.Type.Primitive.findByFqn(n).isPresent()
                        || n.equals("meta::pure::metamodel::type::Any") || n.equals("meta::pure::metamodel::type::Nil")
                        ? null : n;
            }
            case TypeExpression.Generic g -> {
                String e = escapingName(new TypeExpression.NameRef(g.name()), params, universe);
                if (e != null) {
                    return e;
                }
                for (TypeExpression a : g.arguments()) {
                    e = escapingName(a, params, universe);
                    if (e != null) {
                        return e;
                    }
                }
                return null;
            }
            case TypeExpression.FunctionType ft -> {
                for (TypeExpression.TypedParameter p : ft.parameters()) {
                    String e = escapingName(p.type(), params, universe);
                    if (e != null) {
                        return e;
                    }
                }
                return escapingName(ft.result().type(), params, universe);
            }
            default -> {
                return null;   // relation types / schema algebra: columns are scalars or params
            }
        }
    }

    /** The class's constraint block {@code [ … ]} as written in the spec,
     * whitespace-normalized; empty when the header has none. Scans the header
     * at depth 0 (generic arguments and function types carry brackets of
     * their own) up to the body's opening brace. */
    static String constraintsText(String declText, String fqn) {
        int depth = 0;
        boolean inString = false;
        // the header's stereotypes / tagged values precede the name: scan after it
        int from = declText.indexOf(fqn);
        from = from < 0 ? 0 : from + fqn.length();
        for (int i = from; i < declText.length(); i++) {
            char c = declText.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '\'') {
                    inString = false;
                }
                continue;
            }
            if (c == '\'') {
                inString = true;
            } else if (c == '<' || c == '(') {
                depth++;
            } else if (c == '>' || c == ')') {
                depth--;
            } else if (c == '{' && depth == 0) {
                return "";
            } else if (c == '[' && depth == 0) {
                int d = 0;
                for (int j = i; j < declText.length(); j++) {
                    char cj = declText.charAt(j);
                    if (inString) {
                        if (cj == '\\') {
                            j++;
                        } else if (cj == '\'') {
                            inString = false;
                        }
                        continue;
                    }
                    if (cj == '\'') {
                        inString = true;
                    } else if (cj == '[' || cj == '{' || cj == '(') {
                        d++;
                    } else if (cj == ']' || cj == '}' || cj == ')') {
                        d--;
                        if (d == 0) {
                            return declText.substring(i, j + 1).replaceAll("\\s+", " ");
                        }
                    }
                }
            }
        }
        return "";
    }

    /** The {@code import x::y::*;} statements of a spec file, in order. */
    static List<String> importsOf(String text) {
        List<String> out = new ArrayList<>();
        for (String line : text.split("\n")) {
            Matcher im = IMPORT.matcher(line.strip());
            if (im.matches()) {
                out.add(line.strip());
            }
        }
        return out;
    }

    /**
     * A DERIVED property's declaration text, verbatim from the spec class body:
     * {@code name(params){body}:Type[m];} — located by its name at member
     * position, closed by brace matching (string literals skipped) and the
     * terminating {@code ;}. The spec text IS the program; the generator
     * copies it, never re-types it (WORLD_MAP rule 2).
     */
    static String derivedText(String declText, String name) {
        Matcher m = Pattern.compile("(?m)^[ \\t]*(?:<<[^>]*>>[ \\t]*)*(?:\\{[^}\\n]*\\}\\s*)?" + Pattern.quote(name)
                + "\\s*\\(").matcher(declText);
        if (!m.find()) {
            throw new IllegalStateException("prelude generator: derived property '" + name
                    + "' not found in the declaration text");
        }
        int start = m.start();
        while (Character.isWhitespace(declText.charAt(start))) {
            start++;
        }
        int i = m.end() - 1;   // at '('
        int depth = 0;
        boolean inString = false;
        boolean sawBody = false;
        for (; i < declText.length(); i++) {
            char c = declText.charAt(i);
            if (inString) {
                if (c == '\\') {
                    i++;
                } else if (c == '\'') {
                    inString = false;
                }
                continue;
            }
            if (c == '\'') {
                inString = true;
            } else if (c == '(' || c == '{' || c == '[') {
                depth++;
                if (c == '{') {
                    sawBody = true;
                }
            } else if (c == ')' || c == '}' || c == ']') {
                depth--;
            } else if (c == ';' && depth == 0 && sawBody) {
                return declText.substring(start, i + 1).replaceAll("\\s+", " ");
            }
        }
        throw new IllegalStateException("prelude generator: derived property '" + name
                + "' has no terminating ';' in the declaration text");
    }

    /** Every printed declaration must parse back through the prelude's own
     * door (one element, platform dialect) — a printer gap fails HERE with
     * the text, never at class-load. */
    private static void roundTrip(String fqn, String text) {
        try {
            var parsed = com.legend.parser.ElementParser.parse(text,
                    com.legend.parser.Dialect.LEGEND_PLATFORM);
            if (parsed.elements().size() != 1) {
                throw new IllegalStateException("parsed " + parsed.elements().size() + " elements");
            }
        } catch (RuntimeException e) {
            throw new IllegalStateException("prelude generator: printed declaration of " + fqn
                    + " does not parse: " + e.getMessage() + "\n  " + text, e);
        }
    }

    /** The FQNs {@code Pure.java} declares by hand ({@code native Class …}
     * and {@code Enum …} text), read from the source file. */
    static Set<String> handDeclaredFqns() throws IOException {
        String src = Files.readString(Path.of("src/main/java/com/legend/builtin/Pure.java"),
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

    /** {@link #excluded} without the spec-test-package rule: the exclusions that
     * are DECISIONS (named classes, versioned protocol packages). */
    private static boolean excludedByDecision(String fqn) {
        return EXCLUDED_CLASSES.containsKey(fqn)
                || (EXCLUDED_PACKAGE_PREFIXES.stream().anyMatch(fqn::startsWith)
                        && !fqn.startsWith(PROTOCOL_TEMPLATE_M3));
    }

    private static boolean excluded(String fqn) {
        // a spec TEST MODEL (…::tests::Person, …::test::shared::dest::Person)
        // is corpus/library input, never a platform shape — the platform's
        // own test-support namespace (meta::pure::functions::test) stays
        return EXCLUDED_CLASSES.containsKey(fqn)
                || (fqn.matches(".*::tests?::.*") && !fqn.startsWith("meta::pure::functions::test::")
                        // the spec's PCT harness (meta::pure::test::pct / ::surveyor) — the natives
                        // executeTest/executePCTTest/loadPCTManifest name its shapes (batch 150)
                        && !fqn.startsWith("meta::pure::test::"))
                || (EXCLUDED_PACKAGE_PREFIXES.stream().anyMatch(fqn::startsWith)
                        && !fqn.startsWith(PROTOCOL_TEMPLATE_M3));
    }

    /** A declaration header, stereotypes/tags and line breaks tolerated
     * ({@code Class <<typemodifiers.abstract>>\n  meta::…::RoutedValueSpecification}). */
    private static final Pattern DECL_HEADER = Pattern.compile(
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
    // the printer: resolved records -> prelude declaration text
    // ------------------------------------------------------------------

    static Set<String> referencedFqns(ClassDefinition cd) {
        Set<String> out = new LinkedHashSet<>();
        for (TypeExpression t : cd.superClasses()) {
            collect(t, out);
        }
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            collect(p.type(), out);
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

    private static void collect(TypeExpression t, Set<String> out) {
        switch (t) {
            case TypeExpression.NameRef nr -> out.add(nr.name());
            case TypeExpression.Generic g -> {
                out.add(g.name());
                g.arguments().forEach(a -> collect(a, out));
            }
            case TypeExpression.FunctionType ft -> {
                ft.parameters().forEach(p -> collect(p.type(), out));
                collect(ft.result().type(), out);
            }
            case TypeExpression.RelationType rt -> rt.columns().forEach(c -> collect(c.type(), out));
            case TypeExpression.SchemaAlgebra sa -> {
                collect(sa.left(), out);
                collect(sa.right(), out);
            }
        }
    }

    /** The declaration's own text: from its start offset to the brace that
     * closes its body (the spec's verbatim default expressions live there). */
    static String declarationText(String source, int offset) {
        int open = source.indexOf('{', offset);
        if (open < 0) {
            return "";
        }
        int depth = 0;
        for (int i = open; i < source.length(); i++) {
            char c = source.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return source.substring(offset, i + 1);
                }
            }
        }
        return source.substring(offset);
    }

    /** The VERBATIM default expression of a property, sliced from the
     * declaration text ({@code name : Type[m] = <expr>;}); only literal
     * defaults (string, number, boolean) are admitted — an enum or
     * expression default would need qualification and is a loud gap. */
    private static String defaultText(String declText, String property) {
        Matcher m = Pattern.compile("(?m)^\\s*(?:<<[^>]*>>\\s*)*" + Pattern.quote(property)
                + "\\s*:\\s*[^;=]+?=\\s*([^;]+);").matcher(declText);
        if (!m.find()) {
            throw new IllegalStateException("prelude generator: default of '" + property
                    + "' not found in the declaration text");
        }
        String v = m.group(1).strip();
        // admitted verbatim: literals, and expressions whose every capitalised
        // name is already fully qualified (the spec spells
        // `= ^meta::pure::runtime::ExecutionContext()`); a BARE type/enum
        // name would need this file's imports and is a loud gap
        Matcher bare = Pattern.compile("(?<![:A-Za-z0-9_])[A-Z][A-Za-z0-9_]*").matcher(v);
        if (!bare.find() || v.startsWith("'")) {
            return v;
        }
        throw new IllegalStateException("prelude generator: default '" + v + "' on '"
                + property + "' names a bare type — extend the printer");
    }

    static String printClass(ClassDefinition cd) {
        return printClass(cd, "");
    }

    static String printClass(ClassDefinition cd, String declText) {
        StringBuilder sb = new StringBuilder("native Class ").append(cd.qualifiedName());
        if (!cd.typeParams().isEmpty()) {
            sb.append('<').append(String.join(", ", cd.typeParams())).append('>');
        }
        if (!cd.superClasses().isEmpty()) {
            sb.append(" extends ");
            List<String> sups = new ArrayList<>();
            for (TypeExpression s : cd.superClasses()) {
                sups.add(printType(s));
            }
            sb.append(String.join(", ", sups));
        }
        // CONSTRAINTS: the spec's own text, verbatim (§10 — the normalizer lifts
        // them like a user class's; a module copy replacing the graph's must
        // carry what the graph's carried)
        String constraints = constraintsText(declText, cd.qualifiedName());
        if (!constraints.isEmpty()) {
            sb.append(' ').append(constraints);
        } else if (!cd.constraints().isEmpty() && !"1".equals(System.getProperty("prelude.census"))) {
            throw new IllegalStateException("prelude generator: " + cd.qualifiedName()
                    + " declares constraints the declaration text does not show");
        }
        sb.append(" {");
        for (ClassDefinition.PropertyDefinition p : cd.properties()) {
            sb.append(' ');
            boolean key = p.stereotypes().stream().anyMatch(st ->
                    com.legend.compiler.element.type.PlatformTypes.isProfile(st.profileName(),
                            com.legend.compiler.element.type.PlatformTypes.EQUALITY_PROFILE)
                            && st.stereotypeName().equals("Key"));
            if (key) {
                sb.append("<<equality.Key>> ");
            }
            sb.append(p.name()).append(": ").append(printType(p.type()))
                    .append(printMult(p.multiplicity()));
            if (p.hasDefault()) {
                // the parsed record keeps the FACT of a default (NewChecker
                // reads only that); the prelude still carries the spec's
                // VERBATIM literal, sliced from the declaration text
                sb.append(" = ").append(defaultText(declText, p.name()));
            }
            sb.append(';');
        }
        // DERIVED properties: the spec's own text (§10 — the module's section
        // imports qualify their names; the normalizer lifts them)
        for (com.legend.protocol.DerivedPropertyDefinition dp : cd.derivedProperties()) {
            if (!declText.isEmpty()) {
                sb.append(' ').append(derivedText(declText, dp.name()));
            }
        }
        // self-check: a derived-shaped member in the spec text the parser did not
        // list is a PARSER gap — loud here, never a silently narrower shape
        if (!declText.isEmpty()) {
            int inText = 0;
            Matcher dm = Pattern.compile("(?m)^[ \\t]*(?:<<[^>]*>>[ \\t]*)*(?:\\{[^}\\n]*\\}\\s*)?[A-Za-z_][A-Za-z0-9_]*\\s*\\([^)]*\\)\\s*\\{")
                    .matcher(declText.substring(declText.indexOf('{') + 1));
            while (dm.find()) {
                inText++;
            }
            if (inText != cd.derivedProperties().size()) {
                throw new IllegalStateException("prelude generator: " + cd.qualifiedName() + " declares "
                        + inText + " derived propert(ies) in the spec text but the parser lists "
                        + cd.derivedProperties().size());
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    static String printEnum(EnumDefinition ed) {
        return "Enum " + ed.qualifiedName() + " { " + String.join(", ", ed.values()) + " }";
    }

    static String printType(TypeExpression t) {
        return switch (t) {
            case TypeExpression.NameRef nr -> nr.name();
            case TypeExpression.Generic g -> {
                List<String> args = new ArrayList<>();
                g.arguments().forEach(a -> args.add(printType(a)));
                String mults = g.multiplicityArguments().isEmpty() ? ""
                        : "|" + String.join(", ", g.multiplicityArguments());
                yield g.name() + "<" + String.join(", ", args) + mults + ">";
            }
            case TypeExpression.FunctionType ft -> {
                List<String> ps = new ArrayList<>();
                for (TypeExpression.TypedParameter p : ft.parameters()) {
                    ps.add(printType(p.type()) + printMult(p.multiplicity()));
                }
                yield "{" + String.join(", ", ps) + "->" + printType(ft.result().type())
                        + printMult(ft.result().multiplicity()) + "}";
            }
            case TypeExpression.RelationType rt -> {
                List<String> cs = new ArrayList<>();
                for (TypeExpression.Column c : rt.columns()) {
                    cs.add(c.name() + ":" + printType(c.type()) + printMult(c.multiplicity()));
                }
                yield "(" + String.join(", ", cs) + ")";
            }
            case TypeExpression.SchemaAlgebra sa -> throw new IllegalStateException(
                    "prelude generator: schema algebra in a declaration type — extend the printer");
        };
    }

    static String printMult(Multiplicity m) {
        return switch (m) {
            case Multiplicity.Concrete c -> {
                if (c.upperBound() == null) {
                    yield c.lowerBound() == 0 ? "[*]" : "[" + c.lowerBound() + "..*]";
                }
                yield c.lowerBound() == c.upperBound() ? "[" + c.lowerBound() + "]"
                        : "[" + c.lowerBound() + ".." + c.upperBound() + "]";
            }
            case Multiplicity.Parameter p -> "[" + p.name() + "]";
        };
    }

    private static String javaString(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
