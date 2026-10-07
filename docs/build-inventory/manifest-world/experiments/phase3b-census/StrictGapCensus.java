package com.legend.generators;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Phase 3b probe (not in the source tree): what does not work in the reference lane's STRICT load, and why — complete.
 * <ul>
 *   <li>DROPPED: each file the strict load throws away, with the first error that dropped it, and whether the drop is
 *       only a knock-on (the tolerant diagnosis finds no broken element of its own in the file);</li>
 *   <li>BROKEN: every broken element of every file, from the tolerant build used as a diagnostic (one row per element,
 *       its first failure) — every problem in a dropped file, not only the first;</li>
 *   <li>FAILED: every body that fails to type in the strict load, with its error, and knockOn=&lt;file&gt; when the
 *       error names an element declared only in a dropped file.</li>
 * </ul>
 * <pre>StrictGapCensus &lt;out.tsv&gt;</pre>
 */
public final class StrictGapCensus {

    private static final Pattern QUOTED = Pattern.compile("'([^']+)'");

    public static void main(String[] args) throws Exception {
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        List<ManifestWorldCensusTest.Module> world =
                ManifestWorldCensusTest.closure("core_relational", ManifestWorldCensusTest.manifests(engine, pure));
        List<Compiler.ModelSource> all = new ArrayList<>();
        for (ManifestWorldCensusTest.Module m : world) {
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    all.add(new Compiler.ModelSource(m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/'),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(args[0]), StandardCharsets.UTF_8))) {
            w.println("kind\tfile\tsubject\terror\tmessage\tnote");

            // 1. the tolerant build as a DIAGNOSTIC: every broken element with its first failure
            Compiler.ParsedModule full = parse(all);
            Map<String, String> elementSource = full.model().elementSources();
            Compiler.BuiltModule tolerant = Compiler.buildModule(pruned(full));
            Map<String, List<String>> brokenByFile = new LinkedHashMap<>();
            for (var e : tolerant.walls().entrySet()) {
                String file = sourceOf(elementSource, e.getKey());
                brokenByFile.computeIfAbsent(file, k -> new ArrayList<>()).add(e.getKey());
                w.println("BROKEN\t" + file + "\t" + e.getKey() + "\t\t" + clean(e.getValue()) + "\t");
            }

            // 2. the strict load, as the reference lane does it (OurResolutions.dump)
            List<Compiler.ModelSource> sources = new ArrayList<>(all);
            ModelContext ctx = null;
            Map<String, String> droppedFirstError = new LinkedHashMap<>();
            for (int round = 0; round < 4000 && ctx == null; round++) {
                Compiler.ParsedModule module = parse(sources);
                try {
                    ctx = Compiler.buildModel(pruned(module));
                } catch (com.legend.error.ModelException e) {
                    String el = e.element();
                    String src = el == null ? null : sourceOf(module.model().elementSources(), el);
                    if (src == null) {
                        throw e;
                    }
                    droppedFirstError.put(src, "element=" + el + " :: " + clean(e.getMessage()));
                    final String drop = src;
                    sources.removeIf(s -> s.name().equals(drop));
                }
            }
            if (ctx == null) {
                throw new IllegalStateException("the model did not converge");
            }
            // the elements each dropped file declares, by full name and by simple name
            Map<String, String> droppedDeclares = new HashMap<>();
            for (var e : elementSource.entrySet()) {
                if (droppedFirstError.containsKey(e.getValue())) {
                    droppedDeclares.put(e.getKey(), e.getValue());
                    int cut = e.getKey().lastIndexOf("::");
                    droppedDeclares.putIfAbsent(cut < 0 ? e.getKey() : e.getKey().substring(cut + 2), e.getValue());
                }
            }
            Set<String> loaded = new HashSet<>();
            for (PackageableElement el : parse(sources).model().elements()) {
                loaded.add(el.qualifiedName());
                int cut = el.qualifiedName().lastIndexOf("::");
                loaded.add(cut < 0 ? el.qualifiedName() : el.qualifiedName().substring(cut + 2));
            }
            for (var e : droppedFirstError.entrySet()) {
                boolean own = brokenByFile.containsKey(e.getKey());
                w.println("DROPPED\t" + e.getKey() + "\t\t\t" + e.getValue() + "\t"
                        + (own ? "broken elements: " + brokenByFile.get(e.getKey()).size()
                               : "KNOCK-ON: no broken element of its own (the tolerant build keeps it)"));
            }

            // 3. the bodies, each with its knock-on source when its error names an element only a dropped file declares
            SpecCompiler specs = new SpecCompiler(ctx);
            for (String fqn : new TreeSet<>(ctx.functionFqns())) {
                List<TypedFunction> overloads;
                try {
                    overloads = ctx.findFunction(fqn);
                } catch (RuntimeException e) {
                    w.println("UNRESOLVED\t" + sourceOf(elementSource, fqn) + "\t" + fqn + "\t" + e.getClass().getSimpleName()
                            + "\t" + clean(e.getMessage()) + "\t");
                    continue;
                }
                for (TypedFunction fn : overloads) {
                    if (fn.isNative() || fn.body().isEmpty() || ctx.implementations().runsByRule(fn.definition())) {
                        continue;
                    }
                    try {
                        specs.compile(fn);
                    } catch (RuntimeException | StackOverflowError e) {
                        String id = fn.definition() == null ? fqn : com.legend.model.FunctionId.of(fn.definition()).qualified();
                        String msg = clean(e.getMessage());
                        String knock = "";
                        Matcher q = QUOTED.matcher(msg);
                        while (q.find()) {
                            String name = q.group(1);
                            String from = droppedDeclares.get(name);
                            if (from != null && !loaded.contains(name)) {
                                knock = "knockOn=" + from;
                                break;
                            }
                        }
                        w.println("FAILED\t" + sourceOf(elementSource, fqn) + "\t" + id + "\t" + e.getClass().getSimpleName()
                                + "\t" + msg + "\t" + knock);
                    }
                }
            }
        }
    }

    private static Compiler.ParsedModule parse(List<Compiler.ModelSource> sources) {
        return Compiler.parseSources(sources, (n, e) -> { }, com.legend.parser.Dialect.LEGEND_PLATFORM);
    }

    private static ParsedModel pruned(Compiler.ParsedModule module) {
        List<PackageableElement> kept = module.model().elements().stream()
                .filter(e -> !(e instanceof com.legend.model.NativeFunctionDefinition)).toList();
        return new ParsedModel(kept, module.model().imports(), module.model().source(),
                module.model().elementOffsets(), module.model().elementImports(),
                module.model().elementSources(), module.model().unclaimedSections());
    }

    /** The source file of an element, or of its owner for a synthesized member ({@code Owner$prop$x}). */
    private static String sourceOf(Map<String, String> elementSource, String element) {
        String src = elementSource.get(element);
        if (src == null && element.indexOf('$') > 0) {
            src = elementSource.get(element.substring(0, element.indexOf('$')));
        }
        return src == null ? "" : src;
    }

    private static String clean(String s) {
        return s == null ? "" : s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }
}
