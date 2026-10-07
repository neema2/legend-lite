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
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * Phase 3b sizing probe (not in the source tree): the reference lane's own load and typing loop
 * (OurResolutions.dump), printing WHY each source file is dropped and WHY each body fails to type.
 *
 * <pre>FailureCensus &lt;out.tsv&gt;</pre>
 */
public final class FailureCensus {

    public static void main(String[] args) throws Exception {
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        List<ManifestWorldCensusTest.Module> world =
                ManifestWorldCensusTest.closure("core_relational", ManifestWorldCensusTest.manifests(engine, pure));
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (ManifestWorldCensusTest.Module m : world) {
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    sources.add(new Compiler.ModelSource(m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/'),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(Path.of(args[0]), StandardCharsets.UTF_8))) {
            w.println("kind\tsubject\terror\tmessage\tframe");
            ModelContext ctx = null;
            for (int round = 0; round < 4000 && ctx == null; round++) {
                Compiler.ParsedModule module = Compiler.parseSources(sources, (n, e) -> { },
                        com.legend.parser.Dialect.LEGEND_PLATFORM);
                List<PackageableElement> kept = module.model().elements().stream()
                        .filter(e -> !(e instanceof com.legend.model.NativeFunctionDefinition)).toList();
                ParsedModel pruned = new ParsedModel(kept, module.model().imports(), module.model().source(),
                        module.model().elementOffsets(), module.model().elementImports(),
                        module.model().elementSources(), module.model().unclaimedSections());
                try {
                    ctx = Compiler.buildModel(pruned);
                } catch (com.legend.error.ModelException e) {
                    String el = e.element();
                    String src = el == null ? null : module.model().elementSources().get(el);
                    if (src == null && el != null && el.indexOf('$') > 0) {
                        src = module.model().elementSources().get(el.substring(0, el.indexOf('$')));
                    }
                    if (src == null) {
                        throw e;
                    }
                    final String drop = src;
                    w.println("DROPPED\t" + drop + "\t" + e.getClass().getSimpleName() + "\t" + clean(e.getMessage())
                            + "\telement=" + el);
                    sources.removeIf(s -> s.name().equals(drop));
                }
            }
            if (ctx == null) {
                throw new IllegalStateException("the model did not converge");
            }
            SpecCompiler specs = new SpecCompiler(ctx);
            for (String fqn : new TreeSet<>(ctx.functionFqns())) {
                List<TypedFunction> overloads;
                try {
                    overloads = ctx.findFunction(fqn);
                } catch (RuntimeException e) {
                    w.println("UNRESOLVED\t" + fqn + "\t" + e.getClass().getSimpleName() + "\t" + clean(e.getMessage()) + "\t"
                            + frame(e));
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
                        w.println("FAILED\t" + id + "\t" + e.getClass().getSimpleName() + "\t" + clean(e.getMessage()) + "\t"
                                + frame(e));
                    }
                }
            }
        }
    }

    private static String clean(String s) {
        return s == null ? "" : s.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ');
    }

    /** The first stack frame inside legend-lite's own code: where the failure was raised. */
    private static String frame(Throwable e) {
        for (StackTraceElement f : e.getStackTrace()) {
            if (f.getClassName().startsWith("com.legend.")) {
                return f.getClassName().substring("com.legend.".length()) + "." + f.getMethodName() + ":" + f.getLineNumber();
            }
        }
        return "";
    }
}
