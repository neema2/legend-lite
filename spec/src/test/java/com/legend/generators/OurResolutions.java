// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.generators;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.CompiledFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.PackageableElement;
import com.legend.model.ParsedModel;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Stream;

/**
 * OUR RESOLUTIONS, in the shape of the reference's (the RefResolutions driver run against the real Pure compiler):
 * load a module's manifest closure, type every body, and print one row per call node the typer produced — the
 * enclosing function, the node kind, the callee the typer chose (its declaration's full name and signature key). A
 * form's node is a row too, by its kind, so the differential can tell "resolved to X" from "typed as a form" from
 * "absent". Every call row carries the call-name token's line and column and the source id in the REFERENCE's
 * spelling ({@code /module/path.pure}), so the join is call by call.
 *
 * <p>The loader drops a whole source file when the model does not build with it (the loop below). That is
 * SURVIVORSHIP: a regression that makes a file fail removes its rows from the join. So the dump reports what it
 * dropped and what it could not type, and the reference lane pins both (plan W1.1, audit W0-W1 #8).
 */
final class OurResolutions {

    /** What the dump saw: the sources dropped to make the model build, the bodies it could not type. */
    record Result(List<String> droppedSources, int functions, List<String> failedFunctions, long rows) {
    }

    private OurResolutions() {
    }

    static Result dump(String target, PrintWriter w) throws IOException {
        Path engine = com.legend.testing.ProgramPaths.rootOf("legend.engine.root");
        Path pure = com.legend.testing.ProgramPaths.rootOf("legend.pure.root");
        List<ManifestWorldCensusTest.Module> world =
                ManifestWorldCensusTest.closure(target, ManifestWorldCensusTest.manifests(engine, pure));
        List<Compiler.ModelSource> sources = new ArrayList<>();
        for (ManifestWorldCensusTest.Module m : world) {
            try (Stream<Path> walk = Files.walk(m.root())) {
                for (Path f : walk.filter(p -> p.toString().endsWith(".pure")).sorted().toList()) {
                    sources.add(new Compiler.ModelSource(m.name() + ":" + m.root().relativize(f).toString().replace('\\', '/'),
                            Files.readString(f, StandardCharsets.UTF_8)));
                }
            }
        }
        List<String> dropped = new ArrayList<>();
        ModelContext ctx = null;
        Map<String, String> elementSources = Map.of();
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
                elementSources = module.model().elementSources();
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
                dropped.add(referenceSourceId(drop));
                sources.removeIf(s -> s.name().equals(drop));
            }
        }
        if (ctx == null) {
            throw new IllegalStateException("the model did not converge");
        }
        SpecCompiler specs = new SpecCompiler(ctx);
        long rows = 0;
        int functions = 0;
        List<String> failed = new ArrayList<>();
        w.println("sourceId\tline\tcolumn\tkind\tresolvedFqn\tresolvedId\tenclosingFqn\tenclosingId");
        for (String fqn : new TreeSet<>(ctx.functionFqns())) {
            String src = referenceSourceId(elementSources.get(fqn));
            List<TypedFunction> overloads;
            try {
                overloads = ctx.findFunction(fqn);
            } catch (RuntimeException e) {
                continue;
            }
            for (TypedFunction fn : overloads) {
                if (fn.isNative() || fn.body().isEmpty() || ctx.implementations().runsByRule(fn.definition())) {
                    continue;
                }
                functions++;
                String enclosing = fqn + "\t" + idOf(fn);
                CompiledFunction compiled;
                try {
                    compiled = specs.compile(fn);
                } catch (RuntimeException | StackOverflowError e) {
                    failed.add(idOf(fn).isEmpty() ? fqn : idOf(fn));
                    w.println(src + "\t\t\tFAILED\t" + e.getClass().getSimpleName() + "\t\t" + enclosing);
                    continue;
                }
                for (TypedSpec stmt : compiled.body()) {
                    rows += walk(stmt, src, enclosing, w);
                }
            }
        }
        return new Result(List.copyOf(new TreeSet<>(dropped)), functions, List.copyOf(new TreeSet<>(failed)), rows);
    }

    /** The callee's identity in upstream's own spelling (FunctionId), blank for a synthetic callee. */
    private static String idOf(TypedFunction f) {
        return f.definition() == null ? "" : com.legend.model.FunctionId.of(f.definition()).qualified();
    }

    /** Our {@code module:relative/path} as the reference's {@code /module/relative/path}
     * (the reference's source id is the repository-relative path; ours already
     * repeats the module name for most modules and not for {@code platform}). */
    static String referenceSourceId(String ours) {
        if (ours == null) {
            return "?";
        }
        int colon = ours.indexOf(':');
        if (colon < 0) {
            return ours;
        }
        String module = ours.substring(0, colon);
        String path = ours.substring(colon + 1);
        return "/" + (path.startsWith(module + "/") ? path : module + "/" + path);
    }

    /** The call-NAME token's line and column, as the reference prints them: for a
     * qualified spelling ({@code meta::x::f(...)}) the reference's column is the
     * LAST segment's start; our span covers the whole spelled name (1-based,
     * inclusive end), so the last segment starts at end - |simple name| + 1. */
    private static String at(com.legend.protocol.@com.legend.base.Nullable SourceInfo pos, String qualifiedName) {
        if (pos == null) {
            return "\t";
        }
        String simple = qualifiedName.substring(qualifiedName.lastIndexOf("::") + 2);
        int width = pos.endColumn() - pos.startColumn() + 1;
        int column = pos.startLine() == pos.endLine() && width > simple.length()
                ? pos.endColumn() - simple.length() + 1
                : pos.startColumn();
        return pos.startLine() + "\t" + column;
    }

    private static long walk(TypedSpec node, String src, String enclosing, PrintWriter w) {
        long n = 0;
        if (node instanceof TypedNativeCall c) {
            w.println(src + "\t" + at(c.pos(), c.callee().qualifiedName()) + "\tCALL\t" + c.callee().qualifiedName() + "\t" + idOf(c.callee()) + "\t" + enclosing);
            n++;
        } else if (node instanceof TypedUserCall c) {
            w.println(src + "\t" + at(c.pos(), c.callee().qualifiedName()) + "\tCALL\t" + c.callee().qualifiedName() + "\t" + idOf(c.callee()) + "\t" + enclosing);
            n++;
        } else {
            String kind = node.getClass().getSimpleName();
            if (kind.startsWith("Typed") && !kind.equals("TypedLambda") && !kind.equals("TypedVariable")
                    && !kind.equals("TypedLiteral") && !kind.equals("TypedCollection") && !kind.equals("TypedLet")) {
                w.println(src + "\t\t\tNODE\t" + kind + "\t\t" + enclosing);
            }
        }
        for (TypedSpec child : node.children()) {
            n += walk(child, src, enclosing, w);
        }
        return n;
    }
}
