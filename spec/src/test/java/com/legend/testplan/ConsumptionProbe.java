// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.Compiler;
import com.legend.TypedQuery;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.model.ImportScope;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.rcorpus.MinimalCorpus;

import java.util.List;

/** A MEASUREMENT of the element-consumption analysis over corpus bodies (the test-is-the-product plan's M10c item 7b/7c):
 *  per statement of each named test body, the let's element-only and consumed verdicts, the kinds tags, the class; and
 *  for every user call at the body's top level, whether its callee compiles. Deciding nothing.
 *  {@code bazel run //spec:consumption_probe -- <test fqn>...} */
public final class ConsumptionProbe {

    private ConsumptionProbe() {
    }

    /** The value's spine: each node's class, a call's callee and whether the catalog knows it as an effect/handle/routine. */
    private static void describe(TypedSpec n, String indent, int depth) {
        if (depth > 3) {
            return;
        }
        String line = indent + n.getClass().getSimpleName();
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall nc) {
            var id = nc.callee().id();
            line += " " + nc.callee().qualifiedName() + " id=" + id
                    + " effect=" + com.legend.builtin.NativeFn.Effect.of(id).map(Enum::name).orElse("-")
                    + " planningArgs=" + com.legend.builtin.NativeFn.Effect.readsArgumentsAtPlanning(id)
                    + " handle=" + com.legend.builtin.NativeFn.Handle.of(id).isPresent()
                    + " routine=" + com.legend.builtin.NativeFn.JavaRoutine.of(id).isPresent();
        } else if (n instanceof TypedUserCall uc) {
            line += " user " + uc.callee().qualifiedName() + " id=" + uc.callee().id();
        } else if (n instanceof com.legend.compiler.spec.typed.TypedVariable v) {
            line += " $" + v.name();
        } else if (n instanceof com.legend.compiler.spec.typed.TypedPackageableRef r) {
            line += " ref " + r.fullPath();
        }
        System.out.println(line);
        for (TypedSpec c : n.children()) {
            describe(c, indent + "  ", depth + 1);
        }
    }

    public static void main(String[] args) throws Exception {
        MinimalCorpus corpus = new MinimalCorpus();
        ModelContext ctx = corpus.context();
        String runtime = "rcorpus::Rt";
        for (String fqn : args) {
            ValueSpecification resolved = Compiler.resolveQuery(List.of(new AppliedFunction(fqn, List.of())),
                    new ImportScope(List.of()), ctx);
            TypedQuery q = Compiler.queryResolved(ctx, resolved);
            List<TypedSpec> body = q.body();
            System.out.println("#### " + fqn + " statements=" + body.size());
            System.out.println("  kinds=" + BodyKinds.of(q, runtime, null));
            boolean sr = BodyClass.systemRouted(q, runtime);
            System.out.println("  systemRouted=" + sr + " class(oneJudge)=" + BodyClass.of(q, runtime, true));
            ElementConsumption a = new ElementConsumption(body, BodyClass.systemClasses(ctx), ctx, q.specs());
            for (int i = 0; i < body.size(); i++) {
                TypedSpec s = body.get(i);
                String head = s.getClass().getSimpleName() + (s instanceof TypedLet l ? " " + l.name() : "");
                if (s instanceof TypedLet) {
                    System.out.println("  [" + i + "] " + head + " elementOnly=" + a.elementOnlyLet(i)
                            + " consumed=" + (i < body.size() - 1 ? a.consumed(i) : "-"));
                } else {
                    System.out.println("  [" + i + "] " + head);
                }
                TypedSpec bare = s instanceof TypedLet l ? l.value() : s;
                java.util.Set<String> systemClasses = BodyClass.systemClasses(ctx);
                com.legend.planner.QueryDoor.Scan scan = new com.legend.planner.QueryDoor.Scan(q, runtime);
                String letKind = s instanceof TypedLet l && i < body.size() - 1 ? String.valueOf(
                        com.legend.planner.LetPlanner.kind(l, body, i, q.specs(), new java.util.HashMap<>(), n -> false)) : "-";
                boolean sideKind = com.legend.compiler.spec.StatementEffects.reaches(
                        BodyClass.framesAndHandlesAsTheirQueries(s), n -> BodyClass.sideKind(n, systemClasses, ctx) != null,
                        q.specs(), new java.util.HashMap<>());
                System.out.println("      kinds=" + scan.kinds(s) + " letKind=" + letKind
                        + " handle=" + (com.legend.planner.BodyPlanner.handleType(bare, ctx) != null)
                        + " reachesSideKind=" + sideKind
                        + " seedMap=" + BodyClass.seedFormEffectMap(bare, body.subList(0, i))
                        + " plansNoStep=" + BodyClass.plansNoStep(s, body.subList(0, i), ctx));
                describe(bare, "      ", 0);
                if (bare instanceof TypedUserCall uc) {
                    try {
                        var compiled = q.specs().compile(uc.callee());
                        System.out.println("      callee " + uc.callee().qualifiedName() + "/" + uc.args().size()
                                + " compiles: " + compiled.body().size() + " statements");
                    } catch (RuntimeException e) {
                        System.out.println("      callee " + uc.callee().qualifiedName() + " FAILS: "
                                + e.getClass().getSimpleName() + ": " + String.valueOf(e.getMessage()).replace('\n', ' '));
                    }
                }
            }
        }
    }
}
