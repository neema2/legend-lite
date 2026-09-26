// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.validation;

import com.legend.protocol.ConstraintDefinition;

import com.legend.compiler.element.ModelContext;
import com.legend.error.NotImplementedException;
import com.legend.model.ClassDefinition;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * RAW-SPACE desugar of {@code meta::relational::validation::validate}
 * (feature track #45) — the engine's own synthesis (validation.pure
 * {@code generateValidationQuery} + functions.pure
 * {@code generateConstraintNegatedProjectQuery/FilteredQuery}) rebuilt
 * over the PARSED AST: per constraint,
 * <pre>
 *   userQuery-&gt;filter({this | !body})
 *            -&gt;project([|'name', |'Error', |''],
 *                      [CONSTRAINT_ID, ENFORCEMENT_LEVEL, MESSAGE])
 * </pre>
 * folded with {@code concatenate}, handed to the ORDINARY execute path —
 * the pipeline types, resolves and lowers the synthesized query like any
 * user query (constraint bodies with exists/aggregates compile like any
 * predicate; the feature is THIN GLUE, per the feature map §14.1).
 *
 * <p>Runs BEFORE name resolution (EngineTestExecutor entry), so the call matches
 * by simple name + validation-import scope; the constraint bodies are
 * the parser's own {@code ValueSpecification}s ($this references become
 * the filter lambda's parameter).
 */
public final class ValidateDesugar {

    private ValidateDesugar() {
    }

    private static final String PKG = "meta::relational::validation";

    /** {@code stmt} with every {@code validate(...)} call rewritten to
     * the synthesized {@code execute(...)}; unchanged when none. */
    public static ValueSpecification rewrite(ValueSpecification stmt,
            ModelContext ctx, List<String> imports) {
        return switch (stmt) {
            case AppliedFunction af -> {
                List<ValueSpecification> ps = new ArrayList<>();
                boolean changed = false;
                for (ValueSpecification p : af.parameters()) {
                    ValueSpecification r = rewrite(p, ctx, imports);
                    ps.add(r);
                    changed |= r != p;
                }
                AppliedFunction cur = changed ? af.withParameters(ps) : af;
                yield isValidate(cur, imports) ? desugar(cur, ctx, imports)
                        : cur;
            }
            case LambdaFunction lf -> {
                List<ValueSpecification> body = new ArrayList<>();
                boolean changed = false;
                for (ValueSpecification b : lf.body()) {
                    ValueSpecification r = rewrite(b, ctx, imports);
                    body.add(r);
                    changed |= r != b;
                }
                yield changed ? new LambdaFunction(lf.parameters(), body) : lf;
            }
            case PureCollection pc -> {
                List<ValueSpecification> vs = new ArrayList<>();
                boolean changed = false;
                for (ValueSpecification v : pc.values()) {
                    ValueSpecification r = rewrite(v, ctx, imports);
                    vs.add(r);
                    changed |= r != v;
                }
                yield changed ? new PureCollection(vs) : pc;
            }
            default -> stmt;
        };
    }

    private static boolean isValidate(AppliedFunction af,
            List<String> imports) {
        if (af.parameters().size() < 4
                || !(af.parameters().get(0) instanceof LambdaFunction)) {
            return false;
        }
        // the RESOLVER's name (the query is resolved before this pass):
        // exact, or among the candidates it left on a bare call
        return com.legend.compiler.ResolvedNames.names(af, PKG + "::validate");
    }

    private static ValueSpecification desugar(AppliedFunction af,
            ModelContext ctx, List<String> imports) {
        LambdaFunction query = (LambdaFunction) af.parameters().get(0);
        // SHAPE-CLASSIFIED argument scan — the real overload set
        // (validation.pure) permutes [cols] [postTDS] [ids] around the
        // mapping/runtime pair, so position alone cannot dispatch:
        // col-args and a 1-param lambda BEFORE the mapping are the
        // extended projection form; a string collection is constraint
        // ids wherever it sits; the FIRST element pointer is the mapping
        // and the next argument the runtime; instance literals pass;
        // ConstraintContextInformation resolves through the (possibly
        // still unqualified) helper's parsed body.
        int n = af.parameters().size();
        ValueSpecification extensions = af.parameters().get(n - 1);
        List<ValueSpecification> userCols = new ArrayList<>();
        LambdaFunction postTds = null;
        ValueSpecification mapping = null;
        ValueSpecification runtime = null;
        List<String> ids = new ArrayList<>();
        java.util.Map<String, Object[]> overrides =
                new java.util.LinkedHashMap<>();
        for (int i = 1; i < n - 1; i++) {
            ValueSpecification a = af.parameters().get(i);
            if (mapping == null && isColArg(a)) {
                collectCols(a, userCols);
            } else if (mapping == null && a instanceof LambdaFunction pl
                    && pl.parameters().size() == 1) {
                postTds = pl;
            } else if (mapping == null
                    && a instanceof com.legend.protocol.spec
                            .PackageableElementPtr) {
                mapping = a;
                runtime = af.parameters().get(++i);
            } else if (a instanceof PureCollection pc
                    && pc.values().stream()
                            .allMatch(v -> v instanceof CString)) {
                for (ValueSpecification v : pc.values()) {
                    ids.add(((CString) v).value());
                }
            } else if (a instanceof CString cs) {
                ids.add(cs.value());
            } else if (a instanceof com.legend.protocol.spec.NewInstance
                    || (a instanceof AppliedFunction nw
                        && "new".equals(nw.function()))) {
                continue;   // ^RelationalExecutionContext() — PK append is
                            // the pipeline's own root-form concern
            } else if (contextInfo(a, ctx, imports, overrides)) {
                continue;   // ConstraintContextInformation overrides
            } else {
                String probe = a instanceof AppliedFunction pf
                        ? " [fn=" + pf.function() + "]" : "";
                throw new NotImplementedException("validate(...) argument #"
                        + i + " (" + a.getClass().getSimpleName() + ")"
                        + probe + " is not supported yet");
            }
        }
        if (mapping == null || runtime == null) {
            throw new NotImplementedException(
                    "validate(...): no mapping/runtime argument pair");
        }
        String classFqn = rootClassFqn(query, ctx, imports);
        ClassDefinition cd = ctx.findClassDefinition(classFqn).orElseThrow(
                () -> new NotImplementedException("validate: class '"
                        + classFqn + "' has no parsed definition"));
        List<ConstraintDefinition> constraints =
                constraintsInHierarchy(cd, ctx);
        if (!ids.isEmpty()) {
            List<ConstraintDefinition> picked =
                    new ArrayList<>();
            for (String id : ids) {
                constraints.stream().filter(c -> c.name().equals(id))
                        .findFirst().ifPresentOrElse(picked::add, () -> {
                            throw new NotImplementedException(
                                    "validate: cannot find constraint '" + id
                                    + "' in hierarchy of class '" + classFqn
                                    + "'");
                        });
            }
            constraints = picked;
        }
        if (constraints.isEmpty()) {
            throw new NotImplementedException("validate: class '" + classFqn
                    + "' has no constraints in the hierarchy to validate");
        }
        ValueSpecification queryChain = query.body()
                .get(query.body().size() - 1);
        ValueSpecification tds = null;
        for (ConstraintDefinition c : constraints) {
            ValueSpecification one = constraintProject(queryChain, c,
                    userCols, overrides.get(c.name()));
            tds = tds == null ? one
                    : new AppliedFunction("concatenate", List.of(tds, one));
        }
        if (postTds != null) {
            // beta-apply {t|...} over the concatenated violations
            tds = com.legend.compiler.spec.SourceSubst.substitute(
                    postTds.body().get(postTds.body().size() - 1),
                    java.util.Map.of(postTds.parameters().get(0).name(),
                            java.util.Objects.requireNonNull(tds,
                                    "constraint validation without constraints")));
        }
        // engine parity: validate executes under
        // ^RelationalExecutionContext(addDriverTablePkForProject=true) — the
        // option rides the call as a VALUE the one context reader binds
        // (the model compiles whole; the class is in it wherever validate is)
        return new AppliedFunction("execute", List.of(
                new LambdaFunction(List.of(), List.of(tds)),
                mapping, runtime,
                new com.legend.protocol.spec.NewInstance(
                        com.legend.compiler.element.type.PlatformTypes.RELATIONAL_EXECUTION_CONTEXT,
                        List.of(), List.of(new com.legend.protocol.spec.NewInstance.KeyBinding(
                                "addDriverTablePkForProject",
                                new com.legend.protocol.spec.KeyExpression(
                                        new com.legend.protocol.spec.CBoolean(true), false, false)))),
                extensions));
    }

    /** Own constraints first, then supertypes' (the engine's
     * allConstraintsInHierarchy walk). */
    private static List<ConstraintDefinition>
            constraintsInHierarchy(ClassDefinition cd, ModelContext ctx) {
        List<ConstraintDefinition> out =
                new ArrayList<>(cd.constraints());
        ctx.findClass(cd.qualifiedName()).ifPresent(tc -> {
            for (String sup : tc.superClassFqns()) {
                ctx.findClassDefinition(sup).ifPresent(sd ->
                        out.addAll(constraintsInHierarchy(sd, ctx)));
            }
        });
        return out;
    }

    /** {@code chain->filter({this|!body})->project([|'n',|'Error',|''],
     *  [CONSTRAINT_ID, ENFORCEMENT_LEVEL, MESSAGE])}. */
    /** {@code createConstraintContextInformation(id, Class, Level,
     * message)} entries — direct, in collections, or through a 0-arg
     * corpus helper's parsed body. Overrides: [levelName, messageSpec,
     * messageParamName]. Returns false when {@code v} is not this
     * vocabulary (the caller keeps its wall). */
    private static boolean contextInfo(ValueSpecification v,
            ModelContext ctx, List<String> imports,
            java.util.Map<String, Object[]> out) {
        if (v instanceof PureCollection pc) {
            for (ValueSpecification e : pc.values()) {
                if (!contextInfo(e, ctx, imports, out)) {
                    return false;
                }
            }
            return true;
        }
        if (!(v instanceof AppliedFunction af)) {
            return false;
        }
        String simple = af.function()
                .substring(af.function().lastIndexOf(':') + 1);
        if (simple.equals("createConstraintContextInformation")
                && af.parameters().size() >= 4
                && af.parameters().get(0) instanceof CString id) {
            ValueSpecification level = af.parameters().get(2);
            String levelName = level
                    instanceof com.legend.protocol.spec.EnumValue ev
                    ? ev.value() : null;
            ValueSpecification msg = af.parameters().get(3);
            if (msg instanceof CString cs) {
                out.put(id.value(),
                        new Object[]{levelName, cs, null});
                return true;
            }
            if (msg instanceof LambdaFunction ml
                    && ml.parameters().size() == 1
                    && !ml.body().isEmpty()) {
                out.put(id.value(), new Object[]{levelName,
                        ml.body().get(ml.body().size() - 1),
                        ml.parameters().get(0).name()});
                return true;
            }
            return false;
        }
        if (af.parameters().isEmpty()) {
            var fd = ctx.findFunctionDefinition(af.function());
            if (fd.isEmpty()) {
                for (String c : af.candidateFqns()) {
                    fd = ctx.findFunctionDefinition(c);
                    if (fd.isPresent()) {
                        break;
                    }
                }
            }
            if (fd.isEmpty() && !af.function().contains("::")) {
                // pre-resolution body: qualify through the test's imports
                for (String imp : imports) {
                    fd = ctx.findFunctionDefinition(
                            imp + "::" + af.function());
                    if (fd.isPresent()) {
                        break;
                    }
                }
            }
            if (fd.isPresent() && !fd.get().body().isEmpty()) {
                return contextInfo(fd.get().body()
                        .get(fd.get().body().size() - 1), ctx, imports,
                        out);
            }
        }
        return false;
    }

    private static boolean isColArg(ValueSpecification v) {
        if (v instanceof PureCollection pc) {
            return !pc.values().isEmpty()
                    && pc.values().stream().allMatch(ValidateDesugar::isColArg);
        }
        return v instanceof AppliedFunction af
                && "col".equals(af.function()
                        .substring(af.function().lastIndexOf(':') + 1))
                && af.parameters().size() >= 2;
    }

    private static void collectCols(ValueSpecification v,
            List<ValueSpecification> out) {
        if (v instanceof PureCollection pc) {
            pc.values().forEach(e -> collectCols(e, out));
        } else {
            out.add(v);
        }
    }

    /** Textbook variable substitution over the parse tree (the postTDS
     * beta-application). */
    private static ValueSpecification constraintProject(
            ValueSpecification chain,
            ConstraintDefinition c,
            List<ValueSpecification> userCols,
            Object @com.legend.base.Nullable [] override) {
        ValueSpecification body = c.expression();
        // engine negatedFunctionExpression: not(not(x)) collapses
        ValueSpecification negated = body instanceof AppliedFunction nf
                && com.legend.compiler.ResolvedNames.names(nf,
                        com.legend.builtin.Pure.NOT__BOOLEAN_1.qualifiedName())
                && nf.parameters().size() == 1
                ? nf.parameters().get(0)
                : new AppliedFunction("not", List.of(body));
        ValueSpecification filtered = new AppliedFunction("filter", List.of(
                chain,
                new LambdaFunction(
                        List.of(new Variable("this", null, null)),
                        List.of(negated))));
        // ~message is an EXPRESSION over $this (engine: the message
        // function IS the projection lambda); absent = empty string.
        // ~enforcementLevel defaults Error (the engine's own default).
        LambdaFunction messageCol = c.message() == null
                ? constLambda("")
                : c.message() instanceof CString cs
                        ? constLambda(cs.value())
                        : new LambdaFunction(
                                List.of(new Variable("this", null, null)),
                                List.of(c.message()));
        String level = c.enforcementLevel() == null
                ? "Error" : c.enforcementLevel();
        if (override != null) {
            if (override[0] != null) {
                level = (String) override[0];
            }
            if (override[1] instanceof CString oc) {
                messageCol = constLambda(oc.value());
            } else if (override[1] instanceof ValueSpecification ov) {
                messageCol = new LambdaFunction(
                        List.of(new Variable("this", null, null)),
                        List.of(com.legend.compiler.spec.SourceSubst
                                .substitute(ov, java.util.Map.of(
                                        (String) override[2],
                                        new Variable("this", null, null)))));
            }
        }
        List<ValueSpecification> fns = new ArrayList<>(List.of(
                constLambda(c.name()), constLambda(level), messageCol));
        List<ValueSpecification> names = new ArrayList<>(List.of(
                new CString("CONSTRAINT_ID"),
                new CString("ENFORCEMENT_LEVEL"),
                new CString("MESSAGE")));
        for (ValueSpecification col : userCols) {
            AppliedFunction cf = (AppliedFunction) col;
            fns.add(cf.parameters().get(0));
            names.add(cf.parameters().get(1));
        }
        return new AppliedFunction("project", List.of(
                filtered, new PureCollection(fns),
                new PureCollection(names)));
    }

    private static LambdaFunction constLambda(String value) {
        return new LambdaFunction(
                List.of(new Variable("x", null, null)),
                List.of(new CString(value)));
    }

    /** The query lambda's root class ({@code X.all()} — the deepest
     * {@code getAll} target), import-qualified against the model. */
    private static String rootClassFqn(ValueSpecification n, ModelContext ctx,
            List<String> imports) {
        if (n instanceof AppliedFunction af) {
            if (com.legend.compiler.ResolvedNames.names(af, com.legend.builtin.Pure.GET_ALL__CLASS_1.qualifiedName())
                    && !af.parameters().isEmpty()
                    && af.parameters().get(0)
                            instanceof PackageableElementPtr ptr) {
                String name = ptr.fullPath();
                if (ctx.findClassDefinition(name).isPresent()) {
                    return name;
                }
                String first = null;
                for (String imp : imports) {
                    String q = imp + "::" + name;
                    var cd = ctx.findClassDefinition(q);
                    if (cd.isEmpty()) {
                        continue;
                    }
                    // MULTI-IMPORT collision (several Product classes in
                    // scope): the validate contract requires constraints
                    // — a CONSTRAINED candidate beats an unconstrained
                    // first match
                    if (!constraintsInHierarchy(cd.get(), ctx).isEmpty()) {
                        return q;
                    }
                    if (first == null) {
                        first = q;
                    }
                }
                if (first != null) {
                    return first;
                }
                throw new NotImplementedException("validate: cannot qualify"
                        + " root class '" + name + "'");
            }
            for (ValueSpecification p : af.parameters()) {
                try {
                    return rootClassFqn(p, ctx, imports);
                } catch (NotImplementedException e) {
                    throw e;
                } catch (RuntimeException ignore) {
                    // keep scanning siblings
                }
            }
        }
        if (n instanceof LambdaFunction lf) {
            for (ValueSpecification b : lf.body()) {
                try {
                    return rootClassFqn(b, ctx, imports);
                } catch (RuntimeException ignore) {
                    // keep scanning
                }
            }
        }
        throw new IllegalStateException("no getAll root in validate query");
    }
}
