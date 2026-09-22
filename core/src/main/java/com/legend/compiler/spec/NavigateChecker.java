package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.Property;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNavigate;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.LambdaFunction;

import java.util.List;
import java.util.Optional;

/**
 * {@code navigate} (MAPPING_CLEAN_SHEET.md §3) &mdash; the clean-sheet
 * graph-traversal primitive replacing engine's {@code traverse}/extend-nav
 * variants. Dispatch is by shape: arity 2 = the inline constructor-slot form
 * (fully generic); arity 3 dispatches on the source &mdash; a Relation takes the
 * pre-map (sub-row widening) rule, a class collection the post-map
 * (declared-property fill) rule. Every path validates against its registered
 * signature; only the pre-map {@code Z} binding is bespoke (the design fixes the
 * sub-row column at {@code [1]} per output row, §3.4, which the thunk's
 * {@code T[*]} body multiplicity cannot express).
 */
final class NavigateChecker {

    private NavigateChecker() {
    }

    /**
     * The legacy bridge {@code legacyNavigate(rel, ~alias: Target.all(),
     * #>{db.T}#, {s,t|cond})} — the pre-map rule with the target's TABLE
     * row spelled into the call: the slot thunk is the CLASS extent (the
     * sub-row column's type) while the condition speaks table-row scope,
     * so a fourth argument carries the rows that bind {@code T} (the same
     * conform-by-emission cure as legacyAssocPredicate). Only {@code Z}
     * (the class-typed sub-row column) is bespoke.
     */
    static TypedSpec legacy(Typer t, AppliedFunction af, Env env) {
        int arity = af.parameters().size();
        if (arity == 3) {
            return legacyRoutes(t, af, env);
        }
        TypedFunction sig = t.model().findFunction(af.function()).stream()
                .filter(c -> c.parameters().size() == arity)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no " + arity + "-argument legacyNavigate overload is registered"));
        if ((arity != 4 && arity != 5)
                || !(af.parameters().get(1) instanceof ColSpec cs)
                || cs.function1() == null || !cs.function1().parameters().isEmpty()
                || !(af.parameters().get(3) instanceof LambdaFunction condLam)
                || (arity == 5 && !(af.parameters().get(4)
                        instanceof LambdaFunction))) {
            throw new TypeInferenceException("legacyNavigate expects"
                    + " (rel, ~alias: Target.all(), <target rows>, {s,t|cond}"
                    + "[, {s,t|pairedCond}])");
        }
        Bindings b = new Bindings();
        TypedSpec source = t.synth(af.parameters().get(0), env);
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        // The thunk {->C[*]}: C = the target CLASS extent.
        Type.GenericType colspecParam = (Type.GenericType) sig.parameters().get(1).type();
        TypedLambda thunk = (TypedLambda) t.typeLambda(cs.function1(),
                colspecParam.arguments().get(0), b, env);
        Type target = thunk.functionType().result().type();
        if (!(target instanceof Type.ClassType)) {
            throw new TypeInferenceException("legacyNavigate target must be a class"
                    + " extent (Class.all()), got " + target.typeName());
        }
        // The target ROWS bind T — the condition's right-side scope.
        TypedSpec tgtRows = Checkers.unifiedArg(t, sig, 2, af, b, env);

        // Z = (alias : TargetClass[1]) — the class-typed sub-row column.
        b.bindType(schemaVar(sig), new Type.RelationType(List.of(
                new Type.Column(cs.name(), target, Multiplicity.Bounded.ONE))));
        TypedLambda pred = (TypedLambda) t.typeLambda(condLam,
                sig.parameters().get(3).type(), b, env);
        // the STRICT member-paired variant of a MERGED union condition
        // (TypedNavigate.pairedPredicate — graph children consult it)
        Optional<TypedLambda> paired = arity == 5
                ? Optional.of((TypedLambda) t.typeLambda(
                        (LambdaFunction) af.parameters().get(4),
                        sig.parameters().get(4).type(), b, env))
                : Optional.empty();

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        // the ColSpec ALIAS metadata is the FRAME channel (a VIEW-backed
        // navigate names its derived table — the TypedJoinSlot precedent)
        return new TypedNavigate(source, Optional.of(cs.name()), thunk.body().get(0),
                pred, paired, cs.alias(), TypedNavigate.Form.PRE_MAP, out);
    }

    /**
     * The SEVERAL-ROUTE legacy navigate (legacy routes as composition,
     * docs/LEGACY_ROUTES_AS_COMPOSITION_2026_09_13.md §5):
     * {@code legacyNavigate(rel, ~slot: getAll(C), [route(target, rows,
     * {s,t|cond}), ...])}. Every route is typed on its OWN row type; its
     * condition's target-side reads become the union-row keys the routed
     * union projects (ClassSources.routedUnionSource): routes whose
     * conditions have the same SHAPE (target reads erased, source reads
     * kept) share key names, so their disjuncts collapse to one equality
     * the database hashes; different shapes keep their own keys and OR.
     * The node's predicate is that OR over (source row, union row).
     */
    static TypedSpec legacyRoutes(Typer t, AppliedFunction af, Env env) {
        TypedFunction sig = t.model().findFunction(af.function()).stream()
                .filter(c -> c.parameters().size() == 3)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no 3-argument legacyNavigate overload is registered"));
        TypedFunction routeSig = t.model().findFunction(
                        com.legend.builtin.Pure.Lite.ROUTE).stream()
                .filter(c -> c.parameters().size() == 3)
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no route(target, rows, cond) signature is registered"));
        if (!(af.parameters().get(1) instanceof ColSpec cs)
                || cs.function1() == null || !cs.function1().parameters().isEmpty()
                || !(af.parameters().get(2) instanceof com.legend.protocol.spec.PureCollection list)
                || list.values().isEmpty()) {
            throw new TypeInferenceException("legacyNavigate expects"
                    + " (rel, ~alias: Target.all(), [route(target, rows, {s,t|cond}), ...])");
        }
        Bindings b = new Bindings();
        TypedSpec source = t.synth(af.parameters().get(0), env);
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);
        Type.RelationType srcRow = Type.requireRelationSchema(source.info().type());
        Type.GenericType colspecParam = (Type.GenericType) sig.parameters().get(1).type();
        TypedLambda thunk = (TypedLambda) t.typeLambda(cs.function1(),
                colspecParam.arguments().get(0), b, env);
        Type target = thunk.functionType().result().type();
        if (!(target instanceof Type.ClassType)) {
            throw new TypeInferenceException("legacyNavigate slot must be a class"
                    + " extent (Class.all()), got " + target.typeName());
        }
        // each route on its own row type
        record Typed(TypedSpec target, TypedSpec rows, TypedLambda cond,
                List<String> reads, Type.RelationType row, String shape) {}
        List<Typed> typed = new java.util.ArrayList<>();
        for (var v : list.values()) {
            if (!(v instanceof AppliedFunction r)
                    || !com.legend.builtin.Pure.Lite.ROUTE.equals(r.function())
                    || r.parameters().size() != 3
                    || !(r.parameters().get(2) instanceof LambdaFunction condLam)
                    || condLam.parameters().size() != 2) {
                throw new TypeInferenceException("legacyNavigate route list: every"
                        + " element is route(target, rows, {s,t|cond})");
            }
            Bindings rb = new Bindings();
            t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), rb);
            TypedSpec rTarget = t.synth(r.parameters().get(0), env);
            if (!(Type.asClassType(rTarget.info().type()) instanceof Type.ClassType)) {
                throw new TypeInferenceException("route target must be a class extent"
                        + " (a set's function or Class.all()), got "
                        + rTarget.info().type().typeName());
            }
            TypedSpec rRows = t.synth(r.parameters().get(1), env);
            t.kernel().unify(routeSig.parameters().get(1).type(), rRows.info().type(), rb);
            TypedLambda cond = (TypedLambda) t.typeLambda(condLam,
                    routeSig.parameters().get(2).type(), rb, env);
            Type.RelationType row = Type.requireRelationSchema(rRows.info().type());
            List<String> reads = new java.util.ArrayList<>();
            collectReads(cond.body().get(cond.body().size() - 1),
                    cond.parameters().get(1), reads);
            String shape = String.valueOf(eraseReads(cond.body().get(cond.body().size() - 1),
                    cond.parameters().get(1), cond.parameters().get(0)));
            typed.add(new Typed(rTarget, rRows, cond, reads, row, shape));
        }
        // key names by shape (routes of one shape share their keys)
        List<String> shapes = new java.util.ArrayList<>();
        List<Type.Column> keyCols = new java.util.ArrayList<>();
        List<TypedNavigate.Route> routes = new java.util.ArrayList<>();
        for (Typed r : typed) {
            int g = shapes.indexOf(r.shape());
            if (g < 0) {
                shapes.add(r.shape());
                g = shapes.size() - 1;
                for (int k = 0; k < r.reads().size(); k++) {
                    keyCols.add(new Type.Column(keyName(g, k),
                            columnType(r.row(), r.reads().get(k)),
                            Multiplicity.Bounded.ZERO_ONE));
                }
            }
            List<String> names = new java.util.ArrayList<>();
            for (int k = 0; k < r.reads().size(); k++) {
                names.add(keyName(g, k));
            }
            routes.add(new TypedNavigate.Route(r.target(), r.rows(), r.cond(), r.reads(), names));
        }
        Type.RelationType urow = new Type.RelationType(keyCols);
        String sParam = typed.get(0).cond().parameters().get(0);
        var one = Multiplicity.Bounded.ONE;
        var uInfo = new ExprType(urow, one);
        var boolOne = new ExprType(Type.Primitive.BOOLEAN, one);
        TypedFunction orFn = t.model().findFunction("meta::pure::functions::boolean::or")
                .stream().filter(f -> f.parameters().size() == 2).findFirst()
                .orElseThrow(() -> new TypeInferenceException("no 2-argument boolean::or"));
        TypedSpec or = null;
        java.util.Set<String> done = new java.util.HashSet<>();
        for (int i = 0; i < typed.size(); i++) {
            Typed r = typed.get(i);
            if (!done.add(r.shape())) {
                continue;   // one disjunct per shape
            }
            TypedSpec body = r.cond().body().get(r.cond().body().size() - 1);
            TypedSpec re = repointReads(body, r.cond().parameters().get(1), "u", uInfo,
                    r.reads(), routes.get(i).keyNames(), urow);
            re = renameVar(re, r.cond().parameters().get(0), sParam, new ExprType(srcRow, one));
            or = or == null ? re : new TypedNativeCall(orFn, List.of(or, re), boolOne, null);
        }
        TypedLambda pred = new TypedLambda(List.of(sParam, "u"),
                List.of(java.util.Objects.requireNonNull(or)),
                new ExprType(new Type.FunctionType(
                        List.of(new Type.Param(srcRow, one), new Type.Param(urow, one)),
                        new Type.Param(Type.Primitive.BOOLEAN, one)), one));
        b.bindType(schemaVar(sig), new Type.RelationType(List.of(
                new Type.Column(cs.name(), target, one))));
        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedNavigate(source, Optional.of(cs.name()), thunk.body().get(0),
                pred, Optional.empty(), cs.alias(), TypedNavigate.Form.PRE_MAP, out, routes);
    }

    /** A {@code route(...)} outside a legacyNavigate route list. */
    static TypeInferenceException routeAlone() {
        return new TypeInferenceException(
                "route(...) is an element of legacyNavigate's route list, never a value");
    }

    private static String keyName(int shape, int k) {
        return "__route" + shape + "_" + k;
    }

    /** The type of a read path ({@code col}, or {@code slot.col} through a
     * joined sub-row) in the route's rows. */
    private static Type columnType(Type.RelationType row, String path) {
        Type.RelationType at = row;
        String[] parts = path.split("\\.");
        Type found = null;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            Type.Column c = at.columns().stream().filter(x -> x.name().equals(part)).findFirst()
                    .orElseThrow(() -> new TypeInferenceException("route condition reads '"
                            + path + "', not a column of the route's rows"));
            found = c.type();
            if (i + 1 < parts.length) {
                Type.RelationType sub = c.type() instanceof Type.RelationType bare ? bare
                        : Type.relationSchema(c.type());
                if (sub == null) {
                    throw new TypeInferenceException("route condition reads through '" + part
                            + "', which is not a joined sub-row of the route's rows");
                }
                at = sub;
            }
        }
        return java.util.Objects.requireNonNull(found);
    }

    /** The read path of {@code n} off {@code var}: {@code col} or
     * {@code slot.col} (one joined sub-row deep); null when {@code n} is not
     * such a read. */
    private static @com.legend.base.Nullable String readPath(TypedSpec n, String var) {
        if (n instanceof TypedPropertyAccess pa) {
            if (pa.source() instanceof TypedVariable v && v.name().equals(var)) {
                return pa.property();
            }
            if (pa.source() instanceof TypedPropertyAccess inner
                    && inner.source() instanceof TypedVariable v2 && v2.name().equals(var)) {
                return inner.property() + "." + pa.property();
            }
        }
        return null;
    }

    /** The {@code $var.col} / {@code $var.slot.col} reads of {@code n}, in
     * order of appearance. */
    private static void collectReads(TypedSpec n, String var, List<String> out) {
        String path = readPath(n, var);
        if (path != null) {
            if (!out.contains(path)) {
                out.add(path);
            }
            return;
        }
        for (TypedSpec c : n.children()) {
            collectReads(c, var, out);
        }
    }

    /** The condition's SHAPE: target reads erased to a placeholder, the
     * source variable normalized — two routes with equal shapes read the
     * same source columns the same way and may share their keys. */
    private static TypedSpec eraseReads(TypedSpec n, String tVar, String sVar) {
        if (readPath(n, tVar) != null && n instanceof TypedPropertyAccess pa) {
            // the placeholder carries the READ's type only: the route's
            // own row type must not tell two same-shaped routes apart
            return new TypedPropertyAccess(new TypedVariable("?", pa.info()), "?", pa.info());
        }
        String sp = readPath(n, sVar);
        if (sp != null && n instanceof TypedPropertyAccess pa) {
            return new TypedPropertyAccess(new TypedVariable("s", pa.info()), sp, pa.info());
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> erased = kids.stream().map(c -> eraseReads(c, tVar, sVar)).toList();
        if (n instanceof TypedNativeCall nc) {
            // a source position is not part of a shape
            return new TypedNativeCall(nc.callee(), erased, nc.info(), null);
        }
        return n.withChildren(erased);
    }

    /** {@code $t.col} → {@code $u.<key>} by the route's read positions. */
    private static TypedSpec repointReads(TypedSpec n, String tVar, String uVar, ExprType uInfo,
            List<String> reads, List<String> keys, Type.RelationType urow) {
        String path = readPath(n, tVar);
        if (path != null) {
            int k = reads.indexOf(path);
            if (k < 0) {
                throw new IllegalStateException("checker bug: route read '" + path
                        + "' was not collected");
            }
            String key = keys.get(k);
            Type kt = urow.columns().stream().filter(c -> c.name().equals(key)).findFirst()
                    .orElseThrow().type();
            return new TypedPropertyAccess(new TypedVariable(uVar, uInfo), key,
                    new ExprType(kt, Multiplicity.Bounded.ZERO_ONE));
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        return n.withChildren(kids.stream()
                .map(c -> repointReads(c, tVar, uVar, uInfo, reads, keys, urow)).toList());
    }

    private static TypedSpec renameVar(TypedSpec n, String from, String to, ExprType info) {
        if (from.equals(to)) {
            return n;
        }
        if (n instanceof TypedVariable v && v.name().equals(from)) {
            return new TypedVariable(to, info);
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        return n.withChildren(kids.stream().map(c -> renameVar(c, from, to, info)).toList());
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() == 2) {
            return inline(t, af, env);
        }
        if (af.parameters().size() != 3) {
            throw new TypeInferenceException(
                    "navigate expects (source, ~alias: Target.all(), {s,t|pred}) or (Target.all(), {t|pred})");
        }
        TypedSpec source = t.synth(af.parameters().get(0), env);
        return Type.isRelation(source.info().type())
                ? preMap(t, af, source, env)
                : postMap(t, af, source, env);
    }

    /** Inline slot form {@code navigate(T.all(), {t|pred})} — fully generic; {@code T[*]}. */
    private static TypedSpec inline(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        if (!(a.args().get(1) instanceof TypedLambda pred)) {
            throw new TypeInferenceException("navigate expects a predicate lambda");
        }
        return new TypedNavigate(a.args().get(0), Optional.empty(), a.args().get(0),
                pred, TypedNavigate.Form.INLINE, a.out());
    }

    /** Pre-map: widen {@code Relation<S>} with a named class-typed sub-row, {@code S + (alias:Target[1])}. */
    private static TypedSpec preMap(Typer t, AppliedFunction af, TypedSpec source, Env env) {
        TypedFunction sig = overload(t, af, p -> p.type() instanceof Type.GenericType);
        Bindings b = new Bindings();
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        Parts parts = parts(t, sig, af, b, env);
        // Z = (alias : Target[1]) — §3.4: rows multiply, the SUB-ROW COLUMN is to-one.
        b.bindType(schemaVar(sig), new Type.RelationType(List.of(
                new Type.Column(parts.alias(), parts.targetClass(), Multiplicity.Bounded.ONE))));
        TypedLambda pred = (TypedLambda) t.typeLambda(parts.predLam(), sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedNavigate(source, Optional.of(parts.alias()), parts.target(),
                pred, TypedNavigate.Form.PRE_MAP, out);
    }

    /** Post-map: fill a DECLARED property of the class source; the {@code C[*]} passes through. */
    private static TypedSpec postMap(Typer t, AppliedFunction af, TypedSpec source, Env env) {
        if (!(source.info().type() instanceof Type.ClassType ct)) {
            throw new TypeInferenceException("navigate requires a relation or class-collection source, got "
                    + source.info().type().typeName());
        }
        TypedFunction sig = overload(t, af, p -> p.type() instanceof Type.TypeVar);
        Bindings b = new Bindings();
        t.kernel().unify(sig.parameters().get(0).type(), source.info().type(), b);
        t.kernel().unifyMult(sig.parameters().get(0).multiplicity(),
                source.info().multiplicity(), source.info().type(), b);

        Parts parts = parts(t, sig, af, b, env);
        // Post-map fills a CLASS property — a relation target is a pre-map
        // shape only (audit: an Any-typed property accepted a row-struct).
        if (parts.targetClass() instanceof Type.RelationType) {
            throw new TypeInferenceException(
                    "navigate post-map target must be a class extent (Class.all()),"
                            + " got a relation");
        }
        // The slot must be a DECLARED property whose type accepts the navigated target (§3.3).
        Property prop = t.model().findProperty(ct.fqn(), parts.alias()).orElseThrow(() ->
                new TypeInferenceException("navigate: class " + ct.fqn()
                        + " has no property '" + parts.alias() + "' to fill"));
        if (!t.kernel().accepts(prop.type(), parts.targetClass())) {
            throw new TypeInferenceException("navigate: property '" + parts.alias() + "' is "
                    + prop.type().typeName() + ", not " + parts.targetClass().typeName());
        }
        TypedLambda pred = (TypedLambda) t.typeLambda(parts.predLam(), sig.parameters().get(2).type(), b, env);

        ExprType out = t.kernel().resolveOutput(sig.returnType(), sig.returnMultiplicity(), b);
        return new TypedNavigate(source, Optional.of(parts.alias()), parts.target(),
                pred, TypedNavigate.Form.POST_MAP, out);
    }

    /** The shared middle: the {@code ~alias: Target.all()} colspec — thunk typed, {@code T} bound. */
    private record Parts(String alias, TypedSpec target, Type targetClass, LambdaFunction predLam) {
    }

    private static Parts parts(Typer t, TypedFunction sig, AppliedFunction af, Bindings b, Env env) {
        if (!(af.parameters().get(1) instanceof ColSpec cs) || cs.function1() == null
                || !cs.function1().parameters().isEmpty()
                || !(af.parameters().get(2) instanceof LambdaFunction predLam)) {
            throw new TypeInferenceException(
                    "navigate expects (source, ~alias: Target.all(), {s,t|pred})");
        }
        // Type the target thunk against the signature's {->T[*]} — binds T generically.
        Type.GenericType colspecParam = (Type.GenericType) sig.parameters().get(1).type();
        TypedLambda thunk = (TypedLambda) t.typeLambda(cs.function1(),
                colspecParam.arguments().get(0), b, env);
        Type target = thunk.functionType().result().type();
        // Class extent (Class.all()) or a RELATION target (a table/pipeline)
        // — the slot column carries the target's row-struct; the lowerer
        // flattens it as a prefixed LEFT join.
        // — any relation CARRIER (the store accessor a #>{db.table}# literal is,
        // a TDS literal): its schema is the target row-struct
        if (com.legend.compiler.element.type.Type.relationSchema(target) instanceof Type.RelationType rs) {
            target = rs;
        }
        if (!(target instanceof Type.ClassType || target instanceof Type.RelationType)) {
            throw new TypeInferenceException(
                    "navigate target must be a class extent (Class.all()) or a"
                            + " relation, got " + target.typeName());
        }
        // A RELATION target's slot holds ONE ROW of it — the bare
        // schema struct (Row-vs-Relation: reads through the slot are
        // per-row BY TYPE). The signature's thunk interior ({->T[*]})
        // bound T to the whole WRAPPED table (a table target's element
        // is its row — a distinction the shared class/relation thunk
        // spelling cannot express), so the checker BINDS T to the row
        // before the predicate's {S[1],T[1]->Bool} types — the same
        // manual special-form binding JoinChecker's Z uses.
        if (target instanceof Type.RelationType trt) {
            Type tvar = ((Type.FunctionType) ((Type.GenericType)
                    sig.parameters().get(1).type()).arguments().get(0))
                    .result().type();
            if (tvar instanceof Type.TypeVar tv) {
                b.bindType(tv.name(), trt);
            }
        }
        return new Parts(cs.name(), thunk.body().get(0), target, predLam);
    }

    /** The schema variable {@code Z} of the colspec parameter {@code FuncColSpec<{->T[*]}, Z>}. */
    private static String schemaVar(TypedFunction sig) {
        Type.GenericType g = (Type.GenericType) sig.parameters().get(1).type();
        return ((Type.TypeVar) g.arguments().get(1)).name();
    }

    /**
     * The 3-arity overload of the CALLED function (navigate or its legacy
     * bridge legacyNavigate — same pre-map rule, own registration) whose
     * FIRST parameter matches {@code sourceParam}.
     */
    private static TypedFunction overload(Typer t, AppliedFunction af,
            java.util.function.Predicate<com.legend.compiler.element.TypedParameter> sourceParam) {
        return t.model().findFunction(af.function()).stream()
                .filter(c -> c.parameters().size() == 3 && sourceParam.test(c.parameters().get(0)))
                .findFirst()
                .orElseThrow(() -> new TypeInferenceException(
                        "no matching " + af.function() + " overload is registered"));
    }
}
