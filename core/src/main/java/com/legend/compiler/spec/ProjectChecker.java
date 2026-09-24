package com.legend.compiler.spec;


import com.legend.platform.CoreFn;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedProject;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.AppliedProperty;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.LambdaFunction;
import com.legend.protocol.spec.PureCollection;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.spec.Variable;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code project} (engine {@code ProjectChecker}): the legacy TDS form
 * {@code project([lambdas], ['names'])} and bare {@code ~prop} columns DESUGAR
 * into the modern {@code ~[alias:lambda]} form (engine's own rewrite), then ONE
 * generic check types relation- and class-source alike ({@code Relation<Z>} /
 * {@code FuncColSpecArray<{C[1]->Any[*]},T>}); {@code Z} binds from the checked
 * lambda bodies. This class only desugars and emits.
 */
final class ProjectChecker {

    private ProjectChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        af = resolveLetBoundColumns(af, env);
        // FQN spellings (meta::pure::tds::project — the CoreFn FQN
        // dispatch) CANONICALIZE to the parse name up front: the legacy
        // normalization below rebuilds calls that must resolve against
        // the MODERN project signatures, and an FQN-keyed rebuild finds
        // only the legacy /3 candidate at that name.
        if (af.function().contains("::")) {
            af = new AppliedFunction("project", af.parameters());
        }
        // a spec-BUILDING helper call in columns position expands RAW
        // (project(getCols()) — its col() literals feed the same shape
        // normalization as written-out columns)
        if (af.parameters().size() >= 2) {
            ValueSpecification expanded =
                    t.rawSchemaErasedExpansion(af.parameters().get(1));
            if (expanded != null) {
                List<ValueSpecification> np = new ArrayList<>(af.parameters());
                np.set(1, expanded);
                af = af.withParameters(np);
            }
        }
        java.util.Map<String, String> docs = new java.util.LinkedHashMap<>();
        AppliedFunction modern = normalizeLegacyForms(af, docs);
        Application a = t.checkGeneric(withMappedColumns(modern), env);
        List<TypedFuncCol> cols = Args.funcCols(a.args().get(1));
        if (!docs.isEmpty()) {
            // col()'s documentation rides the typed column (the
            // .columns.documentation static fold reads it back)
            cols = cols.stream().map(fc -> docs.containsKey(fc.name())
                    ? new TypedFuncCol(fc.name(), fc.fn(), docs.get(fc.name()))
                    : fc).toList();
        }
        return new TypedProject(a.args().get(0), cols, clampTdsCells(a.out()));
    }

    /**
     * TDS cells are SCALARS (engine contract): a [*]-valued projection
     * column EXPLODES into one row per value — the resulting column is
     * [0..1], never a list cell (a list-typed column would mis-lower
     * downstream cell reads, e.g. isEmpty as list-length).
     */
    private static ExprType clampTdsCells(ExprType out) {
        Type.RelationType rt = Type.relationSchema(out.type());
        if (rt == null) {
            return out;
        }
        List<Type.Column> cols = new java.util.ArrayList<>(rt.columns().size());
        boolean changed = false;
        for (Type.Column c : rt.columns()) {
            if (c.multiplicity().isMany()) {
                cols.add(new Type.Column(c.name(), c.type(),
                        Multiplicity.Bounded.ZERO_ONE));
                changed = true;
            } else {
                cols.add(c);
            }
        }
        return changed ? new ExprType(
                Type.relation(new Type.RelationType(cols, rt.dynamicColumns())),
                out.multiplicity()) : out;
    }

    /**
     * The legacy TDS spellings all funnel into the modern colspec form:
     * <ul>
     *   <li>{@code project(src, [lambdas], [names])} — the classic triple;</li>
     *   <li>{@code project(src, [lambdas], 'name')} — scalar name(s) wrap;</li>
     *   <li>{@code project(src, [paths-or-lambdas])} — names DERIVE from each
     *       column's leaf property (engine's bare-path column naming);
     *       a non-property leaf without a name is loud.</li>
     * </ul>
     */
    /** LET-BOUND column arguments (`let p = [#/Person/firstName#]; let n =
     * ['First Name']; ->project($p, $n)` and `let cols = [col(...)]->cast(
     * @BasicColumnSpecification<Firm>)`): the alias chase binds the raw
     * literal at the consuming call — pure's let is referentially
     * transparent, and a column spec types only against its call
     * (Env.withDeferred's family). A cast around the literal is the
     * corpus's spelling and strips. */
    private static AppliedFunction resolveLetBoundColumns(AppliedFunction af,
            Env env) {
        List<ValueSpecification> ps = new ArrayList<>(af.parameters());
        boolean changed = false;
        for (int i = 1; i < ps.size() && i <= 2; i++) {
            if (ps.get(i) instanceof com.legend.protocol.spec.Variable v) {
                ValueSpecification r = stripCast(env.resolveAlias(v));
                if (r != v && !(r instanceof com.legend.protocol.spec.Variable)
                        && columnLiteral(r)) {
                    ps.set(i, r);
                    changed = true;
                }
            }
        }
        return changed ? af.withParameters(ps) : af;
    }

    private static ValueSpecification stripCast(ValueSpecification v) {
        return v instanceof AppliedFunction c
                && CoreFn.of(c.function()).orElse(null) == CoreFn.CAST
                && c.parameters().size() == 2
                ? c.parameters().get(0) : v;
    }

    private static boolean columnLiteral(ValueSpecification v) {
        return v instanceof ColSpec || v instanceof ColSpecArray
                || v instanceof com.legend.protocol.spec.PathLiteral
                || v instanceof CString || v instanceof LambdaFunction
                || isLegacyColumnCall(v)
                || v instanceof PureCollection pc
                        && pc.values().stream().allMatch(ProjectChecker::columnLiteral);
    }

    private static AppliedFunction normalizeLegacyForms(AppliedFunction af,
            java.util.Map<String, String> docsOut) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() == 3) {
            ValueSpecification lambdas = ps.get(1) instanceof PureCollection ? ps.get(1)
                    : new PureCollection(List.of(ps.get(1)));
            ValueSpecification names = ps.get(2) instanceof PureCollection ? ps.get(2)
                    : new PureCollection(List.of(ps.get(2)));
            return legacyToModern(af.withParameters(List.of(ps.get(0), lambdas, names)));
        }
        if (ps.size() == 2 && (ps.get(1) instanceof LambdaFunction
                || ps.get(1) instanceof com.legend.protocol.spec.PathLiteral
                || isLegacyColumnCall(ps.get(1)))) {
            // scalar legacy column: project(col(fn,'name')) — wrap and recurse
            return normalizeLegacyForms(af.withParameters(List.of(ps.get(0), new PureCollection(List.of(ps.get(1))))),
                    docsOut);
        }
        if (ps.size() == 2 && ps.get(1) instanceof PureCollection lambdas) {
            List<ValueSpecification> exprs = new ArrayList<>(lambdas.values().size());
            List<ValueSpecification> names = new ArrayList<>(lambdas.values().size());
            for (ValueSpecification v : lambdas.values()) {
                // #/Person/address/name!address# — the path ALIAS names the
                // column (real pure Path.name); it rides the path node
                if (v instanceof com.legend.protocol.spec.PathLiteral pl
                        && pl.alias() != null) {
                    exprs.add(pl);
                    names.add(new CString(pl.alias()));
                    continue;
                }
                // legacy TDS col(fn, 'name') inside the project collection;
                // the lambda may be COLLECTION-wrapped: col([o|...], 'n').
                // The 3-arg form adds DOCUMENTATION (real pure
                // tds.pure:289 col(func, name, documentation) —
                // BasicColumnSpecification metadata, no execution
                // semantics; carried to the typed column by NAME).
                if (v instanceof AppliedFunction colCall
                        && com.legend.builtin.TdsLegacy.COL.matches(colCall)
                        && (colCall.parameters().size() == 2
                                || (colCall.parameters().size() == 3
                                        && colCall.parameters().get(2)
                                                instanceof CString))
                        && colCall.parameters().get(1) instanceof CString cname) {
                    ValueSpecification fnArg = colCall.parameters().get(0);
                    if (fnArg instanceof PureCollection pc1
                            && pc1.values().size() == 1) {
                        fnArg = pc1.values().get(0);
                    }
                    if (fnArg instanceof LambdaFunction fn) {
                        if (colCall.parameters().size() == 3) {
                            docsOut.put(cname.value(), ((CString)
                                    colCall.parameters().get(2)).value());
                        }
                        exprs.add(fn);
                        names.add(cname);
                        continue;
                    }
                }
                exprs.add(v);
                names.add(new CString(derivedColumnName(v)));
            }
            return legacyToModern(af.withParameters(List.of(ps.get(0), new PureCollection(exprs), new PureCollection(names))));
        }
        return af;
    }

    /** A column expression's lambda: a path literal stands for its
     *  navigation lambda (the node survives resolution to carry its alias). */
    static ValueSpecification columnLambda(ValueSpecification v) {
        return v instanceof com.legend.protocol.spec.PathLiteral pl ? pl.desugared() : v;
    }

    static boolean isLegacyColumnCall(ValueSpecification v) {
        return v instanceof AppliedFunction c
                        && com.legend.builtin.TdsLegacy.COL.matches(c)
                        && (c.parameters().size() == 2 || c.parameters().size() == 3)
                || v instanceof com.legend.protocol.spec.PathLiteral pl && pl.alias() != null;
    }

    /** The leaf property of a navigation lambda/path names its column (engine parity). */
    private static String derivedColumnName(ValueSpecification v) {
        if (v instanceof com.legend.protocol.spec.PathLiteral pl) {
            // a bare path names its column by its leaf, like the lambda it is
            return derivedColumnName(pl.desugared());
        }
        if (v instanceof LambdaFunction lf && lf.body().size() == 1) {
            ValueSpecification leaf = lf.body().get(0);
            if (leaf instanceof AppliedProperty ap) {
                return ap.property();
            }
            // a MILESTONED property-function leaf (prop(%d), path-literal
            // dated segments) names its column by the property — engine
            // buildColumnNameOutOfPath parity. Audit 13 F6: shape alone
            // admitted ANY function ($p.x->toUpper() named 'toUpper');
            // dated property functions have >= 2 args whose tail args are
            // DATE-ish, and the name must not be a catalog native.
            if (leaf instanceof AppliedFunction laf && laf.parameters().size() >= 2
                    && (laf.parameters().get(0) instanceof Variable
                            || laf.parameters().get(0) instanceof AppliedProperty
                            || laf.parameters().get(0) instanceof AppliedFunction)
                    && laf.parameters().subList(1, laf.parameters().size()).stream()
                            .allMatch(a -> a instanceof com.legend.protocol.spec.CDate
                                    || a instanceof com.legend.protocol.spec.CLatestDate
                                    || a instanceof Variable)) {
                // the promised catalog-native guard (audit 23 A4 — the
                // comment claimed it, the code lacked it): a CATALOG
                // native in this shape is a computed column, never a
                // dated property function
                if (!com.legend.builtin.Pure
                        .nativeKeysAt(laf.function()).isEmpty()) {
                    throw new TypeInferenceException("a name-less project"
                            + " column whose leaf calls the native '"
                            + laf.function() + "' is a computed column —"
                            + " give it an explicit name");
                }
                return laf.function();
            }
        }
        throw new TypeInferenceException("a name-less project column must be a"
                + " property navigation (its leaf names the column); give"
                + " explicit names for computed columns");
    }

    /**
     * Desugar the legacy TDS {@code project(src, [p|expr, …], ['name', …])} into the
     * modern {@code project(src, ~[name:p|expr, …])} &mdash; a pure AST&rarr;AST
     * rewrite (engine {@code ProjectChecker.rewriteLegacyProject}).
     */
    private static AppliedFunction legacyToModern(AppliedFunction af) {
        List<ValueSpecification> ps = af.parameters();
        if (!(ps.get(1) instanceof PureCollection lambdas) || !(ps.get(2) instanceof PureCollection names)) {
            throw new TypeInferenceException(
                    "project(source, [lambdas], [names]) expects two collection literals");
        }
        if (lambdas.values().size() != names.values().size()) {
            throw new TypeInferenceException("project has " + lambdas.values().size()
                    + " column expression(s) but " + names.values().size() + " name(s)");
        }
        List<ColSpec> specs = new ArrayList<>(names.values().size());
        for (int i = 0; i < names.values().size(); i++) {
            if (!(names.values().get(i) instanceof CString name)) {
                throw new TypeInferenceException("expected a string-literal column name");
            }
            // a path literal IS its navigation lambda (the node survives
            // resolution for its alias; the column takes the lambda)
            if (!(columnLambda(lambdas.values().get(i)) instanceof LambdaFunction lf)) {
                throw new TypeInferenceException(
                        "a project column must be a single-parameter, single-expression lambda");
            }
            specs.add(new ColSpec(name.value(), lf));
        }
        return af.withParameters(List.of(ps.get(0), new ColSpecArray(specs)));
    }

    /**
     * Normalize the column argument: a single {@code ~col} wraps into an array, and
     * a bare {@code ~prop} becomes the identity lambda {@code prop:x|$x.prop}
     * (engine's bare-reference desugar) &mdash; so the checked form is always a
     * mapped colspec array.
     */
    private static AppliedFunction withMappedColumns(AppliedFunction af) {
        if (af.parameters().size() != 2) {
            throw new TypeInferenceException("project expects (source, ~[columns])");
        }
        List<ColSpec> specs = switch (af.parameters().get(1)) {
            case ColSpec cs -> List.of(cs);
            case ColSpecArray arr -> arr.colSpecs();
            default -> throw new TypeInferenceException("project expects ~[…] column specifications");
        };
        ColSpecArray mapped = new ColSpecArray(specs.stream().map(ProjectChecker::identityIfBare).toList());
        return af.withParameters(List.of(af.parameters().get(0), mapped));
    }

    /** {@code ~prop} &rarr; {@code prop:x|$x.prop} &mdash; a pass-through column is an identity mapping. */
    private static ColSpec identityIfBare(ColSpec cs) {
        if (cs.function1() != null) {
            return cs;
        }
        return new ColSpec(cs.name(), new LambdaFunction(
                List.of(new Variable("x")),
                List.of(new AppliedProperty(new Variable("x"), cs.name()))));
    }
}
