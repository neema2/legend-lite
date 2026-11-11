// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.base.Nullable;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPlannedValue;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTestDataGen;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;
import com.legend.sql.SqlQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * THE TEST-DATA GENERATOR'S READS SPLICED (B2d.2 of the total-wall program, 2026-11-11; the design the user approved on
 * 2026-11-10, docs/data-delivery-homework/TEST_DATA_GENERATOR_DESIGN_2026_11_10.md): a body's
 * {@code generateTestData(...)}, planned by the test road's {@link Generator} (the generator's planning library, which
 * the compiler does not reach -- the {@link ResultEnvelopeSplice.Frames} pattern: the compiler owns what a splice means,
 * the planner what the value is), and its result's reads made the values they are:
 * <ul>
 *   <li>{@code $r.sqls}: the fetches' texts, a literal list (their text differs from upstream's, which names temporary
 *       tables: the user's ruling of 2026-11-10, a register row);</li>
 *   <li>{@code $r.dataCsvString}: the planned statement's one cell ({@link TypedPlannedValue}), read where the value
 *       stands -- a statement the run sends, the database's work.</li>
 * </ul>
 * The let that bound the generator is spliced away: a read let is evaluated where it is read (lite's let rule), so the
 * generator's statement is sent where its text is read, and a body reading only its {@code sqls} sends none. A read of
 * the result other than those two is refused by name; a carrier of another flavour stays, and the body's class rule
 * refuses it by name. Typed nodes are minted here, in the compiler's layer (AGENTS invariant 7).
 */
public final class GeneratorSplice {

    private GeneratorSplice() {
    }

    /** The properties of upstream's {@code TestDataGenResult} this splice answers. */
    private static final String SQLS = "sqls";
    private static final String CSV = "dataCsvString";

    /** The generator's planning, the test road's to supply. */
    public interface Generator {
        /** {@code g} planned: its fetches' texts and the statement answering its test-data text. */
        Generated plan(TypedTestDataGen g);
    }

    /** One generator planned: its fetches' texts, written in the body's dialect, and the read-only statement whose one
     *  cell is its test-data text. */
    public interface Generated {
        List<String> sqls();

        SqlQuery csv();
    }

    /** Whether {@code body} holds a generator this splice plans ({@code generateTestData}). */
    public static boolean holdsAGenerator(List<TypedSpec> body) {
        for (TypedSpec s : body) {
            if (holds(s)) {
                return true;
            }
        }
        return false;
    }

    private static boolean holds(TypedSpec n) {
        if (n instanceof TypedTestDataGen g && "generate".equals(g.flavor())) {
            return true;
        }
        for (TypedSpec c : n.children()) {
            if (holds(c)) {
                return true;
            }
        }
        return false;
    }

    /** {@code body} with each generator it binds planned by {@code generator} and its reads spliced. */
    public static List<TypedSpec> spliced(List<TypedSpec> body, Generator generator) {
        Map<String, Generated> bound = new HashMap<>();
        List<TypedSpec> out = new ArrayList<>(body.size());
        for (TypedSpec statement : body) {
            TypedSpec s = substituted(statement, bound, generator);
            if (s instanceof TypedLet let) {
                // a rebinding ends the generator's binding (its own value read the earlier one above)
                bound.remove(let.name());
                TypedTestDataGen g = generator(let.value());
                if (g != null) {
                    bound.put(let.name(), generator.plan(g));
                    continue;
                }
            }
            out.add(s);
        }
        return List.copyOf(out);
    }

    /** The generator {@code value} is ({@code generateTestData(...)}, under a {@code toOne()}), or null. */
    private static @Nullable TypedTestDataGen generator(TypedSpec value) {
        TypedSpec v = value;
        while (v instanceof TypedNativeCall c && c.args().size() == 1
                && ResultEnvelopeSplice.TO_ONE_FQN.equals(c.callee().qualifiedName())) {
            v = c.args().get(0);
        }
        return v instanceof TypedTestDataGen g && "generate".equals(g.flavor()) ? g : null;
    }

    /** {@code n} with each read of a bound generator's result -- or of a generator in place -- the value it is. */
    private static TypedSpec substituted(TypedSpec n, Map<String, Generated> bound, Generator generator) {
        if (n instanceof TypedPropertyAccess pa) {
            Generated g = null;
            if (pa.source() instanceof TypedVariable v && bound.containsKey(v.name())) {
                g = bound.get(v.name());
            } else {
                TypedTestDataGen inPlace = generator(pa.source());
                if (inPlace != null) {
                    g = generator.plan(inPlace);
                }
            }
            if (g != null) {
                return read(g, pa);
            }
        }
        if (n instanceof TypedVariable v && bound.containsKey(v.name())) {
            throw new NotImplementedException("the test-data generator's result $" + v.name() + " read other than by"
                    + " its " + SQLS + " or its " + CSV + ": not planned (B2d)");
        }
        Map<String, Generated> scope = bound;
        if (n instanceof TypedLambda lam && lam.parameters().stream().anyMatch(bound::containsKey)) {
            // a lambda parameter of the same name shadows the binding inside the lambda
            scope = new HashMap<>(bound);
            lam.parameters().forEach(scope::remove);
        }
        List<TypedSpec> kids = n.children();
        if (kids.isEmpty()) {
            return n;
        }
        List<TypedSpec> next = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = substituted(k, scope, generator);
            changed |= r != k;
            next.add(r);
        }
        return changed ? n.withChildren(next) : n;
    }

    /** The value of {@code pa}, a read of {@code g}'s result. */
    private static TypedSpec read(Generated g, TypedPropertyAccess pa) {
        return switch (pa.property()) {
            case SQLS -> {
                ExprType one = new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE);
                List<TypedSpec> texts = new ArrayList<>(g.sqls().size());
                for (String t : g.sqls()) {
                    texts.add(new TypedCString(t, one));
                }
                yield new TypedCollection(texts, new ExprType(Type.Primitive.STRING,
                        new Multiplicity.Bounded(texts.size(), texts.size())));
            }
            case CSV -> new TypedPlannedValue(g.csv(), new ExprType(Type.Primitive.STRING, Multiplicity.Bounded.ONE));
            default -> throw new NotImplementedException("the test-data generator's result's " + pa.property()
                    + ": not planned (B2d answers its " + SQLS + " and its " + CSV + ")");
        };
    }
}
