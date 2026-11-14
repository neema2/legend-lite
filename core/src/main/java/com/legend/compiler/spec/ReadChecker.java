package com.legend.compiler.spec;

import com.legend.builtin.Pure;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.error.TypeInferenceException;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.FunctionId;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code read(row, ~name)} (lite surface, docs/SEMANTICS_REGISTER.md S114) &mdash; a row's column through a column spec,
 * which IS the property read {@code row.name}: typed by the one method that types a row's column
 * ({@link Typer#rowColumn}), so its type, its multiplicity and its refusal of a column the row has not are the read's,
 * and lowering sees a column read. Nothing is built for it: the row is typed and its column read, as a compiler binds a
 * member access from the bound receiver and the name. A function generic over relations (S112) is expanded with its
 * parameters substituted, so its {@code $r->read($value)} is {@code $r->read(~close)} by the time it is typed; a column
 * spec that is not written at the call (a variable, a computed one) names no column, and is refused, as is a receiver
 * that is not a row.
 *
 * <p>{@code read(row, ~[a, b])} (2026-11-02, the user: "Yes array read") is the row's values in several columns, one
 * list: each column read as {@code read(row, ~a)} is, and a column with no value contributing none. It is what
 * {@code [$r.a, $r.b]} would be if a list literal took a {@code [0..1]} element, which legend-pure and legend-engine
 * refuse (Typer.collection) -- so a function over any number of a table's columns needs no overload per count. It is
 * typed as a call of its own overload whose arguments are the column reads, the columns' common supertype, its
 * multiplicity the sum of theirs (a literal's rule). It lowers to no SQL list: {@code isEmpty}/{@code isNotEmpty}
 * over it test the cells themselves (Lowerer, Scalars.overCells), and any other use is refused by name.
 */
final class ReadChecker {

    private ReadChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 2) {
            throw new TypeInferenceException("read takes a row and a column spec, got " + ps.size() + " argument(s)");
        }
        List<ColSpec> columns;
        if (ps.get(1) instanceof ColSpec cs) {
            columns = List.of(cs);
        } else if (ps.get(1) instanceof ColSpecArray csa) {
            columns = csa.colSpecs();
            if (columns.isEmpty()) {
                throw new TypeInferenceException("read takes at least one column, got ~[]");
            }
        } else {
            throw new TypeInferenceException("read takes a column spec written at the call (~name or ~[a, b]), got "
                    + ps.get(1).getClass().getSimpleName());
        }
        for (ColSpec c : columns) {
            if (c.function1() != null || c.function2() != null) {
                throw new TypeInferenceException("read takes the names of a row's columns, got ~" + c.name()
                        + " with a function");
            }
        }
        TypedSpec row = t.synth(ps.get(0), env);
        if (!(row.info().type() instanceof Type.RelationType rowT)) {
            throw new TypeInferenceException("read takes a row (an extend's, a filter's or a window's), got "
                    + row.info().type().typeName());
        }
        if (ps.get(1) instanceof ColSpec cs) {
            return t.rowColumn(row, rowT, cs.name());
        }
        List<TypedSpec> cells = new ArrayList<>(columns.size());
        for (ColSpec c : columns) {
            cells.add(t.rowColumn(row, rowT, c.name()));
        }
        Type type = cells.stream().map(c -> c.info().type()).reduce(t.kernel()::commonSupertype).orElseThrow();
        int lo = 0;
        Integer hi = 0;
        for (TypedSpec c : cells) {
            if (!(c.info().multiplicity() instanceof Multiplicity.Bounded b)) {
                throw new IllegalStateException("a row's column read is a bounded multiplicity, got "
                        + c.info().multiplicity().text());
            }
            lo += b.lower();
            hi = hi == null || b.upper() == null ? null : hi + b.upper();
        }
        FunctionId id = FunctionId.of(Pure.READ__T_1__COL_SPEC_ARRAY_1);
        TypedFunction callee = t.model().findFunction(Pure.READ__T_1__COL_SPEC_ARRAY_1.qualifiedName()).stream()
                .filter(f -> f.id().equals(id)).findFirst()
                .orElseThrow(() -> new IllegalStateException("the platform has no " + id + " (Pure.java)"));
        return new TypedNativeCall(callee, cells, new ExprType(type, new Multiplicity.Bounded(lo, hi)), af.pos());
    }
}
