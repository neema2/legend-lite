package com.legend.compiler.spec;

import com.legend.error.TypeInferenceException;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ValueSpecification;

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
 */
final class ReadChecker {

    private ReadChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() != 2) {
            throw new TypeInferenceException("read takes a row and a column spec, got " + ps.size() + " argument(s)");
        }
        if (!(ps.get(1) instanceof ColSpec cs) || cs.function1() != null || cs.function2() != null) {
            throw new TypeInferenceException("read takes a column spec written at the call (~name), got "
                    + ps.get(1).getClass().getSimpleName());
        }
        TypedSpec row = t.synth(ps.get(0), env);
        if (!(row.info().type() instanceof Type.RelationType rowT)) {
            throw new TypeInferenceException("read takes a row (an extend's, a filter's or a window's), got "
                    + row.info().type().typeName());
        }
        return t.rowColumn(row, rowT, cs.name());
    }
}
