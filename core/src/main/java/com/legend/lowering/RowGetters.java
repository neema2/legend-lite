package com.legend.lowering;

import com.legend.builtin.NativeFn;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.sql.SqlExpr;

/**
 * The row accessors ({@code $r.getString('COL')} et al) the typer could not
 * fold to a column read (a NON-literal column name at typing time that
 * inlining or unroll later made literal): the call is upstream's qualified
 * property, lifted ({@code TDSRow$prop$getString(this, colName)}), typed by
 * its own declaration and IMPLEMENTED here — the named column of the row.
 */
final class RowGetters {

    private RowGetters() {
    }

    /** The family is the closed type {@link NativeFn.RowGetter}: membership by
     *  the enum over the lifted callee, never a string set. */
    static boolean isRowGetter(TypedNativeCall g) {
        return NativeFn.RowGetter.ofLifted(g.callee().qualifiedName()).isPresent()
                && g.args().size() == 2
                && g.args().get(0) instanceof TypedVariable
                && g.args().get(1) instanceof TypedCString;
    }

    static SqlExpr read(TypedNativeCall g, Resolvers.ColumnResolver columns) {
        String row = ((TypedVariable) g.args().get(0)).name();
        String column = ((TypedCString) g.args().get(1)).value();
        SqlExpr r = columns.resolve(row, column);
        if (r == null) {
            throw new Resolvers.UnfoldableRef(column);
        }
        return r;
    }
}
