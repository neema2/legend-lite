package com.legend.compiler.spec;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedAggCol;
import com.legend.compiler.spec.typed.TypedAggColSpec;
import com.legend.compiler.spec.typed.TypedAggColSpecArray;
import com.legend.compiler.spec.typed.TypedColSpec;
import com.legend.compiler.spec.typed.TypedFuncCol;
import com.legend.compiler.spec.typed.TypedFuncColSpec;
import com.legend.compiler.spec.typed.TypedFuncColSpecArray;
import com.legend.compiler.spec.typed.TypedLambda;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * Checked-argument extraction shared by the construct checkers: each reads a
 * strongly-typed payload (a lambda, colspec names, mapped/aggregate columns, an
 * output schema) off an {@link Application}'s already-checked arguments for HIR
 * emission. Extraction only &mdash; all validation happened in the check.
 */
final class Args {

    private Args() {
    }

    /** The {@code i}-th checked argument as the lambda the construct's signature requires. */
    static TypedLambda lambda(Application a, int i) {
        if (a.args().size() > i && a.args().get(i) instanceof TypedLambda lam) {
            return lam;
        }
        throw new TypeInferenceException("'" + a.chosen().qualifiedName()
                + "' expects a lambda argument in position " + i);
    }

    /** The single column name of a checked {@code ~col} argument. */
    static String colSpecName(TypedSpec arg) {
        if (arg instanceof TypedColSpec cs) {
            return cs.name();
        }
        throw new TypeInferenceException("expected a single ~column argument, got "
                + arg.getClass().getSimpleName());
    }

    /** The checked columns of a {@code ~alias:x|…} / {@code ~[…]} argument. */
    static List<TypedFuncCol> funcCols(TypedSpec arg) {
        if (arg instanceof TypedFuncColSpec f) {
            return List.of(f.col());
        }
        if (arg instanceof TypedFuncColSpecArray fa) {
            return fa.cols();
        }
        throw new TypeInferenceException("expected mapped column specification(s), got "
                + arg.getClass().getSimpleName());
    }

    /** The checked aggregate columns of a {@code ~alias:map:reduce} / {@code ~[…]} argument. */
    static List<TypedAggCol> aggCols(TypedSpec arg) {
        if (arg instanceof TypedAggColSpec a) {
            return List.of(a.col());
        }
        if (arg instanceof TypedAggColSpecArray aa) {
            return aa.cols();
        }
        throw new TypeInferenceException("expected aggregate column specification(s), got "
                + arg.getClass().getSimpleName());
    }

    /**
     * The column names a checked relation operator outputs &mdash; read off the
     * <em>resolved</em> output schema (which the signature computed), never
     * re-derived. Used by select/distinct emission, where the output schema IS the
     * selection.
     */
    static List<String> outputColumns(Application a) {
        if (a.out().type() instanceof Type.RelationType rt) {
            return rt.columns().stream().map(Type.Column::name).toList();
        }
        throw new TypeInferenceException("expected a relation result, got " + a.out().type().typeName());
    }
}
