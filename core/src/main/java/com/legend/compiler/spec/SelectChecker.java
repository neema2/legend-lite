package com.legend.compiler.spec;

import java.util.List;
import com.legend.compiler.spec.typed.TypedSelect;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.parser.spec.AppliedFunction;

/**
 * Relation {@code select} (engine {@code SelectChecker}) &mdash; fully generic:
 * the {@code ⊆} constraint validates the names and binds the narrowed {@code Z};
 * emission reads the columns off the resolved output schema.
 */
final class SelectChecker {

    private SelectChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        Application a = t.checkGeneric(af, env);
        List<String> columns = Args.outputColumns(a);
        // ~[] is legal where zero columns MEAN something (groupBy's
        // whole-relation aggregate); a zero-column PROJECTION is not it.
        if (columns.isEmpty()) {
            throw new TypeInferenceException("select(~[]) selects no columns");
        }
        return new TypedSelect(a.args().get(0), columns, a.out());
    }
}
