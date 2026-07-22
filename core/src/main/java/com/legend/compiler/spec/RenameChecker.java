package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedRename;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.model.spec.AppliedFunction;
import com.legend.model.spec.ColSpecArray;
import com.legend.model.spec.ValueSpecification;

import java.util.List;

/**
 * Relation {@code rename} (engine {@code RenameChecker}) &mdash; <strong>fully
 * generic</strong> against the real signature {@code rename<T,Z,K,V>(Relation<T>[1],
 * old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1]}: the
 * kernel's {@code ⊆} constraint validates the old column and concretizes {@code Z},
 * the shared {@code K} carries its type (and shadow multiplicity) onto {@code V},
 * and {@code resolveOutput} evaluates {@code T-Z+V}. No schema code here &mdash;
 * this class only desugars and emits.
 *
 * <p>The array form {@code rename(~[a,b], ~[x,y])} <em>desugars</em> to a chain of
 * scalar renames: its signature ({@code ColSpecArray<V>}, no {@code K}) cannot type
 * the new columns &mdash; positional old&harr;new pairing is beyond unification, and
 * engine's checker does it bespoke. Same behavior, one generic rule.
 */
final class RenameChecker {

    private RenameChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        List<ValueSpecification> ps = af.parameters();
        if (ps.size() == 3 && ps.get(1) instanceof ColSpecArray olds && ps.get(2) instanceof ColSpecArray news) {
            if (olds.colSpecs().size() != news.colSpecs().size()) {
                throw new TypeInferenceException("rename has " + olds.colSpecs().size()
                        + " old column(s) but " + news.colSpecs().size() + " new name(s)");
            }
            ValueSpecification chain = ps.get(0);
            for (int i = 0; i < olds.colSpecs().size(); i++) {
                chain = new AppliedFunction(CoreFn.RENAME.parseName(),
                        List.of(chain, olds.colSpecs().get(i), news.colSpecs().get(i)));
            }
            return t.synth(chain, env);
        }
        Application a = t.checkGeneric(af, env);
        if (a.args().size() != 3) {
            throw new TypeInferenceException("rename expects (relation, ~old, ~new)");
        }
        String oldName = Args.colSpecName(a.args().get(1));
        String newName = Args.colSpecName(a.args().get(2));
        return new TypedRename(a.args().get(0),
                List.of(new TypedRename.ColRename(oldName, newName)),
                positionPreserving(a, oldName, newName));
    }

    /**
     * {@code resolveOutput(T-Z+V)} APPENDS the new column — set arithmetic
     * has no position. Renaming must not MOVE the column (engine semantics;
     * corpus pin: restrict(['lastName','averageAge'])->renameColumns(
     * lastName->name) keeps name FIRST): rebuild the schema in the source's
     * column order, the renamed slot carrying V's name with its
     * kernel-resolved type/multiplicity.
     */
    private static com.legend.compiler.element.type.ExprType positionPreserving(
            Application a, String oldName, String newName) {
        if (!(a.out().type() instanceof com.legend.compiler.element.type.Type
                        .RelationType outRt)
                || !(a.args().get(0).info().type()
                        instanceof com.legend.compiler.element.type.Type
                                .RelationType srcRt)) {
            return a.out();
        }
        com.legend.compiler.element.type.Type.Column vCol = outRt.columns().stream()
                .filter(c -> c.name().equals(newName)).findFirst().orElse(null);
        if (vCol == null) {
            return a.out();
        }
        List<com.legend.compiler.element.type.Type.Column> cols =
                srcRt.columns().stream()
                        .map(c -> c.name().equals(oldName) ? vCol : c)
                        .toList();
        return new com.legend.compiler.element.type.ExprType(
                new com.legend.compiler.element.type.Type.RelationType(
                        cols, outRt.dynamicColumns()),
                a.out().multiplicity());
    }
}
