package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * A generically checked application: the chosen overload, the typed arguments,
 * and the resolved output. This is the seam of the <strong>check/emit split</strong>
 * (PHASE_G_SPEC_COMPILER.md §12): the {@link Typer} produces it via the one
 * generic application rule, and each construct checker turns it into that
 * construct's HIR node (a plain call node for library functions).
 */
record Application(TypedFunction chosen, List<TypedSpec> args, ExprType out) {
}
