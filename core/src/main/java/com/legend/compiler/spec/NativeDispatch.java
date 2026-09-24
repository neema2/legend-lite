// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * Catalog-driven staging of Java-implemented natives (charter §4AG):
 * a call whose catalog entry says {@link PlatformTypes.NativeImpl
 * #JAVA_ROUTINE} evaluates WHERE IT STANDS — exact-FQN lookup,
 * position-blind, no statement silhouettes — and its value re-enters
 * the statement as a typed literal minted HERE (Invariant 7: typed
 * nodes are compiler work). {@code HANDLE}-kind calls stay symbolic;
 * their consumers force them. A routine that cannot serve a shape
 * throws its declared wall and NOTHING here catches it — the failure
 * is charged, loudly, to whichever channel demanded the value.
 */
public final class NativeDispatch {

    private NativeDispatch() {
    }

    /** The executor's half: compute the routine's value for a call.
     * String-valued today (the compiler-output surfaces); widens with
     * the ladder migration. Throws the routine's declared wall. */
    @FunctionalInterface
    public interface Routine {
        String value(RoutineCall call,
                java.util.List<TypedSpec> letPrefix);
    }

    /** The call a routine computes — a JAVA_ROUTINE native call, or a call
     *  to the QUALIFIED PROPERTY a routine implements ({@code
     *  toSQL(…).toSQLString(…)}, {@code NativeFn.JavaRoutine.implementedDerived}):
     *  one shape for both node kinds — the callee's FQN, the arguments, the
     *  checked type. */
    public record RoutineCall(String fqn, java.util.List<TypedSpec> args,
            com.legend.compiler.element.type.ExprType info) {
        public static RoutineCall of(TypedNativeCall nc) {
            return new RoutineCall(nc.callee().qualifiedName(), nc.args(), nc.info());
        }

        public static RoutineCall of(com.legend.compiler.spec.typed.TypedUserCall uc) {
            return new RoutineCall(uc.callee().qualifiedName(), uc.args(), uc.info());
        }

        /** The routine member this call dispatches to: the native's own, or
         *  the one implementing the qualified property; empty otherwise. */
        public static java.util.Optional<com.legend.builtin.NativeFn.JavaRoutine> routineOf(TypedSpec n) {
            return switch (n) {
                case TypedNativeCall nc -> com.legend.builtin.NativeFn.JavaRoutine.of(nc.callee().qualifiedName())
                        .or(() -> com.legend.builtin.NativeFn.JavaRoutine.ofDerived(nc.callee().qualifiedName()));
                default -> java.util.Optional.empty();
            };
        }
    }

    /** Stage one statement: every JAVA_ROUTINE call in it (bottom-up)
     * is replaced by its computed literal. */
    public static TypedSpec stage(TypedSpec stmt,
            java.util.List<TypedSpec> letPrefix,
            java.util.Map<String, Routine> routines) {

        // LAMBDA BODIES: staging descends into them, which is
        // CONSTANT HOISTING and semantics-preserving here — measured
        // 2026-08-31 (LL_STAGE_TRACE census): exactly 3 in-lambda
        // firings estate-wide (testAdjustDateTranslationInMappingAndQuery
        // x2, testSortQuotes x1), all evaluable because inlining
        // substitutes the loop parameter over a literal collection
        // BEFORE staging; a call reading an UNsubstituted parameter
        // fails LOUDLY in its routine (non-literal args rejected) —
        // never silently wrong. THE OBLIGATION stands for consumers
        // that establish their own evaluation context (assertError's
        // catch): those migrate ONLY together with a real staged
        // environment (lambda boundary + parameter scope).
        if (stmt instanceof TypedNativeCall co
                && com.legend.builtin.NativeFn.ContextOwner.of(co.callee().qualifiedName()).isPresent()) {
            // the call OWNS its arguments' evaluation context
            // (assertError's catch): staging does not enter them — the
            // arm's own evaluation stages them INSIDE that context
            return stmt;
        }
        TypedSpec n = stmt.mapChildren(
                c -> stage(c, letPrefix, routines));
        java.util.Optional<com.legend.builtin.NativeFn.JavaRoutine> member = RoutineCall.routineOf(n);
        if (member.isEmpty()) {
            return n;
        }
        String fqn = member.get().fqn();
        Routine r = routines.get(fqn);
        if (r == null) {
            throw new com.legend.error.NotImplementedException(
                    "catalog says '" + fqn + "' is JAVA_ROUTINE but the"
                    + " executor registered no routine for it");
        }
        RoutineCall call = n instanceof TypedNativeCall nc ? RoutineCall.of(nc)
                : RoutineCall.of((com.legend.compiler.spec.typed.TypedUserCall) n);
        return new TypedCString(r.value(call, letPrefix), n.info());
    }

}
