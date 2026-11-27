// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.spec.typed.TypedCrossTypeComparison;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * THE COMPARISON SETTING (the edition program's step 2b, 2026-11-26;
 * docs/records/edition-plan-to-finish-stage-b-2026-11-25.md §2, the first of the four kinds of difference): what a
 * comparison across types IS, for the code a door admits. Three audiences disagree -- lite refuses it (the equality
 * rule: two values are equal only when they have the same type and the same value), legend-engine sends it to the
 * database as written, legend-pure answers it by the types alone -- and the type-checker is the one place that sees
 * every piece of code a query reaches: the query, a function's body when the function is used, a mapping's Pure when
 * the store is resolved. So the door hands the type-checker its setting for the query ({@code TypedQuery.comparing}),
 * and the type-checker asks it where it types such a comparison ({@code Overloads.applyGeneric}); the answer is typed
 * code that means the same to everything after it. Until this step a door rewrote the typed query once, after it was
 * typed (the edge's EngineComparisons and PureComparisons walks), which never reached a body.
 *
 * <p>The crossing handed in is the typer's record of the comparison: the call it typed, which comparison it is, the two
 * types, where it was written. A rule returns the call, another typed node of the call's type, the record itself (a
 * marker the planner then refuses by name), or raises.
 *
 * <p>Code written in another language is never asked about: an island's translation (SQL's, Cypher's, the timeseries
 * language's) compares by its own language's rule.
 */
public interface CrossTypeRule {

    /** THE SETTING THAT CHECKS NOTHING: a comparison across types is the call as written, in the query and in every
     *  body. For a reading that is no compile check -- the planner's own account of a program (its facts, a nested
     *  query's lineage), where the code was or will be checked by the door that admitted it -- and for an audience
     *  that sends every comparison to the database as written. A compile check takes the door's own setting
     *  ({@code Compiler.compileErrors}'s), never this by default (the edition program's step 4, 2026-11-26: until
     *  that day a type-checker made for such a reading carried lite's setting, which read no body; once lite's
     *  setting reads bodies, it would have refused a compatibility door's code from inside the planner). */
    CrossTypeRule AS_WRITTEN = new CrossTypeRule() {
        @Override
        public TypedSpec inQuery(TypedCrossTypeComparison crossing) {
            return crossing.comparison();
        }

        @Override
        public boolean readsBodies() {
            return false;
        }

        @Override
        public TypedSpec inBody(TypedCrossTypeComparison crossing) {
            return crossing.comparison();
        }
    };


    /** A comparison across types written in the query or test body the door receives. */
    TypedSpec inQuery(TypedCrossTypeComparison crossing);

    /** Whether this setting answers a body's comparisons with anything but the call. Where it does not (every door's
     *  today), the type-checker never looks for a comparison across types in a body: nothing is read of the model
     *  there that was not read before the setting (the independent audit of 2026-11-26, finding 2: finding one asks
     *  the model whether two classes are related, which a broken superclass refuses). */
    boolean readsBodies();

    /** One written in a body the query reaches: a function's, a derived property's, a constraint's, a mapping's.
     *  Asked only where {@link #readsBodies}. */
    TypedSpec inBody(TypedCrossTypeComparison crossing);

    /** One met in a body typed INTO ITS CALL, over the caller's own columns (a function generic over relations: its
     *  column types are type parameters, which the type-checker fills in at each call). In the function as written
     *  such a comparison is between generic positions, whose types each call gives; that this call's columns differ
     *  is no statement of the function's author (lite's setting keeps it for the call, and refuses it there). Asked
     *  only where
     *  {@link #readsBodies}; a body's answer unless a setting says otherwise. */
    default TypedSpec inGenericBody(TypedCrossTypeComparison crossing) {
        return inBody(crossing);
    }

    /** A comparison written in a body whose sides' types are known only where the function is used: a parameter
     *  declared {@code Number}, the abstract {@code Date}, {@code Any} or a type variable (the edition program's step
     *  6). Asked only where {@link #readsBodies}. The call itself unless a setting says otherwise; a setting that
     *  answers such a comparison by the types its sides have at the call returns the record, and is asked again
     *  once they are known ({@link #atTheCall}). */
    default TypedSpec atRunTime(com.legend.compiler.spec.typed.TypedRunTimeComparison comparison) {
        return comparison.comparison();
    }

    /** {@code comparison} (one {@link #atRunTime} kept) once the functions the query reaches stand in the query, each
     *  argument in its parameter's place. The call itself unless a setting says otherwise. */
    default TypedSpec atTheCall(com.legend.compiler.spec.typed.TypedRunTimeComparison comparison,
            com.legend.compiler.element.ModelContext ctx) {
        return comparison.comparison();
    }
}
