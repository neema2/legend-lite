// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.Calls;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.List;

/**
 * THE TEST-BODY DOOR (B3 step 2 of the compatibility program; the plan of record of 2026-11-20, its E0 = A: upstream's
 * looseness lives at the doors, the run reads lite's rules alone). A harness that runs upstream's tests on lite -- PCT's
 * channel B under the compatibility rules, the relational corpus, the edge's own tests -- passes each typed body through
 * {@link #admitted} once, after typing and before planning; the test planner and the test runner then plan and judge
 * it by lite's rules, with no compatibility form asked. A body's rewrites, each a register row:
 * <ul>
 *   <li>S137 (the user's ruling of 2026-11-05): upstream's {@code assertSize}, {@code assertEmpty} and
 *   {@code assertNotEmpty} take a collection ({@code Any[*]}), so over a relation they count the relation itself, ONE
 *   value -- {@code assertSize($rel, n)} holds only for {@code n == 1}, failing with {@code expected size n, got 1};
 *   {@code assertNotEmpty($rel)} always holds. The relation is replaced by one value and both judges plan the rewritten
 *   call. {@code assertEmpty($rel)} always fails upstream with {@code '%s is not empty'} printing the relation itself,
 *   a text lite does not spell: refused by name (the verdict is a failure either way). Until B3 the test planner asked
 *   the plan style's two-form {@code RelationCounting} for this (census C1, 2026-11-17); the product's form, which
 *   refuses the three over a relation, is now the planner's only one ({@code compiler.spec.RelationCountAsserts}).</li>
 *   <li>S144 (the user's ruling of 2026-11-07, option 2; the plan of record of 2026-11-20: "lite stays strict JSON. The
 *   compatibility door cleans the two goldens' expected JSON literals ... before planning"): upstream's
 *   {@code assertJsonStringsEqual} parses both texts with json-simple 1.1.1, which reads an unterminated string at the
 *   very end of a text as its end; two of legend-engine's milestoned graph-fetch goldens end in such a stray quote
 *   ({@code ...]"}). An expected text written as String literals (one, or joined by {@code +}) that strict JSON refuses
 *   and reads once its one trailing {@code "} is dropped has that quote dropped from its last literal; the judge then
 *   reads both texts as strict JSON, lite's own grammar. Any other text strict JSON refuses is refused by the judge by
 *   name, as in lite's own runs (measured 2026-11-07 on the corpus, both databases: only these two goldens read
 *   differently). Until B3 the run's hearing answered json-simple's grammar, ported ({@code JsonSimpleGrammar}).</li>
 * </ul>
 * The body itself where nothing is rewritten.
 */
public final class UpstreamTestBodies {

    private UpstreamTestBodies() {
    }

    /** {@code typed} as upstream's runners read it: the body rewritten (above), the query itself where nothing is. */
    public static com.legend.TypedQuery admitted(com.legend.TypedQuery typed) {
        List<TypedSpec> read = jsonGoldensCleaned(countedAsOne(typed.body()));
        return read == typed.body() ? typed : typed.withBody(read);
    }

    /** {@code body} with each relation an {@code assertSize} or {@code assertNotEmpty} counts replaced by ONE value
     *  (S137); the body itself where nothing is rewritten. */
    static List<TypedSpec> countedAsOne(List<TypedSpec> body) {
        List<TypedSpec> out = new java.util.ArrayList<>(body.size());
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = asOne(statement);
            changed |= r != statement;
            out.add(r);
        }
        return changed ? out : body;
    }

    /** {@code body} with each {@code assertJsonStringsEqual}'s expected text cleaned of json-simple's stray trailing
     *  quote (S144, above); the body itself where nothing is rewritten. */
    static List<TypedSpec> jsonGoldensCleaned(List<TypedSpec> body) {
        List<TypedSpec> out = new java.util.ArrayList<>(body.size());
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = cleaned(statement);
            changed |= r != statement;
            out.add(r);
        }
        return changed ? out : body;
    }

    private static TypedSpec cleaned(TypedSpec n) {
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = cleaned(k);
            changed |= r != k;
            rebuilt.add(r);
        }
        TypedSpec node = changed ? n.withChildren(rebuilt) : n;
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall call
                && NativeFn.Verdict.of(Calls.calleeIdOf(node)).orElse(null) == NativeFn.Verdict.ASSERT_JSON_STRINGS_EQUAL
                && !call.args().isEmpty()) {
            TypedSpec expected = call.args().get(0);
            List<com.legend.compiler.spec.typed.TypedCString> pieces = new java.util.ArrayList<>();
            if (!literalPieces(expected, pieces) || pieces.isEmpty()) {
                return node;   // not written as literals: the judge reads it as it is
            }
            StringBuilder text = new StringBuilder();
            pieces.forEach(p -> text.append(p.value()));
            if (text.isEmpty() || text.charAt(text.length() - 1) != '"' || strict(text.toString())
                    || !strict(text.substring(0, text.length() - 1))) {
                return node;   // strict JSON reads it, or the quote is not the only difference: the judge decides
            }
            // the piece that holds the text's last character: the last one that is not empty (the review of B3 step
            // 2's L1, 2026-11-24: a trailing '' was trimmed past its start)
            com.legend.compiler.spec.typed.TypedCString last = null;
            for (int i = pieces.size() - 1; i >= 0 && last == null; i--) {
                if (!pieces.get(i).value().isEmpty()) {
                    last = pieces.get(i);
                }
            }
            if (last == null) {
                return node;
            }
            com.legend.compiler.spec.typed.TypedCString trimmed = new com.legend.compiler.spec.typed.TypedCString(
                    last.value().substring(0, last.value().length() - 1), last.info());
            List<TypedSpec> args = new java.util.ArrayList<>(call.args());
            args.set(0, replaced(expected, last, trimmed));
            return call.withChildren(args);
        }
        return node;
    }

    /** Whether {@code e} is String literals alone -- one, or joined by {@code +} -- each added to {@code pieces} in
     *  order. */
    private static boolean literalPieces(TypedSpec e, List<com.legend.compiler.spec.typed.TypedCString> pieces) {
        if (e instanceof com.legend.compiler.spec.typed.TypedCString s) {
            pieces.add(s);
            return true;
        }
        boolean joins = e instanceof com.legend.compiler.spec.typed.TypedNativeCall n
                && com.legend.compiler.element.type.PlatformTypes.isPlus(n.callee().qualifiedName());
        if (!joins && !(e instanceof com.legend.compiler.spec.typed.TypedCollection)) {
            return false;
        }
        for (TypedSpec k : e.children()) {
            if (!literalPieces(k, pieces)) {
                return false;
            }
        }
        return true;
    }

    /** {@code e} with the literal {@code old} (by identity) replaced by {@code now}. */
    private static TypedSpec replaced(TypedSpec e, TypedSpec old, TypedSpec now) {
        if (e == old) {
            return now;
        }
        List<TypedSpec> kids = e.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = replaced(k, old, now);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? e.withChildren(rebuilt) : e;
    }

    /** Whether strict JSON, lite's own grammar, reads {@code text}. */
    private static boolean strict(String text) {
        try {
            com.legend.testexecutionplan.JudgeJson.STRICT.read(text);
            return true;
        } catch (IllegalStateException refused) {
            return false;
        }
    }

    private static TypedSpec asOne(TypedSpec n) {
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = asOne(k);
            changed |= r != k;
            rebuilt.add(r);
        }
        TypedSpec node = changed ? n.withChildren(rebuilt) : n;
        NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(node)).orElse(null);
        if (node instanceof com.legend.compiler.spec.typed.TypedNativeCall call
                && (fn == NativeFn.Verdict.ASSERT_SIZE || fn == NativeFn.Verdict.ASSERT_EMPTY
                        || fn == NativeFn.Verdict.ASSERT_NOT_EMPTY)
                && !call.args().isEmpty() && Type.isRelation(call.args().get(0).info().type())) {
            if (!com.legend.compiler.element.type.Multiplicity.Bounded.ONE.equals(call.args().get(0).info().multiplicity())) {
                // upstream counts each relation the collection holds: one relation is one value; any other
                // multiplicity is refused by name, never rewritten to a count it does not have (the review of B3 step
                // 2's L2, 2026-11-24)
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        fn.bareName() + " over a collection of relations ("
                                + call.args().get(0).info().multiplicity().text() + "): upstream counts the relations it"
                                + " holds, which the compatibility door does not rewrite; say what the test means with"
                                + " size()");
            }
            if (fn == NativeFn.Verdict.ASSERT_EMPTY) {
                throw new com.legend.error.ModelException(com.legend.error.LegendCompileException.Phase.TYPE,
                        "assertEmpty over a relation: upstream counts the relation as ONE value, so it fails with a"
                                + " message printing the relation itself, which lite does not spell; say"
                                + " assertEquals(0, $relation->size())");
            }
            List<TypedSpec> args = new java.util.ArrayList<>(call.args());
            args.set(0, new com.legend.compiler.spec.typed.TypedCInteger(1L,
                    com.legend.compiler.element.type.ExprType.one(Type.Primitive.INTEGER)));
            return call.withChildren(args);
        }
        return node;
    }
}
