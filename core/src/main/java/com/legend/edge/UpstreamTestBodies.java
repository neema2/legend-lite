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
 *   value -- {@code assertSize($rel, n)} holds only for {@code n == 1}, failing with {@code expected size: n, actual size: 1};
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
        List<TypedSpec> read = rowsOrderFree(typed, joinsOrderFree(typed, jsonGoldensCleaned(countedAsOne(typed.body()))));
        return read == typed.body() ? typed : typed.withBody(read);
    }

    /** {@code body} with each {@code assertEquals} one of whose sides has no defined order ({@code OrderView.INCIDENTAL}:
     *  a store or frame read with no sort, or past a grouping, a join, a union) made {@code assertSameElements} of the
     *  same two sides (B3 step 4, 2026-11-24: the user's "we still need multi-set compare for backwards compatibility",
     *  made explicit at the door; until now the judges compared such a pair as a multiset on their own, register S131's
     *  rows and grids). Lite's planner refuses the order-dependent form ({@code compiler.spec.OrderedAsserts}). A
     *  printed or joined text is the lift's and {@link #joinsOrderFree}'s, which run first. The body itself where
     *  nothing is rewritten. */
    static List<TypedSpec> rowsOrderFree(com.legend.TypedQuery typed, List<TypedSpec> body) {
        List<TypedSpec> out = new java.util.ArrayList<>(body.size());
        List<TypedSpec> lets = new java.util.ArrayList<>();
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = rowOrderFree(typed, statement, lets);
            changed |= r != statement;
            out.add(r);
            lets.add(r);
        }
        return changed ? out : body;
    }

    private static TypedSpec rowOrderFree(com.legend.TypedQuery typed, TypedSpec n, List<TypedSpec> lets) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall call && call.args().size() == 2
                && NativeFn.Verdict.of(Calls.calleeIdOf(n)).orElse(null) == NativeFn.Verdict.ASSERT_EQUALS
                && (com.legend.compiler.spec.OrderedAsserts.unorderedRows(call.args().get(0), lets)
                        || com.legend.compiler.spec.OrderedAsserts.unorderedRows(call.args().get(1), lets))) {
            return EngineSubstitutes.call(typed.context(), List.of(com.legend.model.FunctionId.of(
                    com.legend.builtin.Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY)), call.args(), call.pos());
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = rowOrderFree(typed, k, lets);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? n.withChildren(rebuilt) : n;
    }

    /** {@code body} with each order-dependent comparison of a golden text and a join of a list whose order the program
     *  does not define made explicitly order-free (B3 step 4's joins, pulled in with step 2b's deletion of S131's
     *  hidden multiset, 2026-11-24): an {@code assertEquals} or {@code assertSameElements} whose one side is a golden
     *  written as String literals (or a one-element list of one) and whose other is {@code L->joinStrings(sep)} or
     *  {@code L->makeString(sep)} with a literal separator, {@code L} a list of Strings with no defined order (a store or
     *  frame read with no sort: {@code OrderView.INCIDENTAL}), becomes {@code assertSameElements} of the golden's pieces
     *  between separators and {@code L} itself. Upstream's judge compared the two texts' tokens as a multiset there
     *  (register S131's "a join's tokens"); lite's judge compares the two lists. The body itself where nothing is
     *  rewritten. */
    static List<TypedSpec> joinsOrderFree(com.legend.TypedQuery typed, List<TypedSpec> body) {
        List<TypedSpec> out = new java.util.ArrayList<>(body.size());
        List<TypedSpec> lets = new java.util.ArrayList<>();
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = joinOrderFree(typed, statement, lets);
            changed |= r != statement;
            out.add(r);
            lets.add(r);
        }
        return changed ? out : body;
    }

    private static TypedSpec joinOrderFree(com.legend.TypedQuery typed, TypedSpec n, List<TypedSpec> lets) {
        if (n instanceof com.legend.compiler.spec.typed.TypedNativeCall call && call.args().size() == 2) {
            NativeFn.Verdict fn = NativeFn.Verdict.of(Calls.calleeIdOf(n)).orElse(null);
            if (fn == NativeFn.Verdict.ASSERT_EQUALS || fn == NativeFn.Verdict.ASSERT_SAME_ELEMENTS) {
                for (int side = 0; side < 2; side++) {
                    String golden = literalText(com.legend.compiler.spec.typed.Lets.bound(call.args().get(1 - side), lets));
                    // the join where it stands, or the let it is bound to (`let res = L->makeString(','); assertEquals(
                    // golden, $res)`: the corpus's testQualifierWithForkAndOrWithInline)
                    TypedSpec joined = com.legend.compiler.spec.typed.Lets.bound(call.args().get(side), lets);
                    TypedSpec list = unorderedJoined(joined, lets);
                    // an empty golden is left as written (an empty list joins to '' but '' splits to one piece); a
                    // list moved from its let to the assert is never moved past a write (the review of step 2b's M2,
                    // 2026-11-24). A separator inside a value is a known narrowing: the golden's pieces are compared
                    // with the list's values, where upstream split both texts
                    if (golden == null || list == null || golden.isEmpty()
                            || joined != call.args().get(side) && writesSince(call.args().get(side), lets, typed)) {
                        continue;
                    }
                    String sep = ((com.legend.compiler.spec.typed.TypedCString)
                            ((com.legend.compiler.spec.typed.TypedNativeCall) joined).args().get(1)).value();
                    if (sep.isEmpty()) {
                        continue;
                    }
                    List<com.legend.protocol.spec.ValueSpecification> pieces = new java.util.ArrayList<>();
                    for (String piece : golden.split(java.util.regex.Pattern.quote(sep), -1)) {
                        pieces.add(new com.legend.protocol.spec.CString(piece));
                    }
                    TypedSpec tokens = typed.specs().typeExpression(new com.legend.protocol.spec.PureCollection(pieces));
                    List<TypedSpec> args = side == 0 ? List.of(list, tokens) : List.of(tokens, list);
                    return EngineSubstitutes.call(typed.context(), List.of(com.legend.model.FunctionId.of(
                            com.legend.builtin.Pure.ASSERT_SAME_ELEMENTS__ANY_MANY__ANY_MANY)), args, call.pos());
                }
            }
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new java.util.ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = joinOrderFree(typed, k, lets);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? n.withChildren(rebuilt) : n;
    }

    /** Whether {@code list} maps a table's rows each to a bare cell read -- {@code $t.rows->map(r | <r's column>)}, the
     *  typed form of {@code columnValues} and of a getter with a literal column name: the column read itself, the read
     *  under the cell's {@code trustOne}, or the TDSNull-if-empty guard ({@code TdsDesugars}' three forms). A computed
     *  element ({@code $r.getString('a') + ' ' + ...}) is not one. */
    private static boolean cellReads(TypedSpec list) {
        if (!(list instanceof com.legend.compiler.spec.typed.TypedMap map)
                || !(map.source() instanceof com.legend.compiler.spec.typed.TypedPropertyAccess rows)
                || !rows.property().equals("rows")
                || !(map.mapper() instanceof com.legend.compiler.spec.typed.TypedLambda fn) || fn.body().isEmpty()
                || fn.parameters().size() != 1) {
            return false;
        }
        TypedSpec cell = fn.body().get(fn.body().size() - 1);
        if (cell instanceof com.legend.compiler.spec.typed.TypedIf) {
            return true;   // the TDSNull-if-empty guard over the cell
        }
        if (cell instanceof com.legend.compiler.spec.typed.TypedNativeCall c && c.args().size() == 1) {
            cell = c.args().get(0);   // the cell under its trustOne
        }
        return cell instanceof com.legend.compiler.spec.typed.TypedPropertyAccess pa
                && pa.source() instanceof com.legend.compiler.spec.typed.TypedVariable row
                && row.name().equals(fn.parameters().get(0));
    }

    /** Whether a statement after the let {@code variable} is bound to (and before the assert) writes. */
    private static boolean writesSince(TypedSpec variable, List<TypedSpec> lets, com.legend.TypedQuery typed) {
        if (!(variable instanceof com.legend.compiler.spec.typed.TypedVariable v)) {
            return false;
        }
        int at = -1;
        for (int i = lets.size() - 1; i >= 0 && at < 0; i--) {
            if (lets.get(i) instanceof com.legend.compiler.spec.typed.TypedLet l && l.name().equals(v.name())) {
                at = i;
            }
        }
        java.util.Map<com.legend.model.FunctionId, Boolean> memo = new java.util.HashMap<>();
        for (int i = at + 1; i < lets.size(); i++) {
            if (com.legend.compiler.spec.StatementEffects.containsEffect(lets.get(i), typed.specs(), memo)) {
                return true;
            }
        }
        return false;
    }

    /** The list {@code e} joins -- {@code L->joinStrings(sep)} or {@code L->makeString(sep)}, a literal separator, a
     *  list of Strings whose order the program does not define -- or null. */
    private static @com.legend.base.Nullable TypedSpec unorderedJoined(TypedSpec e, List<TypedSpec> lets) {
        if (!(e instanceof com.legend.compiler.spec.typed.TypedNativeCall j) || j.args().size() != 2
                || !(j.args().get(1) instanceof com.legend.compiler.spec.typed.TypedCString)) {
            return null;
        }
        String name = j.callee().qualifiedName();
        if (!name.equals(com.legend.compiler.element.type.PlatformTypes.STRING_JOIN_STRINGS)
                && !name.equals(com.legend.compiler.element.type.PlatformTypes.STRING_MAKE_STRING)) {
            return null;
        }
        TypedSpec list = j.args().get(0);
        // a list of bare table-cell reads ({@code columnValues}, {@code rows->map(r | $r.get('c'))}) is left as written:
        // a cell may be TDSNull, which a join writes as its text and a list holds as no value (the corpus's
        // testProjectionWithEnumQualifierParameter)
        boolean strings = list.info().type() == Type.Primitive.STRING && !cellReads(list);
        return strings && com.legend.compiler.spec.OrderView.of(list, lets) == com.legend.compiler.spec.OrderView.INCIDENTAL
                ? list : null;
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

    /** {@code e}'s text where it is written as String literals -- one, or joined by {@code +}, or a one-element list
     *  of such -- else null (the lift's golden, {@link UpstreamPrints}). */
    static @com.legend.base.Nullable String literalText(TypedSpec e) {
        TypedSpec s = e instanceof com.legend.compiler.spec.typed.TypedCollection c && c.children().size() == 1
                ? c.children().get(0) : e;
        List<com.legend.compiler.spec.typed.TypedCString> pieces = new java.util.ArrayList<>();
        if (s instanceof com.legend.compiler.spec.typed.TypedCollection || !literalPieces(s, pieces)) {
            return null;
        }
        StringBuilder text = new StringBuilder();
        pieces.forEach(p -> text.append(p.value()));
        return text.toString();
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
