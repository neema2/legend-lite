// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * THE LIFT: A PRINTED TEXT, LEGEND-PURE'S, READ BEFORE THE BODY (B3 step 2b of the compatibility program;
 * the user's rulings of 2026-11-24: B2's Q8 needs no plan fact -- "I thought we already have mechanism for this";
 * a print followed by {@code replace} is one printing step -- "do the replace in java too"; and option (B),
 * substitution before planning -- "Great let's do it!"). Upstream's test bodies compare a relation's own text --
 * {@code $rel->toString()}, legend-engine's {@code meta::relational::tests::csv::toCSV(...)}, either followed by
 * {@code replace} calls with literal arguments -- against goldens legend-pure's printer wrote. Lite's product prints a
 * relation its own way (B2a); legend-pure's text is the edge's alone, written in Java from typed rows (B2's printer).
 * So a compatibility harness passes a typed body through {@link #read} once its setups have run and before it is
 * planned: each printed side's relation is read as a query of its own -- the lets before it and the relation -- through
 * lite's public test door on the body's own session ({@link Reads}), the rows printed by the
 * printer, the print's tail applied in Java ({@link PrintTail}: the replaces, and since the edition program's step 8,
 * 2026-11-26, the other calls of its closed set), and the answer substituted for the side as a literal -- a String, or
 * the Integer or Boolean the tail's last step answers; lite's judge then compares at the assert's own place, by lite's
 * rules. Since step 8 a Float's {@code toString}, and that of a DateTime that is a computation and reads no clock, is
 * read the same way where it is a LAST STEP: the side is an assert's
 * own argument (with its tail), nowhere else -- a Float's text the program goes on computing with (joined, in a let, in
 * a list, under a call outside the tail's set) is left to the database, as tenet 1 says (the user's amendment of
 * 2026-11-25; the independent review of this slice, 2026-11-26: the first draft read a value side wherever it stood,
 * which no ruling covers). A relation's printed side is read wherever it stands outside a lambda, as since step 2b.
 * A side whose relation reads a name no earlier let binds (a
 * lambda's parameter) is left as written. A body that writes (its facts' effects) is left as written: its writes could
 * change what a read before it sees (the design review's gate, 2026-11-24).
 *
 * <p>A printed relation whose order the program does not define (a store read with no sort, past a grouping, a join,
 * a union) prints in the database's order: where an {@code assertEquals} compares it with a golden, the assert becomes
 * an explicit {@code assertSameElements} of the two texts' lines (B3 step 4's printed tables; upstream compared their
 * lines as a multiset, register S131); anywhere else it is left as written.
 *
 * <p>The printer is the edge's one printer of legend-pure's table text (B2b's S1, {@link PureRelationTextWriter}).
 */
public final class UpstreamPrints {

    private UpstreamPrints() {
    }

    /** A query run on the body's session through lite's public test door, its answer written to the output: the
     *  harness's reader (the runner's {@code com.legend.test.Audience.Reads}, which this library does not see). */
    @FunctionalInterface
    public interface Reads {
        void read(com.legend.TypedQuery query, com.legend.result.Output answer);
    }

    /** The printer of a printed side ({@link PrintedRelations.Side}, B2's one rule for a print): its relation's rows
     *  read by {@code read} through {@code reads}, printed; null where it prints no such side (left as written). */
    @FunctionalInterface
    public interface Printer {
        @com.legend.base.Nullable Object print(PrintedRelations.Side side, com.legend.TypedQuery read, Reads reads);

        /** No printer: no side is substituted. */
        Printer NONE = (side, read, reads) -> null;
    }

    /** The edge's printer: legend-pure's text of the rows (or the value) the read answers (B2b's S1,
     *  {@link PureRelationTextWriter}: one writer per read, the database's typed cells in; out, legend-pure's text),
     *  and then, here at the door, the print's tail applied to that text ({@link PrintedRelations.Side#answer}). */
    static final Printer PRINTER = (side, read, reads) -> {
        if (side.written() != null) {
            // a literal's print, written at the door: nothing is read
            return side.answer(side.written());
        }
        String[] text = new String[1];
        reads.read(read, new com.legend.result.Output.Rows(new PureRelationTextWriter(side.form(), t -> text[0] = t)));
        // the tail is applied here, at the door: the writer wrote the text and nothing else
        return side.answer(java.util.Objects.requireNonNull(text[0], "the printer wrote no text"));
    };

    /** {@code e} read as a printed side: a relation's print or, where {@code lastStep} (the side is an assert's own
     *  argument) and the harnesses being legend-pure's own runs of the test's code, a value's
     *  ({@link PrintedRelations#side(TypedSpec, boolean)}). */
    private static PrintedRelations.@com.legend.base.Nullable Side side(TypedSpec e, boolean lastStep,
            List<TypedSpec> lets) {
        return PrintedRelations.side(e, lastStep, lets);
    }

    /** Whether {@code n} is an assert: a call whose arguments are sides a judge compares. */
    private static boolean isAssert(TypedSpec n) {
        return n instanceof TypedNativeCall && com.legend.builtin.NativeFn.Verdict.of(
                com.legend.compiler.spec.typed.Calls.calleeIdOf(n)).isPresent();
    }

    /** The answer a printed side's read gave, as the literal put in its place: the text, or the Integer or Boolean its
     *  tail's last step answers ({@link PrintTail#answers}). */
    private static TypedSpec literal(Object answer, com.legend.compiler.element.type.ExprType info) {
        return switch (answer) {
            case String s -> new TypedCString(s, info);
            case Long l -> new com.legend.compiler.spec.typed.TypedCInteger(l, info);
            case Boolean b -> new com.legend.compiler.spec.typed.TypedCBoolean(b, info);
            default -> throw new IllegalStateException("a printed side answers a String, an Integer or a Boolean, not "
                    + answer.getClass().getSimpleName());
        };
    }

    /** {@code typed} with each printed side read, printed and substituted (above); the query itself where nothing is,
     *  or where the body {@code writes} (its facts' effects, the caller's: the test door states them). */
    public static com.legend.TypedQuery read(com.legend.TypedQuery typed, boolean writes, Reads reads) {
        return read(typed, writes, reads, PRINTER);
    }

    /** {@link #read(com.legend.TypedQuery, Reads)} with {@code printer}. */
    static com.legend.TypedQuery read(com.legend.TypedQuery typed, boolean writes, Reads reads,
            Printer printer) {
        List<TypedSpec> body = typed.body();
        if (writes || !anyPrinted(body)) {
            return typed;
        }
        List<TypedSpec> out = new ArrayList<>(body.size());
        List<TypedSpec> lets = new ArrayList<>();
        boolean changed = false;
        for (TypedSpec statement : body) {
            TypedSpec r = substituted(statement, false, typed, lets, reads, printer);
            changed |= r != statement;
            out.add(r);
            if (r instanceof TypedLet) {
                lets.add(r);
            }
        }
        return changed ? typed.withBody(out) : typed;
    }

    /** Whether any statement of {@code body} holds a printed side (a value's counted wherever it stands: the walk
     *  that follows reads only the ones at an assert's argument). A side is read through the body's lets, as the
     *  walk reads it: a computed DateTime bound by a let is a print only through its let (the outside audit of
     *  slices 3 and 4, 2026-11-26, its B1: with no lets here such a body was returned untouched). */
    static boolean anyPrinted(List<TypedSpec> body) {
        List<TypedSpec> lets = body.stream().filter(TypedLet.class::isInstance).toList();
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            TypedSpec n = work.pop();
            if (side(n, true, lets) != null) {
                return true;
            }
            PrintedFormats.Read format = PrintedFormats.of(n);
            if (format != null && !format.floats(lets).isEmpty()) {
                return true;
            }
            work.addAll(n.children());
        }
        return false;
    }

    /** {@code n} with each printed side whose relation the lets before it can read replaced by its answer; an
     *  {@code assertEquals} of a golden text and a printed relation whose order the program does not define made
     *  {@link #unordered}. {@code assertArgument}: {@code n} is an assert's own argument, where a value's print is a
     *  last step. */
    private static TypedSpec substituted(TypedSpec n, boolean assertArgument, com.legend.TypedQuery typed,
            List<TypedSpec> lets, Reads reads, Printer printer) {
        // a lambda's body is its own scope (its parameters and lets may shadow the body's lets) and may never run (an
        // if's branch, assertError's): a print inside one is left as written (the review of step 2b's M1, 2026-11-24)
        if (n instanceof com.legend.compiler.spec.typed.TypedLambda) {
            return n;
        }
        TypedSpec u = unordered(n, typed, lets, reads, printer);
        if (u != null) {
            return u;
        }
        if (assertArgument) {
            TypedSpec formatted = formatted(n, typed, lets, reads, printer);
            if (formatted != null) {
                return formatted;
            }
        }
        PrintedRelations.Side p = side(n, assertArgument, lets);
        if (p != null) {
            // a relation whose order the program does not define arrives in the database's order: its text is compared
            // line by line in any order where an assertEquals compares it with a golden (above), and left as written
            // anywhere else
            if (!readable(p.relation(), lets) || incidental(p, lets)) {
                return n;
            }
            if (p.form() == PureRelationTextWriter.Form.VALUE && p.written() == null
                    && unrepeatable(p.relation(), lets, typed)) {
                return n;
            }
            Object answer = print(p, typed, lets, reads, printer);
            return answer == null ? n : literal(answer, n.info());
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        boolean anAssert = isAssert(n);
        for (TypedSpec k : kids) {
            TypedSpec r = substituted(k, anAssert, typed, lets, reads, printer);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? n.withChildren(rebuilt) : n;
    }

    /** {@code n}, a {@code format} that is a last step, with each computed Float under {@code %s} or {@code %r} read
     *  as a value's print and put back as its text ({@link PrintedFormats}; the user's ruling of 2026-11-28, D7); null
     *  where {@code n} is no such call or nothing in it is read. An argument is held to a value's print's own guards:
     *  one that reads a name no let binds, or whose second reading may answer another value ({@link #unrepeatable}),
     *  stays as written, lite's own text. */
    private static @com.legend.base.Nullable TypedSpec formatted(TypedSpec n, com.legend.TypedQuery typed,
            List<TypedSpec> lets, Reads reads, Printer printer) {
        PrintedFormats.Read format = PrintedFormats.of(n);
        if (format == null) {
            return null;
        }
        java.util.Map<Integer, String> texts = new java.util.TreeMap<>();
        for (int at : format.floats(lets)) {
            TypedSpec argument = format.args().get(at);
            if (!readable(argument, lets) || unrepeatable(argument, lets, typed)) {
                continue;
            }
            Object text = print(new PrintedRelations.Side(argument, PureRelationTextWriter.Form.VALUE, List.of()),
                    typed, lets, reads, printer);
            if (text instanceof String s) {
                texts.put(at, s);
            }
        }
        return texts.isEmpty() ? null : format.with(texts);
    }

    /** A QUERY whose last statement is a {@code format}: that call's computed Floats under {@code %s} and
     *  {@code %r} read and printed as a body's are ({@link #formatted}), the query otherwise as it was -- what a
     *  query door at legend-pure's audience runs in the query's place. The query itself where its last statement
     *  is no such call. */
    public static com.legend.TypedQuery readLastFormat(com.legend.TypedQuery typed, Reads reads) {
        List<TypedSpec> body = typed.body();
        if (body.isEmpty()) {
            return typed;
        }
        List<TypedSpec> lets = body.subList(0, body.size() - 1);
        if (!lets.stream().allMatch(TypedLet.class::isInstance)) {
            return typed;
        }
        TypedSpec formatted = formatted(body.get(body.size() - 1), typed, lets, reads, PRINTER);
        if (formatted == null) {
            return typed;
        }
        List<TypedSpec> out = new ArrayList<>(lets);
        out.add(formatted);
        return typed.withBody(out);
    }

    /** {@code side}'s text: its relation read (the lets and the relation) and printed; null where the printer prints
     *  none. */
    private static @com.legend.base.Nullable Object print(PrintedRelations.Side side, com.legend.TypedQuery typed,
            List<TypedSpec> lets,
            Reads reads, Printer printer) {
        return printer.print(side, typed.withBody(readBody(side.relation(), lets)), reads);
    }

    /** The read of {@code relation} after {@code lets}: the relation with each let it reads substituted
     *  ({@link #inlined}), then each {@code execute(...)} in it (a frame, outside any lambda) bound to a let of its own
     *  before it -- the shape the program road plans a frame's read in ({@code let r = execute(...); $r.values}, the
     *  corpus's idiom), whatever the body wrote (a frame read where it stands, {@code execute(...).values->at(0)} in
     *  a let, a let holding a to-one reader: the program road plans a to-one reader at an answer's root alone -- the
     *  round's catches on PCT's {@code eval::testSimpleEval} and the corpus's {@code testProjectAssociationTdsV2},
     *  2026-11-24). A lifted body writes nothing, so a value read again is the same value. */
    static List<TypedSpec> readBody(TypedSpec relation, List<TypedSpec> lets) {
        List<TypedSpec> frames = new ArrayList<>();
        TypedSpec answer = framesBound(inlined(relation, lets, Set.of()), frames,
                new java.util.IdentityHashMap<>());
        List<TypedSpec> out = new ArrayList<>(frames);
        out.add(answer);
        return out;
    }

    /** {@code e} with each frame call outside a lambda replaced by a variable bound to it, each such let added to
     *  {@code frames} in reading order (one per call, by identity). */
    private static TypedSpec framesBound(TypedSpec e, List<TypedSpec> frames,
            java.util.Map<TypedSpec, com.legend.compiler.spec.typed.TypedVariable> bound) {
        if (e instanceof com.legend.compiler.spec.typed.TypedLambda) {
            return e;
        }
        List<TypedSpec> kids = e.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = framesBound(k, frames, bound);
            changed |= r != k;
            rebuilt.add(r);
        }
        TypedSpec node = changed ? e.withChildren(rebuilt) : e;
        if (node instanceof TypedNativeCall call
                && com.legend.builtin.NativeFn.Handle.runsAFrame(call.callee().id())) {
            com.legend.compiler.spec.typed.TypedVariable v = bound.get(e);
            if (v == null) {
                v = new com.legend.compiler.spec.typed.TypedVariable("liftedFrame" + frames.size(), node.info());
                frames.add(new TypedLet(v.name(), node, node.info()));
                bound.put(e, v);
            }
            return v;
        }
        return node;
    }

    /** {@code e} with each name a let of {@code lets} binds replaced by that let's value, itself read with the lets
     *  before it. A name a lambda binds is its own. */
    static TypedSpec inlined(TypedSpec e, List<TypedSpec> lets, Set<String> shadowed) {
        if (e instanceof com.legend.compiler.spec.typed.TypedVariable v && !shadowed.contains(v.name())) {
            for (int i = lets.size() - 1; i >= 0; i--) {
                if (lets.get(i) instanceof TypedLet let && let.name().equals(v.name())) {
                    return inlined(let.value(), lets.subList(0, i), Set.of());
                }
            }
            return e;
        }
        Set<String> inner = shadowed;
        if (e instanceof com.legend.compiler.spec.typed.TypedLambda l) {
            inner = new java.util.HashSet<>(shadowed);
            inner.addAll(l.parameters());
            inner.addAll(com.legend.compiler.spec.typed.FreeVars.lets(l));
        }
        List<TypedSpec> kids = e.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = inlined(k, lets, inner);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? e.withChildren(rebuilt) : e;
    }

    private static boolean incidental(PrintedRelations.Side side, List<TypedSpec> lets) {
        // a value's print has no row order of its own (what it reads is held by unrepeatable)
        return side.form() != PureRelationTextWriter.Form.VALUE
                && com.legend.compiler.spec.OrderView.of(side.relation(), lets)
                        == com.legend.compiler.spec.OrderView.INCIDENTAL;
    }

    /** {@code n} as an explicit order-free assert, or null (B3 step 4's printed tables, the design review of
     *  2026-11-24: "an unordered printed table becomes an explicit assertSameElements over the two texts' lines (split
     *  in Java at the edge), judged by lite"): an {@code assertEquals} whose one side is a golden text written as
     *  literals (or a one-element list of one) and whose other is a printed relation with no defined order, made
     *  {@code assertSameElements} of the two texts' lines -- each text split at the line break, or at what a literal
     *  {@code replace} put in its place (the rows joined on one line) -- typed by the typer. Upstream compared such a
     *  text's lines as a multiset (register S131); lite's judge compares the two lists of lines as one. */
    private static @com.legend.base.Nullable TypedSpec unordered(TypedSpec n, com.legend.TypedQuery typed,
            List<TypedSpec> lets, Reads reads, Printer printer) {
        if (!(n instanceof TypedNativeCall call)
                || com.legend.builtin.NativeFn.Verdict.of(com.legend.compiler.spec.typed.Calls.calleeIdOf(n))
                        .orElse(null) != com.legend.builtin.NativeFn.Verdict.ASSERT_EQUALS
                || call.args().size() != 2) {
            return null;
        }
        for (int side = 0; side < 2; side++) {
            // each side where it stands or through the let it is bound to (the review of step 2b, item 2)
            // a relation's print alone: a value's has no row order
            PrintedRelations.Side p = side(
                    com.legend.compiler.spec.typed.Lets.bound(call.args().get(side), lets), false, lets);
            String golden = UpstreamTestBodies.literalText(
                    com.legend.compiler.spec.typed.Lets.bound(call.args().get(1 - side), lets));
            if (p == null || golden == null || !readable(p.relation(), lets) || !incidental(p, lets)) {
                continue;
            }
            // lines are cut from a text: a tail that holds a step other than a replace is left as written
            if (!p.tail().stream().allMatch(t -> t instanceof PrintTail.Replace)) {
                continue;
            }
            List<String> goldenLines;
            List<String> printedLines;
            List<PrintTail.Replace> rs = p.tail().stream().map(t -> (PrintTail.Replace) t).toList();
            int joining = -1;
            for (int i = 0; i < rs.size(); i++) {
                if (rs.get(i).target().equals("\n")) {
                    joining = i;
                }
            }
            if (joining < 0) {
                String text = (String) print(p, typed, lets, reads, printer);
                if (text == null) {
                    return null;
                }
                printedLines = List.of(text.split("\n", -1));
                goldenLines = List.of(golden.split("\n", -1));
            } else {
                // the rows joined on one line by a literal replace (the corpus's toCSV(...)->replace('\n', ',')):
                // the table printed with its line breaks, and the golden's cells cut into the same rows by each
                // printed line's cell count -- rows compared, never cells (the review of step 2b's M3, 2026-11-24);
                // anything else is left as written
                String sep = rs.get(joining).replacement();
                if (joining != rs.size() - 1 || sep.isEmpty()) {
                    return null;
                }
                String text = (String) print(new PrintedRelations.Side(p.relation(), p.form(),
                        List.<PrintTail>copyOf(rs.subList(0, joining))), typed, lets, reads, printer);
                if (text == null) {
                    return null;
                }
                printedLines = List.of(text.split("\n", -1));
                goldenLines = rows(golden, sep, printedLines);
                if (goldenLines == null) {
                    return null;
                }
            }
            List<com.legend.protocol.spec.ValueSpecification> e = literals(side == 0 ? printedLines : goldenLines);
            List<com.legend.protocol.spec.ValueSpecification> a = literals(side == 0 ? goldenLines : printedLines);
            return typed.specs().typeExpression(new com.legend.protocol.spec.AppliedFunction(
                    com.legend.builtin.NativeFn.Verdict.ASSERT_SAME_ELEMENTS.fqn(),
                    List.of(new com.legend.protocol.spec.PureCollection(e),
                            new com.legend.protocol.spec.PureCollection(a)),
                    List.of(), call.pos(), false, false, false));
        }
        return null;
    }

    /** Each text a String literal. */
    private static List<com.legend.protocol.spec.ValueSpecification> literals(List<String> texts) {
        List<com.legend.protocol.spec.ValueSpecification> out = new ArrayList<>();
        for (String t : texts) {
            out.add(new com.legend.protocol.spec.CString(t));
        }
        return out;
    }

    /** {@code golden}'s cells between {@code sep}s cut into rows, each as many cells as the printed line at its place
     *  holds, joined again by {@code sep}; null where the counts do not divide the golden exactly. */
    private static @com.legend.base.Nullable List<String> rows(String golden, String sep, List<String> printedLines) {
        String q = java.util.regex.Pattern.quote(sep);
        List<String> cells = List.of(golden.split(q, -1));
        List<String> out = new ArrayList<>();
        int k = 0;
        for (int i = 0; i < printedLines.size(); i++) {
            int w = printedLines.get(i).split(q, -1).length;   // an empty last line (a final line break) one cell
            if (k + w > cells.size()) {
                return null;
            }
            out.add(String.join(sep, cells.subList(k, k + w)));
            k += w;
        }
        return k == cells.size() ? out : null;
    }

    /** Whether {@code id} is a native whose every call answers anew: the clock's and the draws. */
    private static boolean answersAnew(com.legend.model.FunctionId id) {
        return com.legend.builtin.Pure.AT_DATE_NOW.contains(id) || com.legend.builtin.Pure.AT_DATE_TODAY.contains(id)
                || com.legend.builtin.Pure.AT_MATH_RANDOM.contains(id)
                || com.legend.builtin.Pure.AT_STRING_GENERATION_GENERATE_GUID.contains(id);
    }

    /** Whether {@code n} is itself a call of a native that answers anew each time. */
    private static boolean drawsAnew(TypedSpec n) {
        return n instanceof TypedNativeCall c && answersAnew(c.callee().id());
    }

    /** The natives that take their answer BY POSITION among those the order view reads through (the typer writes
     *  a limit, a drop and a slice as nodes of their own, {@link #readsByOrder}'s first arms; listed here so that
     *  the rule does not rest on that). */
    private static final List<com.legend.model.FunctionId> BY_POSITION = com.legend.model.FunctionId.ofAll(
            com.legend.builtin.Pure.AT__T_MANY__INTEGER_1, com.legend.builtin.Pure.TAKE__T_MANY__INTEGER_1,
            com.legend.builtin.Pure.LIMIT__T_MANY__INTEGER_1, com.legend.builtin.Pure.LIMIT__RELATION_1__INTEGER_1,
            com.legend.builtin.Pure.LIMIT__TDS_1__INTEGER_0_1, com.legend.builtin.Pure.DROP__T_MANY__INTEGER_1,
            com.legend.builtin.Pure.DROP__RELATION_1__INTEGER_1,
            com.legend.builtin.Pure.SLICE__T_MANY__INTEGER_1__INTEGER_1,
            com.legend.builtin.Pure.SLICE__RELATION_1__INTEGER_1__INTEGER_1);

    /** The natives that write a TEXT OF THEIR ROWS IN THE ORDER THEY ARRIVE, among those the order view reads through
     *  ({@code makeString}, a relation's {@code toString}, legend-engine's {@code toCSV}; {@code toString} of one
     *  value shares the relation's name): the text of unordered rows is the arrival order's, so a value computed from
     *  one ({@code ...->makeString()->parseFloat()}) is not repeatable (the second independent review of step 8's
     *  slices 3 and 4, 2026-11-26, its finding 3; applied 2026-11-28). By qualified name, as the order view's own
     *  lists are. */
    private static final Set<String> IN_ROW_ORDER = Set.of(
            com.legend.builtin.Pure.MAKE_STRING__ANY_MANY.qualifiedName(),
            com.legend.builtin.Pure.TO_CSV__TDS.qualifiedName(),
            com.legend.builtin.Pure.TO_STRING__RELATION.qualifiedName(),
            com.legend.builtin.Pure.TO_STRING__ANY_1.qualifiedName());

    /** Whether the native {@code c} answers the same whatever order its rows arrive in: the natives the order view
     *  reads through (a map, a filter, a projection, a distinct, {@code toOne}), a sort, the natives that leave no
     *  order (a grouping, a join, a union), and a count or an emptiness test -- but never a native that takes its
     *  answer by position ({@code at}, {@code take}, {@code drop}, {@code slice}, {@code limit}), which the order
     *  view reads through because it keeps the order it is given, nor one that writes its rows' text in the order
     *  they arrive ({@link #IN_ROW_ORDER}). */
    private static boolean orderFree(TypedNativeCall c) {
        com.legend.model.FunctionId id = c.callee().id();
        String fqn = c.callee().qualifiedName();
        if (BY_POSITION.contains(id) || IN_ROW_ORDER.contains(fqn)) {
            return false;
        }
        return com.legend.compiler.spec.OrderView.ORDER_PRESERVING.contains(fqn)
                || com.legend.compiler.spec.OrderView.SORT_FQNS.contains(fqn)
                || com.legend.compiler.spec.OrderView.ORDER_DESTROYING.contains(fqn)
                || com.legend.compiler.spec.OrderView.ORDER_COMBINING.contains(fqn)
                || com.legend.builtin.Pure.AT_LEGEND_LITE_TRUST_ONE.contains(id)
                || com.legend.builtin.Pure.AT_COLLECTION_SIZE.contains(id)
                || com.legend.builtin.Pure.AT_COLLECTION_COUNT.contains(id)
                || com.legend.builtin.Pure.AT_COLLECTION_IS_EMPTY.contains(id)
                || com.legend.builtin.Pure.AT_COLLECTION_IS_NOT_EMPTY.contains(id);
    }

    /** Whether {@code rows} is many values, or a relation, in an order the program does not define
     *  ({@link com.legend.compiler.spec.OrderView}'s INCIDENTAL: a store's or a frame's rows with no sort, rows past
     *  a grouping, a join, a union or a pivot, and a name {@code lets} does not bind -- a lambda's or a function's
     *  parameter, whose rows are not in sight). A relation is any TDS-shaped value: legend-engine's legacy
     *  {@code TabularDataSet[1]} is ONE value of a class type, and holds rows all the same (the second independent
     *  review of step 8's slices 3 and 4, 2026-11-26, its finding 1; applied 2026-11-28: a {@code limit(1)} over an
     *  unsorted projection was not seen). */
    private static boolean inNoDefinedOrder(TypedSpec rows, List<TypedSpec> lets) {
        return (rows.info().multiplicity().isMany()
                || com.legend.compiler.element.type.PlatformTypes.isTdsShaped(rows.info().type()))
                && com.legend.compiler.spec.OrderView.of(rows, lets) == com.legend.compiler.spec.OrderView.INCIDENTAL;
    }

    /** Whether {@code n}'s answer may change with the order its rows arrive in: a {@code limit}, a {@code drop} or
     *  a {@code slice} of unordered rows ({@link #inNoDefinedOrder}), or a native call that reads unordered rows and is
     *  not known to answer the same in any order ({@link #orderFree}) -- {@code at}, {@code first}, {@code last},
     *  and a sum or an average too (a Float's sum is its rows' order's), and every native this door does not know. */
    private static boolean readsByOrder(TypedSpec n, List<TypedSpec> lets) {
        if (n instanceof com.legend.compiler.spec.typed.TypedLimit l) {
            return inNoDefinedOrder(l.source(), lets);
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedDrop d) {
            return inNoDefinedOrder(d.source(), lets);
        }
        if (n instanceof com.legend.compiler.spec.typed.TypedSlice sl) {
            return inNoDefinedOrder(sl.source(), lets);
        }
        if (n instanceof TypedNativeCall c && !orderFree(c)) {
            for (TypedSpec arg : c.args()) {
                if (inNoDefinedOrder(arg, lets)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Whether a second reading of the value {@code e}, through the lets it reads, may answer another value than
     *  the body's own run. A side is read as a query of its own, before the body runs; the assert's statement reads
     *  once for all it compares. So a value's print is left as written, the database's, where its operand, anywhere
     *  in it -- in a lambda's body, and in the compiled body of a user's function or derived property it calls, and
     *  of the functions that one calls ({@link com.legend.compiler.spec.StatementEffects#reaches}; a derived
     *  property's read is typed as a call of its body) --
     *  <ul>
     *    <li>calls a native that answers anew each time ({@code now()}, {@code today()}, {@code random()},
     *        {@code generateGuid()});</li>
     *    <li>reads rows BY THEIR ORDER where the program does not define it ({@link #readsByOrder}): the guard a
     *        printed relation is held to. A sorted source, and pure values, are in their own order and are lifted;
     *        so is a value that a map, a filter or a {@code toOne} takes from unordered rows.</li>
     *  </ul>
     *  It errs to the database's side: a parameter's rows are never in sight, so a pick or a sum of a lambda's or a
     *  called function's own parameter leaves the print as written whatever the caller passed; and a callee whose
     *  body the compiler walls is not in sight at all. (The independent review of the date slice, the independent
     *  audit of slice 2, the review of its repair, the audit's re-check, R1, and its review: 2026-11-26.) */
    private static boolean unrepeatable(TypedSpec e, List<TypedSpec> lets, com.legend.TypedQuery typed) {
        java.util.Map<com.legend.model.FunctionId, Boolean> called = new java.util.HashMap<>();
        // a called body's own lets and parameters are not this body's: none bound there
        java.util.function.Predicate<TypedSpec> inACalledBody = n -> drawsAnew(n) || readsByOrder(n, List.of());
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(List.of(e));
        Set<TypedSpec> seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
        while (!work.isEmpty()) {
            TypedSpec n = com.legend.compiler.spec.typed.Lets.bound(work.pop(), lets);
            if (!seen.add(n)) {
                continue;
            }
            if (drawsAnew(n) || readsByOrder(n, lets)) {
                return true;
            }
            if (n instanceof com.legend.compiler.spec.typed.TypedUserCall call) {
                try {
                    // the callee's body alone: the call's arguments are this body's, walked below with its lets
                    if (com.legend.compiler.spec.StatementEffects.reaches(call.withChildren(List.of()), inACalledBody,
                            typed.specs(), called)) {
                        return true;
                    }
                } catch (com.legend.error.NotImplementedException notInSight) {
                    // a body the compiler walls (a construct not planned): what it reads is not known here, so
                    // the print stays the run's, which meets the same refusal on its own road
                    return true;
                }
            }
            work.addAll(n.children());
        }
        return false;
    }

    /** Whether every name {@code relation} reads is bound by one of {@code lets}: a lambda's parameter is not. */
    private static boolean readable(TypedSpec relation, List<TypedSpec> lets) {
        Set<String> bound = new java.util.HashSet<>();
        for (TypedSpec l : lets) {
            bound.add(((TypedLet) l).name());
        }
        return bound.containsAll(com.legend.compiler.spec.typed.FreeVars.of(relation));
    }
}
