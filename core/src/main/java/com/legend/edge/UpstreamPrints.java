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
 * THE LIFT: A RELATION'S PRINTED TEXT, LEGEND-PURE'S, READ BEFORE THE BODY (B3 step 2b of the compatibility program;
 * the user's rulings of 2026-11-24: B2's Q8 needs no plan fact -- "I thought we already have mechanism for this";
 * a print followed by {@code replace} is one printing step -- "do the replace in java too"; and option (B),
 * substitution before planning -- "Great let's do it!"). Upstream's test bodies compare a relation's own text --
 * {@code $rel->toString()}, legend-engine's {@code meta::relational::tests::csv::toCSV(...)}, either followed by
 * {@code replace} calls with literal arguments -- against goldens legend-pure's printer wrote. Lite's product prints a
 * relation its own way (B2a); legend-pure's text is the edge's alone, written in Java from typed rows (B2's printer).
 * So a compatibility harness passes a typed body through {@link #read} once its setups have run and before it is
 * planned: each printed side's relation is read as a query of its own -- the lets before it and the relation -- through
 * lite's public test door on the body's own session ({@link Reads}), the rows printed by the
 * printer, the replaces applied in Java, and the text substituted for the side as a String literal; lite's judge then
 * compares at the assert's own place, by lite's rules. A side whose relation reads a name no earlier let binds (a
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
        @com.legend.base.Nullable String print(PrintedRelations.Side side, com.legend.TypedQuery read, Reads reads);

        /** No printer: no side is substituted. */
        Printer NONE = (side, read, reads) -> null;
    }

    /** The edge's printer: legend-pure's text of the rows the read answers, the replaces applied (B2b's S1,
     *  {@link PureRelationTextWriter}: one writer per read, the database's typed cells in, legend-pure's text out). */
    static final Printer PRINTER = (side, read, reads) -> {
        String[] text = new String[1];
        reads.read(read, new com.legend.result.Output.Rows(
                new PureRelationTextWriter(side.form(), side.replaces(), t -> text[0] = t)));
        return java.util.Objects.requireNonNull(text[0], "the printer wrote no text");
    };

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
            TypedSpec r = substituted(statement, typed, lets, reads, printer);
            changed |= r != statement;
            out.add(r);
            if (r instanceof TypedLet) {
                lets.add(r);
            }
        }
        return changed ? typed.withBody(out) : typed;
    }

    /** Whether any statement of {@code body} holds a printed side. */
    static boolean anyPrinted(List<TypedSpec> body) {
        java.util.ArrayDeque<TypedSpec> work = new java.util.ArrayDeque<>(body);
        while (!work.isEmpty()) {
            TypedSpec n = work.pop();
            if (PrintedRelations.side(n) != null) {
                return true;
            }
            work.addAll(n.children());
        }
        return false;
    }

    /** {@code n} with each printed side whose relation the lets before it can read replaced by its text; an
     *  {@code assertEquals} of a golden text and a printed relation whose order the program does not define made
     *  {@link #unordered}. */
    private static TypedSpec substituted(TypedSpec n, com.legend.TypedQuery typed, List<TypedSpec> lets,
            Reads reads, Printer printer) {
        TypedSpec u = unordered(n, typed, lets, reads, printer);
        if (u != null) {
            return u;
        }
        PrintedRelations.Side p = PrintedRelations.side(n);
        if (p != null) {
            // a relation whose order the program does not define arrives in the database's order: its text is compared
            // line by line in any order where an assertEquals compares it with a golden (above), and left as written
            // anywhere else
            if (!readable(p.relation(), lets) || incidental(p, lets)) {
                return n;
            }
            String text = print(p, typed, lets, reads, printer);
            return text == null ? n : new TypedCString(text, n.info());
        }
        List<TypedSpec> kids = n.children();
        List<TypedSpec> rebuilt = new ArrayList<>(kids.size());
        boolean changed = false;
        for (TypedSpec k : kids) {
            TypedSpec r = substituted(k, typed, lets, reads, printer);
            changed |= r != k;
            rebuilt.add(r);
        }
        return changed ? n.withChildren(rebuilt) : n;
    }

    /** {@code side}'s text: its relation read (the lets and the relation) and printed; null where the printer prints
     *  none. */
    private static @com.legend.base.Nullable String print(PrintedRelations.Side side, com.legend.TypedQuery typed,
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
        return com.legend.compiler.spec.OrderView.of(side.relation(), lets)
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
            PrintedRelations.Side p = PrintedRelations.side(call.args().get(side));
            String golden = UpstreamTestBodies.literalText(call.args().get(1 - side));
            if (p == null || golden == null || !readable(p.relation(), lets) || !incidental(p, lets)) {
                continue;
            }
            String text = print(p, typed, lets, reads, printer);
            if (text == null) {
                return null;
            }
            String sep = "\n";
            for (PureRelationText.Replace r : p.replaces()) {
                if (r.target().equals(sep) && !r.replacement().isEmpty()) {
                    sep = r.replacement();
                }
            }
            List<com.legend.protocol.spec.ValueSpecification> e = lines(side == 0 ? text : golden, sep);
            List<com.legend.protocol.spec.ValueSpecification> a = lines(side == 0 ? golden : text, sep);
            return typed.specs().typeExpression(new com.legend.protocol.spec.AppliedFunction(
                    com.legend.builtin.NativeFn.Verdict.ASSERT_SAME_ELEMENTS.fqn(),
                    List.of(new com.legend.protocol.spec.PureCollection(e),
                            new com.legend.protocol.spec.PureCollection(a)),
                    List.of(), null, false, false, false));
        }
        return null;
    }

    /** {@code text}'s pieces between {@code sep}s, each a String literal. */
    private static List<com.legend.protocol.spec.ValueSpecification> lines(String text, String sep) {
        List<com.legend.protocol.spec.ValueSpecification> out = new ArrayList<>();
        for (String piece : text.split(java.util.regex.Pattern.quote(sep), -1)) {
            out.add(new com.legend.protocol.spec.CString(piece));
        }
        return out;
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
