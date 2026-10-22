// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;

import java.io.IOException;
import java.math.BigDecimal;
import java.io.Writer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * THE RUNNER (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, step 3): an {@link ExecutionPlan} run with a caller's
 * parameter values, with no model, no compiler and no dialect — what runs is the plan's own statement, reviewed when the
 * plan was made. It checks and converts the caller's values ({@link PlanParameters}), takes the session for the
 * statement's target ({@link PlanSessions}: shared, set up once, by the target's content), checks it is the database and
 * server version the statement is written for, runs the target's session statements, binds each slot to its
 * parameter's value as the slot says (writing a value's type where the statement leaves it to the value, H2's), and
 * passes on the text the database writes, or its typed rows ({@link #rows}: each cell read by the session from the
 * driver by the plan's column, docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §3, the plan path program's step 7).
 */
public final class PlanRunner {

    private PlanRunner() {
    }

    /**
     * Runs {@code plan} with {@code values} (by parameter name, each as legend-engine's execute API makes it from a
     * request's value), on a session of {@code sessions}, writing the result's text to {@code out}, which is flushed
     * and never closed.
     */
    public static void run(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions, Writer out)
            throws IOException {
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(), values);
        switch (plan.root()) {
            case ExecutionPlan.TextResult text -> text(text, checked, sessions, out);
            case ExecutionPlan.Keyed keyed when keyed.format() != null -> {
                keyed(keyed, checked, sessions, new TextSink(keyed, out));
                out.flush();
            }
            case ExecutionPlan.Keyed keyed -> throw new IllegalArgumentException("a keyed plan whose answer is typed"
                    + " rows is run by rows(...), which decodes them; run(...) writes a text");
            case ExecutionPlan.TdsResult tds -> throw new IllegalArgumentException("a plan whose answer is typed rows"
                    + " (a tabular result) is run by rows(...), which decodes them; run(...) writes a text");
            case ExecutionPlan.Sequence sequence -> throw new com.legend.error.NotImplementedException("a plan of"
                    + " several steps is not run yet: phase 2 of the execution plan program");
        }
    }

    /** A plan's typed rows: the columns, with their three type facts, and each row's cells decoded by them. */
    public record Rows(List<ExecutionPlan.Column> columns, List<List<Object>> rows) {
        public Rows {
            columns = List.copyOf(columns);
            rows = List.copyOf(rows);
        }
    }

    /**
     * Runs {@code plan}, whose answer is typed rows ({@link ExecutionPlan.TdsResult}; the plan path program's step 6,
     * 2026-10-18), with {@code values}, on a session of {@code sessions}: each row's cells read by the session's
     * typed method ({@link Statements#rows}: from the driver by the plan's columns, step 7).
     */
    public static Rows rows(ExecutionPlan plan, Map<String, ?> values, PlanSessions.Source sessions)
            throws IOException {
        Map<String, PlanParameters.Checked> checked = PlanParameters.check(plan.parameters(), values);
        return switch (plan.root()) {
            case ExecutionPlan.TdsResult tds -> {
                List<List<Object>> out = new java.util.ArrayList<>();
                send(tds.sql(), checked, sessions, (st, statement, bound) ->
                        st.rows(statement, tds.sql().slots(), bound, tds.columns(), out::add));
                yield new Rows(tds.columns(), out);
            }
            case ExecutionPlan.Keyed keyed when keyed.format() == null -> {
                RowsSink sink = new RowsSink();
                keyed(keyed, checked, sessions, sink);
                yield new Rows(sink.columns, sink.rows);
            }
            case ExecutionPlan.Keyed keyed -> throw new IllegalArgumentException("a keyed plan whose answer is a text"
                    + " is run by run(...), which writes it; rows(...) decodes typed rows");
            case ExecutionPlan.TextResult text -> throw new IllegalArgumentException("a plan whose answer is a text"
                    + " is run by run(...), which writes it; rows(...) decodes typed rows");
            case ExecutionPlan.Sequence sequence -> throw new com.legend.error.NotImplementedException("a plan of"
                    + " several steps is not run yet: phase 2 of the execution plan program");
        };
    }

    private static void text(ExecutionPlan.TextResult text, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Writer out) throws IOException {
        switch (text.format()) {
            case CSV, JSON, TDS_TEXT -> {
                boolean[] written = {false};
                send(text.sql(), checked, sessions, (st, statement, bound) ->
                        st.query(statement, text.sql().slots(), bound, row -> {
                            if (!written[0]) {
                                written[0] = true;
                                out.write(row == null ? "" : row);
                            }
                        }));
                if (!written[0]) {
                    throw new IllegalStateException("the plan's statement wrote no text: no row");
                }
            }
            case JSON_PER_ROW -> {
                // one JSON object per row: the array's brackets and commas are the runner's
                out.write('[');
                boolean[] first = {true};
                send(text.sql(), checked, sessions, (st, statement, bound) ->
                        st.query(statement, text.sql().slots(), bound, row -> {
                            if (!first[0]) {
                                out.write(',');
                            }
                            first[0] = false;
                            out.write(row != null ? row : "null");
                            out.flush();
                        }));
                out.write(']');
            }
        }
        out.flush();
    }

    // ---- a keyed plan (a dynamic pivot's probe plan, 2026-10-20; docs/PLAN_PATH_PROGRAM_2026_10_17.md §9) ----------

    /** Where a keyed plan's answer goes, once its keys are known: its columns, then each row's cells. */
    private interface KeyedSink {
        void columns(List<ExecutionPlan.Column> columns, List<String> names) throws IOException;

        void row(List<Object> cells) throws IOException;

        void end() throws IOException;
    }

    /** A keyed plan's typed rows, collected (a tabular result's edge, as {@link #rows} collects a tabular plan's). */
    private static final class RowsSink implements KeyedSink {
        private final List<ExecutionPlan.Column> columns = new java.util.ArrayList<>();
        private final List<List<Object>> rows = new java.util.ArrayList<>();

        @Override
        public void columns(List<ExecutionPlan.Column> columns, List<String> names) {
            this.columns.addAll(columns);
        }

        @Override
        public void row(List<Object> cells) {
            rows.add(cells);
        }

        @Override
        public void end() {
        }
    }

    /** A keyed plan's text: the database writes every cell and every key's name; the runner writes the format's
     *  punctuation around them (docs/PLAN_AND_RESULT_MODEL_2026_10_18.md §5, F1): a TDS text's header line and its
     *  rows on lines of their own; a CSV's header and rows each ended by CRLF; a JSON array of one object per row. */
    private static final class TextSink implements KeyedSink {
        private final ExecutionPlan.Format format;
        private final Writer out;
        private final List<String> names = new java.util.ArrayList<>();
        private boolean first = true;

        TextSink(ExecutionPlan.Keyed keyed, Writer out) {
            this.format = java.util.Objects.requireNonNull(keyed.format());
            this.out = out;
        }

        @Override
        public void columns(List<ExecutionPlan.Column> columns, List<String> names) throws IOException {
            this.names.addAll(names);
            switch (format) {
                case TDS_TEXT -> out.write(String.join(",", names));
                case CSV -> out.write(String.join(",", names) + "\r\n");
                case JSON -> out.write('[');
                case JSON_PER_ROW -> throw new IllegalStateException("a keyed text's JSON is JSON");
            }
        }

        @Override
        public void row(List<Object> cells) throws IOException {
            List<String> texts = new java.util.ArrayList<>(cells.size());
            for (int i = 0; i < cells.size(); i++) {
                if (!(cells.get(i) instanceof String s)) {
                    throw new IllegalStateException("a keyed text's cell " + (i + 1) + " is " + cells.get(i)
                            + ": the database writes every cell as its text");
                }
                texts.add(s);
            }
            switch (format) {
                case TDS_TEXT -> out.write("\n" + String.join(",", texts));
                case CSV -> out.write(String.join(",", texts) + "\r\n");
                case JSON -> {
                    StringBuilder object = new StringBuilder(first ? "{" : ",{");
                    for (int i = 0; i < texts.size(); i++) {
                        object.append(i == 0 ? "" : ",").append(names.get(i)).append(':').append(texts.get(i));
                    }
                    out.write(object.append('}').toString());
                }
                case JSON_PER_ROW -> throw new IllegalStateException("a keyed text's JSON is JSON");
            }
            first = false;
        }

        @Override
        public void end() throws IOException {
            if (format == ExecutionPlan.Format.JSON) {
                out.write(']');
            }
        }
    }

    /**
     * Runs a keyed plan on one session, inside its snapshot: the keys probe, then the body written for those keys
     * (each per-key region once per key, in the probe's order, each copy binding its key's text), its columns and
     * rows handed to {@code sink}. The runner writes only text the plan carries and the keys' positions; every value
     * and every name is the database's.
     */
    private static void keyed(ExecutionPlan.Keyed keyed, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, KeyedSink sink) throws IOException {
        for (ExecutionPlan.Sql sql : List.of(keyed.keys(), keyed.body())) {
            for (ExecutionPlan.Slot slot : sql.slots()) {
                if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                    nullType(one, slot);
                }
            }
        }
        int perKey = (int) keyed.outputs().stream().filter(o -> o instanceof ExecutionPlan.Keyed.PerKey).count();
        // each statement's cells read by the names it writes them under (a session reading by name, MongoDB's, needs
        // them; JDBC reads by place): the probe's keyFields, a fixed output's field, a per-key output's
        // "_pk<position>_<field>" (ExecutionPlan.Keyed.PerKey)
        List<ExecutionPlan.Column> keyColumns = new java.util.ArrayList<>();
        for (int i = 0; i <= perKey; i++) {
            keyColumns.add(new ExecutionPlan.Column(keyed.keyFields().get(i), "String",
                    new ExecutionPlan.Multiplicity(1, 1), "VARCHAR", null));
        }
        try (Sessions.Session session = sessions.open(keyed.keys().target())) {
            Statements st = session.statements();
            st.check(keyed.keys().target());
            try (var origin = StatementOrigin.enter(StatementOrigin.SESSION)) {
                for (String s : keyed.keys().target().session()) {
                    StatementOrigin.sent(s);
                    st.execute(s);
                }
            }
            st.inSnapshot(keyed.snapshot(), () -> {
                List<List<Object>> keys = new java.util.ArrayList<>();
                ExecutionPlan.Sql probe = keyed.keys();
                List<PlanParameters.Checked> probeValues = new java.util.ArrayList<>(probe.slots().size());
                for (int i = 0; i < probe.slots().size(); i++) {
                    probeValues.add(value(checked, probe.slots().get(i), i + 1));
                }
                st.rows(statement(probe, checked), probe.slots(), probeValues, keyColumns, keys::add);
                int fixed = keyed.outputs().size() - perKey;
                int columns = fixed + keys.size() * perKey;
                if (keyed.maxColumns() != null && columns > keyed.maxColumns()) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot of " + keys.size() + " keys answers "
                            + columns + " columns, more than the database's " + keyed.maxColumns());
                }
                List<ExecutionPlan.Column> answered = new java.util.ArrayList<>(columns);
                List<ExecutionPlan.Column> read = new java.util.ArrayList<>(columns);
                List<String> names = new java.util.ArrayList<>(columns);
                for (int i = 0; i < keyed.outputs().size(); i++) {
                    if (keyed.outputs().get(i) instanceof ExecutionPlan.Keyed.Fixed f) {
                        ExecutionPlan.Column c = keyed.format() == null ? f.column() : text(f.column());
                        answered.add(c);
                        read.add(named(c, f.field()));
                        names.add(f.name());
                    }
                }
                for (int k = 0; k < keys.size(); k++) {
                    for (int i = 0; i < keyed.outputs().size(); i++) {
                        if (keyed.outputs().get(i) instanceof ExecutionPlan.Keyed.PerKey p) {
                            String name = cellText(keys.get(k), p.name());
                            ExecutionPlan.Column c = p.column();
                            ExecutionPlan.Column a = keyed.format() == null
                                    ? new ExecutionPlan.Column(name, c.type(), c.multiplicity(), c.slot(), c.store())
                                    : text(c);
                            answered.add(a);
                            read.add(named(a, "_pk" + k + "_" + p.field()));
                            names.add(name);
                        }
                    }
                }
                Expanded body = expand(keyed.body(), keys, checked);
                sink.columns(answered, names);
                StatementOrigin.sent(body.statement());
                st.rows(body.statement(), body.slots(), body.values(), read, sink::row);
                sink.end();
            });
        }
    }

    /** {@code c} read under the name its statement writes it: the column's facts, that name. */
    private static ExecutionPlan.Column named(ExecutionPlan.Column c, String name) {
        return new ExecutionPlan.Column(name, c.type(), c.multiplicity(), c.slot(), c.store());
    }

    /** A cell of a text format's body, read as the text the database wrote. */
    private static ExecutionPlan.Column text(ExecutionPlan.Column c) {
        return new ExecutionPlan.Column(c.name(), "String", new ExecutionPlan.Multiplicity(1, 1), "VARCHAR", null);
    }

    private static String cellText(List<Object> key, int cell) {
        if (cell >= key.size() || !(key.get(cell) instanceof String s)) {
            throw new IllegalStateException("a keys row's cell " + cell + " is " + (cell < key.size() ? key.get(cell)
                    : "absent") + ": the probe writes each key's text and names");
        }
        return s;
    }

    /** A body written for its keys: the statement, and each placeholder's slot and value in placeholder order. */
    private record Expanded(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
    }

    /**
     * {@code sql} written for {@code keys}: the text outside its regions as it is, its type holes filled; each region
     * once per key ({@code lead} before the first copy, {@code separator} between copies, the key's position at each
     * counter; with no key, {@code empty}, or the statement refused where it has no form without one); each slot
     * outside a region bound once, each inside one bound in every copy, a key's cell to that key's text.
     */
    private static Expanded expand(ExecutionPlan.Sql sql, List<List<Object>> keys,
            Map<String, PlanParameters.Checked> checked) {
        String text = sql.statement();
        // the type holes, each outside every region (the writer refuses one inside): their places and types, in order
        List<int[]> holes = new java.util.ArrayList<>();
        List<String> holeTypes = new java.util.ArrayList<>();
        for (int i = 0; i < sql.slots().size(); i++) {
            ExecutionPlan.Slot each = sql.slots().get(i);
            if (each.binding() instanceof ExecutionPlan.Binding.One one && one.hole() != null) {
                holes.add(new int[] {one.hole().at()});
                holeTypes.add(typeOf(one.hole(), value(checked, each, i + 1), each));
            }
        }
        StringBuilder out = new StringBuilder(text.length() + keys.size() * 64);
        List<ExecutionPlan.Slot> slots = new java.util.ArrayList<>();
        List<PlanParameters.Checked> values = new java.util.ArrayList<>();
        int[] hole = {0};
        int from = 0;
        int slot = 0;
        for (ExecutionPlan.Region r : sql.regions()) {
            outside(out, text, from, r.start(), holes, holeTypes, hole);
            for (; slot < r.firstSlot(); slot++) {
                bindOnce(sql, slot, checked, slots, values);
            }
            if (keys.isEmpty()) {
                if (r.empty() == null) {
                    throw new com.legend.error.NotImplementedException("a dynamic pivot with no fixed column found no key: its"
                            + " relation has no column");
                }
                out.append(r.empty());
            } else {
                out.append(r.lead());
                for (int k = 0; k < keys.size(); k++) {
                    if (k > 0) {
                        out.append(r.separator());
                    }
                    int at = r.start();
                    for (int counter : r.counters()) {
                        out.append(text, at, counter).append(k);
                        at = counter;
                    }
                    out.append(text, at, r.end());
                    for (int s = r.firstSlot(); s < r.firstSlot() + r.slotCount(); s++) {
                        ExecutionPlan.Slot each = sql.slots().get(s);
                        if (each.binding() instanceof ExecutionPlan.Binding.Key key) {
                            slots.add(new ExecutionPlan.Slot("", new ExecutionPlan.Binding.One("VARCHAR", null)));
                            values.add(new PlanParameters.One(cellText(keys.get(k), key.cell())));
                        } else {
                            slots.add(each);
                            values.add(value(checked, each, s + 1));
                        }
                    }
                }
            }
            slot = r.firstSlot() + r.slotCount();
            from = r.end();
        }
        outside(out, text, from, text.length(), holes, holeTypes, hole);
        for (; slot < sql.slots().size(); slot++) {
            bindOnce(sql, slot, checked, slots, values);
        }
        return new Expanded(out.toString(), slots, values);
    }

    /** The statement's text {@code [from, to)} outside its regions, each type hole up to {@code to} filled (one at
     *  {@code to} itself before the region that starts there); {@code next} the first hole not yet written. */
    private static void outside(StringBuilder out, String text, int from, int to, List<int[]> holes,
            List<String> holeTypes, int[] next) {
        int at = from;
        while (next[0] < holes.size() && holes.get(next[0])[0] <= to) {
            int place = holes.get(next[0])[0];
            out.append(text, at, place).append(holeTypes.get(next[0]));
            at = place;
            next[0]++;
        }
        out.append(text, at, to);
    }

    private static void bindOnce(ExecutionPlan.Sql sql, int slot, Map<String, PlanParameters.Checked> checked,
            List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
        ExecutionPlan.Slot each = sql.slots().get(slot);
        slots.add(each);
        values.add(value(checked, each, slot + 1));
    }

    /** What a statement's session is asked, once the session is checked and its own statements have run: the
     *  statement with its type holes filled, and the slots' checked values in slot order. */
    private interface Ask {
        void of(Statements st, String statement, List<PlanParameters.Checked> values) throws IOException;
    }

    /** {@code sql} sent on a session of {@code sessions}: the slots bound to the checked values, the type holes
     *  filled, the session asked ({@code ask}: each row's text, or each row's typed cells). */
    private static void send(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> checked,
            PlanSessions.Source sessions, Ask ask) throws IOException {
        // every null type checked before any session is opened: a plan whose null is of no type is refused whole
        for (ExecutionPlan.Slot slot : sql.slots()) {
            if (slot.binding() instanceof ExecutionPlan.Binding.One one) {
                nullType(one, slot);
            }
        }
        List<PlanParameters.Checked> values = new java.util.ArrayList<>(sql.slots().size());
        for (int i = 0; i < sql.slots().size(); i++) {
            values.add(value(checked, sql.slots().get(i), i + 1));
        }
        // the session's own statements (JDBC's, or a connector's: Statements), 2026-10-17, docs/MONGODB_PLAN_2026_10_16.md
        // D12: the runner holds the plan and the punctuation; the session carries text in and text out
        try (Sessions.Session session = sessions.open(sql.target())) {
            Statements st = session.statements();
            st.check(sql.target());
            try (var origin = StatementOrigin.enter(StatementOrigin.SESSION)) {
                for (String s : sql.target().session()) {
                    StatementOrigin.sent(s);
                    st.execute(s);
                }
            }
            String statement = statement(sql, checked);
            // counted here alone, for every session kind (a session sends, the runner counts: JdbcStatements counted
            // each statement a second time, 2026-10-21)
            StatementOrigin.sent(statement);
            ask.of(st, statement, values);
        }
    }

    /** {@code slot}'s parameter's checked value. */
    private static PlanParameters.Checked value(Map<String, PlanParameters.Checked> checked, ExecutionPlan.Slot slot,
            int index) {
        PlanParameters.Checked value = checked.get(slot.parameter());
        if (value == null) {
            throw new IllegalStateException("slot " + index + " names parameter '" + slot.parameter()
                    + "', which the plan does not declare");
        }
        return value;
    }

    /** The statement to prepare: the plan's, with each type hole filled with its value's type (a statement with none is
     *  the plan's as it is). */
    private static String statement(ExecutionPlan.Sql sql, Map<String, PlanParameters.Checked> checked) {
        StringBuilder out = new StringBuilder();
        int from = 0;
        List<ExecutionPlan.Slot> slots = sql.slots();
        for (int i = 0; i < slots.size(); i++) {
            ExecutionPlan.Slot slot = slots.get(i);
            if (slot.binding() instanceof ExecutionPlan.Binding.One one && one.hole() != null) {
                ExecutionPlan.TypeHole hole = one.hole();
                out.append(sql.statement(), from, hole.at()).append(typeOf(hole, value(checked, slot, i + 1), slot));
                from = hole.at();
            }
        }
        return out.append(sql.statement().substring(from)).toString();
    }

    /** The type a hole takes for {@code value}: the spelling of its kind, with its own digits where the spelling takes
     *  them; an absent value's, its type's name alone (a null has no digits to keep). */
    private static String typeOf(ExecutionPlan.TypeHole hole, PlanParameters.Checked value, ExecutionPlan.Slot slot) {
        Object v = switch (value) {
            case PlanParameters.One one -> one.value();
            case PlanParameters.None none -> null;
            case PlanParameters.Many many -> throw new IllegalStateException("slot of '" + slot.parameter()
                    + "' binds one value, given a list");
        };
        ExecutionPlan.TypeSpelling spelling = spelling(hole, v == null ? hole.absent() : kindOf(v, slot), slot);
        if (v == null) {
            return spelling.name();
        }
        BigDecimal digits = v instanceof Double d ? BigDecimal.valueOf(d) : v instanceof BigDecimal bd ? bd : null;
        return switch (spelling.digits()) {
            case NONE -> spelling.name();
            case PRECISION -> spelling.name() + "(" + digitsOf(digits, slot).precision() + ")";
            case PRECISION_AND_SCALE -> spelling.name() + "(" + digitsOf(digits, slot).precision() + ","
                    + digitsOf(digits, slot).scale() + ")";
        };
    }

    static ExecutionPlan.TypeSpelling spelling(ExecutionPlan.TypeHole hole, ExecutionPlan.ValueKind kind,
            ExecutionPlan.Slot slot) {
        ExecutionPlan.TypeSpelling spelling = hole.types().get(kind);
        if (spelling == null) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "' is given a value of kind " + kind
                    + ", and its type hole spells only " + hole.types().keySet());
        }
        return spelling;
    }

    private static final java.time.format.DateTimeFormatter DATE_TIME_TEXT =
            java.time.format.DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss.SSSSSSSSS", java.util.Locale.ROOT);

    /** {@code t} as the text its cast reads: to the nanosecond, or to {@code fraction} digits of a second, the finer ones
     *  cut, as its literal's are. */
    static String dateTimeText(LocalDateTime t, @com.legend.base.Nullable Integer fraction) {
        int unit = 1;
        for (int digits = fraction == null ? 9 : fraction; digits < 9; digits++) {
            unit *= 10;
        }
        return DATE_TIME_TEXT.format(t.withNano(t.getNano() - t.getNano() % unit));
    }

    private static BigDecimal digitsOf(@com.legend.base.Nullable BigDecimal digits, ExecutionPlan.Slot slot) {
        if (digits == null) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "' spells a type with a value's digits,"
                    + " given a value with none");
        }
        return digits;
    }

    /** The JDBC type number of {@code one}'s null: the plan's null types are {@code java.sql.JDBCType} names (the
     *  plan format's own vocabulary, which every session reads), or the plan is refused by name. */
    static int nullType(ExecutionPlan.Binding.One one, ExecutionPlan.Slot slot) {
        try {
            return java.sql.JDBCType.valueOf(one.nullType()).getVendorTypeNumber();
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("slot of '" + slot.parameter() + "': its null type '" + one.nullType()
                    + "' is no JDBC type", e);
        }
    }

    /** The kind of a checked value (one {@link PlanParameters} makes). */
    static ExecutionPlan.ValueKind kindOf(Object v, ExecutionPlan.Slot slot) {
        return switch (v) {
            case Long l -> ExecutionPlan.ValueKind.INTEGER;
            case BigDecimal d -> ExecutionPlan.ValueKind.DECIMAL;
            case Double d -> ExecutionPlan.ValueKind.FLOATING;
            case LocalDate d -> ExecutionPlan.ValueKind.DATE;
            case LocalDateTime t -> t.getNano() % 1000 == 0 ? ExecutionPlan.ValueKind.DATE_TIME
                    : ExecutionPlan.ValueKind.DATE_TIME_NANOS;
            default -> throw new IllegalStateException("slot of '" + slot.parameter() + "' is typed by its value,"
                    + " given " + v.getClass().getSimpleName() + ", which is of no value kind");
        };
    }
}
