// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.executionplan.PlannedSql;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.types.Decimal128;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A MongoDB session's {@link Statements} (docs/MONGODB_PLAN_2026_10_16.md §5.1 and §5.2): the plan's statement is a
 * command in Extended JSON, as the MongoDb dialect writes it; an {@code aggregate} command runs through the driver's
 * cursor, and each row's one field is the text the pipeline wrote (D10): carried, never read as a value. A plan
 * parameter is the pipeline variable {@code "$$lpN"}, bound through the command's {@code let}: {@code lpN} the N-th of
 * the statement's parameter slots, each value in the driver's own encoding (decision D5), and the pipeline sent as
 * the plan holds it (the C checkpoint's NOTE, 2026-11-12: until then each parameter was a marker document this session
 * replaced in place). A dynamic pivot's key cell is the one marker left ({@code {"$legendParam": "?"}}): the runner
 * writes the per-key region once per key (the charter's A3(a)) and this session binds each copy's key at its marker,
 * as JDBC binds a key's {@code ?}. A raise the pipeline wrote (a
 * conversion of a message between the platform's sentinels) comes back as the platform's own text, as a SQL raise
 * does through the executor's funnel. Since the plan path program's step 7 (2026-10-18) the session answers typed
 * rows too ({@link #rows}: each document's fields read by the plan's columns, {@link MongoDbDecode}).
 */
final class MongoDbStatements implements Statements {

    /** A dynamic pivot's key cell, as the MongoDb dialect writes it in each copy of the per-key region. */
    private static final String KEY_MARKER = "$legendParam";

    /** The first major version of MongoDB whose aggregate command takes a {@code let}, which binds a plan's
     *  parameters. */
    static final int LET_SINCE = 5;

    private final MongoDatabase database;

    /** Whether the server was found to take an aggregate's {@code let} (read once a session). */
    private boolean letChecked;

    MongoDbStatements(MongoDatabase database) {
        this.database = database;
    }

    @Override
    public void check(ExecutionPlan.Target target) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        if (!(target.database() instanceof ExecutionPlan.Database.Declared d)
                || d.connection().databaseType() != com.legend.executionplan.DatabaseType.MongoDB) {
            throw new com.legend.error.NotImplementedException("the query executes on " + target.database().type()
                    + " but the session is MongoDB — dialect/connection mismatch");
        }
        if (!letChecked) {
            // the server's own report of its version, as JDBC's metadata names a database's (a check, no plan text)
            BsonValue version = run(new BsonDocument("buildInfo", new org.bson.BsonInt32(1))).get("version");
            requireLet(version != null && version.isString() ? version.asString().getValue() : String.valueOf(version));
            letChecked = true;
        }
    }

    /** A server of {@code version} refused by name where its aggregate command takes no {@code let} (before 5.0): a
     *  plan's parameters are bound through it, and the session never runs a plan on a server that would not bind
     *  them. */
    static void requireLet(String version) {
        int major;
        try {
            major = Integer.parseInt(version.substring(0, version.indexOf('.') < 0 ? version.length()
                    : version.indexOf('.')));
        } catch (NumberFormatException e) {
            throw new com.legend.error.NotImplementedException("the MongoDB server reports its version as '" + version
                    + "', which names no major version: a plan's parameters are bound through the aggregate command's"
                    + " let, which MongoDB takes from " + LET_SINCE + ".0");
        }
        if (major < LET_SINCE) {
            throw new com.legend.error.NotImplementedException("the MongoDB server is " + version + ": a plan's"
                    + " parameters are bound through the aggregate command's let, which MongoDB takes from "
                    + LET_SINCE + ".0; the server is refused, never sent a plan");
        }
    }

    @Override
    public boolean execute(PlannedSql statement) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        run(command(statement));
        return false;
    }

    /** {@inheritDoc} MongoDB: a keyed plan's two commands share no transaction (the dialect says {@code NONE}: a
     *  server that is no replica set has none across commands), so a snapshot is asked of it only as {@code NONE}. */
    @Override
    public void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        if (snapshot != ExecutionPlan.Keyed.Snapshot.NONE) {
            throw new IllegalStateException("a " + snapshot + " snapshot asked of a MongoDB session: its keyed plans"
                    + " take none");
        }
        work.run();
    }

    /** A write plan's step: not designed on MongoDB (its writes are the bulk load alone, docs/MONGODB_PLAN_2026_10_16.md);
     *  refused by name. */
    @Override
    public long update(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        throw new com.legend.error.NotImplementedException("a write plan's step on MongoDB: a program that changes a"
                + " store is not designed for MongoDB (its writes are the bulk load alone)");
    }

    @Override
    public <T> T inTransaction(java.util.function.Supplier<T> work) {
        com.legend.base.RunScope.opening("a transaction bracket");   // the owner's act: refused while planning
        throw new com.legend.error.NotImplementedException("a transaction over a plan's steps on MongoDB: a program that"
                + " changes a store is not designed for MongoDB");
    }

    @Override
    public boolean ownsTransaction() {
        com.legend.base.RunScope.opening("a transaction bracket");   // the owner's act: refused while planning
        throw new com.legend.error.NotImplementedException("a transaction over a plan's steps on MongoDB: a program that"
                + " changes a store is not designed for MongoDB");
    }

    /** A MongoDB session describes no statement before running it: the body driver's schema probes (a raw grid's
     *  roster, a deferred relation's columns, a wire-decided kind) are not designed for MongoDB; refused by name. */
    @Override
    public List<Reported> columns(PlannedSql statement) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        throw new com.legend.error.NotImplementedException("a statement's reported columns asked of a MongoDB session:"
                + " the body driver's schema probes are not designed for MongoDB");
    }

    @Override
    public List<Reported> describe(PlannedSql statement) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        throw new com.legend.error.NotImplementedException("a statement described before running asked of a MongoDB"
                + " session: the body driver's schema probes are not designed for MongoDB");
    }

    /** A raw grid is an authored SQL text: a MongoDB session answers none, so no relation of reported columns. */
    @Override
    public void reportedRows(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            ReportedColumns columns, Cells cells) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        throw new com.legend.error.NotImplementedException("a relation of the columns its result reports asked of a"
                + " MongoDB session: a raw grid is an authored SQL text, which a MongoDB database does not run");
    }

    /** A MongoDB database reads no runner's file: refused by name (a plan loads nothing into a connector's own). */
    @Override
    public void readsFile(PlannedSql statement, String path) {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        throw new com.legend.error.NotImplementedException("a file's load asked of a MongoDB session: its databases are"
                + " the connector's own, and a plan loads none");
    }

    @Override
    public void query(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            Rows rows) throws IOException {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        try (MongoCursor<BsonDocument> cursor = cursor(statement, slots, values)) {
            while (cursor.hasNext()) {
                rows.row(text(cursor.next()));
            }
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    /** {@inheritDoc} Each document's fields read by the plan's columns ({@link MongoDbDecode}: a field by its
     *  column's name, its value by the slot and the Pure type). */
    @Override
    public void rows(PlannedSql statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
        com.legend.base.RunScope.statement("a statement");   // THE WALL's rule 3, per act (2026-11-06)
        try (MongoCursor<BsonDocument> cursor = cursor(statement, slots, values)) {
            while (cursor.hasNext()) {
                cells.row(MongoDbDecode.row(cursor.next(), columns));
            }
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    /**
     * The aggregate command's cursor, run through the driver: the plan's pipeline as the plan holds it, each plan
     * parameter's slot bound to its variable through the command's {@code let} ({@code lp1}, {@code lp2}, ... in slot
     * order, as the MongoDb dialect names them: {@code MongoJson.parameterVariable}), each key cell's slot -- the
     * plan's {@code Binding.Key}, once per copy of a per-key region, its value the key's text -- at its marker, in
     * document order.
     */
    private MongoCursor<BsonDocument> cursor(PlannedSql statement, List<ExecutionPlan.Slot> slots,
            List<PlanParameters.Checked> values) {
        BsonDocument let = new BsonDocument();
        List<ExecutionPlan.Slot> keySlots = new ArrayList<>();
        List<PlanParameters.Checked> keys = new ArrayList<>();
        for (int i = 0; i < slots.size(); i++) {
            ExecutionPlan.Slot slot = slots.get(i);
            // a key slot by its type (J7, 2026-11-12: until then by an empty name and a One binding, the runner's own)
            if (slot.binding() instanceof ExecutionPlan.Binding.Key) {
                keySlots.add(slot);
                keys.add(values.get(i));
            } else {
                let.append("lp" + (let.size() + 1), literal(value(slot, values.get(i), i + 1)));
            }
        }
        BsonDocument command = command(statement);
        if (!keySlots.isEmpty()) {
            int[] next = {0};
            command = keyed(command, keySlots, keys, next).asDocument();
            if (next[0] != keySlots.size()) {
                throw new IllegalStateException("the plan's statement holds " + next[0] + " key markers, and its"
                        + " regions bind " + keySlots.size() + " keys");
            }
        }
        BsonValue aggregate = command.get("aggregate");
        BsonValue pipeline = command.get("pipeline");
        if (aggregate == null || pipeline == null || !pipeline.isArray()) {
            throw new IllegalStateException("the plan's statement is not an aggregate command: " + command.keySet());
        }
        List<BsonDocument> stages = new ArrayList<>();
        for (BsonValue stage : pipeline.asArray()) {
            stages.add(stage.asDocument());
        }
        AggregateIterable<BsonDocument> run = aggregate.isString()
                ? database.getCollection(aggregate.asString().getValue(), BsonDocument.class).aggregate(stages)
                : database.aggregate(stages, BsonDocument.class);
        return (let.isEmpty() ? run : run.let(let)).iterator();
    }

    /** The row's one text: its one field, a string or null; anything else is not text, refused by name. */
    private static @com.legend.base.Nullable String text(BsonDocument row) {
        if (row.size() != 1) {
            throw new IllegalStateException("the plan's statement wrote a row of " + row.size()
                    + " fields, not one text: " + row.keySet());
        }
        BsonValue v = row.values().iterator().next();
        if (v.isNull()) {
            return null;
        }
        if (v.isString()) {
            return v.asString().getValue();
        }
        throw new IllegalStateException("the plan's statement wrote a " + v.getBsonType() + ", not text");
    }

    private static BsonDocument command(PlannedSql statement) {
        try {
            return BsonDocument.parse(statement.text());
        } catch (org.bson.json.JsonParseException | org.bson.BsonInvalidOperationException e) {
            throw new IllegalStateException("the plan's statement is not a MongoDB command: " + e.getMessage(), e);
        }
    }

    private BsonDocument run(BsonDocument command) {
        try {
            return database.runCommand(command, BsonDocument.class);
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    /** The server's failure as the platform's data error: a raise's message unwrapped from the server's text. */
    private static com.legend.error.DataError failure(MongoException e) {
        String unwrapped = RaisedErrors.unwrap(e.getMessage());
        return new com.legend.error.DataError(unwrapped == null ? String.valueOf(e.getMessage()) : unwrapped, e);
    }

    /** A key slot's value: the key's text, as the runner's expansion gives it (J7, 2026-11-12). */
    private static BsonValue keyText(PlanParameters.Checked checked, int index) {
        if (!(checked instanceof PlanParameters.One one && one.value() instanceof String text)) {
            throw new IllegalStateException("key " + index + " binds a key's cell given no key's text");
        }
        return new BsonString(text);
    }

    /** {@code v} with each key marker replaced, in place and in document order, by the key of the slot at its place
     *  among the markers, as JDBC binds a key's {@code ?}; {@code next} the place of the next marker. */
    private static BsonValue keyed(BsonValue v, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            int[] next) {
        if (v.isDocument()) {
            BsonDocument d = v.asDocument();
            BsonValue marker = d.get(KEY_MARKER);
            if (marker != null && d.size() == 1) {
                int n = next[0]++;
                if (n >= slots.size()) {
                    throw new IllegalStateException("the plan's statement holds more key markers than its regions'"
                            + " " + slots.size() + " keys");
                }
                return literal(keyText(values.get(n), n + 1));
            }
            BsonDocument out = new BsonDocument();
            for (Map.Entry<String, BsonValue> e : d.entrySet()) {
                out.append(e.getKey(), keyed(e.getValue(), slots, values, next));
            }
            return out;
        }
        if (v.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : v.asArray()) {
                out.add(keyed(e, slots, values, next));
            }
            return out;
        }
        return v;
    }

    /** {@code v} as the expression that answers it: MongoDB evaluates a {@code let}'s values and a marker's place as
     *  expressions, so a bound value is a {@code $literal} -- a text {@code "$name"} is that text, never a field path,
     *  a variable or an operator's document (measured 2026-11-12 on 8.0: bare, a {@code let}'s {@code "$name"} is
     *  refused, Location4890500, a command's let reading no field; until then a marker was replaced by the bare
     *  value in an expression's place, which reads a {@code $}-led text as a path). */
    private static BsonDocument literal(BsonValue v) {
        return new BsonDocument("$literal", v);
    }

    /** A slot's value as BSON, as the slot says: one value (an absent one a null), or a list as one array (an absent
     *  one an empty array). */
    private static BsonValue value(ExecutionPlan.Slot slot, PlanParameters.Checked checked, int index) {
        return switch (slot.binding()) {
            case ExecutionPlan.Binding.Array array -> {
                List<Object> items = switch (checked) {
                    case PlanParameters.Many many -> many.values();
                    case PlanParameters.None none -> List.of();
                    case PlanParameters.One one -> List.of(one.value());
                };
                BsonArray out = new BsonArray();
                for (Object item : items) {
                    out.add(bson(item, slot));
                }
                yield out;
            }
            case ExecutionPlan.Binding.One one -> switch (checked) {
                case PlanParameters.One v -> bson(v.value(), slot);
                case PlanParameters.None none -> BsonNull.VALUE;
                case PlanParameters.Many many -> throw new IllegalStateException("slot " + index + " binds one value"
                        + " of '" + slot.parameter() + "', given a list");
            };
            case ExecutionPlan.Binding.Key key -> throw new IllegalStateException("slot " + index + " binds a key's"
                    + " cell: the runner binds each copy's key before the statement reaches the session");
            case ExecutionPlan.Binding.Source src -> throw new com.legend.error.NotImplementedException("a MongoDB"
                    + " statement reads no file source ('" + src.location() + "'): its databases are the connector's own");
            case ExecutionPlan.Binding.Payload payload -> throw new IllegalStateException("slot " + index + " binds a"
                    + " data: payload: MongoDB's delivery is the pipeline's own $documents, never a bound payload");
        };
    }

    /** A checked value's BSON: the driver's own encoding of each kind; a date-time finer than a millisecond is
     *  refused, never cut (BSON's date holds milliseconds). */
    private static BsonValue bson(Object v, ExecutionPlan.Slot slot) {
        return switch (v) {
            case Long l -> new BsonInt64(l);
            case Integer i -> new BsonInt64(i);
            case Double d -> new BsonDouble(d);
            case BigDecimal d -> new BsonDecimal128(new Decimal128(d));
            case String s -> new BsonString(s);
            case Boolean b -> new BsonBoolean(b);
            case LocalDate d -> new BsonDateTime(d.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
            case LocalDateTime t -> {
                if (t.getNano() % 1_000_000 != 0) {
                    throw new com.legend.error.NotImplementedException("slot of '" + slot.parameter() + "' is given a"
                            + " date-time finer than a millisecond (" + t + "), which a BSON date does not hold");
                }
                yield new BsonDateTime(t.toInstant(ZoneOffset.UTC).toEpochMilli());
            }
            default -> throw new IllegalStateException("slot of '" + slot.parameter() + "' is given a "
                    + v.getClass().getSimpleName() + ", which no BSON value encodes");
        };
    }
}
