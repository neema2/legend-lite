// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.executionplan.ExecutionPlan;
import com.legend.model.ConnectionDefinition;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
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
 * command in Extended JSON, as the MongoDb dialect writes it; a plan parameter is the marker document
 * {@code {"$legendParam": "?"}} at its place, replaced in place by the bound value of the slot at its place among the
 * markers, in document order, as SQL's {@code ?} is (decision D5, the driver's own encoding of each value; a dynamic
 * pivot's per-key region is copied as text, 2026-10-20); an {@code aggregate} command runs through the driver's cursor, and each row's one
 * field is the text the pipeline wrote (D10): carried, never read as a value. A raise the pipeline wrote (a
 * conversion of a message between the platform's sentinels) comes back as the platform's own text, as a SQL raise
 * does through the executor's funnel. Since the plan path program's step 7 (2026-10-18) the session answers typed
 * rows too ({@link #rows}: each document's fields read by the plan's columns, {@link MongoDbDecode}).
 */
final class MongoDbStatements implements Statements {

    private static final String PARAMETER_MARKER = "$legendParam";

    private final MongoDatabase database;

    MongoDbStatements(MongoDatabase database) {
        this.database = database;
    }

    @Override
    public void check(ExecutionPlan.Target target) {
        if (!(target.database() instanceof ExecutionPlan.Database.Declared d)
                || d.connection().databaseType() != ConnectionDefinition.DatabaseType.MongoDB) {
            throw new com.legend.error.NotImplementedException("the query executes on " + target.database().type()
                    + " but the session is MongoDB — dialect/connection mismatch");
        }
        switch (target.servers()) {
            case ExecutionPlan.Servers.Every every -> {
                // the spelling depends on no version
            }
            case ExecutionPlan.Servers.Versions versions -> {
                String version = run(new BsonDocument("buildInfo", new BsonInt32(1))).get("version", BsonNull.VALUE)
                        .asString().getValue();
                if (versions.prefixes().stream().noneMatch(version::startsWith)) {
                    throw new com.legend.error.NotImplementedException("the plan's statements are written for MongoDB "
                            + versions.prefixes() + ", and the session is MongoDB " + version + ": not run");
                }
            }
        }
    }

    @Override
    public boolean execute(String statement) {
        run(command(statement));
        return false;
    }

    /** {@inheritDoc} MongoDB: a keyed plan's two commands share no transaction (the dialect says {@code NONE}: a
     *  server that is no replica set has none across commands), so a snapshot is asked of it only as {@code NONE}. */
    @Override
    public void inSnapshot(ExecutionPlan.Keyed.Snapshot snapshot, Work work) throws IOException {
        if (snapshot != ExecutionPlan.Keyed.Snapshot.NONE) {
            throw new IllegalStateException("a " + snapshot + " snapshot asked of a MongoDB session: its keyed plans"
                    + " take none");
        }
        work.run();
    }

    /** A write plan's step: not designed on MongoDB (its writes are the bulk load alone, docs/MONGODB_PLAN_2026_10_16.md);
     *  refused by name. */
    @Override
    public long update(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values) {
        throw new com.legend.error.NotImplementedException("a write plan's step on MongoDB: a program that changes a"
                + " store is not designed for MongoDB (its writes are the bulk load alone)");
    }

    @Override
    public <T> T inTransaction(java.util.function.Supplier<T> work) {
        throw new com.legend.error.NotImplementedException("a transaction over a plan's steps on MongoDB: a program that"
                + " changes a store is not designed for MongoDB");
    }

    @Override
    public boolean ownsTransaction() {
        throw new com.legend.error.NotImplementedException("a transaction over a plan's steps on MongoDB: a program that"
                + " changes a store is not designed for MongoDB");
    }

    /** A MongoDB session describes no statement before running it: the body driver's schema probes (a raw grid's
     *  roster, a deferred relation's columns, a wire-decided kind) are not designed for MongoDB; refused by name. */
    @Override
    public List<Reported> columns(String statement) {
        throw new com.legend.error.NotImplementedException("a statement's reported columns asked of a MongoDB session:"
                + " the body driver's schema probes are not designed for MongoDB");
    }

    @Override
    public List<Reported> describe(String statement) {
        throw new com.legend.error.NotImplementedException("a statement described before running asked of a MongoDB"
                + " session: the body driver's schema probes are not designed for MongoDB");
    }

    /** A MongoDB database is its connector's own and a plan sets none up ({@code PlanSessions.refuseSetup}): a bulk
     *  load of a plan's rows is refused by name. */
    @Override
    public boolean load(ExecutionPlan.SetupStep.Rows rows) {
        throw new com.legend.error.NotImplementedException("a bulk load of a plan's rows asked of a MongoDB session: its"
                + " databases are the connector's own, and a plan sets none up");
    }

    @Override
    public void query(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            Rows rows) throws IOException {
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
    public void rows(String statement, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            List<ExecutionPlan.Column> columns, Cells cells) throws IOException {
        try (MongoCursor<BsonDocument> cursor = cursor(statement, slots, values)) {
            while (cursor.hasNext()) {
                cells.row(MongoDbDecode.row(cursor.next(), columns));
            }
        } catch (MongoException e) {
            throw failure(e);
        }
    }

    /** The aggregate command's cursor: the plan's command with its markers bound, run through the driver. */
    private MongoCursor<BsonDocument> cursor(String statement, List<ExecutionPlan.Slot> slots,
            List<PlanParameters.Checked> values) {
        int[] next = {0};
        BsonDocument command = bound(command(statement), slots, values, next).asDocument();
        if (next[0] != slots.size()) {
            throw new IllegalStateException("the plan's statement holds " + next[0] + " parameter markers, and the plan"
                    + " has " + slots.size() + " slots");
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
        return (aggregate.isString()
                ? database.getCollection(aggregate.asString().getValue(), BsonDocument.class).aggregate(stages)
                : database.aggregate(stages, BsonDocument.class)).iterator();
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

    private static BsonDocument command(String statement) {
        try {
            return BsonDocument.parse(statement);
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

    /** {@code v} with each parameter marker replaced, in place and in document order, by the bound value of the slot
     *  at its place among the markers; {@code next} the place of the next marker. */
    private static BsonValue bound(BsonValue v, List<ExecutionPlan.Slot> slots, List<PlanParameters.Checked> values,
            int[] next) {
        if (v.isDocument()) {
            BsonDocument d = v.asDocument();
            BsonValue marker = d.get(PARAMETER_MARKER);
            if (marker != null && d.size() == 1) {
                int n = next[0]++;
                if (n >= slots.size()) {
                    throw new IllegalStateException("the plan's statement holds more parameter markers than the plan's "
                            + slots.size() + " slots");
                }
                return value(slots.get(n), values.get(n), n + 1);
            }
            BsonDocument out = new BsonDocument();
            for (Map.Entry<String, BsonValue> e : d.entrySet()) {
                out.append(e.getKey(), bound(e.getValue(), slots, values, next));
            }
            return out;
        }
        if (v.isArray()) {
            BsonArray out = new BsonArray();
            for (BsonValue e : v.asArray()) {
                out.add(bound(e, slots, values, next));
            }
            return out;
        }
        return v;
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
