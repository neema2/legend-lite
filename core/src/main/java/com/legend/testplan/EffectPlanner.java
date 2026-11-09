// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testplan;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.StoreCompiler;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.spec.typed.ExecutionContext;
import com.legend.compiler.spec.typed.Lets;
import com.legend.compiler.spec.typed.TypedCInteger;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedCollection;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedNewInstance;
import com.legend.compiler.spec.typed.TypedPackageableRef;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.database.Databases;
import com.legend.error.NotImplementedException;
import com.legend.model.DatabaseDefinition;
import com.legend.model.RelationalDataType;
import com.legend.planner.PlanEnv;
import com.legend.setup.Ddl;
import com.legend.sql.NameQuoting;
import com.legend.sql.RawSql;
import com.legend.sql.SqlDdl;
import com.legend.sql.SqlDml;
import com.legend.sql.dialect.SqlDialect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * An EFFECT's statements, planned (the planner-driver seam program's S2d, 2026-10-31; the body driver's effect arms
 * wrote and sent them until then): the text each of {@code executeInDb}, {@code dropAndCreateSchemaInDb},
 * {@code dropAndCreateTableInDb}, {@code createTempTable} and {@code dropTempTable} sends, in the session's dialect,
 * with the text the replay referee's ledger records. Two phases, so no value is computed here: {@link #stringArgs}
 * names the String arguments a call needs, the driver gives their values (a literal folded, a computed one evaluated
 * by the database: the one place a value feeds a statement's text, as the engine evaluates the string before it runs
 * it), and {@link #statements} writes the statements from them. The driver sends them (an effect segment's script);
 * on the test door's program road ({@link TestKinds}) they are an {@code Effect} step's script. In the test-planning
 * library since the test-is-the-product plan's M3d (R17 C7, 2026-10-31): a test's effects ship in no planner of the
 * plan side.
 */
public final class EffectPlanner {

    private EffectPlanner() {
    }

    /** One statement of an effect: the text sent ({@code executed}, the session's dialect), the text the replay
     *  ledger records ({@code recorded}: the referee's spelling of DDL, the authored text of raw SQL), and whether it
     *  is DDL the platform wrote (the ledger's kind). */
    public record Statement(String executed, String recorded, boolean ddl) {
    }

    /** The replay ledger records DDL in its referee database's spelling on EVERY session. */
    private static SqlDialect refereeDdl() {
        // one per use (2026-11-09): a dialect keeps state while it writes a statement, so none is shared statically
        return Databases.dialect(Databases.REPLAY_REFEREE);
    }

    /** The String arguments {@code call} needs, in the order {@link #statements} takes their values; a call this
     *  planner does not write is refused by name. */
    public static List<TypedSpec> stringArgs(TypedNativeCall call) {
        return switch (effectOf(call)) {
            case EXECUTE_IN_DB, DROP_AND_CREATE_SCHEMA_IN_DB, CREATE_TEMP_TABLE, DROP_TEMP_TABLE ->
                    List.of(call.args().get(0));
            // the CSV's resource path: its value, for statements, the LOCAL FILE the caller resolved it to
            case LOAD_CSV_TO_DB_TABLE -> List.of(call.args().get(0));
            case DROP_AND_CREATE_TABLE_IN_DB -> {
                // the store is read before any argument is evaluated: a call that names none is refused first
                storeOf(call);
                yield call.args().size() == 4 ? List.of(call.args().get(1), call.args().get(2))
                        : List.of(call.args().get(1));
            }
            default -> throw new IllegalStateException("not an effect whose statements the planner writes: "
                    + call.callee().qualifiedName());
        };
    }

    /** {@code call}'s catalog EFFECT row (an effect arm is reached only for one). */
    private static NativeFn.Effect effectOf(TypedNativeCall call) {
        return NativeFn.Effect.of(call.callee().id()).orElseThrow(() ->
                new IllegalStateException("not a catalog effect: " + call.callee().qualifiedName()));
    }

    /** {@code call}'s statements, its String arguments' {@code values} in {@link #stringArgs}' order, over the
     *  statements before it ({@code body}: a raw statement's connection argument, bound by a let, names the language
     *  it is written in). */
    public static List<Statement> statements(TypedNativeCall call, List<String> values,
            List<TypedSpec> body, PlanEnv env) {
        return switch (effectOf(call)) {
            case EXECUTE_IN_DB -> {
                // the database its connection declares (decision D2, lets bound): every corpus call's reads, measured
                // 2026-10-13 (122,823 of 122,823, as H2); one that cannot be read is refused
                String writtenFor = ExecutionContext.rawLanguage(
                        Lets.bound(call.args().get(1), body));
                // split FIRST: translation is per-statement (its recognizers anchor at statement start). The engine's H2
                // text crosses THE boundary translator through the session's dialect, any other runs as written
                // (decision D2); the ledger records each as authored
                List<Statement> out = new ArrayList<>();
                for (String stmt : RawSql.splitStatements(values.get(0))) {
                    out.add(new Statement(env.writtenIn().raw(stmt, writtenFor), stmt, false));
                }
                yield out;
            }
            case DROP_AND_CREATE_SCHEMA_IN_DB -> {
                // create-if-missing: the DDL seeds already own tables in the schema, and the setup's own
                // dropAndCreateTableInDb calls recreate what it manages (BodyDriver's dropAndCreateSchemaInDb says why
                // both channels record it)
                yield List.of(ddl(new SqlDdl.CreateSchema(values.get(0), false), env));
            }
            case DROP_AND_CREATE_TABLE_IN_DB -> {
                TypedPackageableRef db = storeOf(call);
                boolean hasSchema = call.args().size() == 4;
                String schema = hasSchema ? values.get(0) : "default";
                String table = values.get(hasSchema ? 1 : 0);
                String lookup = "default".equals(schema) ? table : schema + "." + table;
                DatabaseDefinition.TableDefinition def =
                        env.ctx().findTableDefinition(db.fullPath(), lookup)
                                .orElseThrow(() -> new IllegalStateException(
                                        "dropAndCreateTableInDb: no table '" + lookup
                                                + "' in store " + db.fullPath()));
                yield List.of(ddl(Ddl.dropDeclaredTable(def, schema), env),
                        ddl(Ddl.createTable(def, schema), env));
            }
            case CREATE_TEMP_TABLE -> {
                List<SqlDdl.Column> cols = new ArrayList<>();
                TypedSpec colsArg = call.args().get(1);
                List<TypedSpec> elements = colsArg instanceof TypedCollection c
                        ? c.elements() : List.of(colsArg);
                for (TypedSpec e : elements) {
                    if (!(e instanceof TypedNewInstance col)
                            || !(col.properties().get("name") instanceof TypedCString name)
                            || !(col.properties().get("type") instanceof TypedNewInstance type)) {
                        throw new IllegalStateException("createTempTable: each column must be a literal"
                                + " ^Column(name='…', type=^<datatype>()), got " + e.getClass().getSimpleName());
                    }
                    cols.add(new SqlDdl.Column(name.value(), false,
                            StoreCompiler.declaredType(datatypeLiteral(type)), false, false));
                }
                yield List.of(ddl(new SqlDdl.CreateTable(null, values.get(0),
                        NameQuoting.BARE, cols, true), env));
            }
            case DROP_TEMP_TABLE -> List.of(ddl(Ddl.dropTable(null, values.get(0)), env));
            case LOAD_CSV_TO_DB_TABLE -> List.of(loadCsv(call, values.get(0), body, env));
            default -> throw new IllegalStateException("not an effect whose statements the planner writes: "
                    + call.callee().qualifiedName());
        };
    }

    /** {@code loadCsvToDbTable(path, table, connection)} (the user's ruling, 2026-11-06): ONE statement the database
     *  runs, reading the CSV file itself ({@link SqlDml.LoadCsv}) -- {@code file} the local file the resource names
     *  (resolved by the caller at planning), never its rows; the table the store navigation the call names, its
     *  columns in declared order, the file's cells loaded positionally after its header (legend-engine's native drops
     *  the header and inserts each row into the table's columns). The referee's ledger records its own reader over the
     *  same file. */
    private static Statement loadCsv(TypedNativeCall call, String file, List<TypedSpec> body, PlanEnv env) {
        var ref = com.legend.compiler.spec.typed.StoreElementIdentity.tableRef(call.args().get(1),
                x -> peelToOne(x, body));
        if (ref == null) {
            throw new NotImplementedException("loadCsvToDbTable: the table argument is not a"
                    + " db->schema(...)->table(...) navigation");
        }
        String schema = "default".equals(ref.schema()) ? null : ref.schema();
        String qualified = schema == null ? ref.table() : schema + "." + ref.table();
        DatabaseDefinition.TableDefinition def = env.ctx().findTableDefinition(ref.dbFqn(), qualified)
                .orElseThrow(() -> new IllegalStateException("loadCsvToDbTable: no table '" + qualified
                        + "' in store " + ref.dbFqn()));
        List<String> columns = env.ctx().findTable(ref.dbFqn(), ref.table()).orElseThrow(() ->
                new IllegalStateException("loadCsvToDbTable: no table '" + ref.table() + "' in store "
                        + ref.dbFqn())).columns().stream().map(c -> c.name()).toList();
        SqlDml.LoadCsv load = new SqlDml.LoadCsv(schema, ref.table(), Ddl.quoting(def, schema), columns,
                com.legend.setup.CsvSeed.quotedColumns(def), file);
        return new Statement(env.writtenIn().render(load), refereeDdl().render(load), false);
    }

    /** {@code n} with its lets chased and its {@code toOne()}s peeled (a store navigation's links). */
    private static TypedSpec peelToOne(TypedSpec n, List<TypedSpec> body) {
        TypedSpec cur = Lets.bound(n, body);
        while (cur instanceof TypedNativeCall nc && nc.args().size() == 1
                && com.legend.builtin.Pure.isToOneCall(nc.callee().qualifiedName())) {
            cur = Lets.bound(nc.args().get(0), body);
        }
        return cur;
    }

    /** A DDL statement: the session's dialect's text sent, the referee's recorded. */
    private static Statement ddl(SqlDdl ddl, PlanEnv env) {
        return new Statement(env.writtenIn().render(ddl), refereeDdl().render(ddl), true);
    }

    /** {@code dropAndCreateTableInDb}'s store: its first argument, a store reference, or refused by name. */
    private static TypedPackageableRef storeOf(TypedNativeCall call) {
        if (!(call.args().get(0)
                instanceof TypedPackageableRef db)) {
            throw new IllegalStateException("dropAndCreateTableInDb: the database"
                    + " argument must be a store reference, got "
                    + call.args().get(0).getClass().getSimpleName());
        }
        return db;
    }

    /** A {@code ^meta::relational::metamodel::datatype::<T>(…)} literal as the store
     * model's data type, by the class's EXACT FQN ({@link com.legend.compiler.element.type.PlatformTypes});
     * the sized / scaled kinds read their literal arguments. */
    private static RelationalDataType datatypeLiteral(
            TypedNewInstance type) {
        Function<TypedNewInstance,
                RelationalDataType> reader = DATATYPE_LITERALS.get(type.classFqn());
        if (reader == null) {
            throw new NotImplementedException(
                    "createTempTable: not a store data type: " + type.classFqn());
        }
        return reader.apply(type);
    }

    /** The data-type classes by exact FQN. */
    private static final Map<String, Function<
            TypedNewInstance, RelationalDataType>> DATATYPE_LITERALS =
            Map.ofEntries(
                    Map.entry(PlatformTypes.DATATYPE_BIGINT, t -> new RelationalDataType.BigInt()),
                    Map.entry(PlatformTypes.DATATYPE_SMALLINT, t -> new RelationalDataType.SmallInt()),
                    Map.entry(PlatformTypes.DATATYPE_TINYINT, t -> new RelationalDataType.TinyInt()),
                    Map.entry(PlatformTypes.DATATYPE_INTEGER, t -> new RelationalDataType.Integer_()),
                    Map.entry(PlatformTypes.DATATYPE_FLOAT, t -> new RelationalDataType.Float_()),
                    Map.entry(PlatformTypes.DATATYPE_DOUBLE, t -> new RelationalDataType.Double_()),
                    Map.entry(PlatformTypes.DATATYPE_REAL, t -> new RelationalDataType.Real()),
                    Map.entry(PlatformTypes.DATATYPE_BIT, t -> new RelationalDataType.Bit()),
                    Map.entry(PlatformTypes.DATATYPE_TIMESTAMP, t -> new RelationalDataType.Timestamp()),
                    Map.entry(PlatformTypes.DATATYPE_DATE, t -> new RelationalDataType.Date_()),
                    Map.entry(PlatformTypes.DATATYPE_TIME, t -> new RelationalDataType.Time()),
                    Map.entry(PlatformTypes.DATATYPE_TIMESTAMP_NS, t -> new RelationalDataType.TimestampNs()),
                    Map.entry(PlatformTypes.DATATYPE_DISTINCT, t -> new RelationalDataType.Distinct()),
                    Map.entry(PlatformTypes.DATATYPE_OTHER, t -> new RelationalDataType.Other()),
                    Map.entry(PlatformTypes.DATATYPE_SEMI_STRUCTURED, t -> new RelationalDataType.SemiStructured()),
                    Map.entry(PlatformTypes.DATATYPE_VARCHAR, t -> new RelationalDataType.Varchar(literalInt(t, "size"))),
                    Map.entry(PlatformTypes.DATATYPE_CHAR, t -> new RelationalDataType.Char_(literalInt(t, "size"))),
                    Map.entry(PlatformTypes.DATATYPE_BINARY, t -> new RelationalDataType.Binary(literalInt(t, "size"))),
                    Map.entry(PlatformTypes.DATATYPE_VARBINARY, t -> new RelationalDataType.Varbinary(literalInt(t, "size"))),
                    Map.entry(PlatformTypes.DATATYPE_DECIMAL, t -> new RelationalDataType.Decimal(literalInt(t, "precision"), literalInt(t, "scale"))),
                    Map.entry(PlatformTypes.DATATYPE_NUMERIC, t -> new RelationalDataType.Numeric(literalInt(t, "precision"), literalInt(t, "scale"))));

    private static int literalInt(TypedNewInstance type, String property) {
        if (!(type.properties().get(property) instanceof TypedCInteger n)) {
            throw new IllegalStateException("createTempTable: " + type.classFqn() + " needs a literal " + property);
        }
        return n.value().intValue();
    }
}
