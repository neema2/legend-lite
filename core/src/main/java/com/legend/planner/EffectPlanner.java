// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;

/**
 * An EFFECT's statements, planned (the planner-driver seam program's S2d, 2026-10-31; the body driver's effect arms
 * wrote and sent them until then): the text each of {@code executeInDb}, {@code dropAndCreateSchemaInDb},
 * {@code dropAndCreateTableInDb}, {@code createTempTable} and {@code dropTempTable} sends, in the session's dialect,
 * with the text the replay referee's ledger records. Two phases, so no value is computed here: {@link #stringArgs}
 * names the String arguments a call needs, the driver gives their values (a literal folded, a computed one evaluated
 * by the database: the one place a value feeds a statement's text, as the engine evaluates the string before it runs
 * it), and {@link #statements} writes the statements from them. The driver sends them (an effect segment's script).
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
    private static final com.legend.sql.dialect.SqlDialect REFEREE_DDL =
            com.legend.database.Databases.dialect(com.legend.database.Databases.REPLAY_REFEREE);

    /** The String arguments {@code call} needs, in the order {@link #statements} takes their values; a call this
     *  planner does not write is refused by name. */
    public static java.util.List<TypedSpec> stringArgs(TypedNativeCall call) {
        return switch (effectOf(call)) {
            case EXECUTE_IN_DB, DROP_AND_CREATE_SCHEMA_IN_DB, CREATE_TEMP_TABLE, DROP_TEMP_TABLE ->
                    java.util.List.of(call.args().get(0));
            case DROP_AND_CREATE_TABLE_IN_DB -> {
                // the store is read before any argument is evaluated: a call that names none is refused first
                storeOf(call);
                yield call.args().size() == 4 ? java.util.List.of(call.args().get(1), call.args().get(2))
                        : java.util.List.of(call.args().get(1));
            }
            default -> throw new IllegalStateException("not an effect whose statements the planner writes: "
                    + call.callee().qualifiedName());
        };
    }

    /** {@code call}'s catalog EFFECT row (an effect arm is reached only for one). */
    private static com.legend.builtin.NativeFn.Effect effectOf(TypedNativeCall call) {
        return com.legend.builtin.NativeFn.Effect.of(call.callee().id()).orElseThrow(() ->
                new IllegalStateException("not a catalog effect: " + call.callee().qualifiedName()));
    }

    /** {@code call}'s statements, its String arguments' {@code values} in {@link #stringArgs}' order, over the
     *  statements before it ({@code body}: a raw statement's connection argument, bound by a let, names the language
     *  it is written in). */
    public static java.util.List<Statement> statements(TypedNativeCall call, java.util.List<String> values,
            java.util.List<TypedSpec> body, PlanEnv env) {
        return switch (effectOf(call)) {
            case EXECUTE_IN_DB -> {
                // the database its connection declares (decision D2, lets bound): every corpus call's reads, measured
                // 2026-10-13 (122,823 of 122,823, as H2); one that cannot be read is refused
                String writtenFor = com.legend.compiler.spec.typed.ExecutionContext.rawLanguage(
                        com.legend.compiler.spec.typed.Lets.bound(call.args().get(1), body));
                // split FIRST: translation is per-statement (its recognizers anchor at statement start). The engine's H2
                // text crosses THE boundary translator through the session's dialect, any other runs as written
                // (decision D2); the ledger records each as authored
                java.util.List<Statement> out = new java.util.ArrayList<>();
                for (String stmt : com.legend.sql.RawSql.splitStatements(values.get(0))) {
                    out.add(new Statement(env.writtenIn().raw(stmt, writtenFor), stmt, false));
                }
                yield out;
            }
            case DROP_AND_CREATE_SCHEMA_IN_DB -> {
                // create-if-missing: the DDL seeds already own tables in the schema, and the setup's own
                // dropAndCreateTableInDb calls recreate what it manages (BodyDriver's dropAndCreateSchemaInDb says why
                // both channels record it)
                yield java.util.List.of(ddl(new com.legend.sql.SqlDdl.CreateSchema(values.get(0), false), env));
            }
            case DROP_AND_CREATE_TABLE_IN_DB -> {
                com.legend.compiler.spec.typed.TypedPackageableRef db = storeOf(call);
                boolean hasSchema = call.args().size() == 4;
                String schema = hasSchema ? values.get(0) : "default";
                String table = values.get(hasSchema ? 1 : 0);
                String lookup = "default".equals(schema) ? table : schema + "." + table;
                com.legend.model.DatabaseDefinition.TableDefinition def =
                        env.ctx().findTableDefinition(db.fullPath(), lookup)
                                .orElseThrow(() -> new IllegalStateException(
                                        "dropAndCreateTableInDb: no table '" + lookup
                                                + "' in store " + db.fullPath()));
                yield java.util.List.of(ddl(com.legend.setup.Ddl.dropDeclaredTable(def, schema), env),
                        ddl(com.legend.setup.Ddl.createTable(def, schema), env));
            }
            case CREATE_TEMP_TABLE -> {
                java.util.List<com.legend.sql.SqlDdl.Column> cols = new java.util.ArrayList<>();
                TypedSpec colsArg = call.args().get(1);
                java.util.List<TypedSpec> elements = colsArg instanceof com.legend.compiler.spec.typed.TypedCollection c
                        ? c.elements() : java.util.List.of(colsArg);
                for (TypedSpec e : elements) {
                    if (!(e instanceof com.legend.compiler.spec.typed.TypedNewInstance col)
                            || !(col.properties().get("name") instanceof com.legend.compiler.spec.typed.TypedCString name)
                            || !(col.properties().get("type") instanceof com.legend.compiler.spec.typed.TypedNewInstance type)) {
                        throw new IllegalStateException("createTempTable: each column must be a literal"
                                + " ^Column(name='…', type=^<datatype>()), got " + e.getClass().getSimpleName());
                    }
                    cols.add(new com.legend.sql.SqlDdl.Column(name.value(), false,
                            com.legend.compiler.element.StoreCompiler.declaredType(datatypeLiteral(type)), false, false));
                }
                yield java.util.List.of(ddl(new com.legend.sql.SqlDdl.CreateTable(null, values.get(0),
                        com.legend.sql.NameQuoting.BARE, cols, true), env));
            }
            case DROP_TEMP_TABLE -> java.util.List.of(ddl(com.legend.setup.Ddl.dropTable(null, values.get(0)), env));
            default -> throw new IllegalStateException("not an effect whose statements the planner writes: "
                    + call.callee().qualifiedName());
        };
    }

    /** A DDL statement: the session's dialect's text sent, the referee's recorded. */
    private static Statement ddl(com.legend.sql.SqlDdl ddl, PlanEnv env) {
        return new Statement(env.writtenIn().render(ddl), REFEREE_DDL.render(ddl), true);
    }

    /** {@code dropAndCreateTableInDb}'s store: its first argument, a store reference, or refused by name. */
    private static com.legend.compiler.spec.typed.TypedPackageableRef storeOf(TypedNativeCall call) {
        if (!(call.args().get(0)
                instanceof com.legend.compiler.spec.typed.TypedPackageableRef db)) {
            throw new IllegalStateException("dropAndCreateTableInDb: the database"
                    + " argument must be a store reference, got "
                    + call.args().get(0).getClass().getSimpleName());
        }
        return db;
    }

    /** A {@code ^meta::relational::metamodel::datatype::<T>(…)} literal as the store
     * model's data type, by the class's EXACT FQN ({@link com.legend.compiler.element.type.PlatformTypes});
     * the sized / scaled kinds read their literal arguments. */
    private static com.legend.model.RelationalDataType datatypeLiteral(
            com.legend.compiler.spec.typed.TypedNewInstance type) {
        java.util.function.Function<com.legend.compiler.spec.typed.TypedNewInstance,
                com.legend.model.RelationalDataType> reader = DATATYPE_LITERALS.get(type.classFqn());
        if (reader == null) {
            throw new com.legend.error.NotImplementedException(
                    "createTempTable: not a store data type: " + type.classFqn());
        }
        return reader.apply(type);
    }

    /** The data-type classes by exact FQN. */
    private static final java.util.Map<String, java.util.function.Function<
            com.legend.compiler.spec.typed.TypedNewInstance, com.legend.model.RelationalDataType>> DATATYPE_LITERALS =
            java.util.Map.ofEntries(
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BIGINT, t -> new com.legend.model.RelationalDataType.BigInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_SMALLINT, t -> new com.legend.model.RelationalDataType.SmallInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TINYINT, t -> new com.legend.model.RelationalDataType.TinyInt()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_INTEGER, t -> new com.legend.model.RelationalDataType.Integer_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_FLOAT, t -> new com.legend.model.RelationalDataType.Float_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DOUBLE, t -> new com.legend.model.RelationalDataType.Double_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_REAL, t -> new com.legend.model.RelationalDataType.Real()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BIT, t -> new com.legend.model.RelationalDataType.Bit()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TIMESTAMP, t -> new com.legend.model.RelationalDataType.Timestamp()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DATE, t -> new com.legend.model.RelationalDataType.Date_()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TIME, t -> new com.legend.model.RelationalDataType.Time()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_TIMESTAMP_NS, t -> new com.legend.model.RelationalDataType.TimestampNs()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DISTINCT, t -> new com.legend.model.RelationalDataType.Distinct()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_OTHER, t -> new com.legend.model.RelationalDataType.Other()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_SEMI_STRUCTURED, t -> new com.legend.model.RelationalDataType.SemiStructured()),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_VARCHAR, t -> new com.legend.model.RelationalDataType.Varchar(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_CHAR, t -> new com.legend.model.RelationalDataType.Char_(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_BINARY, t -> new com.legend.model.RelationalDataType.Binary(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_VARBINARY, t -> new com.legend.model.RelationalDataType.Varbinary(literalInt(t, "size"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_DECIMAL, t -> new com.legend.model.RelationalDataType.Decimal(literalInt(t, "precision"), literalInt(t, "scale"))),
                    java.util.Map.entry(com.legend.compiler.element.type.PlatformTypes.DATATYPE_NUMERIC, t -> new com.legend.model.RelationalDataType.Numeric(literalInt(t, "precision"), literalInt(t, "scale"))));

    private static int literalInt(com.legend.compiler.spec.typed.TypedNewInstance type, String property) {
        if (!(type.properties().get(property) instanceof com.legend.compiler.spec.typed.TypedCInteger n)) {
            throw new IllegalStateException("createTempTable: " + type.classFqn() + " needs a literal " + property);
        }
        return n.value().intValue();
    }
}
