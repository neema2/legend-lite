// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.compiler.NameResolver;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.protocol.SourceInfo;
import com.legend.protocol.spec.SqlIsland;
import com.legend.protocol.spec.ValueSpecification;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.InputLanguages;
import com.legend.protocol.sql.SqlToPure;
import com.legend.protocol.sql.SqlTranslationException;
import com.legend.protocol.sql.iq.IqDialect;
import com.legend.protocol.sql.iq.IqProfile;

import java.util.function.Function;

/**
 * THE ONE PLACE a SQL island becomes a Relation expression (docs/LEGEND_SQL_STUDY_2026_10_11.md §3,
 * docs/IQ_SQL_STUDY_2026_10_13.md §5): {@code #SQL{...}#} read as Postgres
 * ({@link com.legend.protocol.sql.PostgresDialect}), {@code #IQ{...}#} as SAP IQ under its profile
 * ({@link IqDialect}). The protocol layer's translator ({@link SqlToPure}) reads the SQL through the island's dialect
 * and builds the Pure a person would write; this seam answers its one question, what a FROM source's columns are
 * (each column's declared store type too, when the source is a table), by typing the source;
 * it resolves the result's names as the resolver resolves any query, and the Typer then types it once, like any query.
 * A refusal becomes the compile error its kind says, placed in the enclosing source.
 */
final class SqlIslands {

    private SqlIslands() {
    }

    /** The relation expression {@code island} means, names resolved; {@code typeSource} types a resolved source.
     *  IQ island, 2026-10-14 (M1e): {@code model}'s tables are the ones a dialect may name without a source function
     *  (an IQ owner's), and {@code env}'s variables the ones it may read (IQ's host variables); Postgres's reads
     *  neither. */
    static ValueSpecification desugar(SqlIsland island, Function<ValueSpecification, TypedSpec> typeSource,
            ModelContext model, Env env) {
        if (island.language().equals(SqlIsland.CYPHER)) {
            // the Cypher island (docs/CYPHER_STUDY_2026_10_15.md §5.3): its own seam, over the class model
            return CypherIslands.desugar(island, model);
        }
        java.util.Set<String> read = new java.util.TreeSet<>();
        SqlToPure.Sources sources = sources(island, typeSource, model, env, read);
        try {
            ValueSpecification pure = SqlToPure.translate(island.sql(), sources, dialect(island));
            String hidden = read.isEmpty() ? null : hidden(pure, read);
            if (hidden != null) {
                throw new com.legend.error.NotImplementedException(island.opening() + "...}# at "
                        + position(island, 1, 1) + ": the variable '" + hidden + "' is named like a lambda parameter"
                        + " of the island's translation, which would hide it there: rename it");
            }
            return NameResolver.resolveQuery(pure);
        } catch (SqlTranslationException e) {
            throw refusal(island, e);
        }
    }

    /** Whether {@code island} is a {@code #SQL{...}#} statement that writes a table in more than one statement (one
     *  with RETURNING, the writes design's leg 4, 2026-10-20; an ON CONFLICT DO UPDATE, PR #67's second review,
     *  2026-10-24): a program of statements ({@link #statements}), as a Cypher statement that updates the graph is. */
    static boolean isProgram(SqlIsland island) {
        return island.language().equals(SqlIsland.SQL) && island.profile() == null
                && SqlToPure.isProgram(island.sql(), dialect(island));
    }

    /** The statements a {@code #SQL{...}#} statement that returns the rows it writes means (the staged rows' let, the
     *  write, the rows last), names resolved; {@code typeSource} types a resolved source in no scope (a query's own
     *  statement reads no variable of it). */
    static java.util.List<ValueSpecification> statements(SqlIsland island,
            Function<ValueSpecification, TypedSpec> typeSource, ModelContext model) {
        SqlToPure.Sources sources = sources(island, typeSource, model, Env.empty(), new java.util.TreeSet<>());
        try {
            return SqlToPure.program(island.sql(), sources, dialect(island)).stream().map(NameResolver::resolveQuery)
                    .toList();
        } catch (SqlTranslationException e) {
            throw refusal(island, e);
        }
    }

    /** What the translator asks of the model for {@code island}: a source's columns, the model's tables, a table's
     *  primary key, the variables of {@code env} (each one read added to {@code read}), the inline-view marker. */
    private static SqlToPure.Sources sources(SqlIsland island, Function<ValueSpecification, TypedSpec> typeSource,
            ModelContext model, Env env, java.util.Set<String> read) {
        return new SqlToPure.Sources() {
            @Override
            public void columns(ValueSpecification source, SqlToPure.Column column) {
                SqlIslands.columns(typeSource.apply(NameResolver.resolveQuery(source)), island, column);
            }

            @Override
            public void tables(SqlToPure.Table table) {
                model.derived(Catalog.class, Catalog::of).tables().forEach(t -> table.table(t[0], t[1], t[2]));
            }

            @Override
            public java.util.@com.legend.base.Nullable List<String> primaryKey(String database, String table) {
                return SqlIslands.primaryKey(model, database, table);
            }

            @Override
            public boolean variable(String name, SqlToPure.Scoped type) {
                boolean scoped = variableType(env, name, type);
                if (scoped) {
                    read.add(name);
                }
                return scoped;
            }

            @Override
            public void variables(java.util.function.Consumer<String> names) {
                env.names().forEach(names);
            }

            @Override
            public ValueSpecification inlineView(ValueSpecification relation) {
                // IQ island, 2026-10-16: the compiler's marker (InlineViewChecker)
                return Calls.exact(com.legend.builtin.Pure.Lite.INLINE_VIEW, java.util.List.of(relation));
            }
        };
    }

    /** A translator's refusal as the compile error its kind says, placed in the enclosing source (the #SQL and the
     *  #Cypher islands'). */
    static RuntimeException refusal(SqlIsland island, SqlTranslationException e) {
        String message = island.opening() + "...}# at " + position(island, e.line(), e.column()) + ": "
                + e.getMessage();
        return switch (e.kind()) {
            case SYNTAX -> new com.legend.error.IslandSyntaxException(message);
            case UNSUPPORTED -> new com.legend.error.NotImplementedException(message);
            case INVALID -> new TypeInferenceException(message);
        };
    }

    /** The writes design's leg 4 (2026-10-20): the primary key's columns of {@code table} ({@code schema.table}, or a
     *  table of the default schema) of the Database {@code database}, as it declares them; null for a table it does not
     *  declare (SqlToPure.Sources#primaryKey). */
    static java.util.@com.legend.base.Nullable List<String> primaryKey(ModelContext model, String database,
            String table) {
        com.legend.model.DatabaseDefinition db = model.findDatabase(database).orElse(null);
        if (db == null) {
            return null;
        }
        int dot = table.indexOf('.');
        java.util.List<com.legend.model.DatabaseDefinition.TableDefinition> tables = new java.util.ArrayList<>();
        if (dot < 0) {
            tables.addAll(db.defaultSchemaTables());
        } else {
            for (com.legend.model.DatabaseDefinition.SchemaDefinition schema : db.schemas()) {
                if (schema.name().equals(table.substring(0, dot))) {
                    tables.addAll(schema.tables());
                }
            }
        }
        String name = dot < 0 ? table : table.substring(dot + 1);
        for (com.legend.model.DatabaseDefinition.TableDefinition t : tables) {
            if (t.name().equals(name)) {
                return t.columns().stream().filter(com.legend.model.DatabaseDefinition.ColumnDefinition::primaryKey)
                        .map(com.legend.model.DatabaseDefinition.ColumnDefinition::name).toList();
            }
        }
        return null;
    }

    /** IQ island, 2026-10-14 (M1e): the model's tables, by Database, schema ({@code default} for one declared outside
     *  any) and name: each Database's own (an included one's are its own Database's), computed once per model. */
    record Catalog(java.util.List<String[]> tables) {

        static Catalog of(ModelContext model) {
            java.util.List<String[]> out = new java.util.ArrayList<>();
            for (String fqn : new java.util.TreeSet<>(model.elementFqns())) {
                com.legend.model.DatabaseDefinition db = model.findDatabase(fqn).orElse(null);
                if (db == null) {
                    continue;
                }
                for (com.legend.model.DatabaseDefinition.TableDefinition t : db.defaultSchemaTables()) {
                    if (!t.function()) {
                        out.add(new String[] {db.qualifiedName(), "default", t.name()});
                    }
                }
                for (com.legend.model.DatabaseDefinition.SchemaDefinition schema : db.schemas()) {
                    for (com.legend.model.DatabaseDefinition.TableDefinition t : schema.tables()) {
                        if (!t.function()) {
                            out.add(new String[] {db.qualifiedName(), schema.name(), t.name()});
                        }
                    }
                }
            }
            return new Catalog(java.util.List.copyOf(out));
        }
    }

    /** IQ island, 2026-10-14 (M1e): one of {@code names} (variables of the enclosing query the translation reads) that
     *  a lambda of {@code pure} declares as its parameter -- inside it, a reference to the variable would read the
     *  parameter; null for none. */
    private static @com.legend.base.Nullable String hidden(ValueSpecification pure, java.util.Set<String> names) {
        if (pure instanceof com.legend.protocol.spec.LambdaFunction l) {
            for (com.legend.protocol.spec.Variable p : l.parameters()) {
                if (names.contains(p.name())) {
                    return p.name();
                }
            }
        }
        for (ValueSpecification child : pure.children()) {
            String h = hidden(child, names);
            if (h != null) {
                return h;
            }
        }
        return null;
    }

    /** IQ island, 2026-10-14 (M1e): reports the type of {@code env}'s variable {@code name} to {@code type}, as the
     *  translator reads one: its type's name, whether it may hold none, whether it may hold more than one (an unbound
     *  multiplicity counted so); false when none of that name is in scope. */
    private static boolean variableType(Env env, String name, SqlToPure.Scoped type) {
        com.legend.compiler.element.type.ExprType t = env.lookup(name).orElse(null);
        if (t == null) {
            return false;
        }
        boolean bounded = t.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded;
        boolean nullable = t.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                && b.lower() == 0;
        type.variable(t.type().typeName(), nullable, !bounded || t.multiplicity().isMany());
        return true;
    }

    /** The island's dialect: its language's ({@link InputLanguages}), IQ under the island's profile for
     *  {@code #IQ{}}. */
    private static InputDialect dialect(SqlIsland island) {
        InputDialect plain = island.profile() == null ? InputLanguages.plain(island.language()) : null;
        if (plain != null) {
            return plain;
        }
        if (!island.language().equals(SqlIsland.IQ)) {
            throw new TypeInferenceException("an island of language " + island.language() + (island.profile() == null
                    ? "" : " and profile " + island.profile()) + " has no SQL dialect");
        }
        String named = island.profile();
        IqProfile profile = named == null ? IqProfile.DEFAULT : IqProfile.named(named);
        if (profile == null) {
            throw new TypeInferenceException(IqProfile.unknown(java.util.Objects.requireNonNull(named)));
        }
        return IqDialect.of(profile.settings());
    }

    /** A FROM source's columns, as the translator reads them: each column's name, its type's name, whether it may hold
     *  no value, and, when the source is a table, its declared store type, spelled as the store spells it. */
    private static void columns(TypedSpec source, SqlIsland island, SqlToPure.Column column) {
        Type.RelationType schema = Type.relationSchema(source.info().type());
        if (schema == null || !schema.dynamicColumns().isEmpty()) {
            throw new TypeInferenceException(island.opening() + "...}# at " + position(island, 1, 1) + ": a FROM"
                    + " source must be a relation with known columns, not " + source.info().type().typeName());
        }
        java.util.Map<String, com.legend.sql.SqlDdl.ColumnType> stored =
                source instanceof com.legend.compiler.spec.typed.TypedTableReference t ? t.storedTypes()
                        : java.util.Map.of();
        for (Type.Column c : schema.columns()) {
            boolean required = c.multiplicity() instanceof com.legend.compiler.element.type.Multiplicity.Bounded b
                    && b.lower() >= 1;
            com.legend.sql.SqlDdl.ColumnType storeType = stored.get(c.name());
            column.column(c.name(), c.type().typeName(), !required, storeType == null ? null : spelled(storeType));
        }
    }

    /** A declared store type as the store spells it: {@code SMALLINT}, {@code CHAR(5)}, {@code NUMERIC(10,2)}. */
    private static String spelled(com.legend.sql.SqlDdl.ColumnType t) {
        return switch (t) {
            case com.legend.sql.SqlDdl.ColumnType.Plain p -> p.kind().name();
            case com.legend.sql.SqlDdl.ColumnType.Sized s -> s.kind() + "(" + s.size() + ")";
            case com.legend.sql.SqlDdl.ColumnType.Scaled s -> s.kind() + "(" + s.precision() + "," + s.scale() + ")";
        };
    }

    /** A position in the SQL text as a position in the enclosing source (the island's own when it has one). */
    static String position(SqlIsland island, int line, int column) {
        SourceInfo pos = island.pos();
        if (pos == null) {
            return "line " + line + ", column " + column + " of the SQL";
        }
        return line == 1 ? "line " + pos.startLine() + ", column " + (pos.startColumn() + island.opening().length()
                + column - 1)
                : "line " + (pos.startLine() + line - 1) + ", column " + column;
    }
}
