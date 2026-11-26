// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE EDGE'S SQL RENDER TARGET (phase D's D1.0; docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6.1, Q15: "Pure's exact
 * texts at the edge, as the edge's own SQL render target"): the dialect source the compatibility harnesses and
 * routes hand the planner ({@link #UPSTREAM}) -- PCT's channel A, the corpus, pure/v1's upstream routes -- so the
 * product's typed plan is spelled as legend-pure and legend-engine spell it. Each dialect here SUBCLASSES the
 * product's (open recursion: an arm the product's renderer reaches from inside its own walk is overridden, which a
 * wrapper could not do) and overrides ONLY the arms that spell upstream's text; an override that changes a product
 * semantic (a cast, a null rule, an order) is a defect unless it is a ruled register row. The arms each class
 * overrides are pinned ({@code PureTextOverridesTest}): D1.2-D1.5 move each compatibility arm here from the product
 * dialect, and the product's arm then spells lite's own form.
 *
 * <p>A class keeps its product dialect's simple name, so a refusal that names the dialect reads the same on both.
 */
public final class PureText {

    private PureText() {
    }

    /** The database legend-engine's test connections declare, so the one its golden texts are written for (the
     *  judges' engine-test methods, W9's judge half, 2026-11-16). */
    private static final DatabaseType ENGINE_TEST_DATABASE = DatabaseType.H2;

    /** THE DATABASES ON WHICH LEGEND-ENGINE'S {@code substring}, {@code indexOf} AND {@code substr} ARE CHECKED (A3: the
     *  door's table, {@link EngineTable}; the user's ruling of 2026-11-20 ~07:55, "Delete the settings, make
     *  engine only mode for engine for everything", claude/compat-program 25ede64f §1). legend-engine sends those calls
     *  to the database as written, and the database's own functions decide, so its answer is checked per database:
     *  H2 and DuckDB, by the batteries (pct/batteries/agreement_engine_*.tsv, each answer legend-engine's but the
     *  3-argument {@code indexOf}, which legend-engine refuses: E-R2). On any other database a query holding such a
     *  call is refused by name until it is checked (the database lanes); the table's rows name no database. */
    private static final java.util.Set<DatabaseType> CHECKED = java.util.Set.of(DatabaseType.H2, DatabaseType.DuckDB);

    // ---- where each row of legend-engine's door's table answers (EngineTable's rows read these; the table names no
    //      database) -- a set grows only when a lane has read legend-engine's SQL for the function on the database and
    //      run it beside lite's rule (docs/records/edition-homework/lanes/engine-templates-2026-11-26.md)

    /** The databases whose own {@code substring}, position, {@code round} and date difference are read as
     *  legend-engine's answer for the row that writes them: {@link #CHECKED}. */
    static final java.util.Set<DatabaseType> ENGINE_FUNCTIONS_READ = CHECKED;

    /** The databases legend-engine 4.145.0 has that lite has too, each lane's reading of its extension
     *  (docs/records/edition-homework/lanes/engine-templates-2026-11-26.md): where a row that is legend-engine's by
     *  what it writes (the plain parses' cast) answers. Not SQLite, MariaDB, MySQL or MongoDB: legend-engine has no
     *  such database. */
    static final java.util.Set<DatabaseType> ENGINE_DATABASES = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(DatabaseType.H2, DatabaseType.DuckDB, DatabaseType.Postgres, DatabaseType.Trino,
                    DatabaseType.Snowflake, DatabaseType.ClickHouse, DatabaseType.Databricks, DatabaseType.BigQuery));

    private static final java.util.Set<DatabaseType> ON_H2 = java.util.Set.of(DatabaseType.H2);
    private static final java.util.Set<DatabaseType> ON_DUCKDB = java.util.Set.of(DatabaseType.DuckDB);

    /** Where legend-engine's {@code toDecimal}, the database's DECIMAL with no precision written, is answered by the
     *  row: H2 (scale 0: {@code toDecimal(3.8)} is 4, as its own test record says). Not DuckDB yet, though
     *  legend-engine writes the same cast there ({@code DECIMAL(18,3)}): its adapter reads that value through a double
     *  and prints {@code 3.8D} where lite prints the value's own {@code 3.800D}, so PCT's two tests legend-engine
     *  passes would fail (measured 2026-11-26, pct/engine_lane); the row answers there once the door spells a Decimal
     *  as legend-engine's does (the edition program's step 8). */
    static final java.util.Set<DatabaseType> ENGINE_BARE_DECIMAL_READ = ON_H2;

    /** Where legend-engine's {@code isNumeric} is a regular expression, a plain decimal's text: H2, by its dialect
     *  translation ({@code toPostgresModel.pure:367}). */
    static final java.util.Set<DatabaseType> ENGINE_IS_NUMERIC_BY_PATTERN = ON_H2;

    /** Where legend-engine's {@code isNumeric} is {@code lower(s) = upper(s)}: DuckDB
     *  ({@code duckdbExtension.pure:238}). On Postgres, Trino, Snowflake, ClickHouse and BigQuery it has no SQL for
     *  the function (read by their lanes, 2026-11-26). */
    static final java.util.Set<DatabaseType> ENGINE_IS_NUMERIC_BY_CASE = ON_DUCKDB;

    /** Where legend-engine's {@code splitPart} is the database's own {@code split_part}: DuckDB
     *  ({@code duckdbExtension.pure:276}). On H2 it is a function of legend-engine's own Java
     *  ({@code legend_h2_extension_split_part}), which lite's H2 does not hold. */
    static final java.util.Set<DatabaseType> ENGINE_SPLIT_PART_READ = ON_DUCKDB;

    /** Where legend-engine's {@code toString} of a Float literal prints it as the database prints a double: H2, whose
     *  literal processor writes a Float literal {@code CAST(%s AS FLOAT)} ({@code h2Extension2_1_214.pure}'s
     *  {@code getLiteralProcessorsForH2}); every other database legend-engine has keeps the default processor, the
     *  literal bare ({@code extensionDefaults.pure}'s {@code getDefaultLiteralProcessors}). Read in legend-engine
     *  4.145.0; until 2026-11-26 the edge's SQL pass chose between the two ({@code PurePrintForms.FloatLiteral}). */
    static final java.util.Set<DatabaseType> ENGINE_FLOAT_LITERAL_AS_DOUBLE = ON_H2;

    /** Whether legend-engine 4.145.0 has {@code database} ({@link #ENGINE_DATABASES}): what a test that runs one of
     *  the table's rows on its lane's database asks, so that it names no database. */
    public static boolean legendEngineHas(DatabaseType database) {
        return ENGINE_DATABASES.contains(database);
    }

    /** The edge's dialect source: upstream's spellings over the product's dialect of each database. */
    /** Whether {@code style} is one of the edge's ({@link #UPSTREAM}, {@link #PURE_V1}, {@link #ENGINE_TESTS},
     *  {@link #ENGINE_PCT}): a plan in legend-pure's words,
     *  which only the edge's hook hears (TestRunner refuses a mixed run by them; train 17, 2026-11-18). */
    public static boolean isEdgeStyle(PlanStyle style) {
        return style == UPSTREAM || style == PURE_V1 || style == ENGINE_TESTS || style == ENGINE_PCT;
    }

    public static final PlanStyle UPSTREAM = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return PureText.dialect(type);
        }



        /** The closed list's kinds ({@code testplan.PlanStyleFacets.KINDS}), each the edge's own; any other by name. */
        @Override
        public boolean judgesEngineText() {
            return true;
        }

        @Override
        public <F> F facet(Class<F> kind) {
            // the closed list first (the P2 checkpoint audit's S2, 2026-11-16): a kind outside it is refused by name
            // before any answer, so an answer added below without its declaration is unreachable
            if (!com.legend.testplan.PlanStyleFacets.KINDS.contains(kind)) {
                throw new com.legend.error.NotImplementedException("the test-planning facet " + kind.getName()
                        + " is none of the declared kinds (com.legend.testplan.PlanStyleFacets.KINDS)");
            }
            if (kind == com.legend.testplan.EngineTestText.class) {
                return kind.cast(EdgeEngineTestText.INSTANCE);
            }
            if (kind == com.legend.planner.UpstreamPlans.class) {
                return kind.cast(EnginePlans.FACET);
            }
            // (lowering.RelationTexts: gone 2026-11-25, stage B's B2b S5c -- a relation's toString and toCSV are lite's own
            // texts, ProductRelationText; legend-pure's and legend-engine's texts of a last step, a TDS's csv among them,
            // the doors' Java printer's, PrintedRelations)
            if (kind == com.legend.lowering.PureOnlyTexts.class) {
                return kind.cast(EdgePureOnlyTexts.INSTANCE);
            }
            if (kind == com.legend.lowering.ResultEnvelopes.class) {
                return kind.cast(EngineResultEnvelopes.INSTANCE);
            }
            throw new com.legend.error.NotImplementedException("the test-planning facet " + kind.getName()
                    + " is none of the declared kinds (com.legend.testplan.PlanStyleFacets.KINDS)");
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            // the edge answers every declared kind: the call's refusal is the product's
            return facet(kind);
        }

        /** The edge's form of each declared two-form kind (none declared yet: the kinds arrive with their families,
         *  census B9's DateTime and F's TEXT_FORMS); a kind outside the closed list is refused by name. */
        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            java.util.Objects.requireNonNull(productForm, "a two-form hook's product form");
            if (kind == com.legend.lowering.ListTexts.class) {
                return kind.cast(PureListTexts.INSTANCE);
            }
            if (kind == com.legend.sql.dialect.DatabaseSettingsForm.class) {
                // legend-engine's H2 database settings (the audit's J22, 2026-11-17)
                return kind.cast(EngineDatabaseSettings.INSTANCE);
            }
            if (kind == com.legend.lowering.LoweringForms.class) {
                // the lowering's two-form choices (the audit's K5, 2026-11-17), every edge style's alike
                return kind.cast(EdgeLoweringForms.INSTANCE);
            }
            if (kind == com.legend.lowering.DateTimeTexts.class) {
                return kind.cast(EdgeDateTimeTexts.INSTANCE);
            }
            if (kind == com.legend.planner.ExecutedValues.class) {
                // legend-engine's json-builder envelope around a serialized root's text (train 17 item 2)
                return kind.cast(EngineExecutedValues.INSTANCE);
            }
            if (kind == com.legend.testdatagen.TestDataPlanner.Printing.class) {
                // a test-data generator's timestamp text as legend-engine's test-data CSV writes it (L9; census C1)
                // (J7/K12 of the edge audit, 2026-11-17: the rest of the form the function's, lite's own)
                return kind.cast(TestDataRoutes.FUNCTION);
            }
            if (kind == com.legend.planner.ActivityTexts.class) {
                // legend-engine's text, as its activity records the SQL it generated: its upstream plan facet's (J40.PlanStyle,
                // 2026-11-18; until then the planner computed both texts and PlanStyle.activitySql picked this one)
                return kind.cast((com.legend.planner.ActivityTexts) EnginePlans::activitySql);
            }
            if (kind == com.legend.planner.RoutedQueries.class) {
                // legend-engine's AggregationAwareActivity, the routed query's print (J5 of the edge audit, 2026-11-17)
                return kind.cast((com.legend.planner.RoutedQueries) AggAwareActivities::routingActivity);
            }
            if (kind == com.legend.planner.Edition.class) {
                // the door's edition (the edition program's step 2): PCT's door, legend-pure's own tests', hands none
                // yet, the product's as it is (the corpus has its own style since step 3, ENGINE_TESTS)
                return productForm;
            }
            throw new com.legend.error.NotImplementedException("the two-form kind " + kind.getName()
                    + " is none of the declared kinds (com.legend.testplan.PlanStyleFacets.TWO_FORMS)");
        }



        /** legend-engine's key argument: the date's written text, {@code +0000} after a time (D1.2). */
        @Override
        public String graphKeyDate(String written) {
            return written + (written.indexOf('T') >= 0 ? "+0000" : "");
        }

        /** legend-engine's key argument: the String quoted (D1.2). */
        @Override
        public String graphKeyString(String value) {
            return "'" + value + "'";
        }

        /** legend-engine's trace comment around the execution's id (its executor's, RelationalExecutor.java:332; D1.4,
         *  2026-11-15; from the edge's spelling pass to the style, B8 (ii), 2026-11-17). */
        @Override
        public TraceCell traceCell() {
            return new TraceCell("-- \"executionTraceID\" : \"", "\"");
        }

    };

    /** CENSUS CLASS A (docs/records/pure-v1-engine-census-2026-11-17.md): the kinds legend-engine 4.145.0's pure/v1
     *  route writes no check for in its SQL -- the database decides (rem's and divide's {@code mod} and {@code /},
     *  {@code sqrt}, {@code acos} and {@code asin}, the shifts, {@code toOne}'s {@code processNoOp}, {@code slice},
     *  {@code range}, {@code processCast}, the parses' plain casts). */
    static final java.util.Set<com.legend.sql.RaiseKind> ENGINE_UNCHECKED = java.util.Collections.unmodifiableSet(
            java.util.EnumSet.of(com.legend.sql.RaiseKind.DIVIDE_BY_ZERO, com.legend.sql.RaiseKind.SQRT_OF_NEGATIVE,
                    com.legend.sql.RaiseKind.INVERSE_TRIG_OUT_OF_DOMAIN,
                    com.legend.sql.RaiseKind.SHIFT_OUT_OF_RANGE, com.legend.sql.RaiseKind.SIZE_NOT_MULTIPLICITY,
                    com.legend.sql.RaiseKind.SLICE_BOUNDS_INVERTED, com.legend.sql.RaiseKind.RANGE_STEP_ZERO,
                    com.legend.sql.RaiseKind.CAST_IMPOSSIBLE, com.legend.sql.RaiseKind.TEXT_NOT_INTEGER,
                    com.legend.sql.RaiseKind.TEXT_NOT_FLOAT, com.legend.sql.RaiseKind.TEXT_NOT_DATE));

    /** THE PURE/V1 ROUTE'S STYLE (B7, the user's ruling of 2026-11-17: pure/v1 matches legend-engine): {@link #UPSTREAM}
     *  in every dialect and facet but one (a user's call of a plan-text function is refused by name,
     *  {@link #refusePlanText}), and a raise legend-engine writes no check for ({@link #ENGINE_UNCHECKED}) is the plain
     *  operation, the database's own error passing through, and the planner is handed legend-engine's edition
     *  ({@link EngineTable#DOOR}; the edition program's step 2, 2026-11-25: its table and the planner's stamp took the
     *  place of the door's rewrite of the typed query). Handed by the pure/v1 routes alone ({@link PureV1Routes},
     *  {@link EdgeServer#runner}); PCT's legend-pure run keeps {@link #UPSTREAM}, the corpus plans under
     *  {@link #ENGINE_TESTS} and PCT's legend-engine run under {@link #ENGINE_PCT}. */
    public static final PlanStyle PURE_V1 = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return UPSTREAM.dialect(type);
        }

        /** Refused by name (census C4, 2026-11-17, the orchestrator's ruling): the pure/v1 routes plan a query, never a
         *  test body, so no judge asks this style whether it judges legend-engine's text; a judge that did is a bug. */
        @Override
        public boolean judgesEngineText() {
            throw new com.legend.error.NotImplementedException("judging legend-engine's text under the pure/v1 route's"
                    + " style: the pure/v1 routes plan queries, never a test body's asserts (PCT and the corpus plan"
                    + " under UPSTREAM)");
        }

        @Override
        public @com.legend.base.Nullable TraceCell traceCell() {
            return UPSTREAM.traceCell();
        }

        @Override
        public String graphKeyDate(String written) {
            return UPSTREAM.graphKeyDate(written);
        }

        @Override
        public String graphKeyString(String value) {
            return UPSTREAM.graphKeyString(value);
        }

        @Override
        public <F> F facet(Class<F> kind) {
            refusePlanText(kind);
            return UPSTREAM.facet(kind);
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            refusePlanText(kind);
            return UPSTREAM.facet(kind, refusal);
        }

        /** UPSTREAM's form of each two-form hook: pure/v1 differs from it in its edition, legend-engine's, whose table
         *  also says which checks legend-engine's SQL does not write (B7's plain raises; until the edition program's
         *  step 3 a lowering form of this style's own). */
        @Override
        public <F> F twoForm(Class<F> kind, F productForm) {
            if (kind == com.legend.planner.Edition.class) {
                java.util.Objects.requireNonNull(productForm, "a two-form hook's product form");
                return kind.cast(EngineTable.DOOR);
            }
            return UPSTREAM.twoForm(kind, productForm);
        }
    };

    /** LEGEND-ENGINE'S OWN TESTS' STYLE (the relational corpus; the edition program's step 3, 2026-11-26;
     *  docs/records/edition-plan-to-finish-stage-b-2026-11-25.md section 2.3): {@link #UPSTREAM} in every dialect,
     *  facet and form, and the planner is handed legend-engine's tests' edition ({@link EngineTable#TESTS}). A test of
     *  legend-engine's is Pure code that legend-pure runs, around queries it hands to {@code execute()}, which
     *  legend-engine runs: the queries get legend-engine's meaning, as at its API, and every call of the test's own
     *  code is stamped and keeps Pure's. Until this step the corpus planned under {@link #UPSTREAM}, lite's meaning on
     *  both sides. PCT's legend-pure run keeps {@link #UPSTREAM}: legend-pure's own tests, one audience throughout. */
    public static final PlanStyle ENGINE_TESTS = withEdition(UPSTREAM, EngineTable.TESTS);

    /** PCT'S LEGEND-ENGINE RUN'S STYLE (the edition program's step 3's remainder, 2026-11-26): {@link #PURE_V1} in
     *  every dialect and form, {@link #UPSTREAM}'s facets (a test's: the plan-text functions answer there, which
     *  {@link #PURE_V1} refuses a user), and the planner is handed PCT's legend-engine run's edition
     *  ({@link EngineTable#PCT}). A PCT case reaches lite as the one expression legend-engine's own PCT adapter sends
     *  to the database, so all of it is a store query's code: nothing is stamped, and every row of legend-engine's
     *  table answers. At legend-engine's API ({@link #PURE_V1}) a statement that reads no store is its own Java's. */
    public static final PlanStyle ENGINE_PCT = withEdition(PURE_V1, UPSTREAM, EngineTable.PCT);

    /** A USER'S CALL OF A PLAN-TEXT FUNCTION AT LEGEND-ENGINE'S DOOR IS REFUSED BY NAME (the user's ruling of
     *  2026-11-26: "I'm good with refusing those engine functions in the user/product path"; the register's S427).
     *  {@code toSQLString}, {@code toSQLStringPretty}, {@code planToString}, {@code executionPlan} and the DDL text
     *  generators are legend-engine's own tests' and its own Java's: asked of legend-engine 4.145.0 itself, a query or
     *  a model that calls one fails when it is compiled ("Function does not exist"), whoever sends it
     *  (docs/records/edition-homework/r3/measured-plan-text-reachability.md). Until this ruling lite's door answered
     *  some of them. The planner asks a style's {@code UpstreamPlans} facet only where the code it plans calls one, so
     *  the door's style refuses the facet; the routes' own Java (a generated plan's support functions, the test-data
     *  routes' timestamp text) calls the edge's classes directly and is not this. The tests' styles
     *  ({@link #UPSTREAM}, {@link #ENGINE_TESTS}, {@link #ENGINE_PCT}) answer as before: there legend-pure's runtime
     *  runs them. */
    private static void refusePlanText(Class<?> kind) {
        if (kind == com.legend.planner.UpstreamPlans.class) {
            throw new com.legend.error.NotImplementedException("legend-engine's plan-text functions (toSQLString,"
                    + " toSQLStringPretty, toNonExecutableSQLString, planToString, planToStringWithoutFormatting,"
                    + " executionPlan and the DDL text generators) are not a user's to call: legend-engine refuses a"
                    + " query or a model that calls one when it compiles it (\"Function does not exist\"), and so does"
                    + " this door");
        }
    }

    /** {@code base} in every dialect, facet and form, with the planner handed {@code edition} in the place of the
     *  base's own. */
    private static PlanStyle withEdition(PlanStyle base, EngineTable edition) {
        return withEdition(base, base, edition);
    }

    /** {@code base} in every dialect and form, its facets {@code facets}' own, with the planner handed
     *  {@code edition} in the place of the base's own. */
    private static PlanStyle withEdition(PlanStyle base, PlanStyle facets, EngineTable edition) {
        return new PlanStyle() {
            @Override
            public SqlDialect dialect(DatabaseType type) {
                return base.dialect(type);
            }

            @Override
            public boolean judgesEngineText() {
                return base.judgesEngineText();
            }

            @Override
            public @com.legend.base.Nullable TraceCell traceCell() {
                return base.traceCell();
            }

            @Override
            public String graphKeyDate(String written) {
                return base.graphKeyDate(written);
            }

            @Override
            public String graphKeyString(String value) {
                return base.graphKeyString(value);
            }

            @Override
            public <F> F facet(Class<F> kind) {
                return facets.facet(kind);
            }

            @Override
            public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
                return facets.facet(kind, refusal);
            }

            @Override
            public <F> F twoForm(Class<F> kind, F productForm) {
                if (kind == com.legend.planner.Edition.class) {
                    java.util.Objects.requireNonNull(productForm, "a two-form hook's product form");
                    return kind.cast(edition);
                }
                return base.twoForm(kind, productForm);
            }
        };
    }

    /** Refuses a style that is not pure/v1's ({@link #PURE_V1}): legend-engine's door's own plannings answer
     *  legend-engine's meaning, which is that style's edition's, so a caller that hands another would be answered
     *  lite's meaning in silence. */
    static void requirePureV1(PlanStyle style) {
        if (style != PURE_V1) {
            throw new IllegalArgumentException("legend-engine's door plans in pure/v1's style (PureText.PURE_V1): its"
                    + " meaning is that style's edition's");
        }
    }

    // (recording, a fresh recorder of legend-pure's words of one planning call's raise sites (K10b): deleted
    // 2026-11-20 with stage B's B1 -- legend-pure's words are worded after the run from a site's record,
    // PureMessages#worded)

    /** The edge's dialect of {@code type}; a database the product has no dialect for refused by name, as the
     *  product's own source refuses it. */
    public static SqlDialect dialect(DatabaseType type) {
        return switch (type) {
            case DuckDB -> new DuckDb();
            case H2 -> new H2();
            case Postgres -> new Postgres();
            case Databricks -> new Databricks();
            case Snowflake -> new Snowflake();
            case BigQuery -> new BigQuery();
            case SQLite -> new SQLite();
            case MariaDB -> new MariaDb();
            case MySQL -> new MySql();
            case Trino -> new Trino();
            case ClickHouse -> new ClickHouse();
            case MongoDB -> new MongoDb();
            case DB2, MemSQL, Sybase, SybaseIQ, Composite, SqlServer, Hive, Presto, Redshift, Spanner, Athena, Oracle,
                 Aurora -> com.legend.database.Databases.dialect(type);
        };
    }

    /** DuckDB, spelled as upstream spells it. */
    public static final class DuckDb extends com.legend.sql.dialect.DuckDb {

        /** legend-engine's H2 text (its connection declares {@link RawSqlBoundary#ENGINE_TEXT}, as legend-engine's test
         *  corpus's executeInDb calls do) translated for DuckDB ({@link RawSqlBoundary#translated}: keyword column names
         *  quoted, H2's type kinds mapped, {@code CURRENT_TIMESTAMP()} without parens); the product runs a raw statement
         *  as written (Phase 6's B4 (ii), 2026-11-17). */
        @Override
        public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
            return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                    ? RawSqlBoundary.translated(statement, types -> types, bare -> bare) : statement;
        }

        /** legend-engine's scan order made explicit for its corpus's replay (StableScanOrder, behind the corpus lanes'
         *  {@code legend.exec.engineScanOrder}; moved here from the product's dialect, W6, 2026-11-16). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> scanOrderPasses() {
            return Boolean.getBoolean("legend.exec.engineScanOrder") ? java.util.List.of(new StableScanOrder())
                    : java.util.List.of();
        }

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** H2, spelled as upstream spells it. */
    public static final class H2 extends com.legend.sql.dialect.H2 {
        /** On legend-engine's H2 database: the style's two-form {@code DatabaseSettingsForm} (the audit's J22, 2026-11-17). */
        public H2() {
            super(UPSTREAM.twoForm(com.legend.sql.dialect.DatabaseSettingsForm.class,
                    com.legend.sql.dialect.ProductDatabaseSettings.INSTANCE));
        }


        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** Postgres, spelled as upstream spells it. */
    public static final class Postgres extends com.legend.sql.dialect.Postgres {

        /** legend-engine's H2 text translated in the flavor DuckDB shares, its column types Postgres's (2026-10-12,
         *  docs/CLOUD_BACKENDS_PCT_2026_10_11.md §20: the 8-byte double is DOUBLE PRECISION, and SMALLINT is the
         *  smallest integer, there is no TINYINT) and a bare column name in lower case, as Postgres folds a bare name
         *  and spells a store's (decision D8); the product runs a raw statement as written (Phase 6's B4 (ii)). */
        @Override
        public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
            return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                    ? RawSqlBoundary.translated(statement,
                            types -> types.replaceAll("(?i)\\bDOUBLE\\b(?!\\s+PRECISION)", "DOUBLE PRECISION")
                                    .replaceAll("(?i)\\bTINYINT\\b", "SMALLINT"),
                            bare -> bare.toLowerCase(java.util.Locale.ROOT))
                    : statement;
        }

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** Databricks, spelled as upstream spells it. */
    public static final class Databricks extends com.legend.sql.dialect.Databricks {

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** Snowflake, spelled as upstream spells it. */
    public static final class Snowflake extends com.legend.sql.dialect.Snowflake {
        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** BigQuery, spelled as upstream spells it. */
    public static final class BigQuery extends com.legend.sql.dialect.BigQuery {
        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** SQLite, spelled as upstream spells it. */
    public static final class SQLite extends com.legend.sql.dialect.SQLite {
        /** A Decimal's arithmetic as REAL arithmetic, as legend-engine would write it here, where the product refuses it by
         *  name (no exact decimal: the edge program's §6 row 22, the orchestrator's ruling (c) of 2026-11-17, under the
         *  user's 5b approval and the standing no-silent-approximation rule). */
        @Override
        protected boolean refusesDecimalArithmetic() {
            return false;
        }

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** MariaDB, spelled as upstream spells it. */
    public static final class MariaDb extends com.legend.sql.dialect.MariaDb {
        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** MySQL, spelled as upstream spells it. */
    public static final class MySql extends com.legend.sql.dialect.MySql {
        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** Trino, spelled as upstream spells it. */
    public static final class Trino extends com.legend.sql.dialect.Trino {

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** ClickHouse, spelled as upstream spells it. */
    public static final class ClickHouse extends com.legend.sql.dialect.ClickHouse {
        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }

    /** MongoDB, spelled as upstream spells it. */
    public static final class MongoDb extends com.legend.sql.dialect.MongoDb {
        // (dateTimeFormat, legend-pure's text of a date-time to the millisecond with +0000: gone 2026-11-26, the
        // edition program's step 8 -- a date-time's text the program goes on computing with is lite's own on every
        // database, the product's format; the user's ruling of that day, "go with lite in the middle")

        /** legend-engine's {@code TDSNull} for the product's text of a joined cell ({@link PurePrintForms}; D1.3; a
         *  Float's and a DateTime's print forms went 2026-11-26, the edition program's step 8). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

    }
}
