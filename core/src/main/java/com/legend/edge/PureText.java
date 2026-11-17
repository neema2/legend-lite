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

    /** The edge's dialect source: upstream's spellings over the product's dialect of each database. */
    public static final PlanStyle UPSTREAM = new PlanStyle() {
        @Override
        public SqlDialect dialect(DatabaseType type) {
            return PureText.dialect(type);
        }



        /** The closed list's kinds ({@code testplan.PlanStyleFacets.KINDS}), each the edge's own; any other by name. */
        @Override
        public String errorWords(RuntimeException e) {
            return PureMessages.compile(e);
        }

        /** legend-pure's sentence of a site, worded at planning from its literal operands ({@link PureMessages#words}). */
        @Override
        public String siteWords(com.legend.sql.dialect.RaiseSite site) {
            return PureMessages.words(site.kind(), site.operands());
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
            if (kind == com.legend.lowering.RelationTexts.class) {
                return kind.cast(new RelationTextLowering());
            }
            if (kind == com.legend.lowering.PureOnlyTexts.class) {
                return kind.cast(EdgePureOnlyTexts.INSTANCE);
            }
            throw new com.legend.error.NotImplementedException("the test-planning facet " + kind.getName()
                    + " is none of the declared kinds (com.legend.testplan.PlanStyleFacets.KINDS)");
        }

        @Override
        public <F> F facet(Class<F> kind, java.util.function.Supplier<? extends RuntimeException> refusal) {
            // the edge answers every declared kind: the call's refusal is the product's
            return facet(kind);
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

        /** legend-engine's text, as its activity records the SQL it generated. */
        @Override
        public String activitySql(Activity activity) {
            return activity.engines();
        }
    };

    /** legend-engine's exact SQL text for {@code type}, as {@code toSQLString} prints it: Composite takes the engine's
     *  DEFAULT spellings (native trim/pad/cbrt, plain char_length). A fresh printer per use: a dialect keeps state while
     *  it writes a statement (moved here from the planner's {@code EngineText}, phase D, 2026-11-12). */
    public static EngineTextDialect engineText(DatabaseType type) {
        return switch (type) {
            case H2 -> new EngineStyleH2();
            case DB2 -> new EngineStyleDB2();
            case Composite -> new EngineStyleComposite();
            case DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora, MariaDB, MySQL, MongoDB ->
                    throw new com.legend.error.NotImplementedException("toSQLString for DatabaseType." + type
                            + " — only the H2, DB2 and Composite engine-style renderers are built");
        };
    }

    /** legend-engine's exact PLAN text for {@code type}: the plan goldens pin Composite to the DB2-family spelling
     *  (paren-wrapped conjunctions, quoted boolean placeholders) — unlike {@link #engineText}. */
    public static EngineTextDialect enginePlanText(DatabaseType type, boolean quoteIdentifiers,
            @com.legend.base.Nullable String timeZone) {
        return switch (type) {
            case H2 -> new EngineStyleH2(quoteIdentifiers, timeZone);
            case DB2, Composite -> new EngineStyleDB2(quoteIdentifiers, timeZone);
            case DuckDB, Postgres, SQLite, MemSQL, Sybase, SybaseIQ, SqlServer, Hive, Snowflake, Presto, Trino, BigQuery,
                 Redshift, Databricks, Spanner, Athena, Oracle, ClickHouse, Aurora, MariaDB, MySQL, MongoDB ->
                    throw new com.legend.error.NotImplementedException("plan text for DatabaseType." + type
                            + " — only the H2 and DB2-family engine-style renderers are built");
        };
    }

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
        /** A Float literal computes as legend-engine's: its bare print, a DECIMAL (the edge program's §6 row 21, the
         *  orchestrator's ruling R2 of 2026-11-17; the product's is the double, register S20). */
        @Override
        public com.legend.sql.SqlExpr.FloatLit.Kind floatLiteralKind() {
            return com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
        }

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

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** H2, spelled as upstream spells it. */
    public static final class H2 extends com.legend.sql.dialect.H2 {
        /** A Float literal computes as legend-engine's: its bare print, a DECIMAL (the edge program's §6 row 21, the
         *  orchestrator's ruling R2 of 2026-11-17; the product's is the double, register S20). */
        @Override
        public com.legend.sql.SqlExpr.FloatLit.Kind floatLiteralKind() {
            return com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
        }

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes(PurePrintForms.FloatLiteral.PRINT_AS_FLOAT);
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x, PurePrintForms.FloatLiteral.PRINT_AS_FLOAT), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** Postgres, spelled as upstream spells it. */
    public static final class Postgres extends com.legend.sql.dialect.Postgres {
        /** A Float literal computes as legend-engine's: its bare print, a DECIMAL (the edge program's §6 row 21, the
         *  orchestrator's ruling R2 of 2026-11-17; the product's is the double, register S20). */
        @Override
        public com.legend.sql.SqlExpr.FloatLit.Kind floatLiteralKind() {
            return com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
        }

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

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** Databricks, spelled as upstream spells it. */
    public static final class Databricks extends com.legend.sql.dialect.Databricks {
        /** A Float literal computes as legend-engine's: its bare print, a DECIMAL (the edge program's §6 row 21, the
         *  orchestrator's ruling R2 of 2026-11-17; the product's is the double, register S20). */
        @Override
        public com.legend.sql.SqlExpr.FloatLit.Kind floatLiteralKind() {
            return com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
        }

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** Snowflake, spelled as upstream spells it. */
    public static final class Snowflake extends com.legend.sql.dialect.Snowflake {
        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** BigQuery, spelled as upstream spells it. */
    public static final class BigQuery extends com.legend.sql.dialect.BigQuery {
        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
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

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** MariaDB, spelled as upstream spells it. */
    public static final class MariaDb extends com.legend.sql.dialect.MariaDb {
        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** MySQL, spelled as upstream spells it. */
    public static final class MySql extends com.legend.sql.dialect.MySql {
        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** Trino, spelled as upstream spells it. */
    public static final class Trino extends com.legend.sql.dialect.Trino {
        /** A Float literal computes as legend-engine's: its bare print, a DECIMAL (the edge program's §6 row 21, the
         *  orchestrator's ruling R2 of 2026-11-17; the product's is the double, register S20). */
        @Override
        public com.legend.sql.SqlExpr.FloatLit.Kind floatLiteralKind() {
            return com.legend.sql.SqlExpr.FloatLit.Kind.DECIMAL;
        }

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** ClickHouse, spelled as upstream spells it. */
    public static final class ClickHouse extends com.legend.sql.dialect.ClickHouse {
        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }

    /** MongoDB, spelled as upstream spells it. */
    public static final class MongoDb extends com.legend.sql.dialect.MongoDb {
        /** legend-pure's text of a date-time, to the millisecond with {@code +0000} (D1.2: until then the product's). */
        @Override
        protected String dateTimeFormat() {
            return "%Y-%m-%dT%H:%M:%S.%L+0000";
        }

        /** legend-pure's print forms (a Float's, a DateTime's) for the product's text of each ({@link PurePrintForms}; D1.2). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> spellingPasses() {
            return PurePrintForms.passes();
        }

        /** legend-pure's print form of a Float ({@link PurePrintForms}; D1.2): a node no pass reached. */
        @Override
        protected com.legend.sql.dialect.SqlWriter floatToText(com.legend.sql.dialect.SqlWriter writer,
                com.legend.sql.SqlExpr x) {
            return writer.expr(PurePrintForms.floatOf(x), 0);
        }

        /** A raise's site, legend-pure's sentence worded at planning in the plan's site table ({@link PlanStyle#siteWords});
         *  a site it cannot word there (a computed operand) legend-pure's sentence in SQL, the interim
         *  ({@link PureMessages#sentence}; phase D's Phase 2, 2026-11-16). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.wordable(r) ? site(r, UPSTREAM::siteWords)
                    : PureMessages.sentence(r);
        }
    }
}
