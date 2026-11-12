// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.database.DialectSource;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE EDGE'S SQL RENDER TARGET (phase D's D1.0; docs/DATA_DELIVERY_DESIGN_2026_11_10.md §6.1, Q15: "Pure's exact
 * texts at the edge, as the edge's own SQL render target"): the dialect source the compatibility harnesses and
 * routes hand the planner ({@link #DIALECTS}) -- PCT's channel A, the corpus, pure/v1's upstream routes -- so the
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

    /** The edge's dialect source: upstream's spellings over the product's dialect of each database. */
    public static final DialectSource DIALECTS = PureText::dialect;

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
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** H2, spelled as upstream spells it. */
    public static final class H2 extends com.legend.sql.dialect.H2 {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** Postgres, spelled as upstream spells it. */
    public static final class Postgres extends com.legend.sql.dialect.Postgres {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** Databricks, spelled as upstream spells it. */
    public static final class Databricks extends com.legend.sql.dialect.Databricks {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** Snowflake, spelled as upstream spells it. */
    public static final class Snowflake extends com.legend.sql.dialect.Snowflake {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** BigQuery, spelled as upstream spells it. */
    public static final class BigQuery extends com.legend.sql.dialect.BigQuery {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** SQLite, spelled as upstream spells it. */
    public static final class SQLite extends com.legend.sql.dialect.SQLite {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** MariaDB, spelled as upstream spells it. */
    public static final class MariaDb extends com.legend.sql.dialect.MariaDb {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** MySQL, spelled as upstream spells it. */
    public static final class MySql extends com.legend.sql.dialect.MySql {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** Trino, spelled as upstream spells it. */
    public static final class Trino extends com.legend.sql.dialect.Trino {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** ClickHouse, spelled as upstream spells it. */
    public static final class ClickHouse extends com.legend.sql.dialect.ClickHouse {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }

    /** MongoDB, spelled as upstream spells it. */
    public static final class MongoDb extends com.legend.sql.dialect.MongoDb {
        /** legend-pure's sentence of a raise ({@link PureMessages}). */
        @Override
        protected com.legend.sql.SqlExpr raiseMessage(com.legend.sql.SqlExpr.RaiseText r) {
            return PureMessages.sentence(r);
        }
    }
}
