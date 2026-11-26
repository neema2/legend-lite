// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.database.PlanStyle;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.sql.dialect.SqlDialect;

/**
 * THE TESTS' RENDER TARGET (2026-11-26, the edition program's step 9, slice 2): the edge's styles as legend-engine's
 * own tests need them run on a database they were not written for. Two hooks are test machinery and no product's:
 * legend-engine's H2 text translated for DuckDB and Postgres ({@link RawSqlBoundary}: a test body's
 * {@code executeInDb} text and its inline setup SQL, written for the H2 its connection declares, when the lane runs it
 * elsewhere), and legend-engine's scan order made explicit ({@link StableScanOrder}, behind the corpus lanes'
 * {@code legend.exec.engineScanOrder}). Until this slice the edge's own dialects ({@link PureText.DuckDb},
 * {@link PureText.Postgres}) carried both, in the product's sources; a shipped road never reached either (the product's
 * door refuses a statement's own SQL text and inline seeds, and a declared connection's setup SQL is rendered by the
 * dialect of the database it declares).
 *
 * <p>Each style here is one of {@link PureText}'s in every facet, form and edition ({@code PureText.withDialects}),
 * its DuckDB and Postgres the subclasses below; every other database's dialect is the edge's own.
 */
public final class HarnessText {

    private HarnessText() {
    }

    /** {@link PureText#UPSTREAM} for a test that sends legend-engine's H2 text to DuckDB or Postgres (core's edge
     *  tests of {@code executeInDb} and of the referee's replay). */
    public static final PlanStyle UPSTREAM = PureText.withDialects(PureText.UPSTREAM, HarnessText::dialect);

    /** {@link PureText#ENGINE_TESTS} for the relational corpus's lanes. */
    public static final PlanStyle ENGINE_TESTS = PureText.withDialects(PureText.ENGINE_TESTS, HarnessText::dialect);

    /** The harness's dialect of {@code type}: its own DuckDB and Postgres, the edge's for every other database. */
    public static SqlDialect dialect(DatabaseType type) {
        if (type == DatabaseType.DuckDB) {
            return new DuckDb();
        }
        if (type == DatabaseType.Postgres) {
            return new Postgres();
        }
        return PureText.dialect(type);
    }

    /** The edge's DuckDB with the corpus's two hooks. It keeps the dialect's simple name, as the edge's does. */
    public static final class DuckDb extends PureText.DuckDb {

        /** legend-engine's H2 text (its connection declares {@link RawSqlBoundary#ENGINE_TEXT}, as legend-engine's test
         *  corpus's executeInDb calls do) translated for DuckDB ({@link RawSqlBoundary#translated}: keyword column names
         *  quoted, H2's type kinds mapped, {@code CURRENT_TIMESTAMP()} without parens); the product and the edge run a
         *  raw statement as written (Phase 6's B4 (ii), 2026-11-17; the edge since 2026-11-26). */
        @Override
        public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
            return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                    ? RawSqlBoundary.translated(statement, types -> types, bare -> bare) : statement;
        }

        /** legend-engine's scan order made explicit for its corpus's replay (StableScanOrder, behind the corpus lanes'
         *  {@code legend.exec.engineScanOrder}; moved to the edge's dialect from the product's, W6, 2026-11-16, and
         *  here from the edge's, 2026-11-26). */
        @Override
        protected java.util.List<com.legend.sql.SqlRewriter> scanOrderPasses() {
            return Boolean.getBoolean("legend.exec.engineScanOrder") ? java.util.List.of(new StableScanOrder())
                    : java.util.List.of();
        }
    }

    /** The edge's Postgres with the corpus's translation hook. */
    public static final class Postgres extends PureText.Postgres {

        /** legend-engine's H2 text translated in the flavor DuckDB shares, its column types Postgres's (2026-10-12,
         *  docs/CLOUD_BACKENDS_PCT_2026_10_11.md §20: the 8-byte double is DOUBLE PRECISION, and SMALLINT is the
         *  smallest integer, there is no TINYINT) and a bare column name in lower case, as Postgres folds a bare name
         *  and spells a store's (decision D8); the product and the edge run a raw statement as written (Phase 6's B4
         *  (ii)). */
        @Override
        public String raw(String statement, @com.legend.base.Nullable String writtenFor) {
            return RawSqlBoundary.ENGINE_TEXT.equals(writtenFor)
                    ? RawSqlBoundary.translated(statement,
                            types -> types.replaceAll("(?i)\\bDOUBLE\\b(?!\\s+PRECISION)", "DOUBLE PRECISION")
                                    .replaceAll("(?i)\\bTINYINT\\b", "SMALLINT"),
                            bare -> bare.toLowerCase(java.util.Locale.ROOT))
                    : statement;
        }
    }
}
