// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.compiler.spec;

import com.legend.protocol.cypher.CypherAst;
import com.legend.protocol.cypher.CypherParser;
import com.legend.protocol.sql.InputDialect;
import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlParser;
import com.legend.protocol.sql.iq.IqDialect;
import com.legend.protocol.sql.iq.IqParser;
import com.legend.protocol.sql.iq.IqProfile;
import com.legend.protocol.ts.TsAst;
import com.legend.protocol.ts.TsParser;
import com.legend.protocol.ts.TsSql;

/**
 * Classes PLANTED in the compiler's island seams' package to read an island's text past the parse doors, for one test
 * alone (C5-J's last round, A1, 2026-11-15): {@code ArchitectureTest.PARSE_BY_THE_DOORS} fails on each -- one per
 * embedded language's reader, by a call, through a dialect, through the dialect's interface and by a reference. Test
 * classes: the rule's real check reads the product's classes, never these.
 */
public final class PlantedIslandParse {

    private PlantedIslandParse() {
    }

    /** A {@code #SQL} island's text read by the SQL reader itself. */
    public static final class Sql {
        private Sql() {
        }

        public static SqlAst.Statement read(String sql) {
            return SqlParser.parse(sql);
        }
    }

    /** A {@code #TS} statement's SQL read by its dialect -- a dialect class outside the SQL reader's package -- past the
     *  door. */
    public static final class TsSqlReading {
        private TsSqlReading() {
        }

        public static SqlAst.Statement read(String sql) {
            return TsSql.INSTANCE.parse(sql);
        }
    }

    /** An island's text read through the dialects' interface, past the door. */
    public static final class AnyDialect {
        private AnyDialect() {
        }

        public static SqlAst.Statement read(String sql, InputDialect dialect) {
            return dialect.parse(sql);
        }
    }

    /** A {@code #IQ} island's text read by the IQ reader itself. */
    public static final class Iq {
        private Iq() {
        }

        public static IqParser.Parsed read(String sql) {
            return IqParser.parsed(sql, IqProfile.DEFAULT.settings());
        }
    }

    /** A {@code #IQ} island's text read by IQ's dialect, past the door. */
    public static final class IqReading {
        private IqReading() {
        }

        public static SqlAst.Statement read(String sql) {
            return IqDialect.of(IqProfile.DEFAULT.settings()).parse(sql);
        }
    }

    /** A {@code #TS} island's text read by the timeseries reader. */
    public static final class Ts {
        private Ts() {
        }

        public static TsAst.Query read(String text) {
            return TsParser.parse(text);
        }
    }

    /** A {@code #TS} item's text read by the timeseries reader's method's reference, never called. */
    public static final class TsItemByReference {
        private TsItemByReference() {
        }

        public static java.util.function.Function<String, TsAst.Line> reading() {
            return TsParser::item;
        }
    }

    /** A {@code #Cypher} island's text read by the Cypher reader. */
    public static final class Cypher {
        private Cypher() {
        }

        public static CypherAst.Statement read(String text) {
            return CypherParser.parse(text);
        }
    }
}
