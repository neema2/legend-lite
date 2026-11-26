// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.edge;

import com.legend.Compiler;
import com.legend.compiler.element.ModelContext;
import com.legend.model.ConnectionDefinition.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A DATETIME'S TEXT INSIDE A STORE QUERY AT LEGEND-ENGINE'S DOOR IS A ROW OF ITS TABLE, WHERE IT IS NO LITERAL (the
 * edition program's step 8, 2026-11-26; the user's ruling of that day, "agreed on float/date text for engine"):
 * legend-engine writes {@code toString} as {@code cast(%s as varchar)}, so the database's own text of the timestamp
 * is its answer. {@link EngineTable} holds the row, on the databases whose text of that cast has been read
 * ({@link PureText#ENGINE_DATE_TIME_CAST_READ}). Planned here for every database lite implements, read as SQL (no
 * database is run): where the row answers, the statement is the very one that prints the same value declared an
 * abstract Date, the plain cast; a literal, a value outside any store query, a day, and every database off the set
 * keep the door's plan as written.
 */
class EngineDateTimeTextRowTest {

    /** A table with a timestamp and a day, and a runtime on {@code type}. */
    private static String model(DatabaseType type) {
        return String.join("\n",
                "###Relational\nDatabase f::DB ( Table T ( ID INTEGER PRIMARY KEY, AT TIMESTAMP, DAY DATE ) )",
                "###Pure\nClass f::Trade { id: Integer[1]; }",
                "###Mapping\nMapping f::M ( f::Trade: Relational { ~mainTable [f::DB]T id: [f::DB]T.ID } )",
                "###Connection\nRelationalDatabaseConnection f::Conn { store: f::DB; type: " + type.name()
                        + "; specification: LocalH2 { }; auth: DefaultH2; }",
                "###Runtime\nRuntime f::RT { mappings: [f::M]; connections: [ f::DB: [ c: f::Conn ] ]; }");
    }

    private static final String TABLE = "#>{f::DB.T}#";

    /** The databases a relational connection can name and lite implements. */
    private static List<DatabaseType> planned() {
        List<DatabaseType> out = new ArrayList<>();
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            try {
                Compiler.compileModel(model(type));
                out.add(type);
            } catch (com.legend.parser.ParseException notAConnectionsType) {
                // a database a relational connection cannot name (a document store)
                assertTrue(notAConnectionsType.getMessage().contains(type.name()), notAConnectionsType.getMessage());
            }
        }
        return out;
    }

    private static String engines(ModelContext c, String query) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan("f::RT", PureText.PURE_V1).sql();
    }

    private static String asWritten(ModelContext c, String query) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan("f::RT",
                EngineTablePlacementTest.AS_WRITTEN).sql();
    }

    private static String printed(String value) {
        return "|" + TABLE + "->extend(~a: r|" + value + "->toString())->from(f::RT)";
    }

    @Test
    void aDateTimesToStringIsTheDatabasesCastWhereItsTextHasBeenRead() {
        List<DatabaseType> planned = planned();
        assertTrue(planned.containsAll(PureText.ENGINE_DATE_TIME_CAST_READ), "every database of the row is planned: "
                + planned);
        assertTrue(PureText.ENGINE_DATABASES.containsAll(PureText.ENGINE_DATE_TIME_CAST_READ),
                "the row answers only on a database legend-engine has");
        for (DatabaseType type : planned) {
            ModelContext c = Compiler.compileModel(model(type));
            String column = printed("$r.AT->toOne()");
            // a literal has no row: its written text, the constant the edge's lowering writes
            String literal = printed("%2014-01-01T00:00:00.000+0000");
            assertEquals(asWritten(c, literal), engines(c, literal), type + " " + literal);
            assertTrue(engines(c, literal).contains("2014-01-01T00:00:00.000+0000"), type + ": " + engines(c, literal));
            // a day's text is no DateTime's: no row
            String day = printed("$r.DAY->toOne()");
            assertEquals(asWritten(c, day), engines(c, day), type + " " + day);
            if (!PureText.ENGINE_DATE_TIME_CAST_READ.contains(type)) {
                // no row where the database's text of the cast has not been read: the call's own rule
                assertEquals(asWritten(c, column), engines(c, column), type + " " + column);
                continue;
            }
            assertNotEquals(asWritten(c, column), engines(c, column), type + " " + column);
            // the row's statement is exactly the one that prints the same value declared an abstract Date: the one
            // cast of the column to text
            assertEquals(engines(c, printed("$r.AT->toOne()->cast(@Date)")), engines(c, column), type + " " + column);
            // outside any store query legend-engine's own Java prints: no row, the door's plan as written
            String host = "{|let x = %2014-01-01T10:20:30.250+0000->adjust(1,"
                    + " meta::pure::functions::date::DurationUnit.HOURS)->cast(@DateTime)->toString(); " + TABLE
                    + "->filter(r|$r.ID->toOne()->toString() == $x)->from(f::RT);}";
            assertEquals(asWritten(c, host), engines(c, host), type + " " + host);
        }
    }
}
