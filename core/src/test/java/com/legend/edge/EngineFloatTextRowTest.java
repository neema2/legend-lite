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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A FLOAT'S TEXT INSIDE A STORE QUERY AT LEGEND-ENGINE'S DOOR IS A ROW OF ITS TABLE (the edition program's step 8,
 * 2026-11-26; the user's ruling of that day, "agreed on float/date text for engine"): legend-engine writes
 * {@code toString} as {@code cast(%s as varchar)} on every database it has, so the database's own text is its answer.
 * {@link EngineTable} holds the row; until this step a pass of the edge's dialects wrote the cast for every Float text
 * of every compatibility plan ({@code PurePrintForms}). Planned here for every database lite implements, read as SQL
 * (no database is run): where legend-engine has the database the statement holds the cast, a Float literal written
 * as the exact numeric of its shortest digits -- read as a double first where legend-engine's literal processor
 * casts it; where it has none, and outside any store query, the statement is the door's as written, lite's own text.
 * The answers are held by PCT's legend-engine lane ({@code testFloatToStringWithNegativeExponent} on DuckDB and
 * {@code testFloatToStringWithPositiveExponent} on H2) and the corpus's validation showcase.
 */
class EngineFloatTextRowTest {

    /** A table with a number held as an integer, a class whose Float property reads it, and a runtime on {@code type}. */
    private static String model(DatabaseType type) {
        return String.join("\n",
                "###Relational\nDatabase f::DB ( Table T ( ID INTEGER PRIMARY KEY, QTY INTEGER, PRICE DOUBLE ) )",
                "###Pure\nClass f::Trade { qty: Float[1]; held: Integer[1]; price: Float[1]; }",
                "###Mapping\nMapping f::M ( f::Trade: Relational { ~mainTable [f::DB]T qty: [f::DB]T.QTY,"
                        + " held: [f::DB]T.QTY, price: [f::DB]T.PRICE } )",
                "###Connection\nRelationalDatabaseConnection f::Conn { store: f::DB; type: " + type.name()
                        + "; specification: LocalH2 { }; auth: DefaultH2; }",
                "###Runtime\nRuntime f::RT { mappings: [f::M]; connections: [ f::DB: [ c: f::Conn ] ]; }");
    }

    private static final String TABLE = "#>{f::DB.T}#";

    /** The databases a relational connection can name and lite implements, each with its compiled model. */
    private static List<DatabaseType> planned() {
        List<DatabaseType> out = new ArrayList<>();
        for (DatabaseType type : com.legend.database.Databases.implemented()) {
            try {
                Compiler.compileModel(model(type));
                out.add(type);
            } catch (com.legend.parser.ParseException notAConnectionsType) {
                // a database a relational connection cannot name (a document store)
            }
        }
        return out;
    }

    /** {@code query} planned as legend-engine's door plans it, and with the product's edition in legend-engine's
     *  place: the door's own plan of the query as written. */
    private static String engines(ModelContext c, String query) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan("f::RT", PureText.PURE_V1).sql();
    }

    private static String asWritten(ModelContext c, String query) {
        return PureV1Routes.asLegendEngine(Compiler.query(c, query)).plan("f::RT",
                EngineTablePlacementTest.AS_WRITTEN).sql();
    }

    @Test
    void aFloatsToStringIsTheDatabasesCastWhereLegendEngineHasTheDatabase() {
        List<DatabaseType> planned = planned();
        assertTrue(planned.containsAll(PureText.ENGINE_DATABASES), "every database legend-engine has is planned: "
                + planned);
        assertTrue(planned.size() > PureText.ENGINE_DATABASES.size(), "and one it does not have: " + planned);
        for (DatabaseType type : planned) {
            ModelContext c = Compiler.compileModel(model(type));
            boolean row = PureText.ENGINE_DATABASES.contains(type);
            // a computed Float, and a Float literal
            String computed = "|" + TABLE + "->extend(~a: r|($r.PRICE->toOne() * 2.0)->toString())->from(f::RT)";
            String literal = "|" + TABLE + "->extend(~a: r|1.3421e-8->toString())->from(f::RT)";
            if (!row) {
                // no row where legend-engine has no such database: the call's own rule, lite's text
                assertEquals(asWritten(c, computed), engines(c, computed), type + " " + computed);
                assertEquals(asWritten(c, literal), engines(c, literal), type + " " + literal);
                continue;
            }
            assertNotEquals(asWritten(c, computed), engines(c, computed), type + " " + computed);
            // (what the row writes for an operand that is no literal, the one cast of it as it is, is pinned on the
            // rule itself, NamedRulesFloatTextTest, and on a column below)
            String sql = engines(c, literal);
            // the literal as the exact numeric of its shortest digits, laid out plain
            assertTrue(sql.contains("0.000000013421"), type + ": " + sql);
            // read as a double first where legend-engine's literal processor casts it, and only there
            assertEquals(PureText.ENGINE_FLOAT_LITERAL_AS_DOUBLE.contains(type), sql.contains("CAST(CAST(0.000000013421"),
                    type + ": " + sql);
            // outside any store query legend-engine's own Java prints: no row, the door's plan as written
            String host = "{|let x = (1.5 * 2.0)->toString(); " + TABLE + "->filter(r|$r.ID->toOne()->toString() == $x)"
                    + "->from(f::RT);}";
            assertEquals(asWritten(c, host), engines(c, host), type + " " + host);
            // no other type's toString is a row
            String integer = "|" + TABLE + "->extend(~a: r|$r.ID->toOne()->toString())->from(f::RT)";
            assertEquals(asWritten(c, integer), engines(c, integer), type + " " + integer);
        }
    }

    /** A Float the store holds as an integer (the corpus's validation showcase: a Float property over an INT column,
     *  {@code Quantity not in range: 1000000}): the row casts the column as it is held, so the database writes its
     *  digits; the product's own rule reads it as a double first ({@code 1000000.0}). Until this step
     *  {@code FloatTextTest.anIntegerHeldFloatsTextAtTheEdgeIsLegendEnginesCast} held the edge's pass to the same. */
    @Test
    void aFloatHeldAsAnIntegerIsCastAsItIsHeld() {
        for (DatabaseType type : PureText.ENGINE_DATABASES) {
            ModelContext c = Compiler.compileModel(model(type));
            String query = "|f::Trade.all()->project([t|$t.qty->toString()], ['q'])->from(f::M, f::RT)";
            String sql = engines(c, query);
            assertNotEquals(asWritten(c, query), sql, type + " " + query);
            // the row's statement is exactly the one that prints the same column read as the Integer it is held as:
            // one cast of the column to text (the independent review of 2026-11-26; until this step
            // FloatTextTest held the pass to the render of that cast)
            String asHeld = "|f::Trade.all()->project([t|$t.held->toString()], ['q'])->from(f::M, f::RT)";
            assertEquals(engines(c, asHeld), sql, type + " " + query);
            assertFalse(sql.toUpperCase(java.util.Locale.ROOT).contains("DOUBLE"), type + ": " + sql);
        }
    }
}
