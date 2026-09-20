package com.legend;

import com.legend.compiler.element.ModelContext;
import com.legend.sql.OutputCol;
import com.legend.sql.SqlExpr;
import com.legend.sql.SqlSelect;
import com.legend.sql.SqlSource;
import com.legend.sql.SqlType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code TabularFunction} — a named relation that is a FUNCTION CALL.
 *
 * <p>The grammar accepted this, the protocol round-tripped it, and then
 * a reference to one failed with "unknown table": the parser admitted
 * something the compiler denied existed, which is the worst of both.
 * These pin the whole path — resolve, type, lower, render.
 *
 * <p>Upstream models it the same way ({@code TabularFunction} extends
 * {@code NamedRelation} beside {@code Table}, with a per-dialect
 * {@code tabularFunctionProcessor}), and renders it the same way:
 * DuckDB's processor emits {@code schema.fn(params)}. Matching that
 * spelling is deliberate — a divergence here would be a compatibility
 * bug, not a feature.
 *
 * <p>NO ARGUMENTS yet, because upstream's grammar declares columns only
 * and its own examples render as {@code Org.Person()}. Arguments are
 * where a file URL would eventually go; adding them before upstream
 * does would be inventing a dialect of the model.
 */
class TabularFunctionTest {

    private static final String MODEL = """
            ###Relational
            Database tf::DB
            (
              Table PLAIN ( a VARCHAR(64), n DOUBLE )
              TabularFunction FN ( a VARCHAR(64), n DOUBLE )
            )
            """;

    @Test
    void aTabularFunctionResolvesLikeATable() {
        // Same lookup, because both are named relations with declared
        // columns; the TYPE is identical and only the source differs.
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertTrue(ctx.findTable("tf::DB", "FN").isPresent(),
                "a declared TabularFunction must resolve");
        assertTrue(ctx.findTable("tf::DB", "PLAIN").isPresent());
    }

    @Test
    void andIsDistinguishableFromATable() {
        // The distinction has to survive to lowering, because it is the
        // only thing that decides FN() against FN.
        ModelContext ctx = Compiler.compileModel(MODEL);
        assertTrue(ctx.isTabularFunction("tf::DB", "FN"));
        assertFalse(ctx.isTabularFunction("tf::DB", "PLAIN"));
    }

    @Test
    void itRendersAsACallRatherThanAReference() {
        String sql = Compiler.plan(MODEL, "#>{tf::DB.FN}#->select(~[a, n])",
                null).sql();
        assertTrue(sql.contains("FROM FN()"),
                "a tabular function is CALLED: " + sql);
    }

    @Test
    void aPlainTableStillRendersAsAReference() {
        // The guard against the obvious regression: every table
        // acquiring parentheses.
        String sql = Compiler.plan(MODEL, "#>{tf::DB.PLAIN}#->select(~[a, n])",
                null).sql();
        assertTrue(sql.contains("FROM PLAIN"), sql);
        assertFalse(sql.contains("PLAIN()"), sql);
    }

    @Test
    void theRendererCanCarryACTUALARGUMENTS() {
        // UPSTREAM CANNOT AUTHOR THESE. Its metamodel declares
        // `parameters : FunctionParameter[*]`, but the grammar rule
        // accepts column definitions only --
        //
        //   tabularFunction: TABULAR_FUNC relationalIdentifier
        //                    PAREN_OPEN (columnDefinition (COMMA
        //                    columnDefinition)*)? PAREN_CLOSE
        //
        // -- and the protocol class carries `//TODO params ?` with no
        // field at all. So a parameterised call is unreachable from
        // Pure text or from the wire, which is why the lowering emits
        // none and our SQL matches upstream's byte for byte.
        //
        // The SOURCE still carries arguments as structured SqlExpr
        // rather than text, so the day they become authorable nothing
        // here has to change and no literal is ever pasted into SQL.
        // This test is the proof that the path works, exercised at the
        // only layer that can currently express it.
        SqlSource.TableFunction fn = new SqlSource.TableFunction(
                "read_parquet",
                List.of(new SqlExpr.StringLit("s3://bucket/t.parquet")),
                "t0",
                List.of(new OutputCol("a", SqlType.Scalar.VARCHAR, true)));
        String sql = new com.legend.sql.dialect.DuckDb()
                .render(SqlSelect.starOf(fn));
        assertTrue(sql.contains("read_parquet("), sql);
        assertTrue(sql.contains("s3://bucket/t.parquet"), sql);
        assertTrue(sql.contains("AS \"t0\"") || sql.contains("AS t0"), sql);
    }

    @Test
    void anArgumentIsESCAPED_notPasted() {
        // The reason arguments are SqlExpr and not String: a value with
        // a quote in it must not be able to close the literal. This is
        // the same class of bug as the Pure escaping fixed elsewhere.
        SqlSource.TableFunction fn = new SqlSource.TableFunction(
                "read_parquet",
                List.of(new SqlExpr.StringLit("it's.parquet")),
                "t0",
                List.of(new OutputCol("a", SqlType.Scalar.VARCHAR, true)));
        String sql = new com.legend.sql.dialect.DuckDb()
                .render(SqlSelect.starOf(fn));
        assertTrue(sql.contains("it''s.parquet"),
                "SQL doubles the quote: " + sql);
    }

    @Test
    void itSurvivesGroupingAndFiltering() {
        // Lowering rebuilds the typed node on the way through, and the
        // flag has to survive that -- it did not at first, and the
        // query planned as FROM FN, silently back to a table.
        String sql = Compiler.plan(MODEL,
                "#>{tf::DB.FN}#->filter(x|$x.a == 'EMEA')->select(~[a, n])"
                + "->groupBy(~[a], ~[m:x|$x.n:y|$y->sum()])",
                null).sql();
        assertTrue(sql.contains("FROM FN()"), sql);
        assertTrue(sql.contains("GROUP BY"), sql);
    }
}
