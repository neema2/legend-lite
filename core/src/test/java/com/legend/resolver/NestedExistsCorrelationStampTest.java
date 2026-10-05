// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import com.legend.Execution;
import com.legend.compiler.NameResolver;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedFilter;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.testing.Own;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A nested exists keeps the mapping-join semantics of its inner correlation: the join
 * condition is the mapping's own, lowered as plain '=', so a NULL key never matches a NULL
 * key. The resolver stamps the EXISTS relation CORRELATION (Substitution.correlateTarget);
 * the outer scope's re-pass used to rebuild it without the stamp (the relation-material
 * TypedFilter arm of Substitution.rewrite), and the lowerer then emitted IS NOT DISTINCT
 * FROM: {@code [BETA]} for the nested query below, whose NULL keys must not match. Rebuild
 * W0.6 push 3: a rebuild keeps the stamp ({@code TypedFilter.rebuilt}); the constructor
 * that defaulted it is gone.
 */
class NestedExistsCorrelationStampTest {

    private static final String MODEL = """
            Class m::Firm { legal: String[1]; }
            Class m::Person { name: String[1]; }
            Class m::Car { make: String[1]; }
            Association m::Emp { employer: m::Firm[0..1]; staff: m::Person[*]; }
            Association m::Own { owner: m::Person[0..1]; cars: m::Car[*]; }
            ###Relational
            Database s::DB (
              Table F (ID INTEGER, LEGAL VARCHAR(50))
              Table P (ID INTEGER, FID INTEGER, NAME VARCHAR(50))
              Table C (PID INTEGER, MAKE VARCHAR(50))
              Join PF (P.FID = F.ID)
              Join PC (P.ID = C.PID)
            )
            ###Mapping
            Mapping m::M (
              *m::Firm: Relational { ~mainTable [s::DB] F legal: F.LEGAL }
              *m::Person: Relational { ~mainTable [s::DB] P name: P.NAME }
              *m::Car: Relational { ~mainTable [s::DB] C make: C.MAKE }
              m::Emp: Relational { AssociationMapping ( employer: [s::DB] @PF, staff: [s::DB] @PF ) }
              m::Own: Relational { AssociationMapping ( owner: [s::DB] @PC, cars: [s::DB] @PC ) }
            )
            ###Connection
            RelationalDatabaseConnection m::Conn { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime m::RT { mappings: [m::M]; connections: [ s::DB: [ c1: m::Conn ] ]; }
            """;

    private static final String NESTED = "m::Firm.all()->filter(f|$f.staff->exists(s|"
            + "$s.cars->exists(c|$c.make == 'VW')))->project(~[legal: f|$f.legal])";
    private static final String SINGLE = "m::Person.all()->filter(s|$s.cars->exists(c|"
            + "$c.make == 'VW'))->project(~[name: s|$s.name])";

    /** Nul has a NULL id; the VW has a NULL owner key: under '=' they are not related. */
    private static List<String> rows(String query) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:")) {
            try (Statement st = c.createStatement()) {
                st.execute("CREATE TABLE F (ID INTEGER, LEGAL VARCHAR)");
                st.execute("INSERT INTO F VALUES (1, 'ACME'), (2, 'BETA')");
                st.execute("CREATE TABLE P (ID INTEGER, FID INTEGER, NAME VARCHAR)");
                st.execute("INSERT INTO P VALUES (10, 1, 'Ann'), (NULL, 2, 'Nul')");
                st.execute("CREATE TABLE C (PID INTEGER, MAKE VARCHAR)");
                st.execute("INSERT INTO C VALUES (10, 'BMW'), (NULL, 'VW')");
            }
            var r = Execution.execute(MODEL, query, "m::RT", c);
            List<String> out = new ArrayList<>();
            for (var row : r.rows()) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < r.columns().size(); i++) {
                    sb.append(i == 0 ? "" : "|").append(row.get(i));
                }
                out.add(sb.toString());
            }
            return out;
        }
    }

    /** The stamp of every relation that is the direct first argument of an exists call. */
    private static List<TypedFilter.Stamp> existsRelationStamps(String query) {
        var ctx = Compiler.compileModel(MODEL);
        SpecCompiler specs = new SpecCompiler(ctx);
        List<TypedSpec> body = specs.typeQueryBody(
                NameResolver.resolveQuery(Own.spec(query + "->from(m::RT)")));
        List<TypedFilter.Stamp> out = new ArrayList<>();
        for (TypedSpec r : new StoreResolver(ctx, specs).resolve(body, null)) {
            collect(r, out);
        }
        return out;
    }

    private static void collect(TypedSpec n, List<TypedFilter.Stamp> out) {
        if (n instanceof TypedNativeCall c
                && c.callee().qualifiedName().equals("meta::pure::functions::collection::exists")
                && !c.args().isEmpty() && c.args().get(0) instanceof TypedFilter f) {
            out.add(f.stamp());
        }
        for (TypedSpec k : n.children()) {
            collect(k, out);
        }
    }

    @Test
    void singleLevelExistsDoesNotMatchNullKeys() throws Exception {
        assertEquals(List.of(), rows(SINGLE));
        assertEquals(List.of(TypedFilter.Stamp.CORRELATION), existsRelationStamps(SINGLE));
    }

    @Test
    void nestedExistsDoesNotMatchNullKeys() throws Exception {
        assertEquals(List.of(TypedFilter.Stamp.CORRELATION, TypedFilter.Stamp.CORRELATION),
                existsRelationStamps(NESTED));
        String sql = Compiler.query(Compiler.compileModel(MODEL), NESTED).plan("m::RT").sql();
        assertFalse(sql.contains("IS NOT DISTINCT FROM"), sql);
        // was [BETA]
        assertEquals(List.of(), rows(NESTED));
    }

    @Test
    void nestedExistsStillFindsARealMatch() throws Exception {
        // Ann (id 10, firm 1) owns the BMW: ACME has a BMW-owning employee, BETA has none
        assertEquals(List.of("ACME"), rows("m::Firm.all()->filter(f|$f.staff->exists(s|"
                + "$s.cars->exists(c|$c.make == 'BMW')))->project(~[legal: f|$f.legal])"));
    }

    @Test
    void aChainFilterInsideTheNestedExistsKeepsItsOwnStamp() throws Exception {
        // the user's chain filter is NONE; the exists relation under it is CORRELATION
        String q = "m::Firm.all()->filter(f|$f.staff->exists(s|"
                + "$s.cars->filter(c|$c.make != 'BMW')->exists(c|$c.make == 'VW')))->project(~[legal: f|$f.legal])";
        assertEquals(List.of(), rows(q));
        assertFalse(Compiler.query(Compiler.compileModel(MODEL), q).plan("m::RT").sql().contains("IS NOT DISTINCT FROM"));
    }
}
