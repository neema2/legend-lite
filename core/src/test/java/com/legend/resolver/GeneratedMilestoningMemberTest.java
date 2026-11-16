// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.Compiler;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * THE MEMBERS REAL PURE GENERATES ON A TEMPORAL CLASS at every lookup (2026-11-14, the Studio roundtrip's
 * CORE-class-generated-milestoning-properties case): businessDate, processingDate and milestoning are properties of a
 * temporal class to the mapping checks (KnowledgeLayer) and to the constructor check (NewChecker), from the one rule the
 * typer and graph trees already read (Temporal.generatedMember) -- so a relational mapping may map them and a
 * constructor may set them, as legend-engine 4.145.0 allows -- without being stored on the class, so a constructor
 * never demands them. And mapping one explicitly leaves the query's temporal semantics as they were: the milestoning
 * filter is the same SQL.
 */
class GeneratedMilestoningMemberTest {

    private static String model(String explicit) {
        return """
                Class <<temporal.businesstemporal>> t::Business { name: String[1]; }
                ###Relational
                Database t::DB (
                  Table T (
                    milestoning( business(BUS_FROM=from_z, BUS_THRU=thru_z) )
                    ID INTEGER PRIMARY KEY, NAME VARCHAR(64), AS_OF DATE, from_z DATE, thru_z DATE )
                )
                ###Mapping
                Mapping t::M (
                  *t::Business : Relational { ~mainTable [t::DB] T
                    name: [t::DB] T.NAME%s }
                )
                ###Runtime
                Runtime t::RT { mappings: [t::M]; }
                """.formatted(explicit);
    }

    private static final String UNMAPPED = model("");
    private static final String MAPPED = model(",\n    businessDate: [t::DB] T.AS_OF");

    /** The Studio case's shape: each temporal stereotype's generated dates mapped to a column, and constructors that
     *  set the generated date or leave it out. */
    private static final String STUDIO_SHAPE = """
            Class <<meta::pure::profiles::temporal.processingtemporal>> model::Processing { }
            Class <<meta::pure::profiles::temporal.businesstemporal>> model::Business { name: String[0..1]; }
            Class <<meta::pure::profiles::temporal.bitemporal>> model::BiTemporal { }
            Class model::Sub extends model::Business { }
            ###Relational
            Database model::TestDatabase ( Schema TestSchema ( Table TestTable ( testDate DATE PRIMARY KEY ) ) )
            ###Mapping
            Mapping model::MilestoningMapping
            (
              *model::Business: Relational
              {
                ~mainTable [model::TestDatabase]TestSchema.TestTable
                businessDate: [model::TestDatabase]TestSchema.TestTable.testDate
              }
              *model::Processing: Relational
              {
                ~mainTable [model::TestDatabase]TestSchema.TestTable
                processingDate: [model::TestDatabase]TestSchema.TestTable.testDate
              }
              *model::BiTemporal: Relational
              {
                ~mainTable [model::TestDatabase]TestSchema.TestTable
                businessDate: [model::TestDatabase]TestSchema.TestTable.testDate,
                processingDate: [model::TestDatabase]TestSchema.TestTable.testDate
              }
            )
            ###Pure
            function model::unset(): Any[*] { ^model::Business(name='a') }
            function model::set(): Any[*] { ^model::Business(name='a', businessDate=%2020-01-01) }
            function model::inherited(): Any[*] { ^model::Sub(businessDate=%2020-01-01) }
            """;

    @Test
    void theGeneratedDatesAreMappableAndConstructibleButNeverRequired() {
        assertEquals(List.of(), Compiler.compileErrors(STUDIO_SHAPE));
    }

    /** A class that declares businessDate itself keeps its own declaration (the generated member never shadows it). */
    @Test
    void aDeclaredDateWinsOverTheGeneratedOne() {
        String declared = STUDIO_SHAPE.replace("model::Business { name: String[0..1]; }",
                "model::Business { name: String[0..1]; businessDate: StrictDate[1]; }");
        var ctx = Compiler.compileModel(declared);
        assertEquals("StrictDate", ctx.findProperty("model::Business", "businessDate").orElseThrow().type().typeName());
    }

    /** The rule is per axis and per stereotype: a business-temporal class has no processingDate, a plain class has no
     *  generated member at all -- mapping one stays the error it was. */
    @Test
    void anAxisTheClassDoesNotCarryIsStillNoProperty() {
        List<String> processingOnBusiness = Compiler.compileErrors(STUDIO_SHAPE.replace(
                "businessDate: [model::TestDatabase]TestSchema.TestTable.testDate\n  }",
                "processingDate: [model::TestDatabase]TestSchema.TestTable.testDate\n  }"));
        assertTrue(processingOnBusiness.stream().anyMatch(e -> e.contains(
                "PropertyMapping 'processingDate' references property not declared on class 'model::Business'")),
                processingOnBusiness.toString());
        List<String> plain = Compiler.compileErrors(STUDIO_SHAPE.replace(
                "<<meta::pure::profiles::temporal.businesstemporal>> model::Business", "model::Business"));
        assertTrue(plain.stream().anyMatch(e -> e.contains(
                "PropertyMapping 'businessDate' references property not declared on class 'model::Business'")),
                plain.toString());
    }

    /** Mapping businessDate explicitly does not touch the temporal filter: the dated fetch's SQL is the same. */
    @Test
    void anExplicitlyMappedBusinessDateLeavesTheMilestoningFilterUnchanged() {
        String q = "|t::Business.all(%2020-01-01)->project([x|$x.name], ['n'])->from(t::M, t::RT)";
        String unmapped = ResolvedSql.of(UNMAPPED, q);
        assertTrue(unmapped.contains("t0.from_z <= DATE '2020-01-01' AND t0.thru_z > DATE '2020-01-01'"), unmapped);
        assertEquals(unmapped, ResolvedSql.of(MAPPED, q));
        // a filter on the date keeps the same milestoning predicates beside it
        String f = "|t::Business.all(%2020-01-01)->filter(x|$x.businessDate == %2020-01-01)"
                + "->project([x|$x.name], ['n'])->from(t::M, t::RT)";
        for (String m : List.of(UNMAPPED, MAPPED)) {
            String sql = ResolvedSql.of(m, f);
            assertTrue(sql.contains("t0.from_z <= DATE '2020-01-01' AND t0.thru_z > DATE '2020-01-01'"), sql);
        }
    }

    /** Unmapped, the generated date reads the fetch's context date, as before the rule moved. */
    @Test
    void anUnmappedBusinessDateStillReadsTheContextDate() {
        String sql = ResolvedSql.of(UNMAPPED,
                "|t::Business.all(%2020-01-01)->project([x|$x.name, x|$x.businessDate], ['n', 'bd'])->from(t::M, t::RT)");
        assertTrue(sql.contains("DATE '2020-01-01' AS bd"), sql);
    }
}
