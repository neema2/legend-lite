// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.setup;

import com.legend.sql.SqlDdl;

import com.legend.testbackend.TestBackend;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * A seed's block names its table by schema and table APART, so a declared name holding a dot is itself (stream S,
 * 2026-11-14, the orchestrator's item 5). Until then the seed looked its table up by the dotted spelling
 * {@code schema + "." + table}, which the table index split at its FIRST dot: the block {@code s.t} / {@code x} found
 * schema {@code s}'s table {@code "t.x"} -- the seed then made that table in schema {@code s.t} and loaded its rows
 * into {@code s.t.x}, the CSV's spelling, a table it had not made.
 */
class DottedNamesSeedTest {

    private static final String MODEL = """
            ###Relational
            Database x::DB
            (
              Schema s ( Table "t.x" (A INTEGER) )
              Schema "s.t" ( Table x (B INTEGER) )
            )
            """;

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void aBlockFindsTheTableItsSchemaAndTableNameApart(TestBackend backend) {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        List<CsvSeed.Step> steps = CsvSeed.steps("s.t\nx\nB\n1\n", "x::DB", ctx, backend.dialect());
        SqlDdl.CreateTable made = steps.stream()
                .filter(st -> st instanceof CsvSeed.Step.Schema s && s.ddl() instanceof SqlDdl.CreateTable)
                .map(st -> (SqlDdl.CreateTable) ((CsvSeed.Step.Schema) st).ddl()).findFirst().orElseThrow();
        assertEquals("s.t", made.schema());
        assertEquals("x", made.table());
        assertEquals(List.of("B"), made.columns().stream().map(SqlDdl.Column::name).toList());
        RowLoad rows = steps.stream().filter(st -> st instanceof CsvSeed.Step.Rows)
                .map(st -> ((CsvSeed.Step.Rows) st).load()).findFirst().orElseThrow();
        assertEquals("s.t", rows.schema());
        assertEquals("x", rows.table());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.legend.testbackend.TestBackends#inProcess")
    void theOtherTableIsFoundByItsOwnNames(TestBackend backend) {
        var ctx = com.legend.Compiler.compileModel(MODEL);
        List<CsvSeed.Step> steps = CsvSeed.steps("s\nt.x\nA\n1\n", "x::DB", ctx, backend.dialect());
        RowLoad rows = steps.stream().filter(st -> st instanceof CsvSeed.Step.Rows)
                .map(st -> ((CsvSeed.Step.Rows) st).load()).findFirst().orElseThrow();
        assertEquals("s", rows.schema());
        assertEquals("t.x", rows.table());
        assertEquals(List.of("A"), rows.columns());
    }
}
