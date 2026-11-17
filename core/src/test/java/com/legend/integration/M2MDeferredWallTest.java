package com.legend.integration;

import com.legend.testing.LaneFloor;
import com.legend.testing.Lean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * An M2M binding whose composition walls PER KEY (ledger cluster 21, {@code ClassSource.throwIfDeferred}) raises its
 * own wall when a graph fetch's tree names the property. Until 2026-11-14 the tree's leaf read the missing binding as
 * an unmapped property: a {@code [0..1]} property serialized as null (a silent wrong answer: legend-engine answers
 * {@code "B!"} for this model) and a {@code [1]} property as "not mapped" (AGENTS.md invariant 4).
 */
@DisplayName("M2M deferred binding walls at a graph fetch's leaf")
@org.junit.jupiter.api.extension.ExtendWith(LaneFloor.class)
class M2MDeferredWallTest {

    private static final String MODEL = """
            ###Pure
            Class t::Src { k: String[1]; code: String[0..1]; }
            Class t::Tgt { k: String[1]; raw: String[0..1]; }
            ###Mapping
            Mapping t::M
            (
              t::Tgt: Pure { ~src t::Src k: $src.k, raw: $src.code->map(v | $v + '!') }
            )
            ###Relational
            Database store::TestDb ( )
            ###Connection
            RelationalDatabaseConnection store::TestConnection { type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime t::R
            {
              mappings: [ t::M ];
              connections: [ store::TestDb: [ env: store::TestConnection ], ModelStore: [ c: #{ JsonModelConnection { class: t::Src; url: 'data:application/json,[{"k":"a","code":"B"}]'; } }# ] ];
            }
            """;

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = LaneFloor.backend().open();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    @DisplayName("a walled [0..1] binding raises its wall, never serializes null")
    void aWalledOptionalBindingRaisesItsWall() {
        Throwable t = assertThrows(Throwable.class, () -> Lean.run(MODEL,
                "t::Tgt.all()->graphFetch(#{t::Tgt{k,raw}}#)->serialize(#{t::Tgt{k,raw}}#)->from(t::M, t::R)",
                null, connection));
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            all.append(c.getMessage()).append(" | ");
        }
        assertTrue(all.toString().contains("is not substitutable yet"), all.toString());
    }

    @Test
    @DisplayName("a tree that does not name the walled property runs")
    void aTreeWithoutTheWalledPropertyRuns() {
        assertEquals("[{\"k\":\"a\"}]", Lean.run(MODEL,
                "t::Tgt.all()->graphFetch(#{t::Tgt{k}}#)->serialize(#{t::Tgt{k}}#)->from(t::M, t::R)",
                null, connection).json());
    }
}
