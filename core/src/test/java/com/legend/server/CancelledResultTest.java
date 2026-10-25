// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.server;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A result whose reading thread is interrupted -- its caller's {@code Future.cancel(true)} -- stops filling rows
 * ({@code Executor.stopIfCancelled}, the IQ scale program, 2026-10-25): a TPC-H-suite query of a cartesian join of
 * tens of millions of rows kept filling rows after its judge's timeout until the heap was gone. One not interrupted
 * reads every row.
 */
class CancelledResultTest {

    private static final int ROWS = 3000;

    private static final String MODEL = """
            Class proj::Item { id: Integer[1]; }
            ###Relational
            Database proj::ItemDB ( Schema S ( Table ITEM (ID INTEGER PRIMARY KEY) ) )
            ###Mapping
            Mapping proj::ItemMapping
            (
              *proj::Item: Relational
              {
                ~primaryKey ([proj::ItemDB] S.ITEM.ID)
                ~mainTable [proj::ItemDB] S.ITEM
                id: [proj::ItemDB] S.ITEM.ID
              }
            )
            ###Connection
            RelationalDatabaseConnection proj::Conn { store: proj::ItemDB; type: DuckDB; specification: DuckDB { }; auth: Test; }
            ###Runtime
            Runtime proj::RT { mappings: [ proj::ItemMapping ]; connections: [ proj::ItemDB: [ db: proj::Conn ] ]; }
            ###Data
            Data proj::ItemData
            {
              Relational
              #{
                S.ITEM:
                  'ID\\n%s\\n';
              }#
            }
            """.formatted(IntStream.rangeClosed(1, ROWS).mapToObj(String::valueOf).collect(Collectors.joining("\\n")));

    private static final String QUERY = "|proj::Item.all()->project(~[id: i|$i.id])";

    @Test
    void anInterruptedReadStopsAndOneNotInterruptedReadsEveryRow() throws Exception {
        QueryService service = new QueryService();
        assertEquals(ROWS, service.execute(MODEL, QUERY, "proj::RT").rows().size());
        Throwable[] thrown = new Throwable[1];
        Thread reader = new Thread(() -> {
            Thread.currentThread().interrupt();
            try {
                service.execute(MODEL, QUERY, "proj::RT");
            } catch (Throwable t) {
                thrown[0] = t;
            }
        });
        reader.start();
        reader.join();
        CancellationException stopped = assertThrows(CancellationException.class, () -> {
            throw thrown[0];
        });
        assertTrue(stopped.getMessage().startsWith("the query was cancelled after 1024 rows"), stopped.getMessage());
        assertFalse(Thread.currentThread().isInterrupted());
    }
}
