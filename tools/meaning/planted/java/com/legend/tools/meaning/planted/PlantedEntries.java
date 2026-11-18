// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning.planted;

/** PLANTED (census B2, M7): a product entry per Java host -- the server, the tab, the native library, the queries page --
 *  each importing an edge class at load. M7's class reach from each must find the edge. */
public final class PlantedEntries {

    private PlantedEntries() {
    }

    /** The server's entry. */
    public static final class Server {
        public static void main(String[] args) {
            System.out.println(new com.legend.tools.meaning.planted.edge.PlantedEdgeRoutes().mount());
        }
    }

    /** The tab's module entry. */
    public static final class Tab {
        public static String plan(String query) {
            return com.legend.tools.meaning.planted.edge.PlantedEdgeRoutes.answer(query);
        }
    }

    /** The native library's entry. */
    public static final class Native {
        public static Object routes() {
            return com.legend.tools.meaning.planted.edge.PlantedEdgeRoutes.class;
        }
    }

    /** The queries page's entry. */
    public static final class QueriesPage {
        public static com.legend.tools.meaning.planted.edge.PlantedEdgeRoutes routes() {
            return null;
        }
    }
}
