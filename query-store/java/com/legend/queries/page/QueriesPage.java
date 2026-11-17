package com.legend.queries.page;

import com.legend.json.Json;
import com.legend.queries.MemoryCubeRecords;
import com.legend.queries.MemoryRecords;
import com.legend.queries.SavedCubes;
import com.legend.queries.SavedQueries;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * THE SAVED QUERIES IN THE PAGE (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B5), THE PRODUCT'S ENTRY (lite's API v1, H6,
 * the orchestrator's ruling of 2026-11-17: the page answers lite's own {@code /api/lite/v1/queries}, in lite's words,
 * {@link SavedQueries#LITES_WORDS}; until then the edge's, at legend-engine's path in its words, the separation census's
 * A3): {@link SavedQueries}' rules compiled to WebAssembly, the same rules legend-lite's server and Python's native library answer with. Each call is handed the
 * records the page keeps (its IndexedDB's) and answers what the rules answered with every query it wrote, which the
 * page's adapter ({@code query-store/src/wasm-server.ts}) writes back: the module keeps nothing between calls, so two
 * tabs over the one IndexedDB see each other's queries at their next call. DataCube's saved pages' rules
 * ({@link SavedCubes}, B6) are on the same module, so a page fetches it once for both stores.
 */
public final class QueriesPage {
    private QueriesPage() {}

    /**
     * One {@code /api/lite/v1/queries...} call ({@code rest}: the path after {@code /api/lite/v1/queries}) over
     * {@code records} ({@code {"id": [versions...]}}), as {@code user}: {@code {"status", "body", "changed"}}, JSON --
     * {@code changed} each query written, its versions as the JSON TEXT the page keeps (as the server's file holds it).
     */
    @org.teavm.jso.JSExport
    public static String answer(String method, String rest, String rawQuery, String body, String user, String records) {
        MemoryRecords held = MemoryRecords.of(records);
        SavedQueries.Answer a = SavedQueries.answer(new SavedQueries(held, System::currentTimeMillis), method, rest,
                rawQuery.isEmpty() ? null : rawQuery, body, user, SavedQueries.LITES_WORDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", a.status());
        out.put("body", a.json());
        Map<String, Object> changed = new LinkedHashMap<>();
        held.changed().forEach((id, versions) -> changed.put(id, Json.toCompact(versions)));
        out.put("changed", changed);
        return Json.toCompact(out);
    }

    /**
     * One {@code /api/lite/v1/queries/dataCube...} call ({@code rest}: the path after it, {@code rawQuery} its query
     * string, as {@link #answer}'s) over DataCube's saved pages,
     * {@code rows} the page's ({@code {"id": row}}: each record with its content as exact JSON text), as {@code user}:
     * {@code {"status", "body", "changed"}} (each page written, its row, or null for one removed), JSON (the program's
     * B6: DataCube's saved pages on the same module as the saved queries, fetched once for both).
     */
    @org.teavm.jso.JSExport
    public static String cubes(String method, String rest, String rawQuery, String body, String user, String rows) {
        MemoryCubeRecords held = MemoryCubeRecords.of(rows);
        SavedQueries.Answer a = SavedCubes.answer(new SavedCubes(held, System::currentTimeMillis), method, rest,
                rawQuery.isEmpty() ? null : rawQuery, body, user, SavedQueries.LITES_WORDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", a.status());
        out.put("body", a.json());
        out.put("changed", held.changed());
        return Json.toCompact(out);
    }

    public static void main(String[] args) {
        // TeaVM's entry point: the export above is the module's surface
    }
}
