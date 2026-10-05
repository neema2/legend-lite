package com.legend.sdlc;

import com.legend.base.Nullable;

import java.util.List;

/**
 * Where an SDLC keeps what it knows, as keyed text records: git objects ({@code obj/<id>}), refs
 * ({@code ref/<project>/<name>}: the line, workspaces, and {@code version/<v>} tags), project records
 * ({@code project/<id>}), and the SDLC's own records ({@code review/<project>/<id>},
 * {@code note/<project>/<version>}, {@code seq/<project>/<counter>}). The rules ({@link Sdlc})
 * never see more than this, so the same rules run over memory (a test, the page's WebAssembly module,
 * whose records the page keeps in IndexedDB) or over a real repository (the server's git backend).
 * Single-threaded: a caller serializes calls.
 */
public interface Storage {
    @Nullable String get(String key);

    void put(String key, String value);

    void delete(String key);

    /** The keys starting with {@code prefix}, in order. */
    List<String> keys(String prefix);
}
