package com.legend.sdlc.page;

import com.legend.json.Json;
import com.legend.sdlc.CoreGrammar;
import com.legend.sdlc.MemoryStorage;
import com.legend.sdlc.Sdlc;

import java.util.ArrayList;
import java.util.List;

/**
 * THE SDLC IN THE PAGE (design S21, level 0): {@link Sdlc}'s rules compiled to WebAssembly, over memory
 * the page persists. The page's adapter ({@code project-store/src/wasm-server.ts}) turns each
 * {@code fetch} into {@link #handle}, saves {@link #changes} to IndexedDB after it, and {@link #load}s
 * them back when the page opens. One call at a time: the module is single-threaded.
 */
public final class SdlcPage {
    private SdlcPage() {}

    private static MemoryStorage storage = new MemoryStorage();
    private static Sdlc sdlc = make("local", "Local User");

    private static Sdlc make(String userId, String name) {
        return new Sdlc(storage, userId, name, new CoreGrammar(), System::currentTimeMillis);
    }

    /** Who the page's user is (there is no sign-in here); keeps what is loaded. */
    @org.teavm.jso.JSExport
    public static void start(String userId, String name) {
        sdlc = make(userId, name);
    }

    /** Restores one persisted record. */
    @org.teavm.jso.JSExport
    public static void load(String key, String value) {
        storage.load(key, value);
    }

    /** Forgets everything (a test's fresh store). */
    @org.teavm.jso.JSExport
    public static void reset() {
        storage = new MemoryStorage();
        sdlc = make("local", "Local User");
    }

    /**
     * One request under the API root ({@code /projects/x?limit=1}): {@code "<status>\n<body>"}, the body
     * empty for a 204. {@code body} is the request's text, empty for none.
     */
    @org.teavm.jso.JSExport
    public static String handle(String method, String target, String body) {
        Sdlc.Response r = sdlc.handle(method, target, body.isEmpty() ? null : body);
        return r.status() + "\n" + (r.body() == null ? "" : r.body());
    }

    /** What changed since the last call, to persist: {@code [[key, value-or-null], ...]}. */
    @org.teavm.jso.JSExport
    public static String changes() {
        List<Object> out = new ArrayList<>();
        for (String key : storage.drainChanges()) out.add(java.util.Arrays.asList(key, storage.get(key)));
        return Json.toCompact(out);
    }

    public static void main(String[] args) {
        // TeaVM's entry point: the exports above are the module's surface
    }
}
