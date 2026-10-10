package conformance;

import org.teavm.jso.JSExport;

/**
 * The conformance module's entry class (//wasm:conformance_module): TeaVM compiles {@link Families} through these two
 * exports; //wasm:conformance_jvm runs the same families on the JVM ({@link ConformanceJvm}), and conformance.mjs
 * compares the two, family by family, line by line.
 */
public final class ConformanceExports {

    private ConformanceExports() {
    }

    /** The families, one per line. */
    @JSExport
    public static String families() {
        return String.join("\n", Families.NAMES);
    }

    /** One family's answers ({@link Out}'s lines). */
    @JSExport
    public static String answers(String family) {
        return Families.answers(family);
    }

    public static void main(String[] args) {
        // the module's exports are its API; nothing runs on load
    }
}
