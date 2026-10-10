package conformance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * The JVM half of the conformance differential, a build action (//wasm:conformance_jvm): every family's answers on the
 * JDK, as {@code <<<family>>>\n<lines><<<END>>>\n} blocks. TeaVM never compiles this class (it is not reachable from
 * {@link ConformanceExports}), so it may write a file.
 */
public final class ConformanceJvm {

    private ConformanceJvm() {
    }

    /** Usage: {@code ConformanceJvm <out-file>}. */
    public static void main(String[] args) throws IOException {
        StringBuilder out = new StringBuilder();
        for (String family : Families.NAMES) {
            out.append("<<<").append(family).append(">>>\n").append(Families.answers(family)).append("<<<END>>>\n");
        }
        Files.writeString(Path.of(args[0]), out.toString(), StandardCharsets.UTF_8);
    }
}
