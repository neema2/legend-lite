package com.legend.tools.gunzip;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

/** {@code Gunzip IN.gz OUT}: a build action's gunzip on the JDK alone, no shell and no host gzip (Bazel workplan P1-17). */
public final class Gunzip {

    private Gunzip() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) throw new IllegalArgumentException("usage: Gunzip IN.gz OUT");
        Path out = Path.of(args[1]);
        Path parent = out.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);
        try (InputStream in = new GZIPInputStream(Files.newInputStream(Path.of(args[0])), 1 << 16);
                OutputStream o = Files.newOutputStream(out)) {
            in.transferTo(o);
        }
    }
}
