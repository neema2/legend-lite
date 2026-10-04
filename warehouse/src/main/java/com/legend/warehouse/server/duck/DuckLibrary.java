package com.legend.warehouse.server.duck;

import com.legend.base.Nullable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Where DuckDB's native library is: given explicitly ({@code --duckdb-library}; started by Bazel, the server finds
 * //warehouse:duckdb_library in its runfiles), or, in a native image, beside the executable. Never extracted from
 * DuckDB's JDBC jar into a temporary directory (Bazel workplan P1-16): a JVM started without it fails, naming the
 * flag.
 */
public final class DuckLibrary {

    private DuckLibrary() {
    }

    /**
     * Loads DuckDB: from {@code explicit} when given; in a native image, from beside the executable (the image ships
     * with it). On the JVM there is no default: pass the library.
     */
    public static void load(@Nullable Path explicit) throws IOException {
        if (explicit != null) {
            Duck.load(explicit);
        } else if (nativeImage()) {
            Duck.load(besideExecutable());
        } else {
            throw new IOException("DuckDB's library is not given: pass --duckdb-library (" + resourceName()
                    + "; under Bazel, $(rlocationpath //warehouse:duckdb_library))");
        }
    }

    /** The library beside a native executable: under its platform name, or {@code libduckdb_java.so}. */
    private static Path besideExecutable() throws IOException {
        Path dir = executableDir("--duckdb-library");
        for (String name : new String[] {resourceName(), "libduckdb_java.so"}) {
            Path p = dir.resolve(name);
            if (Files.isRegularFile(p)) return p;
        }
        throw new IOException("DuckDB's library is not beside the executable in " + dir + " (" + resourceName()
                + " or libduckdb_java.so); pass --duckdb-library");
    }

    /** Whether this is a native image, whose DuckDB library and extensions ship beside it. */
    public static boolean nativeImage() {
        return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    /** The running executable's directory; {@code flag} is what to pass instead when it cannot be told. */
    public static Path executableDir(String flag) throws IOException {
        String command = ProcessHandle.current().info().command().orElseThrow(
                () -> new IOException("cannot tell where this executable is; pass " + flag));
        Path dir = Path.of(command).toAbsolutePath().getParent();
        if (dir == null) throw new IOException("no directory for " + command + "; pass " + flag);
        return dir;
    }

    /** DuckDB's version, as its drivers report it ("v1.5.5"). */
    public static String version() {
        return Duck.api().version();
    }

    /** The resource name DuckDB's JDBC jar uses for this platform: //warehouse:duckdb_library's file name. */
    public static String resourceName() {
        String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        boolean arm = arch.equals("aarch64") || arch.equals("arm64");
        if (os.contains("mac")) return "libduckdb_java.so_osx_universal";
        if (os.contains("linux")) return arm ? "libduckdb_java.so_linux_arm64" : "libduckdb_java.so_linux_amd64";
        if (os.contains("windows")) return "libduckdb_java.so_windows_amd64";
        throw new IllegalStateException("no DuckDB library for " + os + "/" + arch);
    }
}
