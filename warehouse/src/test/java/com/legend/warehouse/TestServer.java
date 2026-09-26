package com.legend.warehouse;

import com.legend.Nullable;
import com.legend.warehouse.server.Statements;
import com.legend.warehouse.server.WarehouseServer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The warehouse a test talks to: in this JVM, or, when {@code WAREHOUSE_BINARY} names an executable
 * (the native image, W1e), that executable started as a child process on a free port with the same
 * users and limits. Every test reaches it through HTTP only, so the same tests judge both.
 * {@code WAREHOUSE_DUCKDB_LIBRARY}, when set, is passed on as {@code --duckdb-library}.
 */
final class TestServer implements AutoCloseable {

    private static final Pattern READY = Pattern.compile("warehouse listening on 127\\.0\\.0\\.1:(\\d+)");

    private final @Nullable WarehouseServer inProcess;
    private final @Nullable Process process;
    private final int port;

    private TestServer(@Nullable WarehouseServer inProcess, @Nullable Process process, int port) {
        this.inProcess = inProcess;
        this.process = process;
        this.port = port;
    }

    /** Whether the tests are judging an external executable. */
    static boolean external() {
        return System.getenv("WAREHOUSE_BINARY") != null;
    }

    static TestServer start(Path data, List<String[]> users, Statements.Limits limits) throws Exception {
        String binary = System.getenv("WAREHOUSE_BINARY");
        if (binary == null) {
            WarehouseServer s = new WarehouseServer(new WarehouseServer.Config(0, data, List.of("main"), users, null,
                    Duration.ofMinutes(5), limits));
            return new TestServer(s, null, s.port());
        }
        List<String> cmd = new ArrayList<>(List.of(binary, "--port", "0", "--data", data.toString(),
                "--concurrency", Integer.toString(limits.concurrency()), "--queue", Integer.toString(limits.queue()),
                "--max-rows", Long.toString(limits.maxRows()), "--retain-minutes", Long.toString(limits.retain().toMinutes())));
        for (String[] u : users) {
            cmd.add("--user");
            cmd.add(u[0] + ":" + u[1]);
        }
        String library = System.getenv("WAREHOUSE_DUCKDB_LIBRARY");
        if (library != null) {
            cmd.add("--duckdb-library");
            cmd.add(library);
        }
        Process p = new ProcessBuilder(cmd).redirectOutput(ProcessBuilder.Redirect.INHERIT).start();
        BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8));
        String line;
        while ((line = err.readLine()) != null) {
            System.err.println("[warehouse] " + line);
            Matcher m = READY.matcher(line);
            if (m.find()) {
                Thread drain = new Thread(() -> {
                    try {
                        String l;
                        while ((l = err.readLine()) != null) System.err.println("[warehouse] " + l);
                    } catch (java.io.IOException ignored) {
                        // the process ended
                    }
                }, "warehouse-stderr");
                drain.setDaemon(true);
                drain.start();
                return new TestServer(null, p, Integer.parseInt(m.group(1)));
            }
        }
        throw new IllegalStateException(binary + " exited before listening (exit " + p.waitFor() + ")");
    }

    int port() {
        return port;
    }

    @Override
    public void close() throws Exception {
        if (inProcess != null) inProcess.close();
        Process p = process;
        if (p != null) {
            p.destroy();
            if (!p.waitFor(10, TimeUnit.SECONDS)) p.destroyForcibly().waitFor();
        }
    }
}
