package com.legend.warehouse.server;

import com.legend.base.Nullable;
import com.legend.warehouse.server.duck.Conn;
import com.legend.warehouse.server.duck.Database;
import com.legend.warehouse.server.duck.DuckException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * The warehouse's catalogs: in W1, one DuckDB database file per catalog,
 * owned by this process (W0 Q4: a writable file belongs to one process),
 * opened through DuckDB's C API. Behind this class so DuckLake (W0 Q5) can
 * take its place for on-demand readers without the API noticing.
 *
 * <p>Every connection belongs to one principal: what
 * {@code system.main.authenticated_user()} answers on it, from outside SQL.
 */
public final class Catalogs implements AutoCloseable {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");

    private final Path dataDir;
    private final Map<String, Database> databases = new TreeMap<>();

    public Catalogs(Path dataDir, List<String> names) throws IOException, DuckException {
        this.dataDir = dataDir;
        Files.createDirectories(dataDir);
        Files.createDirectories(dataDir.resolve("import"));
        for (String n : names) open(n);
    }

    public static boolean validName(String name) {
        return NAME.matcher(name).matches();
    }

    private void open(String name) throws DuckException {
        if (!validName(name)) throw new IllegalArgumentException("bad catalog name: " + name);
        Database db = Database.open(dataDir.resolve(name + ".duckdb"));
        db.lockDown(importDir());
        databases.put(name, db);
    }

    /** Where an owner puts files to load: the one directory a catalog may read files from. */
    public Path importDir() {
        return dataDir.resolve("import");
    }

    public List<String> names() {
        return List.copyOf(databases.keySet());
    }

    /** A new connection to the catalog for {@code principal}, or null when there is no such catalog. */
    public @Nullable Conn connect(String catalog, String principal) throws DuckException {
        Database db;
        synchronized (this) {
            db = databases.get(catalog);
        }
        return db == null ? null : db.connect(principal);
    }

    @Override
    public synchronized void close() {
        for (Database db : databases.values()) db.close();
        databases.clear();
    }
}
