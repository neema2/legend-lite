package com.legend.warehouse.server;

import com.legend.Nullable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.duckdb.DuckDBConnection;

/**
 * The warehouse's catalogs: in W1, one DuckDB database file per catalog,
 * owned by this process (W0 Q4: a writable file belongs to one process).
 * Behind this class so DuckLake (W0 Q5) can take its place for on-demand
 * readers without the API noticing.
 *
 * <p>Every statement gets its OWN connection, duplicated from the catalog's
 * root: DuckDB variables are per connection (W0), so one user's identity
 * can never be seen from another's statement.
 */
public final class Catalogs implements AutoCloseable {

    private static final Pattern NAME = Pattern.compile("[a-z][a-z0-9_]{0,62}");

    private final Path dataDir;
    private final Map<String, DuckDBConnection> roots = new TreeMap<>();

    public Catalogs(Path dataDir, List<String> names) throws SQLException {
        this.dataDir = dataDir;
        try {
            Files.createDirectories(dataDir);
        } catch (java.io.IOException e) {
            throw new SQLException("cannot create the data directory " + dataDir, e);
        }
        for (String n : names) open(n);
    }

    public static boolean validName(String name) {
        return NAME.matcher(name).matches();
    }

    private void open(String name) throws SQLException {
        if (!validName(name)) throw new IllegalArgumentException("bad catalog name: " + name);
        Connection c = DriverManager.getConnection("jdbc:duckdb:" + dataDir.resolve(name + ".duckdb"));
        roots.put(name, (DuckDBConnection) c);
    }

    public List<String> names() {
        return List.copyOf(roots.keySet());
    }

    /** A new connection to the catalog, or null when there is no such catalog. */
    public synchronized @Nullable Connection connect(String name) throws SQLException {
        DuckDBConnection root = roots.get(name);
        return root == null ? null : root.duplicate();
    }

    @Override
    public synchronized void close() throws SQLException {
        for (DuckDBConnection c : roots.values()) c.close();
        roots.clear();
    }
}
