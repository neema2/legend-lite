package apppostgres;

import com.legend.testing.EmbeddedPostgres;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The Postgres {@code //datacube:verify_app_test} drives (Bazel workplan P1-14b): Postgres 16 from the
 * pinned binaries ({@code @embedded_postgres}, through {@link EmbeddedPostgres}) on a free loopback port,
 * loaded with the guide's sample ({@code datacube/demo/sample-shop.sql}) over JDBC. No {@code psql} and no
 * Postgres of the host's.
 *
 * <p>It prints {@code postgres port <n>} once the sample is loaded, then runs until its stdin closes, which
 * is how the test stops it; Postgres stops with the JVM.
 *
 * <p>The sample is psql input. The only meta-command understood is {@code \c <database>}: the statements
 * before it run in {@code postgres}, the ones after it in that database. Any other meta-command fails.
 */
public final class AppPostgres {

    private AppPostgres() {
    }

    public static void main(String[] args) throws IOException, SQLException {
        if (args.length != 1) {
            System.err.println("usage: app_postgres <sample.sql>");
            System.exit(2);
        }
        String sql = Files.readString(Path.of(args[0]), StandardCharsets.UTF_8);
        EmbeddedPostgres pg = EmbeddedPostgres.shared();
        String database = "postgres";
        StringBuilder statements = new StringBuilder();
        for (String line : sql.split("\r?\n", -1)) {
            if (!line.startsWith("\\")) {
                statements.append(line).append('\n');
                continue;
            }
            String[] words = line.trim().split("\\s+");
            if (words.length != 2 || !(words[0].equals("\\c") || words[0].equals("\\connect"))) {
                throw new IllegalArgumentException("only \\c <database> is understood, not: " + line);
            }
            run(pg, database, statements.toString());
            statements.setLength(0);
            database = words[1];
        }
        run(pg, database, statements.toString());
        System.out.println("postgres port " + pg.port());
        System.out.flush();
        // until the test closes stdin
        while (System.in.read() != -1) {
            // nothing is read from it
        }
    }

    /** One round trip: the statements as one simple query (a CREATE DATABASE must be alone in its own). */
    private static void run(EmbeddedPostgres pg, String database, String statements) throws SQLException {
        if (statements.isBlank()) {
            return;
        }
        try (Connection c = DriverManager.getConnection(pg.jdbcUrl(database));
                Statement s = c.createStatement()) {
            s.execute(statements);
        }
    }
}
