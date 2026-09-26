package com.legend.warehouse.client.jdbc;

import com.legend.Nullable;
import com.legend.warehouse.client.WarehouseClient;
import com.legend.warehouse.sqlapi.NativeBinding;
import com.legend.warehouse.sqlapi.SqlApi;
import com.legend.warehouse.sqlapi.SqlApi.StatementRequest;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC over the warehouse's HTTP SQL API:
 * {@code jdbc:warehouse:http://host:port/catalog?user=u&password=p}
 * (or user and password as connection properties). Found by
 * ServiceLoader, like any driver.
 */
public final class WhDriver implements Driver {

    public static final String PREFIX = "jdbc:warehouse:";

    // JDBC's service loading only INSTANTIATES a driver; registering is the
    // driver's own job, in its static initializer, as every driver does it.
    static {
        try {
            java.sql.DriverManager.registerDriver(new WhDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public @Nullable Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) return null;
        URI uri = URI.create(url.substring(PREFIX.length()));
        String path = uri.getPath() == null ? "" : uri.getPath().replaceAll("^/+|/+$", "");
        String catalog = path.isEmpty() ? StatementRequest.DEFAULT_CATALOG : path;
        String user = info.getProperty("user");
        String password = info.getProperty("password");
        String query = uri.getRawQuery();
        if (query != null) {
            for (String part : query.split("&")) {
                int eq = part.indexOf('=');
                if (eq <= 0) continue;
                String k = part.substring(0, eq);
                String v = URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
                if (k.equals("user") && user == null) user = v;
                if (k.equals("password") && password == null) password = v;
            }
        }
        if (user == null || password == null) throw new SQLException("a user and a password are required");
        URI base = URI.create(uri.getScheme() + "://" + uri.getRawAuthority() + "/");
        WarehouseClient client = new WarehouseClient(base, new NativeBinding());
        try {
            client.login(user, password);
        } catch (java.io.IOException e) {
            throw new SQLException("cannot reach the warehouse at " + base + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while signing in", e);
        } catch (IllegalStateException refused) {
            throw new SQLException("sign-in refused: " + refused.getMessage(), refused);
        }
        SqlApi.Session session;
        try {
            session = client.openSession(catalog);
        } catch (java.io.IOException e) {
            throw new SQLException("cannot open a session: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("interrupted while opening a session", e);
        } catch (IllegalStateException refused) {
            throw new SQLException("no session on catalog '" + catalog + "': " + refused.getMessage(), refused);
        }
        return new WhConnection(client, catalog, session, url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw Unsupported.of("Driver.getParentLogger");
    }
}
