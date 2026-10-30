// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.exec;

import com.legend.model.ConnectionDefinition;
import com.legend.model.ConnectionDefinition.DatabaseType;
import com.legend.model.ConnectionSpecification;


/** Trino's sessions: a declared connection of a Trino specification is opened by its URL (the plan path program's step
 *  9, 2026-10-19: the Trino lane on the plan route); a session handed to execution is checked against its driver's
 *  product name. */
final class TrinoConnector implements DatabaseConnector {

    static final TrinoConnector INSTANCE = new TrinoConnector();

    private TrinoConnector() {
    }

    @Override
    public DatabaseType type() {
        return DatabaseType.Trino;
    }

    /** The Trino JDBC driver (measured, trino-jdbc 483). */
    @Override
    public String jdbcProduct() {
        return "Trino";
    }

    /** {@inheritDoc} legend-engine's TrinoManager.buildURL: the session's catalog, then its schema, in the URL's path;
     *  TLS and query tags are refused by name, never dropped. */
    @Override
    public Sessions.Opening opening(ConnectionDefinition def) {
        return switch (def.specification()) {
            case ConnectionSpecification.Trino t -> new Sessions.Opening.Url(url(def, t));
            default -> DatabaseConnector.refused(def);
        };
    }

    private static String url(ConnectionDefinition def, ConnectionSpecification.Trino t) {
        if (t.ssl()) {
            throw new com.legend.error.NotImplementedException("a Trino connection over TLS is not implemented"
                    + " (connection '" + def.qualifiedName() + "')");
        }
        if (t.clientTags() != null) {
            throw new com.legend.error.NotImplementedException("a Trino connection's client tags are not implemented"
                    + " (connection '" + def.qualifiedName() + "')");
        }
        if (t.catalog() == null && t.schema() != null) {
            throw new com.legend.error.NotImplementedException("a Trino connection with a schema and no catalog"
                    + " names no session (connection '" + def.qualifiedName() + "')");
        }
        String path = t.catalog() == null ? "" : t.catalog() + (t.schema() == null ? "" : "/" + t.schema());
        // explicitPrepare=false: the driver sends a prepared statement as one EXECUTE IMMEDIATE, its parameters still
        // bound by the server, instead of a PREPARE carrying the text in a header and an EXECUTE (measured 2026-10-19,
        // Trino 483: the 689 KB statement of format('%s %f', ['fox', 1.5]) answers in 3 to 14 s so, as it does as a
        // plain statement, and through PREPARE fills the coordinator's 2 GB heap until it dies, 185 s)
        return "jdbc:trino://" + t.host() + ":" + t.port() + "/" + path + "?user=" + user(def)
                + "&explicitPrepare=false";
    }

    /** The user a Trino session names: a session names one even unauthenticated (legend-engine's TrinoManager sends
     *  the credential's user), the test authentications' ({@code AuthenticationSpec.testUser}); no authentication
     *  names none, and is refused by name. A credential is refused before ({@link Sessions#openingFor}). */
    private static String user(ConnectionDefinition def) {
        String user = def.authentication().testUser();
        if (user == null) {
            throw new com.legend.error.NotImplementedException("a Trino session names a user, and a connection of no"
                    + " authentication names none (connection '" + def.qualifiedName() + "')");
        }
        return user;
    }

    /** {@inheritDoc} No: {@code Trino} is a server. */
    @Override
    public boolean inProcess() {
        return false;
    }

    /** {@inheritDoc} None: {@code Trino}'s errors carry their raise. */
    @Override
    public @com.legend.base.Nullable RaiseRecovery raiseRecovery() {
        return null;
    }

    /** {@inheritDoc} The dialect's {@code opensFailedResultsWithoutColumns} until D72 (2026-10-30), carried: Measured 2026-10-12: a date 12,345,678,912 days on opens with no columns, its error on the first
     *  read; a statement that succeeds always reports its columns. */
    @Override
    public DatabaseConnector.Decoding decoding() {
        return new DatabaseConnector.Decoding(true, false, true, false);
    }
}
