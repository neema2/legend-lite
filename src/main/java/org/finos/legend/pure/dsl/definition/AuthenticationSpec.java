package org.finos.legend.pure.dsl.definition;

/**
 * Sealed interface for authentication specifications.
 * Defines how to authenticate to a database.
 */
public sealed interface AuthenticationSpec
        permits AuthenticationSpec.NoAuth,
        AuthenticationSpec.UsernamePassword {

    /**
     * No authentication required.
     * Used for in-memory databases and local development.
     */
    record NoAuth() implements AuthenticationSpec {
    }

    /**
     * Username/password authentication.
     * Password can be a direct value or a vault reference.
     */
    record UsernamePassword(
            String username,
            String passwordVaultRef) implements AuthenticationSpec {
    }
}
