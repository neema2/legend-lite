package com.legend.parser.element;

/**
 * How to authenticate to a database. Sealed root for the auth flavors
 * recognized inside a {@code RelationalDatabaseConnection}'s {@code auth:} key.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.AuthenticationSpec}.
 */
public sealed interface AuthenticationSpec
        permits AuthenticationSpec.NoAuth,
                AuthenticationSpec.DefaultH2,
                AuthenticationSpec.UsernamePassword {

    /** No authentication. Used for in-memory databases and local development. */
    record NoAuth() implements AuthenticationSpec {}

    /** H2-style default test authentication ({@code auth: DefaultH2 {};}). */
    record DefaultH2() implements AuthenticationSpec {}

    /**
     * Username + password authentication. {@code passwordVaultRef} may be a
     * direct password value or a reference into a secret vault &mdash; this
     * record carries the raw text as written; the runtime resolves the
     * reference at execution time.
     */
    record UsernamePassword(String username, String passwordVaultRef) implements AuthenticationSpec {}
}
