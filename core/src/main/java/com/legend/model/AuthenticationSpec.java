package com.legend.model;

/**
 * How to authenticate to a database. Sealed root for the auth flavors
 * recognized inside a {@code RelationalDatabaseConnection}'s {@code auth:} key.
 *
 * <p>Mirrors engine's {@code com.gs.legend.model.def.AuthenticationSpec}.
 */
public sealed interface AuthenticationSpec
        permits AuthenticationSpec.NoAuth,
                AuthenticationSpec.DefaultH2,
                AuthenticationSpec.UsernamePassword,
                AuthenticationSpec.TestAuth,
                AuthenticationSpec.DelegatedKerberos,
                AuthenticationSpec.VaultUserNamePassword,
                AuthenticationSpec.SnowflakePublic,
                AuthenticationSpec.GCPApplicationDefaultCredentials,
                AuthenticationSpec.ApiToken,
                AuthenticationSpec.MiddleTierUserNamePassword,
        AuthenticationSpec.GcpWorkloadIdentityFederation,
                AuthenticationSpec.OAuth,
                AuthenticationSpec.PreSharedKey {

    /** Whether opening a session with it needs no credential: no authentication, the engine's test default and the
     *  test harness's. Every other carries one (a password, a token, a key), which no session the platform opens reads
     *  yet (exec.Sessions refuses it by name). Each flavour answers (the dialect program's Phase 5, 2026-10-16: until
     *  then exec.Sessions switched on them). */
    default boolean credentialFree() {
        return switch (this) {
            case NoAuth a -> true;
            case DefaultH2 a -> true;
            case TestAuth a -> true;
            case UsernamePassword a -> false;
            case DelegatedKerberos a -> false;
            case VaultUserNamePassword a -> false;
            case SnowflakePublic a -> false;
            case GCPApplicationDefaultCredentials a -> false;
            case ApiToken a -> false;
            case MiddleTierUserNamePassword a -> false;
            case OAuth a -> false;
            case GcpWorkloadIdentityFederation a -> false;
            case PreSharedKey a -> false;
        };
    }

    /** The user a session opened with it names where a server's session names one even unauthenticated (Trino's):
     *  the test authentications' {@code sa} (legend-engine's DefaultH2AuthenticationStrategy, which its
     *  TestDatabaseAuthenticationStrategy extends, names it); null for no authentication, which names none, and for
     *  one that carries a credential, whose user its credential names (refused before a session opens). Each flavour
     *  answers (main's merge, 2026-10-22: the Trino opening switched on them). */
    default @com.legend.base.Nullable String testUser() {
        return switch (this) {
            case DefaultH2 a -> "sa";
            case TestAuth a -> "sa";
            case NoAuth a -> null;
            case UsernamePassword a -> null;
            case DelegatedKerberos a -> null;
            case VaultUserNamePassword a -> null;
            case SnowflakePublic a -> null;
            case GCPApplicationDefaultCredentials a -> null;
            case ApiToken a -> null;
            case MiddleTierUserNamePassword a -> null;
            case OAuth a -> null;
            case GcpWorkloadIdentityFederation a -> null;
        };
    }

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

    /** Engine's {@code auth: Test;} test authentication. */
    record TestAuth() implements AuthenticationSpec {}

    /** Engine's {@code auth: DelegatedKerberos;}, optional server principal. */
    /** Both kerberos flavors ride here — the plain strategy leaves the
     *  Trino-only fields null; the Trino flavor carries them (they were
     *  silently DROPPED before — deep-audit #2 §3, fixed 2026-08-14). */
    record DelegatedKerberos(@com.legend.base.Nullable String serverPrincipal,
            @com.legend.base.Nullable String kerberosRemoteServiceName,
            @com.legend.base.Nullable Boolean kerberosUseCanonicalHostname)
            implements AuthenticationSpec {}

    /** Engine's {@code auth: UserNamePassword { ...VaultReference... };} —
     *  ALL THREE fields are vault references, unlike
     *  {@link UsernamePassword}'s literal username. */
    record VaultUserNamePassword(@com.legend.base.Nullable String baseVaultReference,
            String userNameVaultReference, String passwordVaultReference)
            implements AuthenticationSpec {}

    /** Engine's Snowflake public-key auth; carried for parse coverage. */
    record SnowflakePublic(String publicUserName,
            String privateKeyVaultReference, String passPhraseVaultReference)
            implements AuthenticationSpec {}

    /** Engine's bodyless GCP application-default-credentials auth. */
    record GCPApplicationDefaultCredentials() implements AuthenticationSpec {}

    /** Engine's API-token auth (Databricks). */
    record ApiToken(String apiToken) implements AuthenticationSpec {}

    /** Engine's middle-tier vault-reference auth. */
    record MiddleTierUserNamePassword(String vaultReference)
            implements AuthenticationSpec {}

    /** Engine's {@code auth: OAuth { oauthKey; scopeName; };}. */
    record OAuth(String oauthKey, String scopeName)
            implements AuthenticationSpec {}

    /** GCP workload identity federation (census C12 flavor leg). */
    record GcpWorkloadIdentityFederation(String serviceAccountEmail,
            java.util.List<String> additionalGcpScopes)
            implements AuthenticationSpec {
    }

    /** A pre-shared key, named by its vault reference and never written itself (lite's own, for a Deephaven server:
     *  docs/DEEPHAVEN_PLAN_2026_10_22.md §4). Not the engine's {@code #PSK{ psk: '...'; }#} island, which carries the
     *  key. */
    record PreSharedKey(String keyVaultReference) implements AuthenticationSpec {}
}
