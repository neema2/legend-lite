package com.legend.model;

import com.legend.protocol.spec.ValueSpecification;

import java.util.List;
import java.util.Objects;

/**
 * A parsed Pure {@code Service} declaration &mdash; a REST endpoint bound to a
 * Pure query expression with optional mapping &amp; runtime references.
 *
 * <p>Pure syntax (excerpt):
 * <pre>
 *   Service my::api::GetPerson
 *   {
 *     pattern: '/api/person/{id}';
 *     documentation: 'fetch by id';
 *     execution: Single {
 *       query: |Person.all()->filter(p|$p.id == $id);
 *       mapping: my::PersonMapping;
 *       runtime: my::PersonRuntime;
 *     }
 *     testSuites: [ ... ]
 *   }
 * </pre>
 *
 * <h2>Deliberate divergences from engine's {@code ServiceDefinition}</h2>
 * <ul>
 *   <li><strong>No {@code toRegexPattern()} / {@code extractPathParams()}.</strong>
 *       Those convert {@code pattern} into HTTP-matching machinery and belong
 *       in a future {@code runtime/rest/} layer, not on a parser record.
 *       Derive on demand from {@link #pattern()}.</li>
 *   <li><strong>No convenience factories.</strong> Records get the canonical
 *       constructor; callers pass {@code null} for missing optional fields.</li>
 *   <li><strong>{@code testSuites} and {@code test} are the TYPED protocol
 *       records</strong> ({@link com.legend.protocol.Protocol.PServiceTestSuite},
 *       {@link com.legend.protocol.Protocol.PLegacyServiceTest}) — the model
 *       is a transform on the protocol, and the suites are what a service
 *       test runner executes (docs/DEFERRED_TEST_EXECUTION.md step 1; the
 *       raw-text placeholder of decision D-3 is retired).</li>
 *   <li><strong>Unknown top-level keys throw</strong> (engine silently
 *       {@code skipToSemicolon}'s). Matches AGENTS.md invariant 4 (no fallbacks).</li>
 * </ul>
 *
 * @param qualifiedName     fully qualified service name
 * @param pattern           the URL pattern as written (e.g. {@code "/api/person/{id}"});
 *                          never {@code null} &mdash; defaults to {@code "/"} when absent
 * @param functionBody      parsed query expression (the AST between
 *                          {@code |...|} after {@code query:})
 * @param documentation     human-readable description, or {@code null} if absent
 * @param mappingRef        qualified name of the {@code Mapping} bound to this service,
 *                          or {@code null} if absent
 * @param runtimeRef        qualified name of the {@code Runtime} bound to this service,
 *                          or {@code null} if absent
 * @param testSuites        the typed {@code testSuites: [...]} block, or {@code null}
 *                          if absent
 * @param test              the typed legacy {@code test: Single {...}} block, or
 *                          {@code null} if absent
 */
public record ServiceDefinition(
        String qualifiedName,
        String pattern,
        ValueSpecification functionBody,
        @com.legend.base.Nullable String documentation,
        @com.legend.base.Nullable String mappingRef,
        @com.legend.base.Nullable String runtimeRef,
        @com.legend.base.Nullable List<com.legend.protocol.Protocol.PServiceTestSuite> testSuites,
        List<String> owners,
        @com.legend.base.Nullable Boolean autoActivateUpdates,
        @com.legend.base.Nullable MultiExecution multiExecution,
        @com.legend.base.Nullable com.legend.protocol.Protocol.PLegacyServiceTest test)
        implements PackageableElement {

    public ServiceDefinition {
        Objects.requireNonNull(qualifiedName, "Qualified name cannot be null");
        Objects.requireNonNull(pattern, "Pattern cannot be null");
        Objects.requireNonNull(functionBody, "Function body cannot be null");
        owners = owners == null ? List.of() : List.copyOf(owners);
        testSuites = testSuites == null ? null : List.copyOf(testSuites);
    }

    /** The single-execution shape most callers build. */
    public ServiceDefinition(String qualifiedName, String pattern,
            ValueSpecification functionBody,
            @com.legend.base.Nullable String documentation,
            @com.legend.base.Nullable String mappingRef,
            @com.legend.base.Nullable String runtimeRef,
            @com.legend.base.Nullable List<com.legend.protocol.Protocol.PServiceTestSuite> testSuites) {
        this(qualifiedName, pattern, functionBody, documentation, mappingRef,
                runtimeRef, testSuites, List.of(), null, null, null);
    }

    /** {@code execution: Multi} — one shared query, an execution-key
     *  parameter, and per-key mapping/runtime environments. When set,
     *  {@link #mappingRef()}/{@link #runtimeRef()} are null; execution
     *  machinery selects the environment by key at request time (not yet
     *  wired — services with a multi execution parse and carry). */
    public record MultiExecution(String key, List<KeyedExecution> executions) {
        public MultiExecution {
            Objects.requireNonNull(key, "Execution key cannot be null");
            executions = List.copyOf(executions);
        }
    }

    /** One keyed environment: {@code executions['QA']: { mapping; runtime; }}. */
    public record KeyedExecution(String keyValue,
            @com.legend.base.Nullable String mapping,
            @com.legend.base.Nullable String runtime) {
        public KeyedExecution {
            Objects.requireNonNull(keyValue, "Key value cannot be null");
        }
    }
}
