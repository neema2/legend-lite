package com.legend.parser;

import com.legend.lexer.Lexer;
import com.legend.lexer.TokenStream;
import com.legend.parser.element.PackageableElement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Demand-driven parser entry point. Constructed once with a Pure source
 * string, the orchestrator:
 *
 * <ol>
 *   <li>tokenises the source once
 *       (via {@link Lexer#tokenize(String)});</li>
 *   <li>builds a shallow {@link ModelIndex} mapping every declared FQN
 *       to its token range
 *       (via {@link ModelIndexer#scan(TokenStream)});</li>
 *   <li>deep-parses individual elements <em>on demand</em> via
 *       {@link #resolve(String)}, caching each result so subsequent
 *       lookups return the same instance.</li>
 * </ol>
 *
 * <p>The cache is plain memoization on an immutable input: the source
 * cannot mutate during the orchestrator's lifetime, so cache entries
 * never need invalidation. When source changes, callers construct a
 * fresh orchestrator and let the old one be garbage-collected.
 *
 * <h2>Why demand-driven?</h2>
 * For large models (10K+ elements) most queries reach only a tiny
 * subset of declared elements. Deep-parsing only what's needed cuts
 * cold-start cost from O(model size) toward O(reachable set). See
 * {@code docs/STRESS_TEST_BENCHMARKS.md} in {@code legend-lite/} for
 * baseline numbers that motivate this design.
 *
 * <h2>Resolve-all escape hatch</h2>
 * {@link #resolveAll()} forces every declared element into the cache
 * and returns a {@link ParsedModel}, recovering the eager-parse
 * semantics that downstream tests (and batch validators) rely on.
 *
 * <h2>Thread safety</h2>
 * The orchestrator is <strong>not</strong> safe for concurrent use.
 * Callers that need shared access should externally synchronise. The
 * cache field is a plain {@link HashMap} for simplicity; switching to
 * {@code ConcurrentHashMap} with {@code computeIfAbsent} is a one-line
 * change should that become necessary.
 */
public final class ModelOrchestrator {

    private final TokenStream tokens;
    private final ModelIndex index;
    private final Map<String, PackageableElement> cache = new HashMap<>();
    /** Lazily parsed import scope; same memoization invariant as {@link #cache}. */
    private ImportScope cachedImports;

    /** Build an orchestrator from raw Pure source. Lexes and shallow-scans eagerly. */
    public ModelOrchestrator(String source) {
        this(Lexer.tokenize(Objects.requireNonNull(source, "source")));
    }

    /** Build an orchestrator from a pre-lexed token stream. Shallow-scans eagerly. */
    public ModelOrchestrator(TokenStream tokens) {
        this.tokens = Objects.requireNonNull(tokens, "tokens");
        this.index = ModelIndexer.scan(tokens);
    }

    /** The shallow index. Cheap to retrieve and immutable. */
    public ModelIndex index() {
        return index;
    }

    /** All declared FQNs, in source order. Does not force any deep parse. */
    public Collection<String> declaredFqns() {
        return index.fqns();
    }

    /**
     * Deep-parse the element declared as {@code fqn}, memoising the
     * result. Subsequent calls with the same FQN return the same
     * instance.
     *
     * @throws UnknownFqnException if {@code fqn} is not declared in the source.
     * @throws ParseException      if the element's body fails to parse.
     */
    public PackageableElement resolve(String fqn) {
        PackageableElement cached = cache.get(fqn);
        if (cached != null) return cached;

        ModelIndex.Entry entry = index.get(fqn);
        if (entry == null) {
            throw new UnknownFqnException(fqn);
        }
        TokenStream slice = tokens.slice(entry.startTokenInclusive(), entry.endTokenExclusive());
        PackageableElement parsed = ElementParser.parseSingle(slice);
        cache.put(fqn, parsed);
        return parsed;
    }

    /**
     * Force-parse every declared element and return a {@link ParsedModel}
     * equivalent to what {@link ElementParser#parse(String)} would
     * produce. Element order matches source order.
     *
     * <p>Use this for batch validation, CI checks, or tests that want
     * full-model semantics. Per-element work is memoised in the
     * orchestrator's cache, so a subsequent {@link #resolve(String)} for
     * any FQN is a hash lookup.
     */
    public ParsedModel resolveAll() {
        List<PackageableElement> elements = new ArrayList<>(index.size());
        for (String fqn : index.fqns()) {
            elements.add(resolve(fqn));
        }
        return new ParsedModel(elements, imports());
    }

    /**
     * Parse the file's import statements. Imports are tiny and downstream
     * name resolution needs them regardless of which elements end up
     * parsed; they are parsed once on first access and cached.
     */
    public ImportScope imports() {
        if (cachedImports == null) {
            ImportScope.Builder builder = new ImportScope.Builder();
            for (ModelIndex.ImportEntry imp : index.imports()) {
                TokenStream slice = tokens.slice(imp.startTokenInclusive(), imp.endTokenExclusive());
                builder.add(ElementParser.parseSingleImport(slice));
            }
            cachedImports = builder.build();
        }
        return cachedImports;
    }

    /**
     * Thrown by {@link #resolve(String)} when the FQN is not declared
     * in the orchestrator's source. Caller error rather than parser
     * error: the orchestrator cannot magic an element into existence.
     */
    public static final class UnknownFqnException extends RuntimeException {
        private final String fqn;

        public UnknownFqnException(String fqn) {
            super("no element declared with FQN '" + fqn + "'");
            this.fqn = fqn;
        }

        public String fqn() {
            return fqn;
        }
    }
}
