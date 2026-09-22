package com.legend.resolver;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
/**
 * The resolver's view of one (mapping, class) pair &mdash; the synthesized
 * mapping body split at its {@code map(row|^Class(...))} terminal:
 *
 * <pre>
 *   tableReference -&gt; [joinslots] -&gt; [~filter] -&gt; [~groupBy/~distinct]   = pipeline
 *   map(row | ^Class(prop = expr, ...))                                  = rowVar + bindings
 * </pre>
 *
 * The binding table is the map-terminal invariant's currency: every
 * relation-returning consumer resolves {@code $p.prop} to
 * {@code bindings.get(prop)} (a typed expression over {@link #rowVar},
 * {@code toOne} wrappers explicit per H1's emission), and the terminal
 * itself never survives into relation-shaped output.
 *
 * <p>IMMUTABLE and memoized per (mapping, class) &mdash; shared node objects
 * are safe (typed records are immutable; the lowerer allocates aliases per
 * occurrence); only {@link #rowVar}-bearing expressions are freshened per
 * instantiation by the substitution engine.
 *
 * @param mappingFqn the mapping this extraction was resolved against
 * @param classFqn   the mapped class
 * @param setId      the binding's set id, or {@code null} for the default set
 * @param pipeline   the relation pipeline (body minus the map terminal) —
 *                   mapping ~filter / joinslots / groupBy / distinct intact
 * @param rowVar     the map lambda's row parameter name
 * @param bindings   property name &rarr; the {@code ^Class} value expression
 *                   (LinkedHashMap: declaration order is load-bearing)
 * @param rowType    the pipeline's output row schema
 */
public record ClassSource(
        String mappingFqn,
        String classFqn,
        @com.legend.base.Nullable String setId,
        TypedSpec pipeline,
        String rowVar,
        Map<String, TypedSpec> bindings,
        Type.RelationType rowType,
        @com.legend.base.Nullable String sourceClass,
        Map<String, String> deferredWalls,
        String composedPrefix,
        @com.legend.base.Nullable String castGate,
        @com.legend.base.Nullable String scope) {

    /** The setId of a resolver-built UNION source (an operation-set union
     * synthesized here over its members, or a concatenated-navigation
     * union head). */
    public static final String UNION_SET_ID = "union";


    public ClassSource {
        bindings = Collections.unmodifiableMap(new LinkedHashMap<>(bindings));
        deferredWalls = Collections.unmodifiableMap(
                new LinkedHashMap<>(deferredWalls));
    }

    /** Without a scope. */
    public ClassSource(String mappingFqn, String classFqn,
            @com.legend.base.Nullable String setId, TypedSpec pipeline,
            String rowVar, Map<String, TypedSpec> bindings,
            Type.RelationType rowType,
            @com.legend.base.Nullable String sourceClass,
            Map<String, String> deferredWalls, String composedPrefix,
            @com.legend.base.Nullable String castGate) {
        this(mappingFqn, classFqn, setId, pipeline, rowVar, bindings,
                rowType, sourceClass, deferredWalls, composedPrefix, castGate, null);
    }

    /**
     * THE SCOPE a class source was resolved under (the constructed-tree
     * id when this source reads a query's CONSTRUCTED instance as inline
     * rows; null for the graph's stores). Every navigation target,
     * subtype cast and association join fetched FOR this source passes
     * it on ({@code ClassSources.get} requires it) — a target resolved
     * outside its source's scope would read the store table where the
     * source reads the query's inline rows (user ruling 2026-09-02: the
     * system database is read-only; a query carries its own constants).
     */
    public ClassSource withScope(@com.legend.base.Nullable String scope) {
        return new ClassSource(mappingFqn, classFqn, setId, pipeline, rowVar,
                bindings, rowType, sourceClass, deferredWalls, composedPrefix, castGate,
                scope);
    }

    /** Without a cast gate. */
    public ClassSource(String mappingFqn, String classFqn,
            @com.legend.base.Nullable String setId, TypedSpec pipeline,
            String rowVar, Map<String, TypedSpec> bindings,
            Type.RelationType rowType,
            @com.legend.base.Nullable String sourceClass,
            Map<String, String> deferredWalls, String composedPrefix) {
        this(mappingFqn, classFqn, setId, pipeline, rowVar, bindings,
                rowType, sourceClass, deferredWalls, composedPrefix, null, null);
    }

    public ClassSource(String mappingFqn, String classFqn,
            @com.legend.base.Nullable String setId, TypedSpec pipeline,
            String rowVar, Map<String, TypedSpec> bindings,
            Type.RelationType rowType,
            @com.legend.base.Nullable String sourceClass,
            Map<String, String> deferredWalls) {
        this(mappingFqn, classFqn, setId, pipeline, rowVar, bindings,
                rowType, sourceClass, deferredWalls, "");
    }

    /** The chain was ->cast(@gate) over a PARTIAL-membership row: reads
     * of the gate class's own properties route through the row's subtype
     * columns, witness-gated (the value-position cast rule); rows that
     * do not conform were made to RAISE by the chain's gate filter. */
    public ClassSource withCastGate(@com.legend.base.Nullable String gate) {
        return new ClassSource(mappingFqn, classFqn, setId, pipeline, rowVar,
                bindings, rowType, sourceClass, deferredWalls, composedPrefix, gate, scope);
    }

    public ClassSource(String mappingFqn, String classFqn,
            @com.legend.base.Nullable String setId, TypedSpec pipeline,
            String rowVar, Map<String, TypedSpec> bindings,
            Type.RelationType rowType,
            @com.legend.base.Nullable String sourceClass) {
        this(mappingFqn, classFqn, setId, pipeline, rowVar, bindings,
                rowType, sourceClass, Map.of());
    }

    /** A FLATTENED (re-rooted) source: this class's own physical columns
     * ride the composed row under {@code prefix} — a chained association
     * hop's column-space condition re-points its parent reads through it
     * (the depth leg, 2026-09-02). Empty on a root source. */
    public ClassSource withComposedPrefix(String prefix) {
        return new ClassSource(mappingFqn, classFqn, setId, pipeline, rowVar,
                bindings, rowType, sourceClass, deferredWalls, prefix, castGate, scope);
    }

    /** A binding whose M2M composition walled PER KEY (ledger cluster
     * 21): the wall throws at READ time — a query that never demands the
     * property composes cleanly. Consult on a null bindings().get(). */
    public void throwIfDeferred(String prop) {
        String wall = deferredWalls.get(prop);
        if (wall != null) {
            throw new com.legend.error.NotImplementedException(wall);
        }
    }

    /** No known upstream source class (relational sets, synthesized
     * unions) — consumers requiring source identity fall back to
     * structural checks (audit 24 F4). */
    public ClassSource(String mappingFqn, String classFqn,
            @com.legend.base.Nullable String setId,
            TypedSpec pipeline, String rowVar,
            Map<String, TypedSpec> bindings, Type.RelationType rowType) {
        this(mappingFqn, classFqn, setId, pipeline, rowVar, bindings,
                rowType, null);
    }
}
