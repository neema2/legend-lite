package com.legend.resolver;

import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;

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
        String setId,
        TypedSpec pipeline,
        String rowVar,
        Map<String, TypedSpec> bindings,
        Type.RelationType rowType) {

    public ClassSource {
        bindings = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(bindings));
    }
}
