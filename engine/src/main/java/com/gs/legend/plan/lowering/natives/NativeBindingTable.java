package com.gs.legend.plan.lowering.natives;

import com.gs.legend.compiler.NativeFunctionDef;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Single dialect-agnostic binding table keyed by resolved
 * {@link NativeFunctionDef} identity. Analogous to LLVM frontend intrinsic
 * lowering: every overload registered by {@link com.gs.legend.compiler.BuiltinRegistry}
 * has exactly one binding here that lowers calls of that overload to typed
 * {@link com.gs.legend.sqlgen.SqlExpr} IR.
 *
 * <p>Strict semantics:
 * <ul>
 *   <li>{@link #register} throws on duplicate registration — every overload's
 *       binding is declared exactly once.</li>
 *   <li>{@link #override_} requires an existing binding — used by dialect-specific
 *       extensions when (and only when) ANSI default is wrong.</li>
 *   <li>{@link #lookup} throws if missing — lowering must not silently
 *       fall back to legacy emit paths (post Phase 3.8 totality is enforced
 *       at boot time).</li>
 *   <li>{@link #find} is non-throwing for the migration period when only
 *       a subset of overloads have bindings.</li>
 * </ul>
 *
 * <p>Keying: {@code HashMap<NativeFunctionDef,...>}. {@code NativeFunctionDef}
 * is a record with value-based {@code equals}/{@code hashCode}, so callers
 * may construct logically-equal keys without breaking lookups. Identity-keying
 * was rejected as fragile.
 */
public final class NativeBindingTable {
    private final Map<NativeFunctionDef, NativeBinding> bindings = new HashMap<>();

    public void register(NativeFunctionDef def, NativeBinding binding) {
        if (bindings.containsKey(def)) {
            throw new IllegalStateException(
                    "[native-binding] duplicate registration for " + def.rawSignature());
        }
        bindings.put(def, binding);
    }

    /**
     * Replace an existing binding. Named with a trailing underscore because
     * {@code override} is a Java keyword in annotation form and the linter
     * complains. Used for dialect-specific overrides where the ANSI default
     * is wrong.
     */
    public void override_(NativeFunctionDef def, NativeBinding binding) {
        if (!bindings.containsKey(def)) {
            throw new IllegalStateException(
                    "[native-binding] override target missing for " + def.rawSignature());
        }
        bindings.put(def, binding);
    }

    public NativeBinding lookup(NativeFunctionDef def) {
        NativeBinding b = bindings.get(def);
        if (b == null) {
            throw new IllegalStateException(
                    "[native-binding] no binding for " + def.rawSignature());
        }
        return b;
    }

    public Optional<NativeBinding> find(NativeFunctionDef def) {
        return Optional.ofNullable(bindings.get(def));
    }

    public boolean has(NativeFunctionDef def) {
        return bindings.containsKey(def);
    }

    public int size() {
        return bindings.size();
    }
}
