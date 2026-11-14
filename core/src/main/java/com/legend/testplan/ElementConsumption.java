package com.legend.testplan;

import com.legend.builtin.NativeFn;
import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.CompiledFunction;
import com.legend.compiler.spec.SpecCompiler;
import com.legend.compiler.spec.typed.TypedFrom;
import com.legend.compiler.spec.typed.TypedLet;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedUserCall;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.model.FunctionId;
import com.legend.planner.BodyPlanner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The CONSUMPTION of a body's ELEMENT BINDINGS (the test-is-the-product plan's M10c item 7b; the mixed-bodies design
 * note's §6.4): which lets bind system elements alone and are read in SOURCE FORM only, so that neither road sends a
 * statement for them -- the walk splices such a let into the orchestration argument, the plan text or the planning-time
 * native it ends in, and the class rules may count it as no read of the system store.
 *
 * <p>A let is ELEMENT-ONLY ({@link #elementOnlyLet}) when its value CARRIES an element -- a tracked element reference
 * ({@code BodyClass.systemElementReference}: a mapping, a store, a function used as a value) reached outside the
 * planning-time positions below, or the variable of an element-only let before it read as a value (a chain:
 * {@code let r0 = testRuntime(); let r = ^$r0(connectionStores = ...)}; a let that only HANDS the variable to a
 * planning-time native, {@code let p = executionPlan(q, m, $rt, ext)}, carries nothing: the audit's HIGH-1) -- and
 * evaluates no other read (no navigation of an element's properties, no system class's {@code all()}, no system table,
 * no system function, no store write: {@code BodyClass.readsStore}' walk, lambdas walked too, planning-time natives'
 * arguments left aside).
 *
 * <p>A binding is CONSUMED ({@link #consumed}) when no later statement reads its variable as a VALUE
 * ({@link #valueRead}): an occurrence outside the arguments of a planning-time native (a handle's, a Java routine's,
 * a planning-time effect's: {@link NativeFn.Effect#argumentReadAtPlanning} -- the seed forms' data argument is a value),
 * outside an effect's ORCHESTRATION arguments (a connection, a store, a table: a handle's or a tracked element's type,
 * the step's target), outside a {@code from()}'s mapping and runtime (its context), and outside the argument position
 * of a user call whose PARAMETER is itself consumed in the callee's body ({@link #parameterConsumed}: the same analysis
 * over the callee, the parameter seeded as a carried name), and outside the arguments of a SUBSUMED engine program
 * ({@code builtin/Subsumed}: an opaque value never evaluated). A lambda literal's body is walked (the audit's LOW-4: a
 * lambda a collection native runs, {@code [1]->map(i | $db.schemas...)}, evaluates where the let is spliced; a lambda
 * handed to a planning-time native sits inside the excluded arguments). A read inside the value of a CARRIER let -- an
 * element-only let or an orchestration-handle let whose own binding is consumed -- is no read ({@link #absorbs}): the
 * whole chain is spliced where it ends.
 *
 * <p>A static rule (R16), the walk's own behaviour as a predicate: the run-time census of 2026-11-02 read zero
 * statements over the system store for every body of these shapes. One instance per body, memoized by statement; the
 * callees' parameter table is shared with the nested analyses.
 */
final class ElementConsumption {

    private final List<TypedSpec> body;
    private final Set<String> systemClasses;
    private final ModelContext ctx;
    private final SpecCompiler specs;
    /** Names carried from outside the body: a helper's parameter under analysis. */
    private final Set<String> seeded;
    private final Map<Integer, Boolean> elementOnly = new HashMap<>();
    private final Map<Integer, Boolean> consumedAt = new HashMap<>();
    /** Per callee, each parameter's consumption; a row in progress reads false (a cycle scores not consumed). */
    private final Map<FunctionId, boolean[]> parameters;

    ElementConsumption(List<TypedSpec> body, Set<String> systemClasses, ModelContext ctx, SpecCompiler specs) {
        this(body, systemClasses, ctx, specs, Set.of(), new HashMap<>());
    }

    private ElementConsumption(List<TypedSpec> body, Set<String> systemClasses, ModelContext ctx, SpecCompiler specs,
            Set<String> seeded, Map<FunctionId, boolean[]> parameters) {
        this.body = body;
        this.systemClasses = systemClasses;
        this.ctx = ctx;
        this.specs = specs;
        this.seeded = seeded;
        this.parameters = parameters;
    }

    /** Whether statement {@code i} is an element-only let whose binding is consumed: no read of the system store on
     *  either road. */
    boolean consumedElementLet(int i) {
        return elementOnlyLet(i) && consumed(i);
    }

    /** Whether statement {@code i} is a let before the last statement whose value carries an element (a tracked element
     *  reference, or the variable of an element-only let before it, read as a value: the class javadoc) and evaluates
     *  no other read. */
    boolean elementOnlyLet(int i) {
        Boolean known = elementOnly.get(i);
        if (known != null) {
            return known;
        }
        boolean r = false;
        if (body.get(i) instanceof TypedLet let && i < body.size() - 1) {
            boolean carries = BodyClass.readsStore(let.value(), n -> systemElementReference(n), specs, new HashMap<>(),
                    true);
            for (String c : carriedNames(i)) {
                carries |= valueRead(let.value(), c);
            }
            r = carries && !BodyClass.readsStore(let.value(), n -> BodyClass.elementNavigation(n, systemClasses)
                    || BodyClass.sideKind(n, systemClasses, ctx) != null && !systemElementReference(n), specs,
                    new HashMap<>(), true);
        }
        elementOnly.put(i, r);
        return r;
    }

    /** Whether the let at {@code i} is read as a value by no later statement (no cycle: a let's consumption asks only
     *  the lets after it). */
    boolean consumed(int i) {
        Boolean known = consumedAt.get(i);
        if (known != null) {
            return known;
        }
        String name = ((TypedLet) body.get(i)).name();
        boolean r = true;
        for (int j = i + 1; j < body.size(); j++) {
            if (valueRead(body.get(j), name) && !absorbs(j)) {
                r = false;
                break;
            }
        }
        consumedAt.put(i, r);
        return r;
    }

    private boolean systemElementReference(TypedSpec n) {
        return BodyClass.systemElementReference(n, systemClasses, ctx);
    }

    /** The names carried before statement {@code i}: the seeded ones (a helper's parameter under analysis) and those
     *  of the element-only lets before {@code i} (the LAST binding of a name decides, as {@code Lets.bound} reads it). */
    private Set<String> carriedNames(int i) {
        Set<String> out = new java.util.LinkedHashSet<>(seeded);
        Set<String> seen = new java.util.HashSet<>();
        for (int j = i - 1; j >= 0; j--) {
            if (body.get(j) instanceof TypedLet let && seen.add(let.name()) && elementOnlyLet(j)) {
                out.add(let.name());
            }
        }
        return out;
    }

    /** Whether statement {@code j} is a CARRIER: a let before the last statement, element-only or an orchestration
     *  handle, whose own binding is consumed -- a read of a variable inside its value is in source form. */
    private boolean absorbs(int j) {
        if (!(body.get(j) instanceof TypedLet let) || j >= body.size() - 1) {
            return false;
        }
        return (elementOnlyLet(j) || BodyPlanner.handleType(let.value(), ctx) != null) && consumed(j);
    }

    /** Whether {@code n} reads the variable {@code name} as a VALUE (the class javadoc's definition; a lambda's body is
     *  walked, its own parameters are not {@code name}). */
    private boolean valueRead(TypedSpec n, String name) {
        if (n instanceof TypedVariable v) {
            return name.equals(v.name());
        }
        if (n instanceof TypedNativeCall call) {
            FunctionId id = call.callee().id();
            if (NativeFn.Handle.of(call.callee().id()).isPresent()
                    || NativeFn.JavaRoutine.of(call.callee().id()).isPresent()) {
                return false;
            }
            if (NativeFn.Effect.readsArgumentsAtPlanning(id)) {
                // the seed forms' data argument is a value; the store, the config and connectionByElement's arguments
                // are read at planning (its audit's MEDIUM-2)
                for (int k = 0; k < call.args().size(); k++) {
                    if (!NativeFn.Effect.argumentReadAtPlanning(id, k) && valueRead(call.args().get(k), name)) {
                        return true;
                    }
                }
                return false;
            }
            if (NativeFn.Effect.isDbEffect(id)) {
                // an effect's orchestration arguments are the step's target, read at planning; its others (a
                // statement's text, a table's name) are values
                for (TypedSpec arg : call.args()) {
                    if (!orchestration(arg) && valueRead(arg, name)) {
                        return true;
                    }
                }
                return false;
            }
        }
        if (n instanceof TypedUserCall uc) {
            if (com.legend.builtin.Subsumed.of(uc.callee().qualifiedName()).isPresent()) {
                // a SUBSUMED engine program (createDbConfig): an opaque typed value never evaluated on any road, so its
                // arguments are read by nothing (item 7c)
                return false;
            }
            List<TypedSpec> args = uc.args();
            for (int k = 0; k < args.size(); k++) {
                TypedSpec arg = args.get(k);
                if (arg instanceof TypedVariable v && name.equals(v.name())) {
                    if (!parameterConsumed(uc.callee(), k)) {
                        return true;
                    }
                } else if (valueRead(arg, name)) {
                    return true;
                }
            }
            return false;   // the callee's body is another scope
        }
        if (n instanceof TypedFrom from) {
            // a from()'s mapping and runtime are its context, read at planning (BodyClass.framesAndHandlesAsTheirQueries)
            return valueRead(from.source(), name);
        }
        for (TypedSpec c : n.children()) {
            if (valueRead(c, name)) {
                return true;
            }
        }
        return false;
    }

    /** An effect argument that is the step's target: an orchestration handle (a connection, a runtime, a connection
     *  store: {@link BodyPlanner#handleType}) or a tracked element (a store, a table). */
    private boolean orchestration(TypedSpec arg) {
        if (BodyPlanner.handleType(arg, ctx) != null) {
            return true;
        }
        return arg.info().type() instanceof Type.ClassType ct && ctx.tracksClassifier(ct.fqn())
                && systemClasses.contains(ct.fqn());
    }

    /** Whether parameter {@code k} of {@code callee} is consumed in its body: the same analysis over the callee's
     *  statements with the parameter seeded as carried, the last statement included (its value is the caller's). */
    private boolean parameterConsumed(TypedFunction callee, int k) {
        FunctionId id = callee.id();
        boolean[] known = parameters.get(id);
        if (known == null) {
            List<com.legend.compiler.element.TypedParameter> params = callee.parameters();
            int n = params == null ? 0 : params.size();
            known = new boolean[n];
            // in progress: false. A cycle (F calls G calls F) scores F's parameters not consumed while G's row is being
            // filled, and G's row keeps that pessimism for the body: every dependency is monotone in the conservative
            // direction (a parameter not consumed means more reads, fewer carriers), so the leak is never an optimism
            parameters.put(id, known);
            try {
                CompiledFunction compiled = specs.compile(callee);
                for (int p = 0; p < n; p++) {
                    String pname = params.get(p).name();
                    ElementConsumption inner = new ElementConsumption(compiled.body(), systemClasses, ctx, specs,
                            Set.of(pname), parameters);
                    known[p] = inner.consumedName(pname);
                }
            } catch (com.legend.error.TypeInferenceException e) {
                // an un-typeable callee: not consumed (the scan does not decide on it)
            }
        }
        return k < known.length && known[k];
    }

    /** Whether a seeded name is read as a value by no statement of this body, the last one included. */
    private boolean consumedName(String name) {
        for (int j = 0; j < body.size(); j++) {
            if (valueRead(body.get(j), name) && !absorbs(j)) {
                return false;
            }
        }
        return true;
    }
}
