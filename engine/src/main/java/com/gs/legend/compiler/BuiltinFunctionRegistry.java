package com.gs.legend.compiler;

import java.util.*;

/**
 * Registry of built-in function signatures parsed from Pure native function declarations.
 *
 * <p>
 * This is the single source of truth for function type-checking. Every built-in
 * function (relation functions from legend-engine, scalar/aggregate/window functions
 * from legend-pure) must be registered here. If a function isn't registered, the
 * TypeChecker rejects it — <b>no passthrough</b>.
 *
 * <h3>Architecture</h3>
 * <pre>
 * Pure signature string  →  PureParser.parseNativeFunction()  →  NativeFunctionDef
 *                                                                       ↓
 *                                                              BuiltinFunctionRegistry
 *                                                                       ↓
 *                                                              TypeChecker.resolve()
 * </pre>
 *
 * <h3>Two tiers</h3>
 * <ul>
 *   <li><b>Tier 1 — Relation functions</b> (~50 overloads): transform data shape
 *       (rich type vars, schema algebra). Custom compile methods in TypeChecker.</li>
 *   <li><b>Tier 2 — DynaFunctions</b> (~225): transform data values
 *       (scalars, aggregates, windows). Generic compilation via registry.</li>
 * </ul>
 *
 * @see NativeFunctionDef
 * @see PType
 * @see Mult
 */
public class BuiltinFunctionRegistry {

    /** All overloads keyed by simple function name. */
    private final Map<String, List<NativeFunctionDef>> functions = new LinkedHashMap<>();

    /** Total count of registered overloads (for reporting). */
    private int totalOverloads = 0;

    /**
     * Register a native function definition.
     * Multiple overloads of the same function name are allowed.
     */
    public void register(NativeFunctionDef def) {
        functions.computeIfAbsent(def.name(), k -> new ArrayList<>()).add(def);
        totalOverloads++;
    }

    /**
     * Register a native function from its Pure signature string.
     * The string must be a valid Pure native function declaration, e.g.:
     * <pre>
     * native function filter&lt;T&gt;(rel:Relation&lt;T&gt;[1], f:Function&lt;{T[1]-&gt;Boolean[1]}&gt;[1]):Relation&lt;T&gt;[1];
     * </pre>
     *
     * TODO: Wire to PureParser.parseNativeFunction() once that entry point exists.
     * For now, stores the raw signature string as a placeholder.
     */
    public void registerSignature(String pureSignature) {
        // Phase 1 placeholder: extract simple name from signature and store raw string.
        // This will be replaced with full PureParser integration in Phase 2.
        String name = extractFunctionName(pureSignature);
        var placeholder = new NativeFunctionDef(
                name,
                List.of(),    // TODO: parse type params
                List.of(),    // TODO: parse mult params
                List.of(),    // TODO: parse params
                new PType.Concrete("Any"),  // TODO: parse return type
                Mult.ONE,     // TODO: parse return mult
                pureSignature.trim()
        );
        register(placeholder);
    }

    /**
     * Look up all overloads of a function by simple name.
     * Returns empty list if not registered.
     */
    public List<NativeFunctionDef> resolve(String name) {
        return functions.getOrDefault(name, List.of());
    }

    /**
     * Check if a function is registered (any overload).
     */
    public boolean isRegistered(String name) {
        return functions.containsKey(name);
    }

    /**
     * All registered function names (unordered).
     */
    public Set<String> allFunctionNames() {
        return Collections.unmodifiableSet(functions.keySet());
    }

    /**
     * Total number of registered function names (not counting overloads).
     */
    public int functionCount() {
        return functions.size();
    }

    /**
     * Total number of registered overloads across all functions.
     */
    public int overloadCount() {
        return totalOverloads;
    }

    // ===== Singleton =====

    private static final BuiltinFunctionRegistry INSTANCE = createDefault();

    /**
     * Returns the singleton registry with all built-in functions registered.
     */
    public static BuiltinFunctionRegistry instance() {
        return INSTANCE;
    }

    // ===== Internal helpers =====

    /**
     * Extracts the simple function name from a native function signature string.
     * E.g., "native function filter<T>(...)" → "filter"
     */
    static String extractFunctionName(String sig) {
        String s = sig.trim();
        // Strip "native function " or "function " prefix
        if (s.startsWith("native function ")) {
            s = s.substring("native function ".length()).trim();
        } else if (s.startsWith("function ")) {
            s = s.substring("function ".length()).trim();
        }
        // Strip any annotation prefix like <<PCT.function>>
        if (s.startsWith("<<")) {
            int end = s.indexOf(">>");
            if (end > 0) s = s.substring(end + 2).trim();
        }
        // Strip any doc annotation like {doc.doc = '...'}
        if (s.startsWith("{doc.")) {
            int end = s.indexOf('}');
            if (end > 0) s = s.substring(end + 1).trim();
        }
        // Strip qualified name prefix (meta::pure::functions::relation::)
        int lastColon = s.lastIndexOf("::");
        if (lastColon >= 0) s = s.substring(lastColon + 2);
        // Take name before < or (
        int lt = s.indexOf('<');
        int lp = s.indexOf('(');
        int end = Math.min(
                lt >= 0 ? lt : Integer.MAX_VALUE,
                lp >= 0 ? lp : Integer.MAX_VALUE
        );
        if (end < Integer.MAX_VALUE) s = s.substring(0, end);
        return s.trim();
    }

    // ===== Registration =====

    private static BuiltinFunctionRegistry createDefault() {
        var reg = new BuiltinFunctionRegistry();
        registerRelationFunctions(reg);
        registerScalarFunctions(reg);
        return reg;
    }

    /**
     * Tier 1: Relation functions from legend-engine.
     * ~50 overloads across ~20 functions that transform data shape.
     */
    private static void registerRelationFunctions(BuiltinFunctionRegistry reg) {
        // Shape-preserving
        reg.registerSignature("native function filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];");
        reg.registerSignature("native function sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1];");
        reg.registerSignature("native function limit<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("native function drop<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("native function slice<T>(rel:Relation<T>[1], start:Integer[1], stop:Integer[1]):Relation<T>[1];");
        reg.registerSignature("native function concatenate<T>(rel1:Relation<T>[1], rel2:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("native function size<T>(rel:Relation<T>[1]):Integer[1];");

        // Distinct
        reg.registerSignature("native function distinct<T>(rel:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("native function distinct<X,T>(rel:Relation<T>[1], columns:ColSpecArray<X⊆T>[1]):Relation<X>[1];");

        // Select
        reg.registerSignature("native function select<T>(r:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("native function select<T,Z>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1]):Relation<Z>[1];");
        reg.registerSignature("native function select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1];");

        // Rename
        reg.registerSignature("native function rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];");

        // Extend — scalar
        reg.registerSignature("native function extend<T,Z>(r:Relation<T>[1], f:FuncColSpec<{T[1]->Any[0..1]},Z>[1]):Relation<T+Z>[1];");
        reg.registerSignature("native function extend<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<T+Z>[1];");
        // Extend — aggregate
        reg.registerSignature("native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        // Extend — window scalar
        reg.registerSignature("native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpec<{Relation<T>[1],_Window<T>[1],T[1]->Any[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->Any[*]},R>[1]):Relation<T+R>[1];");

        // GroupBy
        reg.registerSignature("native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");

        // Aggregate
        reg.registerSignature("native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");
        reg.registerSignature("native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");

        // Join
        reg.registerSignature("native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
        reg.registerSignature("native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
        reg.registerSignature("native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1], join:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");

        // Pivot
        reg.registerSignature("native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], values:Any[1..*], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");

        // Project
        reg.registerSignature("native function project<C,T>(cl:C[*], x:FuncColSpecArray<{C[1]->Any[*]},T>[1]):Relation<T>[1];");
        reg.registerSignature("native function project<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<Z>[1];");

        // Flatten
        reg.registerSignature("native function flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:ColSpec<Z=(?:T)>[1]):Relation<Z>[1];");

        // Window functions (native)
        reg.registerSignature("native function first<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("native function last<T>(w:Relation<T>[1], f:_Window<T>[1], row:T[1]):T[0..1];");
        reg.registerSignature("native function nth<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("native function offset<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("native function rowNumber<T>(rel:Relation<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("native function ntile<T>(rel:Relation<T>[1], row:T[1], tileCount:Integer[1]):Integer[1];");
        reg.registerSignature("native function rank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("native function denseRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("native function percentRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];");
        reg.registerSignature("native function cumulativeDistribution<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];");

        // Window functions (non-native, delegate to offset)
        reg.registerSignature("function lag<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("function lag<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("function lead<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("function lead<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
    }

    /**
     * Tier 2: Scalar/aggregate/window functions from legend-pure.
     * These transform data values (not shape).
     */
    private static void registerScalarFunctions(BuiltinFunctionRegistry reg) {
        // ===== String =====
        reg.registerSignature("native function toLower(source:String[1]):String[1];");
        reg.registerSignature("native function toUpper(source:String[1]):String[1];");
        reg.registerSignature("native function trim(str:String[1]):String[1];");
        reg.registerSignature("native function ltrim(str:String[1]):String[1];");
        reg.registerSignature("native function rtrim(str:String[1]):String[1];");
        reg.registerSignature("native function substring(str:String[1], start:Integer[1], end:Integer[1]):String[1];");
        reg.registerSignature("native function indexOf(str:String[1], toFind:String[1]):Integer[1];");
        reg.registerSignature("native function indexOf(str:String[1], toFind:String[1], fromIndex:Integer[1]):Integer[1];");
        reg.registerSignature("native function startsWith(source:String[1], val:String[1]):Boolean[1];");
        reg.registerSignature("native function endsWith(source:String[1], val:String[1]):Boolean[1];");
        reg.registerSignature("native function reverseString(str:String[1]):String[1];");

        // ===== Math =====
        reg.registerSignature("native function abs(int:Integer[1]):Integer[1];");
        reg.registerSignature("native function abs(float:Float[1]):Float[1];");
        reg.registerSignature("native function abs(number:Number[1]):Number[1];");
        reg.registerSignature("native function abs(decimal:Decimal[1]):Decimal[1];");
        reg.registerSignature("native function ceiling(number:Number[1]):Integer[1];");
        reg.registerSignature("native function floor(number:Number[1]):Integer[1];");
        reg.registerSignature("native function round(number:Number[1]):Integer[1];");
        reg.registerSignature("native function round(decimal:Decimal[1], scale:Integer[1]):Decimal[1];");
        reg.registerSignature("native function round(float:Float[1], scale:Integer[1]):Float[1];");
        reg.registerSignature("native function sqrt(number:Number[1]):Float[1];");
        reg.registerSignature("native function pow(base:Number[1], exponent:Number[1]):Number[1];");
        reg.registerSignature("native function exp(exponent:Number[1]):Float[1];");
        reg.registerSignature("native function log(value:Number[1]):Float[1];");
        reg.registerSignature("native function log10(value:Number[1]):Float[1];");
        reg.registerSignature("native function sign(number:Number[1]):Integer[1];");
        reg.registerSignature("native function sin(number:Number[1]):Float[1];");
        reg.registerSignature("native function tan(number:Number[1]):Float[1];");

        // ===== Arithmetic =====
        reg.registerSignature("native function plus(numbers:Number[*]):Number[1];");
        reg.registerSignature("native function plus(float:Float[*]):Float[1];");
        reg.registerSignature("native function plus(decimal:Decimal[*]):Decimal[1];");
        reg.registerSignature("native function minus(numbers:Number[*]):Number[1];");
        reg.registerSignature("native function minus(float:Float[*]):Float[1];");
        reg.registerSignature("native function minus(decimal:Decimal[*]):Decimal[1];");
        reg.registerSignature("native function times(numbers:Number[*]):Number[1];");
        reg.registerSignature("native function times(ints:Float[*]):Float[1];");
        reg.registerSignature("native function times(decimal:Decimal[*]):Decimal[1];");

        // ===== Date =====
        reg.registerSignature("native function dateDiff(d1:Date[1], d2:Date[1], du:DurationUnit[1]):Integer[1];");
        reg.registerSignature("native function year(d:Date[1]):Integer[1];");
        reg.registerSignature("native function monthNumber(d:Date[1]):Integer[1];");
        reg.registerSignature("native function hour(d:Date[1]):Integer[1];");
        reg.registerSignature("native function minute(d:Date[1]):Integer[1];");
        reg.registerSignature("native function second(d:Date[1]):Integer[1];");

        // ===== Conversion =====
        reg.registerSignature("native function parseInteger(string:String[1]):Integer[1];");
        reg.registerSignature("native function parseFloat(string:String[1]):Float[1];");
        reg.registerSignature("native function parseDate(string:String[1]):Date[1];");
        reg.registerSignature("native function parseBoolean(string:String[1]):Boolean[1];");
        reg.registerSignature("native function toDecimal(number:Number[1]):Decimal[1];");
        reg.registerSignature("native function toFloat(number:Number[1]):Float[1];");

        // ===== Collection =====
        reg.registerSignature("native function toOne<T>(values:T[*], message:String[1]):T[1];");
        reg.registerSignature("native function toOneMany<T>(values:T[*], message:String[1]):T[1..*];");
        reg.registerSignature("native function last<T>(set:T[*]):T[0..1];");
        reg.registerSignature("native function reverse<T|m>(set:T[m]):T[m];");
        reg.registerSignature("native function slice<T>(set:T[*], start:Integer[1], end:Integer[1]):T[*];");
        reg.registerSignature("native function take<T>(set:T[*], count:Integer[1]):T[*];");
        reg.registerSignature("native function drop<T>(set:T[*], count:Integer[1]):T[*];");
        reg.registerSignature("native function exists<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];");
        reg.registerSignature("native function forAll<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];");
        reg.registerSignature("native function find<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[0..1];");
        reg.registerSignature("native function map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>[1]):V[*];");
        reg.registerSignature("native function map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>[1]):V[0..1];");
        reg.registerSignature("native function zip<T,U>(set1:T[*], set2:U[*]):Pair<T,U>[*];");
        reg.registerSignature("native function indexOf<T>(set:T[*], value:T[1]):Integer[1];");
        reg.registerSignature("native function removeAllOptimized<T>(set:T[*], other:T[*]):T[*];");

        // ===== Boolean / Meta =====
        reg.registerSignature("native function instanceOf(instance:Any[1], type:Type[1]):Boolean[1];");

        // ===== Collection sort (legend-pure) =====
        reg.registerSignature("native function sort<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1], comp:Function<{U[1],U[1]->Integer[1]}>[0..1]):T[m];");
        reg.registerSignature("native function sort<T|m>(col:T[m]):T[m];");
        reg.registerSignature("native function sort<T|m>(col:T[m], comp:Function<{T[1],T[1]->Integer[1]}>[0..1]):T[m];");
    }
}
