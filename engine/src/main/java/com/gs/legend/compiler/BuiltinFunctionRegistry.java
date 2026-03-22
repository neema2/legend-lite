package com.gs.legend.compiler;

import java.util.*;

/**
 * Registry of built-in function signatures parsed from Pure native function
 * declarations.
 *
 * <p>
 * This is the single source of truth for function type-checking. Every built-in
 * function (relation functions from legend-engine, scalar/aggregate/window
 * functions
 * from legend-pure) must be registered here. If a function isn't registered,
 * the
 * TypeChecker rejects it — <b>no passthrough</b>.
 *
 * <h3>Architecture</h3>
 * 
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
 * <li><b>Tier 1 — Relation functions</b> (~50 overloads): transform data shape
 * (rich type vars, schema algebra). Custom compile methods in TypeChecker.</li>
 * <li><b>Tie r 2 — DynaFunctions</b> (~225): transform data values
 * (scalars, aggregates, windows). Generic compilation via registry.</li>
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
     * Register a native function by name and Pure signature string.
     * Parses the signature via PureParser into a structured NativeFunctionDef.
     *
     * Pre-processes the signature to strip Pure-specific constraint syntax
     * that the ANTLR grammar doesn't handle directly:
     * - Z⊆T → Z (subset constraint, preserved in rawSignature)
     * - Z=(?:K)⊆T → Z (type-match constraint, preserved in rawSignature)
     * - Relation<T+V> → Relation<T_plus_V> (schema union)
     * - Relation<T-Z+V> → Relation<T_minus_Z_plus_V> (schema remove+add)
     *
     * @param name          Simple function name (e.g., "filter", "plus", "toLower")
     * @param pureSignature Full Pure native function declaration string
     */
    public void registerSignature(String name, String pureSignature) {
        String rawSignature = pureSignature.trim();
        var def = com.gs.legend.parser.PureParser.parseNativeFunction(rawSignature);
        register(def);
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
     * All registered functions with their overloads.
     */
    public Map<String, List<NativeFunctionDef>> allRegistered() {
        return Collections.unmodifiableMap(functions);
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

    // ===== Aggregate convenience accessors =====
    // Cached at init for identity-based dispatch (==) in PlanGenerator.

    private NativeFunctionDef wavgDef, rowMapperDef, hashCodeDef;
    private NativeFunctionDef corrDef, covarSampleDef, covarPopulationDef;
    private NativeFunctionDef maxByDef, minByDef;

    private void cacheConvenienceDefs() {
        wavgDef = resolve("wavg").get(0);
        rowMapperDef = resolve("rowMapper").get(0);
        hashCodeDef = resolve("hashCode").get(0);
        corrDef = resolve("corr").get(0);
        covarSampleDef = resolve("covarSample").get(0);
        covarPopulationDef = resolve("covarPopulation").get(0);
        maxByDef = resolve("maxBy").get(0);
        minByDef = resolve("minBy").get(0);
    }

    public NativeFunctionDef wavg()              { return wavgDef; }
    public NativeFunctionDef rowMapper()         { return rowMapperDef; }
    public NativeFunctionDef hashCodeAgg()       { return hashCodeDef; }
    public NativeFunctionDef corr()              { return corrDef; }
    public NativeFunctionDef covarSample()       { return covarSampleDef; }
    public NativeFunctionDef covarPopulation()   { return covarPopulationDef; }
    public NativeFunctionDef maxBy()             { return maxByDef; }
    public NativeFunctionDef minBy()             { return minByDef; }

    // ===== Singleton =====

    private static final BuiltinFunctionRegistry INSTANCE = createDefault();

    /**
     * Returns the singleton registry with all built-in functions registered.
     */
    public static BuiltinFunctionRegistry instance() {
        return INSTANCE;
    }

    // ===== Registration =====

    private static BuiltinFunctionRegistry createDefault() {
        var reg = new BuiltinFunctionRegistry();
        registerRelationFunctions(reg);
        registerScalarFunctions(reg);
        reg.cacheConvenienceDefs();
        return reg;
    }

    /**
     * Tier 1: Relation functions from legend-engine.
     * ~50 overloads across ~20 functions that transform data shape.
     */
    private static void registerRelationFunctions(BuiltinFunctionRegistry reg) {
        // Shape-preserving
        reg.registerSignature("filter",
                "native function filter<T>(rel:Relation<T>[1], f:Function<{T[1]->Boolean[1]}>[1]):Relation<T>[1];");
        reg.registerSignature("sort",
                "native function sort<X,T>(rel:Relation<T>[1], sortInfo:SortInfo<X⊆T>[*]):Relation<T>[1];");
        reg.registerSignature("limit", "native function limit<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("drop", "native function drop<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("slice",
                "native function slice<T>(rel:Relation<T>[1], start:Integer[1], stop:Integer[1]):Relation<T>[1];");
        reg.registerSignature("concatenate",
                "native function concatenate<T>(rel1:Relation<T>[1], rel2:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("size", "native function size<T>(rel:Relation<T>[1]):Integer[1];");

        // Distinct
        reg.registerSignature("distinct", "native function distinct<T>(rel:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("distinct",
                "native function distinct<X,T>(rel:Relation<T>[1], columns:ColSpecArray<X⊆T>[1]):Relation<X>[1];");

        // Select
        reg.registerSignature("select", "native function select<T>(r:Relation<T>[1]):Relation<T>[1];");
        reg.registerSignature("select",
                "native function select<T,Z>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1]):Relation<Z>[1];");
        reg.registerSignature("select",
                "native function select<T,Z>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1]):Relation<Z>[1];");

        // Rename
        reg.registerSignature("rename",
                "native function rename<T,Z,K,V>(r:Relation<T>[1], old:ColSpec<Z=(?:K)⊆T>[1], new:ColSpec<V=(?:K)>[1]):Relation<T-Z+V>[1];");
        reg.registerSignature("rename",
                "native function rename<T,Z,V>(r:Relation<T>[1], oldCols:ColSpecArray<Z⊆T>[1], newCols:ColSpecArray<V>[1]):Relation<T-Z+V>[1];");

        // Extend — scalar
        reg.registerSignature("extend",
                "native function extend<T,Z>(r:Relation<T>[1], f:FuncColSpec<{T[1]->Any[0..1]},Z>[1]):Relation<T+Z>[1];");
        reg.registerSignature("extend",
                "native function extend<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<T+Z>[1];");
        // Extend — aggregate
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("extend",
                "native function extend<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<T+R>[1];");
        // Extend — window scalar
        reg.registerSignature("extend",
                "native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpec<{Relation<T>[1],_Window<T>[1],T[1]->Any[0..1]},R>[1]):Relation<T+R>[1];");
        reg.registerSignature("extend",
                "native function extend<T,Z,W,R>(r:Relation<T>[1], window:_Window<T>[1], f:FuncColSpecArray<{Relation<T>[1],_Window<T>[1],T[1]->Any[*]},R>[1]):Relation<T+R>[1];");

        // GroupBy
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");
        reg.registerSignature("groupBy",
                "native function groupBy<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Z+R>[1];");

        // Aggregate
        reg.registerSignature("aggregate",
                "native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");
        reg.registerSignature("aggregate",
                "native function aggregate<T,K,V,R>(r:Relation<T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<R>[1];");

        // Join
        reg.registerSignature("join",
                "native function join<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], joinKind:JoinKind[1], f:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
        reg.registerSignature("asOfJoin",
                "native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");
        reg.registerSignature("asOfJoin",
                "native function asOfJoin<T,V>(rel1:Relation<T>[1], rel2:Relation<V>[1], match:Function<{T[1],V[1]->Boolean[1]}>[1], join:Function<{T[1],V[1]->Boolean[1]}>[1]):Relation<T+V>[1];");

        // Pivot
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], values:Any[1..*], agg:AggColSpec<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpecArray<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");
        reg.registerSignature("pivot",
                "native function pivot<T,Z,K,V,R>(r:Relation<T>[1], cols:ColSpec<Z⊆T>[1], agg:AggColSpecArray<{T[1]->K[0..1]},{K[*]->V[0..1]},R>[1]):Relation<Any>[1];");

        // Project
        reg.registerSignature("project",
                "native function project<C,T>(cl:C[*], x:FuncColSpecArray<{C[1]->Any[*]},T>[1]):Relation<T>[1];");
        reg.registerSignature("project",
                "native function project<T,Z>(r:Relation<T>[1], fs:FuncColSpecArray<{T[1]->Any[*]},Z>[1]):Relation<Z>[1];");

        // Flatten
        reg.registerSignature("flatten",
                "native function flatten<T,Z>(valueToFlatten:T[*], columnWithFlattenedValue:ColSpec<Z=(?:T)>[1]):Relation<Z>[1];");

        // Window functions (native)
        reg.registerSignature("first", "native function first<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("last", "native function last<T>(w:Relation<T>[1], f:_Window<T>[1], row:T[1]):T[0..1];");
        reg.registerSignature("nth",
                "native function nth<T>(w:Relation<T>[1], f:_Window<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("offset",
                "native function offset<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("rowNumber", "native function rowNumber<T>(rel:Relation<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("ntile",
                "native function ntile<T>(rel:Relation<T>[1], row:T[1], tileCount:Integer[1]):Integer[1];");
        reg.registerSignature("rank",
                "native function rank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("denseRank",
                "native function denseRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Integer[1];");
        reg.registerSignature("percentRank",
                "native function percentRank<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];");
        reg.registerSignature("cumulativeDistribution",
                "native function cumulativeDistribution<T>(rel:Relation<T>[1], w:_Window<T>[1], row:T[1]):Float[1];");

        // Window functions (delegate to offset)
        reg.registerSignature("lag", "native function lag<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("lag", "native function lag<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
        reg.registerSignature("lead", "native function lead<T>(w:Relation<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("lead", "native function lead<T>(w:Relation<T>[1], r:T[1], offset:Integer[1]):T[0..1];");
    }

    /**
     * Tier 2: Scalar/aggregate/window functions.
     * Organized by category from DynaFunctionRegistry.
     */
    private static void registerScalarFunctions(BuiltinFunctionRegistry reg) {
        // ===== String =====
        reg.registerSignature("toLower", "native function toLower(source:String[1]):String[1];");
        reg.registerSignature("toUpper", "native function toUpper(source:String[1]):String[1];");
        reg.registerSignature("trim", "native function trim(str:String[1]):String[1];");
        reg.registerSignature("ltrim", "native function ltrim(str:String[1]):String[1];");
        reg.registerSignature("rtrim", "native function rtrim(str:String[1]):String[1];");
        reg.registerSignature("substring",
                "native function substring(str:String[1], start:Integer[1], end:Integer[1]):String[1];");
        reg.registerSignature("indexOf", "native function indexOf(str:String[1], toFind:String[1]):Integer[1];");
        reg.registerSignature("indexOf",
                "native function indexOf(str:String[1], toFind:String[1], fromIndex:Integer[1]):Integer[1];");
        reg.registerSignature("startsWith", "native function startsWith(source:String[1], val:String[1]):Boolean[1];");
        reg.registerSignature("endsWith", "native function endsWith(source:String[1], val:String[1]):Boolean[1];");
        reg.registerSignature("contains", "native function contains(source:String[1], val:String[1]):Boolean[1];");
        reg.registerSignature("reverseString", "native function reverseString(str:String[1]):String[1];");
        reg.registerSignature("replace",
                "native function replace(str:String[1], toFind:String[1], replacement:String[1]):String[1];");
        reg.registerSignature("length", "native function length(str:String[1]):Integer[1];");
        reg.registerSignature("toString", "native function toString(any:Any[1]):String[1];");
        reg.registerSignature("format", "native function format(format:String[1], args:Any[*]):String[1];");
        reg.registerSignature("joinStrings",
                "native function joinStrings(strings:String[*], separator:String[1]):String[1];");
        reg.registerSignature("split", "native function split(str:String[1], delimiter:String[1]):String[*];");
        reg.registerSignature("matches", "native function matches(str:String[1], regex:String[1]):Boolean[1];");
        reg.registerSignature("ascii", "native function ascii(str:String[1]):Integer[1];");
        reg.registerSignature("char", "native function char(code:Integer[1]):String[1];");
        reg.registerSignature("lpad", "native function lpad(str:String[1], len:Integer[1], pad:String[1]):String[1];");
        reg.registerSignature("rpad", "native function rpad(str:String[1], len:Integer[1], pad:String[1]):String[1];");
        reg.registerSignature("splitPart",
                "native function splitPart(str:String[1], delimiter:String[1], index:Integer[1]):String[1];");
        reg.registerSignature("left", "native function left(str:String[1], len:Integer[1]):String[1];");
        reg.registerSignature("right", "native function right(str:String[1], len:Integer[1]):String[1];");
        reg.registerSignature("toUpperFirstCharacter",
                "native function toUpperFirstCharacter(str:String[1]):String[1];");
        reg.registerSignature("toLowerFirstCharacter",
                "native function toLowerFirstCharacter(str:String[1]):String[1];");
        reg.registerSignature("encodeBase64", "native function encodeBase64(str:String[1]):String[1];");
        reg.registerSignature("decodeBase64", "native function decodeBase64(str:String[1]):String[1];");
        reg.registerSignature("hash", "native function hash(str:String[1], algorithm:String[1]):String[1];");
        reg.registerSignature("levenshteinDistance",
                "native function levenshteinDistance(s1:String[1], s2:String[1]):Integer[1];");
        reg.registerSignature("jaroWinklerSimilarity",
                "native function jaroWinklerSimilarity(s1:String[1], s2:String[1]):Float[1];");
        reg.registerSignature("hashCode", "native function hashCode(str:String[1]):Integer[1];");

        // ===== Math (basic) =====
        reg.registerSignature("abs", "native function abs<T>(number:T[1]):T[1];");
        reg.registerSignature("ceiling", "native function ceiling(number:Number[1]):Integer[1];");
        reg.registerSignature("floor", "native function floor(number:Number[1]):Integer[1];");
        reg.registerSignature("round", "native function round(number:Number[1]):Integer[1];");
        reg.registerSignature("round", "native function round(decimal:Decimal[1], scale:Integer[1]):Decimal[1];");
        reg.registerSignature("round", "native function round(float:Float[1], scale:Integer[1]):Float[1];");
        reg.registerSignature("sqrt", "native function sqrt(number:Number[1]):Float[1];");
        reg.registerSignature("cbrt", "native function cbrt(number:Number[1]):Float[1];");
        reg.registerSignature("pow", "native function pow(base:Number[1], exponent:Number[1]):Number[1];");
        reg.registerSignature("exp", "native function exp(exponent:Number[1]):Float[1];");
        reg.registerSignature("log", "native function log(value:Number[1]):Float[1];");
        reg.registerSignature("log10", "native function log10(value:Number[1]):Float[1];");
        reg.registerSignature("sign", "native function sign(number:Number[1]):Integer[1];");
        reg.registerSignature("mod", "native function mod(dividend:Integer[1], divisor:Integer[1]):Integer[1];");
        reg.registerSignature("rem", "native function rem(dividend:Number[1], divisor:Number[1]):Number[1];");

        // ===== Trigonometry =====
        reg.registerSignature("sin", "native function sin(number:Number[1]):Float[1];");
        reg.registerSignature("cos", "native function cos(number:Number[1]):Float[1];");
        reg.registerSignature("tan", "native function tan(number:Number[1]):Float[1];");
        reg.registerSignature("asin", "native function asin(number:Number[1]):Float[1];");
        reg.registerSignature("acos", "native function acos(number:Number[1]):Float[1];");
        reg.registerSignature("atan", "native function atan(number:Number[1]):Float[1];");
        reg.registerSignature("atan2", "native function atan2(y:Number[1], x:Number[1]):Float[1];");
        reg.registerSignature("sinh", "native function sinh(number:Number[1]):Float[1];");
        reg.registerSignature("cosh", "native function cosh(number:Number[1]):Float[1];");
        reg.registerSignature("tanh", "native function tanh(number:Number[1]):Float[1];");
        reg.registerSignature("cot", "native function cot(number:Number[1]):Float[1];");
        reg.registerSignature("toDegrees", "native function toDegrees(radians:Number[1]):Float[1];");
        reg.registerSignature("toRadians", "native function toRadians(degrees:Number[1]):Float[1];");
        reg.registerSignature("pi", "native function pi():Float[1];");

        // ===== Arithmetic =====
        reg.registerSignature("plus", "native function plus<T>(values:T[*]):T[1];");
        reg.registerSignature("minus", "native function minus<T>(values:T[*]):T[1];");
        reg.registerSignature("times", "native function times<T>(values:T[*]):T[1];");
        reg.registerSignature("divide", "native function divide(dividend:Number[1], divisor:Number[1]):Float[1];");

        // ===== Comparison =====
        reg.registerSignature("equal", "native function equal(left:Any[1], right:Any[1]):Boolean[1];");
        reg.registerSignature("eq", "native function eq(left:Any[1], right:Any[1]):Boolean[1];");
        reg.registerSignature("greaterThan",
                "native function greaterThan(left:Number[1], right:Number[1]):Boolean[1];");
        reg.registerSignature("lessThan", "native function lessThan(left:Number[1], right:Number[1]):Boolean[1];");
        reg.registerSignature("greaterThanEqual",
                "native function greaterThanEqual(left:Number[1], right:Number[1]):Boolean[1];");
        reg.registerSignature("lessThanEqual",
                "native function lessThanEqual(left:Number[1], right:Number[1]):Boolean[1];");
        reg.registerSignature("between",
                "native function between(value:Number[1], low:Number[1], high:Number[1]):Boolean[1];");
        reg.registerSignature("compare", "native function compare(left:Any[1], right:Any[1]):Integer[1];");
        reg.registerSignature("greatest", "native function greatest(values:Any[*]):Any[0..1];");
        reg.registerSignature("least", "native function least(values:Any[*]):Any[0..1];");
        reg.registerSignature("coalesce", "native function coalesce(values:Any[*]):Any[0..1];");
        reg.registerSignature("in", "native function in(value:Any[1], collection:Any[*]):Boolean[1];");

        // ===== Boolean =====
        reg.registerSignature("and", "native function and(left:Boolean[1], right:Boolean[1]):Boolean[1];");
        reg.registerSignature("or", "native function or(left:Boolean[1], right:Boolean[1]):Boolean[1];");
        reg.registerSignature("not", "native function not(value:Boolean[1]):Boolean[1];");
        reg.registerSignature("xor", "native function xor(left:Boolean[1], right:Boolean[1]):Boolean[1];");
        reg.registerSignature("isEmpty", "native function isEmpty<T>(value:T[*]):Boolean[1];");
        reg.registerSignature("isNotEmpty", "native function isNotEmpty<T>(value:T[*]):Boolean[1];");

        // ===== Bitwise =====
        reg.registerSignature("bitAnd", "native function bitAnd(left:Integer[1], right:Integer[1]):Integer[1];");
        reg.registerSignature("bitOr", "native function bitOr(left:Integer[1], right:Integer[1]):Integer[1];");
        reg.registerSignature("bitXor", "native function bitXor(left:Integer[1], right:Integer[1]):Integer[1];");
        reg.registerSignature("bitShiftLeft",
                "native function bitShiftLeft(value:Integer[1], bits:Integer[1]):Integer[1];");
        reg.registerSignature("bitShiftRight",
                "native function bitShiftRight(value:Integer[1], bits:Integer[1]):Integer[1];");

        // ===== Date/Time =====
        reg.registerSignature("dateDiff",
                "native function dateDiff(d1:Date[1], d2:Date[1], du:DurationUnit[1]):Integer[1];");
        reg.registerSignature("datePart", "native function datePart(d:Date[1]):StrictDate[1];");
        reg.registerSignature("date",
                "native function date(year:Integer[1], month:Integer[1], day:Integer[1]):StrictDate[1];");
        reg.registerSignature("adjust",
                "native function adjust(d:Date[1], amount:Integer[1], unit:DurationUnit[1]):Date[1];");
        reg.registerSignature("timeBucket",
                "native function timeBucket(d:Date[1], amount:Integer[1], unit:DurationUnit[1]):Date[1];");
        reg.registerSignature("year", "native function year(d:Date[1]):Integer[1];");
        reg.registerSignature("monthNumber", "native function monthNumber(d:Date[1]):Integer[1];");
        reg.registerSignature("dayOfMonth", "native function dayOfMonth(d:Date[1]):Integer[1];");
        reg.registerSignature("hour", "native function hour(d:Date[1]):Integer[1];");
        reg.registerSignature("minute", "native function minute(d:Date[1]):Integer[1];");
        reg.registerSignature("second", "native function second(d:Date[1]):Integer[1];");
        reg.registerSignature("now", "native function now():DateTime[1];");
        reg.registerSignature("today", "native function today():StrictDate[1];");
        reg.registerSignature("hasHour", "native function hasHour(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasMinute", "native function hasMinute(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasMonth", "native function hasMonth(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasDay", "native function hasDay(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasSecond", "native function hasSecond(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasSubsecond", "native function hasSubsecond(d:Date[1]):Boolean[1];");
        reg.registerSignature("hasSubsecondWithAtLeastPrecision",
                "native function hasSubsecondWithAtLeastPrecision(d:Date[1], precision:Integer[1]):Boolean[1];");

        // ===== Conversion =====
        reg.registerSignature("parseInteger", "native function parseInteger(string:String[1]):Integer[1];");
        reg.registerSignature("parseFloat", "native function parseFloat(string:String[1]):Float[1];");
        reg.registerSignature("parseDecimal", "native function parseDecimal(string:String[1]):Decimal[1];");
        reg.registerSignature("parseDate", "native function parseDate(string:String[1]):Date[1];");
        reg.registerSignature("parseBoolean", "native function parseBoolean(string:String[1]):Boolean[1];");
        reg.registerSignature("toDecimal", "native function toDecimal(number:Number[1]):Decimal[1];");
        reg.registerSignature("toFloat", "native function toFloat(number:Number[1]):Float[1];");

        // ===== Aggregation =====
        reg.registerSignature("count", "native function count<T>(values:T[*]):Integer[1];");
        reg.registerSignature("sum", "native function sum(numbers:Number[*]):Number[1];");
        reg.registerSignature("average", "native function average(numbers:Number[*]):Float[1];");
        reg.registerSignature("avg", "native function avg(numbers:Number[*]):Float[1];");
        reg.registerSignature("mean", "native function mean(numbers:Number[*]):Float[1];");
        reg.registerSignature("median", "native function median(numbers:Number[*]):Number[1];");
        reg.registerSignature("mode", "native function mode(values:Any[*]):Any[0..1];");
        reg.registerSignature("min", "native function min(numbers:Number[*]):Number[0..1];");
        reg.registerSignature("max", "native function max(numbers:Number[*]):Number[0..1];");
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], key:Function<{T[1]->Any[1]}>[1]):T[0..1];");
        reg.registerSignature("maxBy",
                "native function maxBy<T>(values:T[*], key:Function<{T[1]->Any[1]}>[1], count:Integer[1]):T[*];");
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], key:Function<{T[1]->Any[1]}>[1]):T[0..1];");
        reg.registerSignature("minBy",
                "native function minBy<T>(values:T[*], key:Function<{T[1]->Any[1]}>[1], count:Integer[1]):T[*];");
        reg.registerSignature("stdDev", "native function stdDev(numbers:Number[*]):Number[1];");
        reg.registerSignature("stdDevSample", "native function stdDevSample(numbers:Number[*]):Number[1];");
        reg.registerSignature("variance", "native function variance(numbers:Number[*]):Number[1];");
        reg.registerSignature("varianceSample", "native function varianceSample(numbers:Number[*]):Number[1];");
        reg.registerSignature("variancePopulation", "native function variancePopulation(numbers:Number[*]):Number[1];");
        reg.registerSignature("covarPopulation",
                "native function covarPopulation(x:Number[*], y:Number[*]):Number[1];");
        reg.registerSignature("corr", "native function corr(x:Number[*], y:Number[*]):Number[1];");
        reg.registerSignature("percentile", "native function percentile(numbers:Number[*], p:Number[1]):Number[1];");
        reg.registerSignature("wavg", "native function wavg(numbers:Number[*]):Float[1];");
        reg.registerSignature("covarSample", "native function covarSample(x:Number[*], y:Number[*]):Number[1];");
        reg.registerSignature("stdDevPopulation", "native function stdDevPopulation(numbers:Number[*]):Number[1];");
        reg.registerSignature("percentileCont",
                "native function percentileCont(numbers:Number[*], p:Number[1]):Number[1];");
        reg.registerSignature("percentileDisc",
                "native function percentileDisc(numbers:Number[*], p:Number[1]):Number[1];");

        // ===== Window aggregate overloads =====
        // These model the 3-param window call pattern: (Relation<T>, _Window<T>, T) → T
        // The TypeVar return type causes compileTypePropagating to propagate
        // relationType in window context (e.g., $p->sum($w,$r).salary).
        reg.registerSignature("sum", "native function sum<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("avg", "native function avg<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("average", "native function average<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("mean", "native function mean<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("min", "native function min<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("max", "native function max<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("count", "native function count<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):Integer[1];");
        reg.registerSignature("median", "native function median<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("mode", "native function mode<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("percentile", "native function percentile<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDev", "native function stdDev<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDevSample", "native function stdDevSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("stdDevPopulation", "native function stdDevPopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("variance", "native function variance<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("varianceSample", "native function varianceSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("variancePopulation", "native function variancePopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("corr", "native function corr<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("covarPopulation", "native function covarPopulation<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");
        reg.registerSignature("covarSample", "native function covarSample<T>(rel:Relation<T>[1], w:_Window<T>[1], r:T[1]):T[0..1];");

        // ===== Collection =====
        reg.registerSignature("toOne", "native function toOne<T>(values:T[*]):T[1];");
        reg.registerSignature("toOne", "native function toOne<T>(values:T[*], message:String[1]):T[1];");
        reg.registerSignature("toOneMany", "native function toOneMany<T>(values:T[*], message:String[1]):T[1..*];");
        reg.registerSignature("last", "native function last<T>(set:T[*]):T[0..1];");
        reg.registerSignature("head", "native function head<T>(set:T[*]):T[0..1];");
        reg.registerSignature("at", "native function at<T>(set:T[*], index:Integer[1]):T[1];");
        reg.registerSignature("reverse", "native function reverse<T|m>(set:T[m]):T[m];");
        reg.registerSignature("slice", "native function slice<T>(set:T[*], start:Integer[1], end:Integer[1]):T[*];");
        reg.registerSignature("take", "native function take<T>(set:T[*], count:Integer[1]):T[*];");
        reg.registerSignature("drop", "native function drop<T>(set:T[*], count:Integer[1]):T[*];");
        reg.registerSignature("exists",
                "native function exists<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];");
        reg.registerSignature("forAll",
                "native function forAll<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):Boolean[1];");
        reg.registerSignature("find",
                "native function find<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[0..1];");
        reg.registerSignature("filter",
                "native function filter<T>(value:T[*], func:Function<{T[1]->Boolean[1]}>[1]):T[*];");
        reg.registerSignature("map", "native function map<T,V>(value:T[*], func:Function<{T[1]->V[*]}>[1]):V[*];");
        reg.registerSignature("map",
                "native function map<T,V>(value:T[0..1], func:Function<{T[1]->V[0..1]}>[1]):V[0..1];");
        reg.registerSignature("zip", "native function zip<T,U>(set1:T[*], set2:U[*]):Pair<T,U>[*];");
        reg.registerSignature("indexOf", "native function indexOf<T>(set:T[*], value:T[1]):Integer[1];");
        reg.registerSignature("removeAllOptimized",
                "native function removeAllOptimized<T>(set:T[*], other:T[*]):T[*];");
        reg.registerSignature("size", "native function size<T>(col:T[*]):Integer[1];");
        reg.registerSignature("range", "native function range(start:Integer[1], stop:Integer[1], step:Integer[1]):Integer[*];");
        reg.registerSignature("range", "native function range(start:Integer[1], stop:Integer[1]):Integer[*];");
        reg.registerSignature("range", "native function range(stop:Integer[1]):Integer[*];");
        reg.registerSignature("init", "native function init<T>(set:T[*]):T[*];");
        reg.registerSignature("tail", "native function tail<T>(set:T[*]):T[*];");
        reg.registerSignature("add", "native function add<T>(set:T[*], val:T[1]):T[*];");
        reg.registerSignature("add", "native function add<T>(set:T[*], index:Integer[1], val:T[1]):T[*];");
        reg.registerSignature("removeDuplicates", "native function removeDuplicates<T>(col:T[*]):T[*];");
        reg.registerSignature("removeDuplicates",
                "native function removeDuplicates<T>(col:T[*], eql:Function<{T[1],T[1]->Boolean[1]}>[0..1]):T[*];");
        reg.registerSignature("removeDuplicatesBy",
                "native function removeDuplicatesBy<T,V>(col:T[*], key:Function<{T[1]->V[1]}>[1]):T[*];");
        reg.registerSignature("first", "native function first<T>(set:T[*]):T[0..1];");

        // ===== Relation overloads for limit/take/drop/slice/first =====
        reg.registerSignature("limit",
                "native function limit<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("take",
                "native function take<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("drop",
                "native function drop<T>(rel:Relation<T>[1], size:Integer[1]):Relation<T>[1];");
        reg.registerSignature("slice",
                "native function slice<T>(rel:Relation<T>[1], start:Integer[1], stop:Integer[1]):Relation<T>[1];");
        reg.registerSignature("first",
                "native function first<T>(rel:Relation<T>[1]):Relation<T>[0..1];");
        // Scalar overloads for drop/slice (array operations)
        reg.registerSignature("drop",
                "native function drop<T>(set:T[*], count:Integer[1]):T[*];");
        reg.registerSignature("slice",
                "native function slice<T>(set:T[*], start:Integer[1], end:Integer[1]):T[*];");
        reg.registerSignature("limit",
                "native function limit<T>(set:T[*], size:Integer[1]):T[*];");
        reg.registerSignature("take",
                "native function take<T>(set:T[*], size:Integer[1]):T[*];");

        // ===== Boolean / Meta =====
        reg.registerSignature("instanceOf", "native function instanceOf(instance:Any[1], type:Type[1]):Boolean[1];");

        // ===== Collection sort (legend-pure) =====
        reg.registerSignature("sort",
                "native function sort<T,U|m>(col:T[m], key:Function<{T[1]->U[1]}>[0..1], comp:Function<{U[1],U[1]->Integer[1]}>[0..1]):T[m];");
        reg.registerSignature("sort", "native function sort<T|m>(col:T[m]):T[m];");
        reg.registerSignature("sort",
                "native function sort<T|m>(col:T[m], comp:Function<{T[1],T[1]->Integer[1]}>[0..1]):T[m];");

        // ===== Misc =====
        reg.registerSignature("pair", "native function pair<T,U>(first:T[1], second:U[1]):Pair<T,U>[1];");
        reg.registerSignature("list", "native function list<T>(values:T[*]):List<T>[1];");
        reg.registerSignature("type", "native function type(any:Any[1]):Type[1];");
        reg.registerSignature("generateGuid", "native function generateGuid():String[1];");
        reg.registerSignature("rowMapper", "native function rowMapper<T>(rel:Relation<T>[1], row:T[1]):T[0..1];");
    }
}
