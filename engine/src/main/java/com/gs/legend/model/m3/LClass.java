package com.gs.legend.model.m3;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Dispatch-time catalog of platform-defined <em>Pure classes</em> that carry semantic meaning in
 * the type checker — {@link #RELATION}, {@link #COL_SPEC}, {@link #WINDOW}, and friends. The type
 * system's counterpart to {@link Primitive} for classes: where {@code Primitive.INTEGER} is both a
 * declaration and a usable {@link Type} value, each {@code LClass} constant is a usable {@code Type}
 * value representing a specific platform class's identity (by FQN).
 *
 * <h2>Why an enum, why this name</h2>
 *
 * <p>Dispatch sites that previously switched on raw simple-name strings
 * ({@code "Relation".equals(p.rawType())}) now pattern-match against {@code LClass} variants:
 * <pre>{@code
 * switch (generic.rawType()) {
 *     case LClass.RELATION   -> handleRelation(generic.typeArgs());
 *     case LClass.COL_SPEC   -> handleColSpec(generic.typeArgs());
 *     case ClassType userCls -> handleUserClass(userCls);
 *     // ...
 * }
 * }</pre>
 * The compiler catches typos ({@code LClass.RELATIN}) at compile time, dispatch cases are constant
 * expressions (not {@code when} guards), and {@code implements Type} means every constant IS a
 * {@code Type} — no wrapping required to pass into the type system.
 *
 * <p>The {@code L} prefix dodges the {@link java.lang.Class} collision while retaining a terse
 * one-word name. It is not used on {@link Primitive} — no collision, so no prefix.
 *
 * <h2>Relationship to {@link com.gs.legend.compiler.BuiltinClassRegistry}</h2>
 *
 * <p>{@code LClass} owns dispatch <em>identity</em> — the FQNs the compiler knows about.
 * {@code BuiltinClassRegistry} owns class <em>definitions</em> — the Pure source that produces a
 * {@link com.gs.legend.model.def.ClassDefinition} for each entry. Two concerns, two artifacts.
 * When definitions eventually move from hardcoded Pure strings to loaded {@code .pure} files, only
 * {@code BuiltinClassRegistry} changes; this enum stays the same.
 *
 * <p>A static drift-check in {@code BuiltinClassRegistry} verifies every {@code LClass} variant
 * has a matching {@code ClassDefinition} — so if the two fall out of sync the JVM fails to start
 * with a clear diagnostic.
 */
public enum LClass implements Type {

    // ===== M3 bootstrap =====
    TYPE("meta::pure::metamodel::type::Type"),
    CLASS("meta::pure::metamodel::type::Class"),

    // ===== Function / Relation roots =====
    FUNCTION("meta::pure::metamodel::function::Function"),
    FUNCTION_DEFINITION("meta::pure::metamodel::function::FunctionDefinition"),
    RELATION("meta::pure::metamodel::relation::Relation"),
    ROOT_GRAPH_FETCH_TREE("meta::pure::graphFetch::RootGraphFetchTree"),

    // ===== Collections =====
    PAIR("meta::pure::functions::collection::Pair"),
    LIST("meta::pure::functions::collection::List"),
    MAP("meta::pure::functions::collection::Map"),
    TREE_NODE("meta::pure::functions::collection::TreeNode"),
    VALUE_HOLDER("meta::pure::functions::collection::ValueHolder"),

    // ===== Relation column specs =====
    COL_SPEC("meta::pure::metamodel::relation::ColSpec"),
    COL_SPEC_ARRAY("meta::pure::metamodel::relation::ColSpecArray"),
    FUNC_COL_SPEC("meta::pure::metamodel::relation::FuncColSpec"),
    FUNC_COL_SPEC_ARRAY("meta::pure::metamodel::relation::FuncColSpecArray"),
    AGG_COL_SPEC("meta::pure::metamodel::relation::AggColSpec"),
    AGG_COL_SPEC_ARRAY("meta::pure::metamodel::relation::AggColSpecArray"),

    // ===== Window / sort =====
    SORT_INFO("meta::pure::functions::relation::SortInfo"),
    WINDOW("meta::pure::functions::relation::_Window"),

    // ===== Frame hierarchy =====
    FRAME("meta::pure::functions::relation::Frame"),
    FRAME_VALUE("meta::pure::functions::relation::FrameValue"),
    FRAME_INT_VALUE("meta::pure::functions::relation::FrameIntValue"),
    FRAME_NUMERIC_VALUE("meta::pure::functions::relation::FrameNumericValue"),
    FRAME_INTERVAL_VALUE("meta::pure::functions::relation::FrameIntervalValue"),
    UNBOUNDED_FRAME_VALUE("meta::pure::functions::relation::UnboundedFrameValue"),
    RANGE("meta::pure::functions::relation::_Range"),
    ROWS("meta::pure::functions::relation::Rows"),

    // ===== Math aggregator =====
    ROW_MAPPER("meta::pure::functions::math::mathUtility::RowMapper"),

    // ===== Legend-lite only =====
    TRAVERSAL("meta::pure::functions::relation::_Traversal"),
    ;

    private final String fqn;
    private final String simpleName;

    LClass(String fqn) {
        this.fqn = fqn;
        int idx = fqn.lastIndexOf("::");
        this.simpleName = idx < 0 ? fqn : fqn.substring(idx + 2);
    }

    /** Fully-qualified Pure name, e.g. {@code "meta::pure::metamodel::relation::Relation"}. */
    public String qualifiedName() {
        return fqn;
    }

    /**
     * {@inheritDoc} Returns the simple Pure name (e.g. {@code "Relation"}), matching
     * the contract used by {@link Primitive#typeName()} and {@link Type.ClassType#typeName()}.
     */
    @Override
    public String typeName() {
        return simpleName;
    }

    // ===== Reverse lookup =====

    private static final Map<String, LClass> BY_FQN =
            Arrays.stream(values()).collect(Collectors.toUnmodifiableMap(LClass::qualifiedName, c -> c));

    /**
     * Reverse lookup by FQN. Returns empty for FQNs not in the platform catalog (user classes,
     * typos, unknown names).
     */
    public static Optional<LClass> findByFqn(String fqn) {
        return Optional.ofNullable(BY_FQN.get(fqn));
    }
}
