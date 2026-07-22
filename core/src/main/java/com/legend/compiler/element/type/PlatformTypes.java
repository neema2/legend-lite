package com.legend.compiler.element.type;

/**
 * EXACT identification of the platform's distinguished types — the ONE home
 * for these checks. Suffix matching ({@code endsWith("::List")}) was a bug
 * class, not a convenience: a user class that happens to share a simple name
 * (my::domain::List) must never be mistaken for the platform carrier.
 *
 * <p>The FQN constants mirror the {@code builtin/Pure} prelude declarations;
 * {@code PlatformTypesDriftTest} pins the two against each other so neither
 * can move alone. (Constants rather than {@code Pure.X.qualifiedName()}
 * references keep this package free of a dependency on the parser-level
 * prelude classes.)
 */
public final class PlatformTypes {

    /**
     * The Typer's {@code .rows} MARKER property (identity over a relation
     * value — the K result frame's row-index/envelope disambiguator).
     * ONE constant for producer (Typer desugars) and consumers
     * (StoreResolver erasure floor, Lowerer defensive floor) — audit 23 A6.
     */
    public static final String ROWS_MARKER = "rows";

    /**
     * The TDS null-cell SENTINEL spelling — real pure's {@code ^TDSNull()}
     * instance prints as this string (tds.pure). ONE constant for every
     * producer (Typer toString-get desugar, makeString/joinStrings NULL
     * coalesce) and parser (TDS-literal cells, harness wire compares) —
     * audit 23 C-d. Divergence note: pure DROPS empty elements from
     * ordinary collections; we print the sentinel only where TDS-row
     * semantics apply (ledgered in AUDIT_23_SPECIAL_CASING.md).
     */
    public static final String TDS_NULL_CELL = "TDSNull";

    private PlatformTypes() {
    }

    public static final String ANY = "meta::pure::metamodel::type::Any";
    public static final String NIL = "meta::pure::metamodel::type::Nil";
    public static final String VARIANT = "meta::pure::metamodel::variant::Variant";
    public static final String LIST = "meta::pure::functions::collection::List";
    public static final String PAIR = "meta::pure::functions::collection::Pair";
    public static final String FUNCTION = "meta::pure::metamodel::function::Function";
    /** The legacy TDS surface — ≡ the relation carrier at the value level
     * ({@code cast(@TabularDataSet)} is a type ASSERTION, never a wire
     * conversion). */
    public static final String TABULAR_DATA_SET = "meta::pure::tds::TabularDataSet";

    /**
     * The K-native JDBC boundary: raw-SQL execution over the ambient
     * connection ({@code Compiler}'s executeInDb dispatch). A FUNCTION
     * FQN, not a type — it lives here because this class is the one home
     * for exact platform-FQN identification.
     */
    public static final String EXECUTE_IN_DB = "meta::relational::metamodel::execute::executeInDb";

    /** K-native sibling of {@link #EXECUTE_IN_DB}: model-derived drop+create DDL. */
    public static final String DROP_AND_CREATE_TABLE_IN_DB =
            "meta::relational::functions::toDDL::dropAndCreateTableInDb";

    /** Schema (re)creation K-native (toDDL.pure:108). */
    public static final String DROP_AND_CREATE_SCHEMA_IN_DB =
            "meta::relational::functions::toDDL::dropAndCreateSchemaInDb";

    /** DDL STRING generators (toDDL.pure deprecated 1-/3-arg forms):
     * evaluate in the EXECUTOR (the engine walks its Database metamodel;
     * we render from the compiled store model — model access the lowerer
     * does not have). Engine golden spellings: testDDL.pure:42-45. */
    public static final String DROP_SCHEMA_STATEMENT =
            "meta::relational::functions::toDDL::dropSchemaStatement";
    public static final String CREATE_SCHEMA_STATEMENT =
            "meta::relational::functions::toDDL::createSchemaStatement";
    public static final String CREATE_TABLE_STATEMENT =
            "meta::relational::functions::toDDL::createTableStatement";

    /** One of the DDL string-generator natives. */
    public static boolean isDdlStatementFn(String fqn) {
        return DROP_SCHEMA_STATEMENT.equals(fqn)
                || CREATE_SCHEMA_STATEMENT.equals(fqn)
                || CREATE_TABLE_STATEMENT.equals(fqn);
    }

    /** The engine's SQL-text surface — K-dispatched: the query lambda
     * lowers through the platform's own G½->H->I against the given mapping
     * and renders with the engine-style dialect (audit 19d B3: this was a
     * name-intercepting harness arm; the corpus's own toSQLString body is
     * engine plan-generation internals, suppressed like toDDL). */
    public static final String TO_SQL_STRING =
            "meta::relational::functions::sqlstring::toSQLString";

    /** The engine's CSV-seed SQL generator — K-dispatched (CsvSeed). */
    public static final String SET_UP_DATA_SQLS_V2 =
            "meta::alloy::service::execution::setUpDataSQLsV2";

    /** The engine's execution entry — K-dispatched as a RESULT FRAME
     * (audit 19d B2: {@code Result} is a typing surface plus an
     * orchestration handle, never a host object graph; reads over it
     * splice into SQL-bound typed queries in the statement executor). */
    public static final String EXECUTE = "meta::pure::mapping::execute";

    /**
     * PLATFORM-OWNED function FQNs: legend-lite's native IS the definition
     * — user re-definitions (the real engine's toDDL.pure bodies walk the
     * Database METAMODEL, M3 reflection this platform doesn't model) are
     * suppressed at the overload merge, exactly like real pure natives
     * replacing their stub bodies. executeInDb is NOT owned: the corpus's
     * 2-arg wrapper there is legitimate pure code over the 4-arg leaf.
     */
    public static boolean isPlatformOwnedFunction(String fqn) {
        return DROP_AND_CREATE_TABLE_IN_DB.equals(fqn)
                || DROP_AND_CREATE_SCHEMA_IN_DB.equals(fqn)
                || isDdlStatementFn(fqn)
                || TO_SQL_STRING.equals(fqn)
                || EXECUTE.equals(fqn);
    }

    /** Debug output — K-dispatched as a NO-OP, arguments never evaluated. */
    public static final String PRINT = "meta::pure::functions::io::print";
    public static final String PRINTLN = "meta::pure::functions::io::println";

    /**
     * K-natives with REAL side effects (raw SQL over the connection).
     * print/println are K-DISPATCHED but effect-FREE (no-op arm) — the
     * effectful-let guard and statement-orchestration routing key on THIS,
     * not on {@link #isKNative} (audit 17: counting print as an effect
     * made harmless let bindings refuse loudly).
     */
    public static boolean isEffectfulNative(String fqn) {
        return EXECUTE_IN_DB.equals(fqn)
                || DROP_AND_CREATE_TABLE_IN_DB.equals(fqn)
                || DROP_AND_CREATE_SCHEMA_IN_DB.equals(fqn);
    }

    /** All K-natives: calls that EXECUTE at the K boundary and never lower. */
    public static boolean isKNative(String fqn) {
        return EXECUTE_IN_DB.equals(fqn)
                || DROP_AND_CREATE_TABLE_IN_DB.equals(fqn)
                || DROP_AND_CREATE_SCHEMA_IN_DB.equals(fqn)
                || TO_SQL_STRING.equals(fqn)
                || SET_UP_DATA_SQLS_V2.equals(fqn)
                || EXECUTE.equals(fqn)
                || PRINT.equals(fqn) || PRINTLN.equals(fqn);
    }

    /** The top type. */
    public static boolean isAny(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(ANY);
    }

    /** The bottom type (the []-born element type). */
    public static boolean isNil(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(NIL);
    }

    /** The semi-structured JSON carrier. */
    public static boolean isVariant(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(VARIANT);
    }

    /** The {@code List<T>} collection carrier (parameterized form). */
    public static boolean isListCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(LIST)
                && g.arguments().size() == 1;
    }

    /** The {@code Pair<U,V>} value carrier (parameterized form). */
    public static boolean isPairCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(PAIR)
                && g.arguments().size() == 2;
    }

    /** The {@code Map<U,V>} collection carrier (parameterized form). */
    public static boolean isMapCarrier(Type t) {
        return t instanceof Type.GenericType g
                && g.rawFqn().equals("meta::pure::functions::collection::Map")
                && g.arguments().size() == 2;
    }

    /** The {@code Function<{…}>} value carrier (parameterized form). */
    public static boolean isFunctionCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(FUNCTION);
    }
}
