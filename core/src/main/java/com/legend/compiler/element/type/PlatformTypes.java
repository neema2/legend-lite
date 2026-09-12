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
    /** The Class metaclass FQN — type VALUES at the lowering boundary
     * travel as canonical simple-name strings (task #78). */
    public static final String CLASS_METACLASS =
            "meta::pure::metamodel::type::Class";


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

    /** The TDS null-cell CLASS (engine tds.pure:127) — {@code ^TDSNull()}
     * types as an instance of it, stamped [1] (a VALUE, never an empty). */
    public static final String TDS_NULL_FQN = "meta::pure::tds::TDSNull";

    private PlatformTypes() {
    }

    public static final String ANY = "meta::pure::metamodel::type::Any";
    public static final String NIL = "meta::pure::metamodel::type::Nil";
    public static final String VARIANT = "meta::pure::metamodel::variant::Variant";
    public static final String LIST = "meta::pure::functions::collection::List";
    public static final String PAIR = "meta::pure::functions::collection::Pair";
    public static final String FUNCTION = "meta::pure::metamodel::function::Function";
    /** The m3 function-carrier hierarchy under {@link #FUNCTION}:
     * {@code LambdaFunction<F>} and {@code ConcreteFunctionDefinition<F>}
     * extend {@code FunctionDefinition<F>} extends {@code Function<F>}
     * (m3.pure). A value-level function's CLASSIFIER is one of these
     * carriers; the structural {@code FunctionType} it wraps is the
     * signature. Lambda literals classify as LambdaFunction; references
     * to body-bearing user functions as ConcreteFunctionDefinition;
     * native-function references are NOT FunctionDefinitions. */
    public static final String FUNCTION_DEFINITION = "meta::pure::metamodel::function::FunctionDefinition";
    public static final String LAMBDA_FUNCTION = "meta::pure::metamodel::function::LambdaFunction";
    public static final String CONCRETE_FUNCTION_DEFINITION = "meta::pure::metamodel::function::ConcreteFunctionDefinition";

    /** The classifier of a lambda literal: {@code LambdaFunction<ft>}. */
    public static Type lambdaType(Type.FunctionType ft) {
        return new Type.GenericType(LAMBDA_FUNCTION, java.util.List.of(ft));
    }

    /** The classifier of a reference to a body-bearing user function:
     * {@code ConcreteFunctionDefinition<ft>}. */
    public static Type concreteFunctionDefinitionType(Type.FunctionType ft) {
        return new Type.GenericType(CONCRETE_FUNCTION_DEFINITION, java.util.List.of(ft));
    }

    /** The structural signature a function-valued type carries — the bare
     * {@code FunctionType}, or the one inside a carrier spelling
     * ({@code Function<{…}>}, {@code LambdaFunction<{…}>}, …); {@code null}
     * when {@code t} is not function-valued (including carriers whose
     * argument is nominal, e.g. {@code FunctionDefinition<Any>}). */
    public static Type.@com.legend.Nullable FunctionType functionTypeOf(Type t) {
        if (t instanceof Type.FunctionType ft) {
            return ft;
        }
        if (t instanceof Type.GenericType g && g.arguments().size() == 1
                && g.arguments().get(0) instanceof Type.FunctionType ft) {
            return ft;
        }
        return null;
    }
    /** The legacy TDS surface — ≡ the relation carrier at the value level
     * ({@code cast(@TabularDataSet)} is a type ASSERTION, never a wire
     * conversion). */
    public static final String TABULAR_DATA_SET = "meta::pure::tds::TabularDataSet";

    public static final String TDS_ROW = com.legend.builtin.NativeFn.TDS_ROW_OWNER;

    /** TDS ERASURE (docs/TDS_ERASURE_DESIGN_2026_09_11.md §4b): an ERASED ROW
     * class — an owner of the row-accessor family, TDSRow and the ResultSet's
     * execute::Row — in a TYPE position (a signature parameter, a lambda
     * annotation, a class property) IS the erased row: a row struct whose
     * columns are late-bound. Applied where a type expression becomes a Type
     * (TypeClassifier, TypeAnnotations), so nothing downstream ever sees the
     * nominal class. */
    public static Type eraseTdsRow(Type t) {
        return t instanceof Type.ClassType c && com.legend.builtin.NativeFn.RowGetter.isOwner(c.fqn())
                ? Type.RelationType.lateBound() : t;
    }

    /** Whether a LIFTED derived-property function ({@code <owner>$prop$<name>})
     *  is one the platform IMPLEMENTS (the row accessors — RowGetters): its
     *  lifted definition types the call, its body is never spliced. */
    public static boolean isPlatformImplementedDerived(String liftedFqn) {
        return com.legend.builtin.NativeFn.RowGetter.ofLifted(liftedFqn).isPresent()
                || com.legend.builtin.NativeFn.JavaRoutine.ofDerived(liftedFqn).isPresent();
    }
    /** The mapping METACLASS (platform_dsl_mapping mapping.pure:26) — a prelude
     * module class since batch 165 (mapping leg B); Java names it, the boot
     * layer defines it. */
    public static final String MAPPING = "meta::pure::mapping::Mapping";
    /** The function-carrier nominals of real pure's m3 hierarchy —
     * LambdaFunction&lt;T&gt; extends FunctionDefinition&lt;T&gt; extends
     * Function&lt;T&gt;; each is a wrapper spelling of the bare FunctionType
     * it carries (InferenceKernel unwraps them; Type.classFqn excludes
     * them — a carrier value is a lambda, never a row). */
    public static final java.util.Set<String> FUNCTION_CARRIERS = java.util.Set.of(
            FUNCTION, FUNCTION_DEFINITION, LAMBDA_FUNCTION, CONCRETE_FUNCTION_DEFINITION);
    /** The relational store metaclass (relational.pure:29) — a prelude module class
     * since batch 164 (leg A of the mapping legs); the seeds and the extents name it here. */
    public static final String DATABASE = "meta::relational::metamodel::Database";
    /** The relational Table (the element a #>{db.table}# store accessor
     * DENOTES — upstream's RelationStoreAccessor.sourceElement): what
     * tableToTDS(table:Table[1]) is declared over. */
    public static final String RELATIONAL_TABLE = "meta::relational::metamodel::relation::Table";
    /** m3 metaclasses — prelude module classes since batch 161 (phase 2 family 4);
     * every Java site names them here, definitions come from the boot layer. */
    public static final String ELEMENT_OVERRIDE = "meta::pure::metamodel::type::ElementOverride";
    public static final String GENERIC_TYPE = "meta::pure::metamodel::type::generics::GenericType";
    public static final String MEASURE = "meta::pure::metamodel::type::Measure";
    public static final String UNIT = "meta::pure::metamodel::type::Unit";
    public static final String PACKAGE = "meta::pure::metamodel::Package";
    public static final String ENUMERATION = "meta::pure::metamodel::type::Enumeration";
    /** The relation carrier {@code Relation<T>} (legend-pure relation.pure / m3). */
    public static final String RELATION = "meta::pure::metamodel::relation::Relation";
    /** The store accessor a {@code #>{db.table}#} literal IS (upstream:
     * {@code RelationStoreAccessor<T> extends RelationElementAccessor<T>
     * extends Relation<T>}, all three in the prelude) — batch 5: write's
     * second parameter is declared over the accessor, as upstream spells it. */
    public static final String RELATION_STORE_ACCESSOR = "meta::pure::store::RelationStoreAccessor";
    public static final String RELATION_ELEMENT_ACCESSOR =
            "meta::pure::metamodel::relation::RelationElementAccessor";
    public static final String TDS_RELATION_ACCESSOR =
            "meta::pure::metamodel::relation::TDSRelationAccessor";
    /** The m3 Profile metaclass (a prelude module class since batch 159). */
    public static final String PROFILE = "meta::pure::metamodel::extension::Profile";

    /** The relation-algebra column-specification family (legend-pure
     * relation.pure:17-50) — prelude module classes since batch 157; the
     * Typer names them here and reads their definitions from the model. */
    public static final String COL_SPEC = "meta::pure::metamodel::relation::ColSpec";
    public static final String COL_SPEC_ARRAY = "meta::pure::metamodel::relation::ColSpecArray";
    public static final String FUNC_COL_SPEC = "meta::pure::metamodel::relation::FuncColSpec";
    public static final String FUNC_COL_SPEC_ARRAY = "meta::pure::metamodel::relation::FuncColSpecArray";
    public static final String AGG_COL_SPEC = "meta::pure::metamodel::relation::AggColSpec";
    public static final String AGG_COL_SPEC_ARRAY = "meta::pure::metamodel::relation::AggColSpecArray";
    /** {@code over(...)}'s row-frame marker class (engine rows.pure). */
    public static final String ROWS = "meta::pure::functions::relation::Rows";
    /** The window value {@code over(...)} builds — {@code extend}'s second
     * parameter; an {@code over} argument types like a lambda, AFTER the
     * enclosing overload is chosen, with the expected {@code _Window<T>}. */
    public static final String WINDOW = "meta::pure::functions::relation::_Window";
    /** The mapping execution result envelope (legend-pure result.pure). */
    public static final String RESULT = "meta::pure::mapping::Result";

    /** The {@code TDS<T>} relation class (tds.pure:17) — a relation
     * literal's own type; {@code csv: String[1]} (tds.pure:19) is its text. */
    public static final String TDS_RELATION_CLASS = "meta::pure::metamodel::relation::TDS";
    /** Every raw class whose ONE type argument is a relation schema: the
     * relation itself and its accessor subclasses (the prelude's declared
     * hierarchy; PlatformTypesSpellingsTest pins each as a Relation subclass). */
    public static final java.util.Set<String> RELATION_CARRIERS = java.util.Set.of(
            RELATION, RELATION_ELEMENT_ACCESSOR, RELATION_STORE_ACCESSOR, TDS_RELATION_ACCESSOR,
            TDS_RELATION_CLASS);

    /** The TDS class's csv property name (tds.pure:19); a read of it over a
     * TDS-shaped value is the csv TEXT the engine prints for the relation. */
    public static final String TDS_CSV_PROPERTY = "csv";

    /** Whether {@code t} is TDS-SHAPED: a schema-bearing {@code Relation<..>},
     * the {@code TDS<T>} relation class, or TabularDataSet — the types a
     * cast between which is a type-level no-op (exact FQNs, never a
     * suffix match). */
    public static boolean isTdsShaped(Type t) {
        return Type.isRelation(t)
                || isTdsType(t)
                || t instanceof Type.GenericType g
                        && TDS_RELATION_CLASS.equals(g.rawFqn());
    }

    /** Whether {@code t} is the TDS carrier type (exact FQN, never a
     * suffix match). */
    public static boolean isTdsType(Type t) {
        return t instanceof Type.ClassType ct
                        && TABULAR_DATA_SET.equals(ct.fqn())
                || t instanceof Type.GenericType gt
                        && TABULAR_DATA_SET.equals(gt.rawFqn());
    }

    /**
     * The K-native JDBC boundary: raw-SQL execution over the ambient
     * connection ({@code Compiler}'s executeInDb dispatch). A FUNCTION
     * FQN, not a type — it lives here because this class is the one home
     * for exact platform-FQN identification.
     */
    public static final String EXECUTE_IN_DB = "meta::relational::metamodel::execute::executeInDb";

    /** executeInDbToTDS(sql, connectionFunction) — the engine's program is
     * executeInDb(sql, fn)->resultSetToTDS() (execute.pure:73-90): a VALUE
     * MAPPING of the result set into a TDS, which our raw-grid relation
     * already is. Platform-owned (the program never inlines); the Typer's
     * raw-grid arm binds the call ONCE to the late-bound relation exactly
     * as executeInDb over a single-query literal. Batch 82. */
    public static final String EXECUTE_IN_DB_TO_TDS =
            "meta::relational::metamodel::execute::executeInDbToTDS";

    /** JDBC DatabaseMetaData reads — HOST-evaluated against the H2
     * second target (engine-parity metadata casing), never lowered. */
    public static final String FETCH_DB_TABLES_META_DATA =
            "meta::relational::metamodel::execute::fetchDbTablesMetaData";
    public static final String FETCH_DB_COLUMNS_META_DATA =
            "meta::relational::metamodel::execute::fetchDbColumnsMetaData";
    public static final String FETCH_DB_SCHEMAS_META_DATA =
            "meta::relational::metamodel::execute::fetchDbSchemasMetaData";
    public static final String FETCH_DB_PRIMARY_KEYS_META_DATA =
            "meta::relational::metamodel::execute::fetchDbPrimaryKeysMetaData";




    /** K-native sibling of {@link #EXECUTE_IN_DB}: model-derived drop+create DDL. */
    public static final String DROP_AND_CREATE_TABLE_IN_DB =
            "meta::relational::functions::toDDL::dropAndCreateTableInDb";

    /** loadCsvToDbTable(filePath, table, connection) — the engine's native
     * (legend-pure LoadCsvToDbTable) reads a classpath CSV, drops its
     * header row and inserts the rows positionally into the table
     * (execute.pure:57-66 delegate to it). An EFFECT at the execution
     * boundary; the CSV is TEST INPUT the harness resolves
     * (exec.TestResources). Batch 85. */
    public static final String LOAD_CSV_TO_DB_TABLE =
            "meta::relational::metamodel::execute::loadCsvToDbTable";

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
    public static final String DROP_TABLE_STATEMENT =
            "meta::relational::functions::toDDL::dropTableStatement";

    /** Store-metamodel NAVIGATION natives (platform_store_relational/
     * functions.pure:227/:249) — HOST-evaluated over the compiled store
     * model (the reflection leg's store domain). */
    public static final String STORE_SCHEMA_NAV =
            "meta::relational::metamodel::schema";
    public static final String STORE_TABLE_NAV =
            "meta::relational::metamodel::table";

    public static boolean isStoreNavFn(String fqn) {
        return STORE_SCHEMA_NAV.equals(fqn) || STORE_TABLE_NAV.equals(fqn);
    }


    /** The engine's SQL-text surface — K-dispatched: the query lambda
     * lowers through the platform's own G½->H->I against the given mapping
     * and renders with the engine-style dialect (audit 19d B3: this was a
     * name-intercepting harness arm; the corpus's own toSQLString body is
     * engine plan-generation internals, suppressed like toDDL). */
    public static final String TO_SQL_STRING =
            "meta::relational::functions::sqlstring::toSQLString";

    /** toSQLString with the engine's pretty Format — same K-dispatch
     * (sqlRemoveFormatting normalizes the whitespace difference away in
     * every golden compare; engine toSQLString.pure:35). */
    public static final String TO_SQL_STRING_PRETTY =
            "meta::relational::functions::sqlstring::toSQLStringPretty";

    /** toSQL(f, mapping, runtime, ext) — the SQLResult HANDLE of the
     * same doctrine (engine toSQLString.pure:46): the query lambda and
     * mapping ride on it and {@code SQLResult.toSQLString(dbType, tz,
     * quote, format)} (:151, the 5-argument toSQLString overload)
     * forces it through the one renderer. Batch 75. */
    public static final String TO_SQL =
            "meta::relational::functions::sqlstring::toSQL";

    /** toNonExecutableSQLString(f, mapping, dbType, ext) — toSQLString with
     * the engine's nonExecutable post-processor installed (toSQLString.pure:83-86):
     * the same K-routine, the nonExecutable IR pass applied before the
     * render. Batch 81. */
    public static final String TO_NON_EXECUTABLE_SQL_STRING =
            "meta::relational::functions::sqlstring::toNonExecutableSQLString";

    /** The engine's CSV-seed SQL generator — K-dispatched (CsvSeed). */
    public static final String SET_UP_DATA_SQLS_V2 =
            "meta::alloy::service::execution::setUpDataSQLsV2";

    /** The deprecated plain spelling — PLATFORM-OWNED (the corpus's own
     * ladder is M3-reflective and its DatabaseType wrapper cannot type
     * against createDbConfig's Any); same CsvSeed K-arm. */
    public static final String SET_UP_DATA_SQLS =
            "meta::alloy::service::execution::setUpDataSQLs";

    /** The plan surface (#47) — PLATFORM-OWNED opaque handle + K-native
     * literal plan-text rendering (toSQLString doctrine). */
    public static final String EXECUTION_PLAN =
            "meta::pure::executionPlan::executionPlan";
    /** The lineage surface (harness burn-down group E): a PLATFORM-OWNED
     * opaque handle whose relation tree is rows (LineageRows). */
    public static final String SCAN_RELATIONS =
            "meta::pure::lineage::scanRelations::scanRelations";
    /** The column-lineage chain (group I): two opaque intermediate handles
     * and the rows-bearing terminal. */
    public static final String SCAN_PROPERTIES =
            "meta::pure::lineage::scanProperties::scanProperties";
    public static final String BUILD_PROPERTY_TREE =
            "meta::pure::lineage::scanProperties::propertyTree::buildPropertyTree";
    public static final String SCAN_COLUMNS =
            "meta::pure::lineage::scanColumns::scanColumns";

    /** The metaclass a HANDLE native's rows extend as (the chain root
     * re-roots at its extent keyed by the handle's content id): the
     * native's DECLARED return class — a handle's rows ARE its result
     * (executionPlan → ExecutionPlan, scanRelations → RelationTree,
     * scanColumns → ColumnWithContext, execute → Result whose activities
     * are the execution's activity rows). Null for a non-handle native
     * or a handle whose result is not a class (preval's function value).
     * No per-FQN table: NativeFn.Handle labels the kind, the signature
     * names the class. */
    public static @com.legend.Nullable String handleRowClass(String fqn, Type returnType) {
        if (com.legend.builtin.NativeFn.Handle.of(fqn).isEmpty()) {
            return null;
        }
        return switch (returnType) {
            case Type.ClassType c -> c.fqn();
            case Type.GenericType g -> g.rawFqn();
            default -> null;
        };
    }
    /** Plan-time constant pre-evaluation — a FUNCTION-VALUED identity
     * for plan construction (the wrapped lambda IS the query). */
    public static final String PREVAL =
            "meta::pure::router::preeval::preval";
    public static final String PLAN_TO_STRING =
            "meta::pure::executionPlan::toString::planToString";
    /** {@code planToString} minus newlines and spaces (real
     * executionPlan_print.pure:27). */
    public static final String PLAN_TO_STRING_WITHOUT_FORMATTING =
            "meta::pure::executionPlan::toString::planToStringWithoutFormatting";

    /** The engine's execution entry — K-dispatched as a RESULT FRAME
     * (audit 19d B2: {@code Result} is a typing surface plus an
     * orchestration handle, never a host object graph; reads over it
     * splice into SQL-bound typed queries in the statement executor). */
    /** THE execute entry point — real pure's meta::pure::router::execute
     * (router_entry.pure; reachable bare via m3.pure's auto-import of
     * meta::pure::router). The old meta::pure::mapping::execute alias
     * was an invented FQN (audit R8) and is deleted. */
    public static final String EXECUTE = "meta::pure::router::execute";

    /** The ROUTER'S STRING ENTRY — real engine devUtils.pure:30/:35
     * {@code meta::legend::executeLegendQuery(f, vars, [exeCtx,] ext)}:
     * the same result frame as {@link #EXECUTE} with the query lambda's
     * parameters bound from the vars pairs, read as the engine's RESULT
     * JSON string ({@code ExecuteChainAssembly.prepareLegendQuery} /
     * {@code legendQueryEnvelope}). */
    public static final String EXECUTE_LEGEND_QUERY =
            "meta::legend::executeLegendQuery";


    /** The engine's PLAN-EXECUTE entry — real pure's
     * meta::pure::executionPlan::execute(plan, parametersValues,
     * extensions) (executionPlan_execution.pure:20). The platform
     * NORMALIZES it to the ordinary execute frame by peeling the plan
     * argument to its executionPlan(...) build (same positional arg
     * shape: query, mapping, runtime, extensions) — one execution
     * semantics, the one router; plan TEXT is engine-text
     * (EngineStyleH2) and never executes on the session connection. */
    public static final String EXECUTION_PLAN_EXECUTE =
            "meta::pure::executionPlan::execute";


    /** The m3 profiles the compiler reads semantics from (legend-pure
     * m3.pure / profiles.pure). */
    public static final String EQUALITY_PROFILE = "meta::pure::profiles::equality";
    public static final String TEMPORAL_PROFILE = "meta::pure::profiles::temporal";
    public static final String PCT_PROFILE = "meta::pure::test::pct::PCT";

    /** Whether a stereotype's RESOLVED profile name is {@code profileFqn}.
     * The resolver qualifies a bare profile name through the file's
     * imports and the core-import group when the profile is declared in
     * the model; a model that does not declare it keeps the bare spelling
     * — that spelling is the same profile (the m3 profiles are the only
     * ones the compiler reads). */
    public static boolean isProfile(String resolvedName, String profileFqn) {
        return profileFqn.equals(resolvedName)
                || profileFqn.substring(profileFqn.lastIndexOf(':') + 1).equals(resolvedName);
    }


    /**
     * PLATFORM-OWNED function FQNs: legend-lite's native IS the definition
     * — user re-definitions (the real engine's toDDL.pure bodies walk the
     * Database METAMODEL, M3 reflection this platform doesn't model) are
     * suppressed at the overload merge, exactly like real pure natives
     * replacing their stub bodies. executeInDb is NOT owned: the corpus's
     * 2-arg wrapper there is legitimate pure code over the 4-arg leaf.
     */
    /** The RENDER phase's CSV text fn (F4.2) — the corpus's own
     *  M3-reflective body never joins the overload set. */
    public static final String TO_CSV = "meta::relational::tests::csv::toCSV";

    /** toRepresentation: the platform native (Phase 4 — the pure body is
     * m3-reflective and unportable; the native is the definition). */
    public static final String TO_REPRESENTATION =
            "meta::pure::functions::string::toRepresentation";

    /** assertError: the platform native (Phase 4 — the pure /2 and /4
     * bodies delegate to a PCT.platformOnly matcher native over a
     * SourceInformation value our model does not carry; the K-orchestrated
     * catch IS the definition). */
    public static final String ASSERT_INSTANCE_OF =
            "meta::pure::functions::asserts::assertInstanceOf";

    public static final String ASSERT_ERROR =
            "meta::pure::functions::asserts::assertError";

    /** TDG lane S1: the CSV-census native — a COMPILE-TIME reflection
     * fact (model-space, no database) that FOLDS in the checker to
     * instance literals; the production TestDataGenerator IS the
     * implementation, the real pure body is the spec (verified by
     * signature, never loaded). */
    public static final String GET_RELATIONAL_CSV_DATA =
            "meta::relational::testDataGeneration::getRelationalCSVDataFromQuery";

    /** TDG lane S2: the RUNTIME data-extraction native — the checker
     * captures the call's protocol (carrier), the ORCHESTRATOR executes
     * the fetches through the database and splices the result as
     * literals. */
    public static final String GENERATE_TEST_DATA =
            "meta::relational::testDataGeneration::generateTestData";
    /** The TDG plan (testDataGeneration.pure:818/823): a plan HANDLE whose
     * planToString is the engine's MultiResultSequence text. */
    public static final String PLAN_TEST_DATA_GENERATION =
            "meta::relational::testDataGeneration::executionPlan::planTestDataGeneration";
    public static final String GENERATE_SEED_DATA_STRING =
            "meta::relational::testDataGeneration::generateSeedDataString";

    // ---- the execution context's vocabulary (ExecutionContext.Reader is
    // the only reader of these classes' fields) ----
    public static final String RUNTIME = "meta::core::runtime::Runtime";
    /** A {@code ###Runtime} ELEMENT is upstream's PackageableRuntime (its
     * {@code runtimeValue} is the Runtime): what a runtime reference types as
     * — from(T[m], PackageableRuntime[1]) takes it as declared (batch 5). */
    public static final String PACKAGEABLE_RUNTIME = "meta::pure::runtime::PackageableRuntime";
    public static final String CONNECTION_STORE = "meta::core::runtime::ConnectionStore";
    public static final String WITH_CHAINED_MAPPINGS = "meta::pure::mapping::withChainedMappings";
    public static final String MODEL_CHAIN_CONNECTION =
            "meta::external::store::model::ModelChainConnection";
    public static final String JSON_MODEL_CONNECTION =
            "meta::external::store::model::JsonModelConnection";
    public static final String LOCAL_H2_DATASOURCE_SPECIFICATION =
            "meta::pure::alloy::connections::alloy::specification::LocalH2DatasourceSpecification";
    public static final String DATABASE_CONNECTION =
            "meta::external::store::relational::runtime::DatabaseConnection";
    public static final String RELATIONAL_DATABASE_CONNECTION =
            "meta::external::store::relational::runtime::RelationalDatabaseConnection";
    public static final String TEST_DATABASE_CONNECTION =
            "meta::external::store::relational::runtime::TestDatabaseConnection";
    /** The corpus's runtime builder — a platform-owned function (the
     * quoteIdentifiers overload carries the flag as its argument). */
    public static final String TEST_RUNTIME =
            "meta::external::store::relational::tests::testRuntime";
    public static final String IS_EMPTY = "meta::pure::functions::collection::isEmpty";
    // -- exact identities the front-end reads through ResolvedNames.names
    // (the 2026-09-11 simple-name census: no bare or suffix compares)
    public static final String FIRST = "meta::pure::functions::collection::first";
    public static final String PAIR_FN = "meta::pure::functions::collection::pair";
    public static final String COUNT = "meta::pure::functions::collection::count";
    public static final String ADD = "meta::pure::functions::collection::add";
    public static final String ZIP = "meta::pure::functions::collection::zip";
    public static final String AND = "meta::pure::functions::boolean::and";
    public static final String OR = "meta::pure::functions::boolean::or";
    public static final String TIMES = "meta::pure::functions::math::times";
    public static final String MINUS = "meta::pure::functions::math::minus";
    public static final String RANK = "meta::pure::functions::relation::rank";
    public static final String DENSE_RANK = "meta::pure::functions::relation::denseRank";
    public static final String ROW_NUMBER = "meta::pure::functions::relation::rowNumber";
    public static final String INSTANCE_OF = "meta::pure::functions::meta::instanceOf";
    public static final String TO_STRING = "meta::pure::functions::string::toString";
    /** The engine's relational execution OPTIONS class (executionContext.pure) — the one
     *  context reader spells its fields. */
    public static final String RELATIONAL_EXECUTION_CONTEXT =
            "meta::relational::runtime::RelationalExecutionContext";
    /** The engine's execution-OPTION context and its feature-flag option
     *  (executionPlan_generation.pure / executionPlanFeature.pure) — the
     *  context reader folds the flags into {@code ExecutionContext.features}. */
    public static final String EXECUTION_OPTION_CONTEXT =
            "meta::pure::executionPlan::ExecutionOptionContext";
    public static final String FEATURE_FLAG_OPTION =
            "meta::pure::executionPlan::featureFlag::FeatureFlagOption";
    public static final String FEATURE = com.legend.compiler.spec.typed.Feature.FQN;
    /** A sort key's explicit null placement (sort.pure, 4.145.0) — the
     *  two-argument ascending/descending overloads' enum argument. */
    public static final String NULL_ORDER = "meta::pure::functions::relation::NullOrder";
    /** The group-lambda aggregate bodies (groupBy/aggregate over a FuncColSpec
     *  whose lambda takes the GROUP as a relation, 4.145.0): {@code $g->joinStrings(…)}
     *  and {@code $g->size()} — GroupLambdaAggs desugars them to the map/reduce form. */
    public static final String RELATION_JOIN_STRINGS = "meta::pure::functions::relation::joinStrings";
    public static final String RELATION_SIZE = "meta::pure::functions::relation::size";
    public static final String DURATION = "meta::pure::functions::date::Duration";
    public static final String MAP = "meta::pure::functions::collection::map";
    public static final String PLUS = "meta::pure::functions::math::plus";
    /** String concatenation — upstream's own {@code string::plus(String[*])}
     *  (a string run resolves HERE, never to math::plus — batch 5 leg 5). */
    public static final String STRING_PLUS = "meta::pure::functions::string::plus";

    /** The three relational connection classes (exact FQN). */
    public static boolean isRelationalConnectionClass(String fqn) {
        return DATABASE_CONNECTION.equals(fqn)
                || RELATIONAL_DATABASE_CONNECTION.equals(fqn)
                || TEST_DATABASE_CONNECTION.equals(fqn);
    }

    /** The plan-text simple name of a relational connection class, given
     * its FQN or (in an UNCHECKED helper body) its bare class name; null
     * for any other class. */
    public static @com.legend.Nullable String relationalConnectionSimpleName(String nameOrFqn) {
        if (DATABASE_CONNECTION.equals(nameOrFqn) || "DatabaseConnection".equals(nameOrFqn)) {
            return "DatabaseConnection";
        }
        if (RELATIONAL_DATABASE_CONNECTION.equals(nameOrFqn)
                || "RelationalDatabaseConnection".equals(nameOrFqn)) {
            return "RelationalDatabaseConnection";
        }
        if (TEST_DATABASE_CONNECTION.equals(nameOrFqn)
                || "TestDatabaseConnection".equals(nameOrFqn)) {
            return "TestDatabaseConnection";
        }
        return null;
    }

    /** FQN or bare (unchecked helper body) spelling. */
    public static boolean isModelChainConnection(String nameOrFqn) {
        return MODEL_CHAIN_CONNECTION.equals(nameOrFqn) || "ModelChainConnection".equals(nameOrFqn);
    }

    public static boolean isJsonModelConnection(String nameOrFqn) {
        return JSON_MODEL_CONNECTION.equals(nameOrFqn) || "JsonModelConnection".equals(nameOrFqn);
    }

    public static boolean isLocalH2DatasourceSpecification(String nameOrFqn) {
        return LOCAL_H2_DATASOURCE_SPECIFICATION.equals(nameOrFqn)
                || "LocalH2DatasourceSpecification".equals(nameOrFqn);
    }

    /** The string/number {@code +} in a raw body: bare or either FQN
     *  spelling (math::plus for numbers, string::plus for strings). */
    public static boolean isPlus(String nameOrFqn) {
        return PLUS.equals(nameOrFqn) || STRING_PLUS.equals(nameOrFqn) || "plus".equals(nameOrFqn);
    }

    /** The asserts package: every function in it is a VERDICT the
     * statement channel adjudicates (AssertVerdicts). */
    public static final String ASSERTS_PACKAGE = "meta::pure::functions::asserts::";
    public static final String ASSERT_EQUALS = ASSERTS_PACKAGE + "assertEquals";
    /** VERDICT functions declared OUTSIDE the asserts package — user
     * functions in the model whose calls the statement channel
     * adjudicates by exact FQN instead of running their Pure bodies
     * (AssertVerdicts' root arms). */
    public static final String ASSERT_SAME_SQL =
            "meta::relational::functions::asserts::assertSameSQL";
    public static final String ASSERT_SQL_EQUALS_TDG =
            "meta::relational::testDataGeneration::tests::assertSqlEquals";
    public static final String ASSERT_EQUALS_H2_COMPATIBLE =
            "meta::relational::functions::sqlQueryToString::h2::assertEqualsH2Compatible";
    public static final String ASSERT_TDS_EQUIVALENT =
            "meta::pure::functions::relation::assertTdsEquivalent";

    /** A call the statement channel ADJUDICATES as a verdict (never runs
     * as Pure): the asserts package (package membership, exact spelling)
     * and the named verdict functions. */
    public static boolean isVerdictFunction(String fqn) {
        return fqn.startsWith(ASSERTS_PACKAGE)
                || ASSERT_SAME_SQL.equals(fqn)
                || ASSERT_SQL_EQUALS_TDG.equals(fqn)
                || ASSERT_EQUALS_H2_COMPATIBLE.equals(fqn)
                || ASSERT_TDS_EQUIVALENT.equals(fqn);
    }


    /** The ASSERT FAMILY is platform-owned WHOLESALE (V7 tenet
     * correction 2026-08-28: asserts are verdicts ALWAYS —
     * AssertVerdicts/the K-arm IS the implementation; the real pure
     * bodies are the SPEC, verified by signature in the registry,
     * NEVER loaded as runtime components). Parsed twins — PCT trees,
     * any corpus/library source — suppress loudly. */
    private static final java.util.Set<String> ASSERT_FAMILY_OWNED =
            java.util.Set.of(
                    "meta::pure::functions::asserts::assert",
                    "meta::pure::functions::asserts::assertFalse",
                    "meta::pure::functions::asserts::assertEquals",
                    "meta::pure::functions::asserts::assertNotEquals",
                    "meta::pure::functions::asserts::assertSameElements",
                    "meta::pure::functions::asserts::assertSize",
                    "meta::pure::functions::asserts::assertEq",
                    "meta::pure::functions::asserts::assertEmpty",
                    "meta::pure::functions::asserts::assertNotEmpty",
                    "meta::pure::functions::asserts::assertIs",
                    "meta::pure::functions::asserts::assertContains",
                    "meta::pure::functions::asserts::assertEqWithinTolerance",
                    "meta::pure::functions::asserts::assertJsonStringsEqual");

    /** {@code meta::pure::functions::string::format}: its {@code %s} slots
     * print an argument by the argument's own {@code toString()} — real
     * pure's format calls toString on each value, so a CLASS-typed slot
     * (a Pair, a List, a user class) types as {@code $arg->toString()} and
     * reaches the class's own body (batch 152: the Pair/List Java arms in
     * lowering/Scalars, ports of the spec bodies, are gone). */
    public static final String FORMAT = "meta::pure::functions::string::format";

    /** A value whose text form is its class's {@code toString()}: an
     * instance of a class or a class carrier — never a primitive, an enum,
     * {@code Any}/{@code Nil} (variant-carried scalars print as
     * themselves), a variant, or a relation. */
    public static boolean printsByOwnToString(Type t) {
        return switch (t) {
            case Type.ClassType c -> !isAny(c) && !isNil(c) && !isVariant(c)
                    && Type.schemaView(c) == null;
            case Type.GenericType g -> isPairCarrier(g) || isListCarrier(g);
            default -> false;
        };
    }



    public static boolean isPlatformOwnedFunction(String fqn) {
        return PLATFORM_OWNED_FUNCTIONS.contains(fqn)
                || TO_REPRESENTATION.equals(fqn)
                || ASSERT_FAMILY_OWNED.contains(fqn)
                || PLAN_TEST_DATA_GENERATION.equals(fqn)
                || GENERATE_SEED_DATA_STRING.equals(fqn);
    }

    /** The registered natives whose NAME the platform owns outright — a
     * per-FQN fact, NOT "every NativeFn member" (batch 4b measured that:
     * owning executeInDb's name shadowed the corpus's own ConnectionStore
     * overload and lost a test). Spelled through the family enums so the
     * set cannot name a native the platform does not register. */
    private static final java.util.Set<String> PLATFORM_OWNED_FUNCTIONS = java.util.Set.of(
            com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_TABLE_IN_DB.fqn(),
            com.legend.builtin.NativeFn.Effect.DROP_AND_CREATE_SCHEMA_IN_DB.fqn(),
            com.legend.builtin.NativeFn.Effect.LOAD_CSV_TO_DB_TABLE.fqn(),
            com.legend.builtin.NativeFn.Effect.SET_UP_DATA_SQLS.fqn(),
            com.legend.builtin.NativeFn.ContextOwner.ASSERT_ERROR.fqn(),
            com.legend.builtin.NativeFn.Verdict.ASSERT_INSTANCE_OF.fqn(),
            com.legend.builtin.NativeFn.Verdict.TO_CSV.fqn(),
            com.legend.builtin.NativeFn.DdlStatement.CREATE_SCHEMA_STATEMENT.fqn(),
            com.legend.builtin.NativeFn.DdlStatement.CREATE_TABLE_STATEMENT.fqn(),
            com.legend.builtin.NativeFn.DdlStatement.DROP_SCHEMA_STATEMENT.fqn(),
            com.legend.builtin.NativeFn.DdlStatement.DROP_TABLE_STATEMENT.fqn(),
            com.legend.builtin.NativeFn.Carrier.EXECUTE_IN_DB_TO_TDS.fqn(),
            com.legend.builtin.NativeFn.Carrier.GET_RELATIONAL_CSV_DATA.fqn(),
            com.legend.builtin.NativeFn.Carrier.GENERATE_TEST_DATA.fqn(),
            com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING.fqn(),
            com.legend.builtin.NativeFn.JavaRoutine.TO_SQL_STRING_PRETTY.fqn(),
            com.legend.builtin.NativeFn.JavaRoutine.TO_NON_EXECUTABLE_SQL_STRING.fqn(),
            com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING.fqn(),
            com.legend.builtin.NativeFn.JavaRoutine.PLAN_TO_STRING_WITHOUT_FORMATTING.fqn(),
            com.legend.builtin.NativeFn.Handle.TO_SQL.fqn(),
            com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN.fqn(),
            com.legend.builtin.NativeFn.Handle.EXECUTE.fqn(),
            com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN_EXECUTE.fqn());

    /** A call only the STATEMENT channel can run — an execution, a store
     * effect or a test-data generator: it never lowers inside an
     * expression, so a user function whose own statements reach one is
     * a PROGRAM, and a call to a program splices at statement level
     * ({@link com.legend.compiler.StatementInline}). A verdict is NOT on
     * this list: a helper that only asserts β-reduces to an assert root
     * and is adjudicated as that verdict (the statement channel's
     * inlined-assert routes), never run as statements. The seed-SQL form
     * (setUpDataSQLs) is a statement-channel form — executed when mapped
     * over executeInDb, compared as engine text under a TDG assert. */
    public static boolean isStatementOnly(String fqn) {
        var handle = com.legend.builtin.NativeFn.Handle.of(fqn).orElse(null);
        return com.legend.builtin.NativeFn.Effect.isDbEffect(fqn)
                || com.legend.builtin.NativeFn.Effect.isSeedSqlForm(fqn)
                || handle == com.legend.builtin.NativeFn.Handle.EXECUTE
                || handle == com.legend.builtin.NativeFn.Handle.EXECUTION_PLAN_EXECUTE
                || handle == com.legend.builtin.NativeFn.Handle.EXECUTE_LEGEND_QUERY
                || com.legend.builtin.NativeFn.Carrier.of(fqn).orElse(null)
                        == com.legend.builtin.NativeFn.Carrier.GENERATE_TEST_DATA
                || GENERATE_SEED_DATA_STRING.equals(fqn);
    }

    /** Debug output — K-dispatched as a NO-OP, arguments never evaluated. */
    public static final String PRINT = "meta::pure::functions::io::print";
    public static final String PRINTLN = "meta::pure::functions::io::println";


    /** Post-processor CONFIG property names (runtime/connection hook
     * slots): their values are plan-time SQL-rewrite config, never Pure
     * the executor evaluates — the effect scan and the inliner treat
     * them as config, not query code (ledger cluster 63). */
    public static boolean isPostProcessorConfigProperty(String name) {
        return "sqlQueryPostProcessors".equals(name)
                || "sqlQueryPostProcessorsConnectionAware".equals(name)
                || "queryPostProcessorsWithParameter".equals(name);
    }



    /** The top type. */
    public static boolean isAny(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(ANY);
    }

    /** The bottom type (the []-born element type). */
    public static boolean isNil(Type t) {
        return t instanceof Type.ClassType c && c.fqn().equals(NIL);
    }

    /** The semi-structured JSON carrier — the Variant class, and the
     * {@code meta::json} tree classes (real json.pure:32-70), whose values
     * RIDE the same carrier (a JSON element IS a JSON value; the classes
     * are its kinds). JSONKeyValue rides it too: a member is represented
     * by its value, the key being spelled by the access. */
    public static boolean isVariant(Type t) {
        return t instanceof Type.ClassType c
                && (c.fqn().equals(VARIANT) || JSON_FAMILY.contains(c.fqn()));
    }

    public static final String JSON_ELEMENT = "meta::json::JSONElement";
    public static final String JSON_OBJECT = "meta::json::JSONObject";
    public static final String JSON_ARRAY = "meta::json::JSONArray";
    public static final String JSON_STRING = "meta::json::JSONString";
    public static final String JSON_NUMBER = "meta::json::JSONNumber";
    public static final String JSON_BOOLEAN = "meta::json::JSONBoolean";
    public static final String JSON_NULL = "meta::json::JSONNull";
    public static final String JSON_KEY_VALUE = "meta::json::JSONKeyValue";
    /** The {@code meta::json} tree classes (json.pure:32-70). */
    public static final java.util.Set<String> JSON_FAMILY = java.util.Set.of(
            JSON_ELEMENT, JSON_OBJECT, JSON_ARRAY, JSON_STRING, JSON_NUMBER,
            JSON_BOOLEAN, JSON_NULL, JSON_KEY_VALUE);

    /** A {@code meta::json} tree class (element kinds + the key-value pair). */
    public static boolean isJsonElement(Type t) {
        return t instanceof Type.ClassType c && JSON_FAMILY.contains(c.fqn());
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

    /** A PARAMETERIZED platform VALUE carrier — a struct the lowering
     * carries (Pair/List/Map), a plan handle (Result&lt;T|m&gt;), a
     * variant, a column specification, a function carrier: never a row
     * the store serves. Type.classFqn excludes these; every OTHER
     * parameterized class (Class&lt;Any&gt;, Property&lt;Nil,Any|*&gt;,
     * EnumerationMapping&lt;Any&gt; — the spec's metaclass-typed
     * properties, mapping leg B) is a row of its raw class. */
    public static boolean isValueCarrier(Type t) {
        if (!(t instanceof Type.GenericType g)) {
            return false;
        }
        return isPairCarrier(t) || isListCarrier(t) || isMapCarrier(t)
                || FUNCTION_CARRIERS.contains(g.rawFqn())
                || VALUE_CARRIER_FQNS.contains(g.rawFqn());
    }

    private static final java.util.Set<String> VALUE_CARRIER_FQNS = java.util.Set.of(
            RESULT, VARIANT, COL_SPEC, COL_SPEC_ARRAY, FUNC_COL_SPEC, FUNC_COL_SPEC_ARRAY,
            AGG_COL_SPEC, AGG_COL_SPEC_ARRAY);

    /** The {@code Function<{…}>} value carrier (parameterized form). */
    public static boolean isFunctionCarrier(Type t) {
        return t instanceof Type.GenericType g && g.rawFqn().equals(FUNCTION);
    }


    /** The engine's runtime connection lookup — an orchestration value
     * our session model answers with null (was a RAW STRING LITERAL at
     * its dispatch site; ladder census §10m). */
    public static final String CONNECTION_BY_ELEMENT =
            "meta::core::runtime::connectionByElement";




}
