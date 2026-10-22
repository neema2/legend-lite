package com.legend.sql.dialect;

import com.legend.sql.SqlQuery;

/**
 * The dialect seam: rendering (Phase J) AND value normalization (Phase K).
 * A backend's JDBC driver may hand back dialect-flavored Java objects
 * (SQLite: dates as Strings, booleans as ints); {@link #normalize} converts
 * to the canonical representation for a PURE type so the typed-result
 * contract holds on every backend.
 */
public interface SqlDialect {

    /** A query as SQL text: a statement with no bound parameters (one reaching the text is refused). */
    String render(SqlQuery query);

    /** {@link #render} on one line, its clauses separated by single spaces (written so, never collapsed: a string
     *  literal keeps its own whitespace): the form legend-engine prints a statement it reports, such as the
     *  test-data generator's fetches, which its tests compare with their newlines removed. */
    String renderInline(SqlQuery query);

    /** A query as a statement for execution with bound values: its text with a {@code ?} for each bound parameter, and
     *  the parameters in placeholder order (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10, E). */
    RenderedStatement renderStatement(SqlQuery query);

    /** B6 (truthfulness burn) — SESSION SETUP IS DIALECT-OWNED as a
     * FACT: the statements a backend's session needs for the
     * platform's value contracts. The dialect DECIDES; the exec layer
     * EXECUTES (F1.3: java.sql never enters this package) — applied at
     * {@code Compiler.dialectOf}'s connection seam, idempotent by
     * contract. Default: none. */
    default java.util.List<String> sessionSetup() {
        return java.util.List.of();
    }

    /** Whether this database has a table only one session sees (a TEMPORARY table, {@link #sessionTable}): where it
     *  has none, the system metamodel store's tables a plan reads are the query's own rows, inline
     *  ({@code PlanMaker.metamodel}). Default: it has. */
    default boolean temporaryTables() {
        return true;
    }

    /** The statements that make {@code table} (TEMPORARY) a table only this session sees, holding {@code rows} (null:
     *  none) and only read after (a server engine's system database, exec.SystemDatabase). Default: the table, then
     *  one INSERT of its rows -- this interface's own DDL and DML renders. */
    default java.util.List<String> sessionTable(com.legend.sql.SqlDdl.CreateTable table,
            com.legend.sql.SqlDml.@com.legend.base.Nullable InsertValues rows) {
        return rows == null ? java.util.List.of(render(table)) : java.util.List.of(render(table), render(rows));
    }

    /** A STORED name — a table, schema or column as its database holds it, or a quote-bearing declaration
     *  ({@code "date"}) that is its own spelling — as this dialect's queries reference it: a seed writing the
     *  table and a query reading it spell one name (C3c). */
    String physicalName(String name);

    /** A stored table, schema or column name as this dialect's queries reference it, given how its store DECLARED it:
     *  one declared quoted ({@code Table "Orders"}) delimited, its case kept; one declared bare by
     *  {@link #physicalName} (decision D8: Postgres folds it). The declaration is data ({@code sql.NameQuoting}, a
     *  column's {@code declaredQuoted}), never quote characters inside the name. */
    String storedName(String name, boolean declaredQuoted);

    /** This dialect for the server version a session reports ({@code DatabaseMetaData}'s product version):
     *  a dialect whose SQL differs by version says how. Default: every version is spelled alike. */
    default SqlDialect forServer(String version) {
        return this;
    }

    /** The server versions this dialect's text is written for ({@link #forServer} keeps it for these and takes another
     *  dialect for any other), which a plan names so its runner refuses another; null when its text depends on no
     *  server's version (the dialect program's Phase 5, 2026-10-16: until then {@code Databases.servers} listed each
     *  database). */
    default @com.legend.base.Nullable java.util.List<String> serverVersions() {
        return null;
    }

    /** Whether a target on this database loads its declared rows through the database's own bulk loader (a plan's
     *  {@code SetupStep.Rows}, the statements that stage them this dialect's) rather than one INSERT
     *  (docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §9, "setup at plan time, one form per database"; the dialect
     *  program's Phase 5, 2026-10-16: until then {@code Databases.loadsRowsInBulk} listed each database). */
    default boolean loadsRowsInBulk() {
        return false;
    }

    /** Whether a {@code RelationalDatabaseConnection} declares this database: its text is SQL over relational tables.
     *  Every dialect's but MongoDB's, whose text is an aggregation pipeline its {@code MongoDBConnection} runs
     *  (docs/MONGODB_PLAN_2026_10_16.md D1, D4; the grammar's relational types do not list it). A test or tool that
     *  declares a relational connection on every database iterates these ({@code Databases.relational}; main's merge,
     *  2026-10-22, when MongoDB joined the implemented databases). Default: relational. */
    default boolean relational() {
        return true;
    }

    /** Whether this database runs IN THIS PROCESS (an embedded engine, a private instance of which the platform opens:
     *  H2, DuckDB, SQLite) or on a SERVER: a plan writes the system metamodel store's tables as its target's setup on
     *  the one and as its sessions' own TEMPORARY tables on the other (the plan path program's step 9, 2026-10-19; until
     *  main's dialect program Phase 5 merged in, 2026-10-22, {@code Databases.inProcess} listed each database). The
     *  execution side's connector answers the same of its sessions ({@code exec.DatabaseConnector#inProcess}), and
     *  {@code ConnectorsTest} holds the two together. Default: a server. */
    default boolean inProcess() {
        return false;
    }

    /** Whether this dialect's driver re-fetches a timestamp cell as {@code java.time.LocalDateTime} and a date cell
     *  as {@code java.time.LocalDate} (the BC-faithful carriers the executor reads every timestamp and date as; dates
     *  since 2026-10-17, measured then on pgjdbc, DuckDB's and H2's drivers: the other drivers' JDBC 4.2 support
     *  of LocalDate is what their LocalDateTime re-fetch already relies on, and is first measured in their cloud
     *  lanes). Default: it does, and a refusal fails loudly. */
    default boolean refetchesTimestampAsJavaTime() {
        return true;
    }

    /** JDBC cell value → canonical Java value for {@code type}. Default: identity. */
    default @com.legend.base.Nullable Object normalize(@com.legend.base.Nullable Object jdbcValue,
            com.legend.sql.@com.legend.base.Nullable SqlType type) {
        return jdbcValue;
    }

    /** A result column's type as this database's driver names it ({@code getColumnTypeName}), in the platform's wire
     *  vocabulary ({@link WireNames}): the type census compares a plan's label with it (the dialect program's Phase 4,
     *  2026-10-15: until then the census held each database's names, keyed by the dialect's class name). Default: the
     *  names that say one type whichever driver gives them. */
    default String wireTypeName(@com.legend.base.Nullable String driverTypeName) {
        return WireNames.fold(driverTypeName);
    }

    /** The wire name a late-bound or pivot-made column is TYPED by, from the name its driver gives the column's type:
     *  {@link #wireTypeName}'s, or null where that name alone does not say the type. The census reads such a name with
     *  the column's precision and scale; typing by the name would be a guess (AGENTS.md invariant 4), so the column is
     *  not typed by it (2026-10-15, the audit of Phase 4: Snowflake's NUMBER had typed integer columns Decimal).
     *  Default: every name this database's driver gives says its type. */
    default @com.legend.base.Nullable String typingWireName(@com.legend.base.Nullable String driverTypeName) {
        return wireTypeName(driverTypeName);
    }

    /** Whether this database delivers a column the platform labels {@code label} as the wire type {@code wire} (in
     *  {@link #wireTypeName}'s vocabulary) by a carrier convention of its own: the census counts it delivered, not a
     *  divergence. Default: it has none. */
    default boolean deliversOwn(com.legend.sql.SqlType label, String wire) {
        return false;
    }

    /** Whether this database's driver opens the result of a statement that fails before its columns are known with
     *  none, its error surfacing on the first read: a result with no columns is then no claim to check. */
    default boolean opensFailedResultsWithoutColumns() {
        return false;
    }

    /** How a dynamic pivot's probe plan's two statements (the keys probe and the body written for its keys) see one
     *  state of the data (the plan path program's probe plan, 2026-10-20). */
    enum KeysSnapshot {
        /** a transaction, the database's own isolation kept: every transaction there is one snapshot */
        TRANSACTION,
        /** a transaction at REPEATABLE READ, the isolation that makes its reads one snapshot there */
        REPEATABLE_READ,
        /** a transaction at SERIALIZABLE */
        SERIALIZABLE,
        /** no transaction across statements: a key written between the two is not the body's (register S103) */
        NONE
    }

    /** This database's {@link KeysSnapshot}, said by each dialect that runs a dynamic pivot's probe plan; a dialect
     *  that says none has no probe plan, refused by name. */
    default KeysSnapshot keysSnapshot() {
        throw new DialectCapability("a dynamic pivot's probe plan reached " + getClass().getSimpleName()
                + ", which says no snapshot for it");
    }

    /** The most columns one result answers on this database (measured on the in-process ones, the database's own
     *  documented limit on the servers), a probe plan asked for more refused by name; null where the database has
     *  none (DuckDB answered 70,000) or none is known. Said by each dialect that runs a probe plan. */
    default @com.legend.base.Nullable Integer resultColumnLimit() {
        throw new DialectCapability("a dynamic pivot's probe plan reached " + getClass().getSimpleName()
                + ", which says no column limit for it");
    }

    /** Whether dynamic PIVOT needs the two-phase staticization
     * pre-pass ({@link com.legend.exec.DynamicPivot} — no native
     * dynamic pivot on this backend). */
    default boolean needsStaticPivot() {
        return false;
    }

    /**
     * A raw statement as this session runs it (decision D2, the dialect program's Phase 1c): raw text is in the language
     * of the database its connection DECLARES ({@code writtenFor}, that type's name), and is translated only when it
     * runs on another. The engine's H2 text (legend-engine's test database's: its corpus's setup, an H2 connection's
     * {@code executeInDb}) is this dialect's {@link #fromEngineText}; text written for any other database, or by this
     * session's own dialect ({@code writtenFor} null), runs as written, as legend-engine sends it.
     */
    String raw(String statement, @com.legend.base.Nullable String writtenFor);

    /** One statement of the engine's H2 text as this database runs it: H2's own, translated, or refused by name. */
    String fromEngineText(String statement);

    /** The path a written model's specification names for this database's in-memory session, where legend-engine must
     *  be given one (it opens the path a specification names, and refuses one that names none); null when this
     *  database's specification names no path for one. The platform reads a pathless specification as in memory (the
     *  dialect program's Phase 7, 2026-10-18: the model writer, {@code server.CreateClass}, asked the protocol's tag
     *  until then). */
    default @com.legend.base.Nullable String inMemoryPath() {
        return null;
    }

    /** This database's host operations (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C5 and D5): its own, written in its
     *  dialect's package, or each refused, naming the database. Until 2026-10-15 {@code HostOperations.of} chose them by
     *  the dialect's class, naming two databases outside their owners (the dialect program's owner guard). */
    default HostOperations hostOperations() {
        return new HostOperations.Refused(getClass().getSimpleName());
    }

    /** An effect segment's statements as ONE script (block-compiler stage 3): the
     *  target decides its transaction bracket INSIDE its dialect — DuckDB brackets
     *  the segment so a failure applies nothing (transactional DDL); H2 cannot roll
     *  DDL back and sends the statements bare. */
    default String script(java.util.List<String> statements) {
        return String.join(";\n", statements) + ";";
    }

    /** The statement that ABORTS a failed script's bracket, or null when the dialect
     *  brackets nothing — a DuckDB transaction a failing statement left open must be
     *  rolled back, or every later transaction on the connection is refused. */
    default @com.legend.base.Nullable String scriptAbort() {
        return null;
    }

    /** Which statement of a failed script the engine's message names (0-based), or
     *  empty when the engine does not say — H2 quotes the failing statement
     *  ({@code SQL statement: …}); DuckDB names values and columns only. */
    default java.util.OptionalInt failingStatement(String message, java.util.List<String> statements) {
        return java.util.OptionalInt.empty();
    }

    /** DDL rendered like a query (2026-09-16): the dialect spells the
     *  store's declared shape — its identifier rule, its type names.
     *  Retires the {@code Ddl.Flavor} enum and the {@code rawH2IsNative()}
     *  ternaries that chose it: a target is decided INSIDE its dialect. */
    String render(com.legend.sql.SqlDdl ddl);

    /** Row writes rendered like DDL (2026-09-23): the dialect's identifier
     *  rule and literal spelling. */
    String render(com.legend.sql.SqlDml dml);

    /** A store column's name in the column list of a row this dialect writes as TEXT (a seed's insert, the engine
     *  text's {@code setUpDataSQLs}): the rule its DDL names the column by, so the statement that fills a table names
     *  the columns the one that created it declared; a declared-quoted name keeps its quotes. */
    String insertColumnName(String name, boolean declaredQuoted);

    /** A column as this database's CATALOG describes it (structured: its canonical type, a
     *  decimal's precision and scale), read into what a Pure Database declares and the
     *  conversion it needs at the source (T2: a store's model comes from the database's own
     *  catalog, decided inside its dialect). Refused where unprobed; a type the dialect cannot
     *  declare is refused, naming it -- never guessed as text. */
    default CatalogType catalogType(CatalogModel.Column column) {
        throw new DialectCapability("reading this database's catalog type '" + column.dataType()
                + "' is not implemented for " + getClass().getSimpleName());
    }

    /** THE CATALOG QUESTION for one table: the statement whose rows are the table's columns as {@link CatalogModel}
     *  reads them (each column's name, its own type name, its canonical type, a decimal's precision and scale, whether
     *  it holds no null: {@link #catalogType}'s input), {@code schema} and {@code table} written in as this database's
     *  string literals. Refused, naming the dialect, where the platform does not ask the catalog itself (an attached
     *  database's tables are listed by their host). The dialect program's Phase 7, 2026-10-18: the tab's planner read
     *  DuckDB's question off its class until then. */
    default String catalogColumnsQuery(String schema, String table) {
        throw new UnsupportedOperationException("the platform asks no catalog question of " + getClass().getSimpleName());
    }

    /** Whether a store that declares the catalog's name {@code catalogName} BARE reaches the object the catalog
     *  names (CatalogModel declares the name quoted where it does not): a database that matches names
     *  case-insensitively always does; one that folds a bare name (Postgres, decision D8) only when the name is its
     *  own fold. Refused where the catalog is not read ({@link #catalogType}). */
    default boolean bareNameReaches(String catalogName) {
        throw new DialectCapability("reading this database's catalog names is not implemented for "
                + getClass().getSimpleName());
    }

    /** The select list a COPY of a table applies when the copy is made IN this database, so that it holds the
     *  declared types: each of {@code conversions} (the source catalog's, {@link #catalogType}, written for where
     *  copies run) under its column's name, every other column as it is; {@code *} when no column converts. Refused
     *  where this database cannot replace a column in a star select ({@link DuckDb#copySelectList} can: every host
     *  copies into its own DuckDB, the tab's or Python's). */
    default String copySelectList(java.util.List<CatalogModel.Conversion> conversions) {
        if (conversions.isEmpty()) {
            return "*";
        }
        throw new DialectCapability("a copy that converts columns in its star select is not implemented for "
                + getClass().getSimpleName());
    }

}
