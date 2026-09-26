package com.legend.warehouse.client.jdbc;

/**
 * What a connection says about the database behind it: the engine the
 * session reported (its name and version, as that engine's own driver gives
 * them), because the SQL this connection accepts is that engine's.
 */
final class WhDatabaseMetaData implements java.sql.DatabaseMetaData {

    private final WhConnection conn;
    private final String url;

    WhDatabaseMetaData(WhConnection conn, String url) {
        this.conn = conn;
        this.url = url;
    }

    @Override
    public String getDatabaseProductName() {
        return conn.engine;
    }

    @Override
    public String getDatabaseProductVersion() {
        return conn.engineVersion;
    }

    @Override
    public String getDriverName() {
        return "legend-lite warehouse";
    }

    @Override
    public String getDriverVersion() {
        return "0.1";
    }

    @Override
    public int getDriverMajorVersion() {
        return 0;
    }

    @Override
    public int getDriverMinorVersion() {
        return 1;
    }

    @Override
    public String getURL() {
        return url;
    }

    @Override
    public java.sql.Connection getConnection() {
        return conn;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.unwrap");
    }

    // -- not supported: each says so, loudly --------------------------------

    @Override
    public java.sql.ResultSet getAttributes(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getAttributes");
    }

    @Override
    public boolean isReadOnly() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.isReadOnly");
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsDifferentTableCorrelationNames");
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsIntegrityEnhancementFacility");
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSchemasInPrivilegeDefinitions");
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCatalogsInPrivilegeDefinitions");
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOpenStatementsAcrossRollback");
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsDataManipulationTransactionsOnly");
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.dataDefinitionCausesTransactionCommit");
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.dataDefinitionIgnoredInTransactions");
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsStoredFunctionsUsingCallSyntax");
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.autoCommitFailureClosesAllResultSets");
    }

    @Override
    public java.lang.String getUserName() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getUserName");
    }

    @Override
    public boolean nullsAreSortedHigh() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.nullsAreSortedHigh");
    }

    @Override
    public boolean nullsAreSortedLow() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.nullsAreSortedLow");
    }

    @Override
    public boolean usesLocalFiles() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.usesLocalFiles");
    }

    @Override
    public java.lang.String getSQLKeywords() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSQLKeywords");
    }

    @Override
    public java.lang.String getStringFunctions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getStringFunctions");
    }

    @Override
    public java.lang.String getSystemFunctions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSystemFunctions");
    }

    @Override
    public boolean supportsConvert(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsConvert");
    }

    @Override
    public boolean supportsConvert() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsConvert");
    }

    @Override
    public boolean supportsGroupBy() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsGroupBy");
    }

    @Override
    public boolean supportsOuterJoins() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOuterJoins");
    }

    @Override
    public java.lang.String getSchemaTerm() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSchemaTerm");
    }

    @Override
    public java.lang.String getProcedureTerm() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getProcedureTerm");
    }

    @Override
    public java.lang.String getCatalogTerm() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getCatalogTerm");
    }

    @Override
    public boolean isCatalogAtStart() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.isCatalogAtStart");
    }

    @Override
    public boolean supportsUnion() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsUnion");
    }

    @Override
    public boolean supportsUnionAll() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsUnionAll");
    }

    @Override
    public int getMaxConnections() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxConnections");
    }

    @Override
    public int getMaxIndexLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxIndexLength");
    }

    @Override
    public int getMaxRowSize() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxRowSize");
    }

    @Override
    public int getMaxStatements() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxStatements");
    }

    @Override
    public java.sql.ResultSet getProcedures(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getProcedures");
    }

    @Override
    public java.sql.ResultSet getTables(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String[] p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getTables");
    }

    @Override
    public java.sql.ResultSet getSchemas() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSchemas");
    }

    @Override
    public java.sql.ResultSet getSchemas(java.lang.String p0, java.lang.String p1) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSchemas");
    }

    @Override
    public java.sql.ResultSet getCatalogs() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getCatalogs");
    }

    @Override
    public java.sql.ResultSet getTableTypes() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getTableTypes");
    }

    @Override
    public java.sql.ResultSet getColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getColumns");
    }

    @Override
    public java.sql.ResultSet getTablePrivileges(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getTablePrivileges");
    }

    @Override
    public java.sql.ResultSet getVersionColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getVersionColumns");
    }

    @Override
    public java.sql.ResultSet getPrimaryKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getPrimaryKeys");
    }

    @Override
    public java.sql.ResultSet getImportedKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getImportedKeys");
    }

    @Override
    public java.sql.ResultSet getExportedKeys(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getExportedKeys");
    }

    @Override
    public java.sql.ResultSet getCrossReference(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3, java.lang.String p4, java.lang.String p5) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getCrossReference");
    }

    @Override
    public java.sql.ResultSet getTypeInfo() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getTypeInfo");
    }

    @Override
    public java.sql.ResultSet getIndexInfo(java.lang.String p0, java.lang.String p1, java.lang.String p2, boolean p3, boolean p4) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getIndexInfo");
    }

    @Override
    public boolean updatesAreDetected(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.updatesAreDetected");
    }

    @Override
    public boolean deletesAreDetected(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.deletesAreDetected");
    }

    @Override
    public boolean insertsAreDetected(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.insertsAreDetected");
    }

    @Override
    public java.sql.ResultSet getUDTs(java.lang.String p0, java.lang.String p1, java.lang.String p2, int[] p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getUDTs");
    }

    @Override
    public boolean supportsSavepoints() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSavepoints");
    }

    @Override
    public java.sql.ResultSet getSuperTypes(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSuperTypes");
    }

    @Override
    public java.sql.ResultSet getSuperTables(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSuperTables");
    }

    @Override
    public int getSQLStateType() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSQLStateType");
    }

    @Override
    public boolean locatorsUpdateCopy() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.locatorsUpdateCopy");
    }

    @Override
    public java.sql.RowIdLifetime getRowIdLifetime() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getRowIdLifetime");
    }

    @Override
    public java.sql.ResultSet getFunctions(java.lang.String p0, java.lang.String p1, java.lang.String p2) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getFunctions");
    }

    @Override
    public java.sql.ResultSet getFunctionColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getFunctionColumns");
    }

    @Override
    public java.sql.ResultSet getPseudoColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getPseudoColumns");
    }

    @Override
    public boolean allProceduresAreCallable() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.allProceduresAreCallable");
    }

    @Override
    public boolean allTablesAreSelectable() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.allTablesAreSelectable");
    }

    @Override
    public boolean nullsAreSortedAtStart() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.nullsAreSortedAtStart");
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.nullsAreSortedAtEnd");
    }

    @Override
    public boolean usesLocalFilePerTable() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.usesLocalFilePerTable");
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMixedCaseIdentifiers");
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesUpperCaseIdentifiers");
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesLowerCaseIdentifiers");
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesMixedCaseIdentifiers");
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMixedCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesUpperCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesLowerCaseQuotedIdentifiers");
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.storesMixedCaseQuotedIdentifiers");
    }

    @Override
    public java.lang.String getIdentifierQuoteString() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getIdentifierQuoteString");
    }

    @Override
    public java.lang.String getNumericFunctions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getNumericFunctions");
    }

    @Override
    public java.lang.String getTimeDateFunctions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getTimeDateFunctions");
    }

    @Override
    public java.lang.String getSearchStringEscape() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getSearchStringEscape");
    }

    @Override
    public java.lang.String getExtraNameCharacters() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getExtraNameCharacters");
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsAlterTableWithAddColumn");
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsAlterTableWithDropColumn");
    }

    @Override
    public boolean supportsColumnAliasing() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsColumnAliasing");
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.nullPlusNonNullIsNull");
    }

    @Override
    public boolean supportsTableCorrelationNames() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsTableCorrelationNames");
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsExpressionsInOrderBy");
    }

    @Override
    public boolean supportsOrderByUnrelated() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOrderByUnrelated");
    }

    @Override
    public boolean supportsGroupByUnrelated() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsGroupByUnrelated");
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsGroupByBeyondSelect");
    }

    @Override
    public boolean supportsLikeEscapeClause() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsLikeEscapeClause");
    }

    @Override
    public boolean supportsMultipleResultSets() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMultipleResultSets");
    }

    @Override
    public boolean supportsMultipleTransactions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMultipleTransactions");
    }

    @Override
    public boolean supportsNonNullableColumns() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsNonNullableColumns");
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMinimumSQLGrammar");
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCoreSQLGrammar");
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsExtendedSQLGrammar");
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsANSI92EntryLevelSQL");
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsANSI92IntermediateSQL");
    }

    @Override
    public boolean supportsANSI92FullSQL() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsANSI92FullSQL");
    }

    @Override
    public boolean supportsFullOuterJoins() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsFullOuterJoins");
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsLimitedOuterJoins");
    }

    @Override
    public java.lang.String getCatalogSeparator() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getCatalogSeparator");
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSchemasInDataManipulation");
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSchemasInProcedureCalls");
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSchemasInTableDefinitions");
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSchemasInIndexDefinitions");
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCatalogsInDataManipulation");
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCatalogsInProcedureCalls");
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCatalogsInTableDefinitions");
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCatalogsInIndexDefinitions");
    }

    @Override
    public boolean supportsPositionedDelete() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsPositionedDelete");
    }

    @Override
    public boolean supportsPositionedUpdate() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsPositionedUpdate");
    }

    @Override
    public boolean supportsSelectForUpdate() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSelectForUpdate");
    }

    @Override
    public boolean supportsStoredProcedures() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsStoredProcedures");
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSubqueriesInComparisons");
    }

    @Override
    public boolean supportsSubqueriesInExists() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSubqueriesInExists");
    }

    @Override
    public boolean supportsSubqueriesInIns() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSubqueriesInIns");
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsSubqueriesInQuantifieds");
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsCorrelatedSubqueries");
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOpenCursorsAcrossCommit");
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOpenCursorsAcrossRollback");
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsOpenStatementsAcrossCommit");
    }

    @Override
    public int getMaxBinaryLiteralLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxBinaryLiteralLength");
    }

    @Override
    public int getMaxCharLiteralLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxCharLiteralLength");
    }

    @Override
    public int getMaxColumnNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnNameLength");
    }

    @Override
    public int getMaxColumnsInGroupBy() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnsInGroupBy");
    }

    @Override
    public int getMaxColumnsInIndex() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnsInIndex");
    }

    @Override
    public int getMaxColumnsInOrderBy() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnsInOrderBy");
    }

    @Override
    public int getMaxColumnsInSelect() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnsInSelect");
    }

    @Override
    public int getMaxColumnsInTable() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxColumnsInTable");
    }

    @Override
    public int getMaxCursorNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxCursorNameLength");
    }

    @Override
    public int getMaxSchemaNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxSchemaNameLength");
    }

    @Override
    public int getMaxProcedureNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxProcedureNameLength");
    }

    @Override
    public int getMaxCatalogNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxCatalogNameLength");
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.doesMaxRowSizeIncludeBlobs");
    }

    @Override
    public int getMaxStatementLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxStatementLength");
    }

    @Override
    public int getMaxTableNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxTableNameLength");
    }

    @Override
    public int getMaxTablesInSelect() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxTablesInSelect");
    }

    @Override
    public int getMaxUserNameLength() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getMaxUserNameLength");
    }

    @Override
    public int getDefaultTransactionIsolation() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getDefaultTransactionIsolation");
    }

    @Override
    public boolean supportsTransactions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsTransactions");
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsTransactionIsolationLevel");
    }

    @Override
    public java.sql.ResultSet getProcedureColumns(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getProcedureColumns");
    }

    @Override
    public java.sql.ResultSet getColumnPrivileges(java.lang.String p0, java.lang.String p1, java.lang.String p2, java.lang.String p3) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getColumnPrivileges");
    }

    @Override
    public java.sql.ResultSet getBestRowIdentifier(java.lang.String p0, java.lang.String p1, java.lang.String p2, int p3, boolean p4) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getBestRowIdentifier");
    }

    @Override
    public boolean supportsResultSetType(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsResultSetType");
    }

    @Override
    public boolean supportsResultSetConcurrency(int p0, int p1) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsResultSetConcurrency");
    }

    @Override
    public boolean ownUpdatesAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.ownUpdatesAreVisible");
    }

    @Override
    public boolean ownDeletesAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.ownDeletesAreVisible");
    }

    @Override
    public boolean ownInsertsAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.ownInsertsAreVisible");
    }

    @Override
    public boolean othersUpdatesAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.othersUpdatesAreVisible");
    }

    @Override
    public boolean othersDeletesAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.othersDeletesAreVisible");
    }

    @Override
    public boolean othersInsertsAreVisible(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.othersInsertsAreVisible");
    }

    @Override
    public boolean supportsBatchUpdates() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsBatchUpdates");
    }

    @Override
    public boolean supportsNamedParameters() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsNamedParameters");
    }

    @Override
    public boolean supportsMultipleOpenResults() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsMultipleOpenResults");
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsGetGeneratedKeys");
    }

    @Override
    public boolean supportsResultSetHoldability(int p0) throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsResultSetHoldability");
    }

    @Override
    public int getResultSetHoldability() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getResultSetHoldability");
    }

    @Override
    public int getDatabaseMajorVersion() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getDatabaseMajorVersion");
    }

    @Override
    public int getDatabaseMinorVersion() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getDatabaseMinorVersion");
    }

    @Override
    public int getJDBCMajorVersion() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getJDBCMajorVersion");
    }

    @Override
    public int getJDBCMinorVersion() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getJDBCMinorVersion");
    }

    @Override
    public boolean supportsStatementPooling() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsStatementPooling");
    }

    @Override
    public java.sql.ResultSet getClientInfoProperties() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.getClientInfoProperties");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.generatedKeyAlwaysReturned");
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws java.sql.SQLException {
        throw Unsupported.of("DatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions");
    }
}
