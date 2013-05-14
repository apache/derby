/*

   Derby - Class org.apache.derby.client.am.LogicalDatabaseMetaData

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.client.am;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * A metadata object to be used with logical connections when connection
 * pooling is being used.
 * <p>
 * The purpose of this object is to make sure references to the underlying
 * physical connection don't leak to the client / user, and to make the
 * lifetime of the metadata object equal to the logical connection instead
 * of the underlying physical connection.
 */
class LogicalDatabaseMetaData implements DatabaseMetaData {

    /** The associated logical connection. */
    private final LogicalConnection logicalCon;
    /** Error message destination, if any. */
    private final LogWriter logWriter;
    /** Cached value for the driver major version. */
    private final int driverMajorVersion;
    /** Cached value for the driver minor version. */
    private final int driverMinorVersion;

    /**
     * Creates a new logical database metadata object.
     *
     * @param logicalCon the associated logical connection
     * @param logWriter destination for log/error messages
     * @throws SQLException if obtaining the JDBC driver versions fail
     */
    LogicalDatabaseMetaData(LogicalConnection logicalCon,
                            LogWriter logWriter)
            throws SQLException {
        this.logicalCon = logicalCon;
        this.logWriter = logWriter;
        // Implementation note: Cache values, as these two methods are not
        // allowed to throw SQLException.
        this.driverMajorVersion =
                logicalCon.getRealMetaDataObject().getDriverMajorVersion();
        this.driverMinorVersion =
                logicalCon.getRealMetaDataObject().getDriverMinorVersion();
    }

    /**
     * Returns the real metadata object if appropriate.
     * <p>
     * This is just a convenience wrapper method.
     *
     * @return Metadata object from the underlying physical connection.
     * @throws SQLException if the logical connection has been closed, or
     *      obtaining the metadata object fails
     */
    final DatabaseMetaData getRealMetaDataObject()
            throws SQLException {
        return this.logicalCon.getRealMetaDataObject();
    }

    public boolean allProceduresAreCallable() throws SQLException {
        return getRealMetaDataObject().allProceduresAreCallable();
    }

    public boolean allTablesAreSelectable() throws SQLException {
        return getRealMetaDataObject().allTablesAreSelectable();
    }

    public String getURL() throws SQLException {
        return getRealMetaDataObject().getURL();
    }

    public String getUserName() throws SQLException {
        return getRealMetaDataObject().getUserName();
    }

    public boolean isReadOnly() throws SQLException {
        return getRealMetaDataObject().isReadOnly();
    }

    public boolean nullsAreSortedHigh() throws SQLException {
        return getRealMetaDataObject().nullsAreSortedHigh();
    }

    public boolean nullsAreSortedLow() throws SQLException {
        return getRealMetaDataObject().nullsAreSortedLow();
    }

    public boolean nullsAreSortedAtStart() throws SQLException {
        return getRealMetaDataObject().nullsAreSortedAtStart();
    }

    public boolean nullsAreSortedAtEnd() throws SQLException {
        return getRealMetaDataObject().nullsAreSortedAtEnd();
    }

    public String getDatabaseProductName() throws SQLException {
        return getRealMetaDataObject().getDatabaseProductName();
    }

    public String getDatabaseProductVersion() throws SQLException {
        return getRealMetaDataObject().getDatabaseProductVersion();
    }

    public String getDriverName() throws SQLException {
        return getRealMetaDataObject().getDriverName();
    }

    public String getDriverVersion() throws SQLException {
        return getRealMetaDataObject().getDriverVersion();
    }

    public int getDriverMajorVersion() {
        return this.driverMajorVersion;
    }

    public int getDriverMinorVersion() {
        return this.driverMinorVersion;
    }

    public boolean usesLocalFiles() throws SQLException {
        return getRealMetaDataObject().usesLocalFiles();
    }

    public boolean usesLocalFilePerTable() throws SQLException {
        return getRealMetaDataObject().usesLocalFilePerTable();
    }

    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return getRealMetaDataObject().supportsMixedCaseIdentifiers();
    }

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesUpperCaseIdentifiers();
    }

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesLowerCaseIdentifiers();
    }

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesMixedCaseIdentifiers();
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return getRealMetaDataObject().supportsMixedCaseQuotedIdentifiers();
    }

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesUpperCaseQuotedIdentifiers();
    }

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesLowerCaseQuotedIdentifiers();
    }

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return getRealMetaDataObject().storesMixedCaseQuotedIdentifiers();
    }

    public String getIdentifierQuoteString() throws SQLException {
        return getRealMetaDataObject().getIdentifierQuoteString();
    }

    public String getSQLKeywords() throws SQLException {
        return getRealMetaDataObject().getSQLKeywords();
    }

    public String getNumericFunctions() throws SQLException {
        return getRealMetaDataObject().getNumericFunctions();
    }

    public String getStringFunctions() throws SQLException {
        return getRealMetaDataObject().getStringFunctions();
    }

    public String getSystemFunctions() throws SQLException {
        return getRealMetaDataObject().getSystemFunctions();
    }

    public String getTimeDateFunctions() throws SQLException {
        return getRealMetaDataObject().getTimeDateFunctions();
    }

    public String getSearchStringEscape() throws SQLException {
        return getRealMetaDataObject().getSearchStringEscape();
    }

    public String getExtraNameCharacters() throws SQLException {
        return getRealMetaDataObject().getExtraNameCharacters();
    }

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return getRealMetaDataObject().supportsAlterTableWithAddColumn();
    }

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return getRealMetaDataObject().supportsAlterTableWithDropColumn();
    }

    public boolean supportsColumnAliasing() throws SQLException {
        return getRealMetaDataObject().supportsColumnAliasing();
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        return getRealMetaDataObject().nullPlusNonNullIsNull();
    }

    public boolean supportsConvert() throws SQLException {
        return getRealMetaDataObject().supportsConvert();
    }

    public boolean supportsConvert(int fromType, int toType)
            throws SQLException {
        return getRealMetaDataObject().supportsConvert(fromType, toType);
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        return getRealMetaDataObject().supportsTableCorrelationNames();
    }

    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException {
        return getRealMetaDataObject().supportsDifferentTableCorrelationNames();
    }

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return getRealMetaDataObject().supportsExpressionsInOrderBy();
    }

    public boolean supportsOrderByUnrelated() throws SQLException {
        return getRealMetaDataObject().supportsOrderByUnrelated();
    }

    public boolean supportsGroupBy() throws SQLException {
        return getRealMetaDataObject().supportsGroupBy();
    }

    public boolean supportsGroupByUnrelated() throws SQLException {
        return getRealMetaDataObject().supportsGroupByUnrelated();
    }

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return getRealMetaDataObject().supportsGroupByBeyondSelect();
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        return getRealMetaDataObject().supportsLikeEscapeClause();
    }

    public boolean supportsMultipleResultSets() throws SQLException {
        return getRealMetaDataObject().supportsMultipleResultSets();
    }

    public boolean supportsMultipleTransactions() throws SQLException {
        return getRealMetaDataObject().supportsMultipleTransactions();
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        return getRealMetaDataObject().supportsNonNullableColumns();
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return getRealMetaDataObject().supportsMinimumSQLGrammar();
    }

    public boolean supportsCoreSQLGrammar() throws SQLException {
        return getRealMetaDataObject().supportsCoreSQLGrammar();
    }

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return getRealMetaDataObject().supportsExtendedSQLGrammar();
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return getRealMetaDataObject().supportsANSI92EntryLevelSQL();
    }

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return getRealMetaDataObject().supportsANSI92IntermediateSQL();
    }

    public boolean supportsANSI92FullSQL() throws SQLException {
        return getRealMetaDataObject().supportsANSI92FullSQL();
    }

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return getRealMetaDataObject().supportsIntegrityEnhancementFacility();
    }

    public boolean supportsOuterJoins() throws SQLException {
        return getRealMetaDataObject().supportsOuterJoins();
    }

    public boolean supportsFullOuterJoins() throws SQLException {
        return getRealMetaDataObject().supportsFullOuterJoins();
    }

    public boolean supportsLimitedOuterJoins() throws SQLException {
        return getRealMetaDataObject().supportsLimitedOuterJoins();
    }

    public String getSchemaTerm() throws SQLException {
        return getRealMetaDataObject().getSchemaTerm();
    }

    public String getProcedureTerm() throws SQLException {
        return getRealMetaDataObject().getProcedureTerm();
    }

    public String getCatalogTerm() throws SQLException {
        return getRealMetaDataObject().getCatalogTerm();
    }

    public boolean isCatalogAtStart() throws SQLException {
        return getRealMetaDataObject().isCatalogAtStart();
    }

    public String getCatalogSeparator() throws SQLException {
        return getRealMetaDataObject().getCatalogSeparator();
    }

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return getRealMetaDataObject().supportsSchemasInDataManipulation();
    }

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return getRealMetaDataObject().supportsSchemasInProcedureCalls();
    }

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return getRealMetaDataObject().supportsSchemasInTableDefinitions();
    }

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return getRealMetaDataObject().supportsSchemasInIndexDefinitions();
    }

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return getRealMetaDataObject().supportsSchemasInPrivilegeDefinitions();
    }

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return getRealMetaDataObject().supportsCatalogsInDataManipulation();
    }

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return getRealMetaDataObject().supportsCatalogsInProcedureCalls();
    }

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return getRealMetaDataObject().supportsCatalogsInTableDefinitions();
    }

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return getRealMetaDataObject().supportsCatalogsInIndexDefinitions();
    }

    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException {
        return getRealMetaDataObject().supportsCatalogsInPrivilegeDefinitions();
    }

    public boolean supportsPositionedDelete() throws SQLException {
        return getRealMetaDataObject().supportsPositionedDelete();
    }

    public boolean supportsPositionedUpdate() throws SQLException {
        return getRealMetaDataObject().supportsPositionedUpdate();
    }

    public boolean supportsSelectForUpdate() throws SQLException {
        return getRealMetaDataObject().supportsSelectForUpdate();
    }

    public boolean supportsStoredProcedures() throws SQLException {
        return getRealMetaDataObject().supportsStoredProcedures();
    }

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return getRealMetaDataObject().supportsSubqueriesInComparisons();
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        return getRealMetaDataObject().supportsSubqueriesInExists();
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        return getRealMetaDataObject().supportsSubqueriesInIns();
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return getRealMetaDataObject().supportsSubqueriesInQuantifieds();
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return getRealMetaDataObject().supportsCorrelatedSubqueries();
    }

    public boolean supportsUnion() throws SQLException {
        return getRealMetaDataObject().supportsUnion();
    }

    public boolean supportsUnionAll() throws SQLException {
        return getRealMetaDataObject().supportsUnionAll();
    }

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return getRealMetaDataObject().supportsOpenCursorsAcrossCommit();
    }

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return getRealMetaDataObject().supportsOpenCursorsAcrossRollback();
    }

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return getRealMetaDataObject().supportsOpenStatementsAcrossCommit();
    }

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return getRealMetaDataObject().supportsOpenStatementsAcrossRollback();
    }

    public int getMaxBinaryLiteralLength() throws SQLException {
        return getRealMetaDataObject().getMaxBinaryLiteralLength();
    }

    public int getMaxCharLiteralLength() throws SQLException {
        return getRealMetaDataObject().getMaxCharLiteralLength();
    }

    public int getMaxColumnNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxColumnNameLength();
    }

    public int getMaxColumnsInGroupBy() throws SQLException {
        return getRealMetaDataObject().getMaxColumnsInGroupBy();
    }

    public int getMaxColumnsInIndex() throws SQLException {
        return getRealMetaDataObject().getMaxColumnsInIndex();
    }

    public int getMaxColumnsInOrderBy() throws SQLException {
        return getRealMetaDataObject().getMaxColumnsInOrderBy();
    }

    public int getMaxColumnsInSelect() throws SQLException {
        return getRealMetaDataObject().getMaxColumnsInSelect();
    }

    public int getMaxColumnsInTable() throws SQLException {
        return getRealMetaDataObject().getMaxColumnsInTable();
    }

    public int getMaxConnections() throws SQLException {
        return getRealMetaDataObject().getMaxConnections();
    }

    public int getMaxCursorNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxCursorNameLength();
    }

    public int getMaxIndexLength() throws SQLException {
        return getRealMetaDataObject().getMaxIndexLength();
    }

    public int getMaxSchemaNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxSchemaNameLength();
    }

    public int getMaxProcedureNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxProcedureNameLength();
    }

    public int getMaxCatalogNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxCatalogNameLength();
    }

    public int getMaxRowSize() throws SQLException {
        return getRealMetaDataObject().getMaxRowSize();
    }

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return getRealMetaDataObject().doesMaxRowSizeIncludeBlobs();
    }

    public int getMaxStatementLength() throws SQLException {
        return getRealMetaDataObject().getMaxStatementLength();
    }

    public int getMaxStatements() throws SQLException {
        return getRealMetaDataObject().getMaxStatements();
    }

    public int getMaxTableNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxTableNameLength();
    }

    public int getMaxTablesInSelect() throws SQLException {
        return getRealMetaDataObject().getMaxTablesInSelect();
    }

    public int getMaxUserNameLength() throws SQLException {
        return getRealMetaDataObject().getMaxUserNameLength();
    }

    public int getDefaultTransactionIsolation() throws SQLException {
        return getRealMetaDataObject().getDefaultTransactionIsolation();
    }

    public boolean supportsTransactions() throws SQLException {
        return getRealMetaDataObject().supportsTransactions();
    }

    public boolean supportsTransactionIsolationLevel(int level)
            throws SQLException {
        return getRealMetaDataObject().supportsTransactionIsolationLevel(level);
    }

    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException {
        return getRealMetaDataObject().
                supportsDataDefinitionAndDataManipulationTransactions();
    }

    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException {
        return getRealMetaDataObject().
                supportsDataManipulationTransactionsOnly();
    }

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return getRealMetaDataObject().dataDefinitionCausesTransactionCommit();
    }

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return getRealMetaDataObject().dataDefinitionIgnoredInTransactions();
    }

    public ResultSet getProcedures(
            String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getProcedures(
                catalog, schemaPattern, procedureNamePattern);
    }

    public ResultSet getProcedureColumns(
            String catalog,
            String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern) throws SQLException {

        return getRealMetaDataObject().getProcedureColumns(catalog,
                schemaPattern, procedureNamePattern, columnNamePattern);
    }

    public ResultSet getTables(String catalog, String schemaPattern,
            String tableNamePattern, String[] types)
            throws SQLException {
        return getRealMetaDataObject().getTables(
                catalog, schemaPattern, tableNamePattern, types);
    }

    public ResultSet getSchemas() throws SQLException {
        return getRealMetaDataObject().getSchemas();
    }

    public ResultSet getCatalogs() throws SQLException {
        return getRealMetaDataObject().getCatalogs();
    }

    public ResultSet getTableTypes() throws SQLException {
        return getRealMetaDataObject().getTableTypes();
    }

    public ResultSet getColumns(String catalog, String schemaPattern,
            String tableNamePattern, String columnNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getColumns(
                catalog, schemaPattern, tableNamePattern, columnNamePattern);
    }

    public ResultSet getColumnPrivileges(String catalog, String schema,
            String table, String columnNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getColumnPrivileges(
                catalog, schema, table, columnNamePattern);
    }

    public ResultSet getTablePrivileges(
            String catalog,
            String schemaPattern,
            String tableNamePattern) throws SQLException {

        return getRealMetaDataObject().getTablePrivileges(
                catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getBestRowIdentifier(
            String catalog,
            String schema,
            String table,
            int scope,
            boolean nullable) throws SQLException {

        return getRealMetaDataObject().getBestRowIdentifier(
                catalog, schema, table, scope, nullable);
    }

    public ResultSet getVersionColumns(
            String catalog, String schema, String table)
            throws SQLException {
        return getRealMetaDataObject().getVersionColumns(
                catalog, schema, table);
    }

    public ResultSet getPrimaryKeys(
            String catalog,
            String schema,
            String table) throws SQLException {

        return getRealMetaDataObject().getPrimaryKeys(catalog, schema, table);
    }

    public ResultSet getImportedKeys(
            String catalog, String schema, String table)
            throws SQLException {
        return getRealMetaDataObject().getImportedKeys(catalog, schema, table);
    }

    public ResultSet getExportedKeys(
            String catalog, String schema, String table)
            throws SQLException {
        return getRealMetaDataObject().getExportedKeys(catalog, schema, table);
    }

    public ResultSet getCrossReference(String parentCatalog,
            String parentSchema, String parentTable, String foreignCatalog,
            String foreignSchema, String foreignTable)
            throws SQLException {
        return getRealMetaDataObject().getCrossReference(
                parentCatalog, parentSchema, parentTable, foreignCatalog,
                foreignSchema, foreignTable);
    }

    public ResultSet getTypeInfo() throws SQLException {
        return getRealMetaDataObject().getTypeInfo();
    }

    public ResultSet getIndexInfo(
            String catalog,
            String schema,
            String table,
            boolean unique,
            boolean approximate) throws SQLException {

        return getRealMetaDataObject().getIndexInfo(
                catalog, schema, table, unique, approximate);
    }

    public boolean supportsResultSetType(int type) throws SQLException {
        return getRealMetaDataObject().supportsResultSetType(type);
    }

    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException {
        return getRealMetaDataObject().supportsResultSetConcurrency(
                type, concurrency);
    }

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().ownUpdatesAreVisible(type);
    }

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().ownDeletesAreVisible(type);
    }

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().ownInsertsAreVisible(type);
    }

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().othersUpdatesAreVisible(type);
    }

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().othersDeletesAreVisible(type);
    }

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return getRealMetaDataObject().othersInsertsAreVisible(type);
    }

    public boolean updatesAreDetected(int type) throws SQLException {
        return getRealMetaDataObject().updatesAreDetected(type);
    }

    public boolean deletesAreDetected(int type) throws SQLException {
        return getRealMetaDataObject().deletesAreDetected(type);
    }

    public boolean insertsAreDetected(int type) throws SQLException {
        return getRealMetaDataObject().insertsAreDetected(type);
    }

    public boolean supportsBatchUpdates() throws SQLException {
        return getRealMetaDataObject().supportsBatchUpdates();
    }

    public ResultSet getUDTs(String catalog, String schemaPattern,
            String typeNamePattern, int[] types)
            throws SQLException {
        return getRealMetaDataObject().getUDTs(
                catalog, schemaPattern, typeNamePattern, types);
    }

    public Connection getConnection() throws SQLException {
        getRealMetaDataObject(); // Just check if the connection is still open.
        return this.logicalCon;
    }

    public boolean supportsSavepoints() throws SQLException {
        return getRealMetaDataObject().supportsSavepoints();
    }

    public boolean supportsNamedParameters() throws SQLException {
        return getRealMetaDataObject().supportsNamedParameters();
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        return getRealMetaDataObject().supportsMultipleOpenResults();
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        return getRealMetaDataObject().supportsGetGeneratedKeys();
    }

    public ResultSet getSuperTypes(
            String catalog, String schemaPattern, String typeNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getSuperTypes(
                catalog, schemaPattern, typeNamePattern);
    }

    public ResultSet getSuperTables(
            String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getSuperTables(
                catalog, schemaPattern, tableNamePattern);
    }

    public ResultSet getAttributes(
            String catalog,
            String schemaPattern,
            String typeNamePattern,
            String attributeNamePattern) throws SQLException {

        return getRealMetaDataObject().getAttributes(
                catalog, schemaPattern, typeNamePattern, attributeNamePattern);
    }

    public boolean supportsResultSetHoldability(int holdability)
            throws SQLException {
        return getRealMetaDataObject().supportsResultSetHoldability(
                holdability);
    }

    public int getResultSetHoldability() throws SQLException {
        return getRealMetaDataObject().getResultSetHoldability();
    }

    public int getDatabaseMajorVersion() throws SQLException {
        return getRealMetaDataObject().getDatabaseMajorVersion();
    }

    public int getDatabaseMinorVersion() throws SQLException {
        return getRealMetaDataObject().getDatabaseMinorVersion();
    }

    public int getJDBCMajorVersion() throws SQLException {
        return getRealMetaDataObject().getJDBCMajorVersion();
    }

    public int getJDBCMinorVersion() throws SQLException {
        return getRealMetaDataObject().getJDBCMinorVersion();
    }

    public int getSQLStateType() throws SQLException {
        return getRealMetaDataObject().getSQLStateType();
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        return getRealMetaDataObject().locatorsUpdateCopy();
    }

    public boolean supportsStatementPooling() throws SQLException {
        return getRealMetaDataObject().supportsStatementPooling();
    }

    // JDBC 4.0 methods

    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException {
        return getRealMetaDataObject().autoCommitFailureClosesAllResultSets();
    }

    public ResultSet getClientInfoProperties()
            throws SQLException {
        return getRealMetaDataObject().getClientInfoProperties();
    }

    public ResultSet getFunctions(String catalog, String schemaPattern,
                                  String functionNamePattern)
            throws SQLException {
        return getRealMetaDataObject().getFunctions(
                catalog, schemaPattern, functionNamePattern);
    }

    public ResultSet getFunctionColumns(
            String catalog, String schemaPattern,
            String functionNamePattern,
            String columnNamePattern) throws SQLException {

        return getRealMetaDataObject().getFunctionColumns(
                catalog, schemaPattern, functionNamePattern, columnNamePattern);
    }

    public RowIdLifetime getRowIdLifetime()
            throws SQLException {
        return getRealMetaDataObject().getRowIdLifetime();
    }

    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException {
        return getRealMetaDataObject().getSchemas(catalog, schemaPattern);
    }

    public boolean isWrapperFor(Class<?> interfaces)
            throws SQLException {
        getRealMetaDataObject(); // Check for open connection.
        return interfaces.isInstance(this);
    }

    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException {
        return getRealMetaDataObject().supportsStoredFunctionsUsingCallSyntax();
    }

    public <T> T unwrap(Class<T> interfaces)
            throws SQLException {
        getRealMetaDataObject(); // Check for open connection.
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(
                                logWriter,
                                new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                                interfaces
                            ).getSQLException();
        }
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  JDBC 4.1 - New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /** See DatabaseMetaData javadoc */
    public  boolean generatedKeyAlwaysReturned() throws SQLException
    {
        return ((ClientDatabaseMetaData)getRealMetaDataObject()).
            generatedKeyAlwaysReturned();
    }

    /**
    * See DatabaseMetaData javadoc. Empty ResultSet because Derby does
    * not support pseudo columns.
    */
    public ResultSet getPseudoColumns
        ( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern )
        throws SQLException
    {
        return ((ClientDatabaseMetaData)getRealMetaDataObject()).
            getPseudoColumns(catalog,
                             schemaPattern,
                             tableNamePattern,
                             columnNamePattern);
    }

    /////////////////////////////////////////////////////////////////////////
    //
    //  JDBC 4.2 - New public methods
    //
    /////////////////////////////////////////////////////////////////////////

    /** See DatabaseMetaData javadoc */
    public  long getMaxLogicalLobSize() throws SQLException
    {
        return ((ClientDatabaseMetaData)getRealMetaDataObject()).
            getMaxLogicalLobSize();
    }
    public  boolean supportsRefCursors() throws SQLException
    {
        return ((ClientDatabaseMetaData)getRealMetaDataObject()).
            supportsRefCursors();
    }
}
