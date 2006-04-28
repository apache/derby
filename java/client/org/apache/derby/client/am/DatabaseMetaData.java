/*

   Derby - Class org.apache.derby.client.am.DatabaseMetaData

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.client.am;

import java.sql.SQLException;

import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.JDBC30Translation;

// Note:
//   Tag members using the strictest visibility.
// Note:
//   Mark methods synchronized if and only if they update object state and are public.
// Not yet done:
//   Application heap data should be copied for shiraz.
//   Save for future pass to avoid clutter during development.
// Not yet done:
//   Apply meaning-preserving program transformations for performance,
//   including the replacement of slow ADTs with faster unsynchronized ADTs.
//   Save for future pass to avoid clutter during development.
// Not yet done:
//   Assign an ErrorKey, ResourceKey, and Resource for each throw statement.
//   Save for future pass to avoid maintenance during development.

public abstract class DatabaseMetaData implements java.sql.DatabaseMetaData {
    //----------------------------- constants  -----------------------------------

    private final static short SQL_BEST_ROWID = 1;
    private final static short SQL_ROWVER = 2;

    private final static short SQL_INDEX_UNIQUE = 0;
    private final static short SQL_INDEX_ALL = 1;

    //---------------------navigational members-----------------------------------

    protected Agent agent_;
    protected Connection connection_;

    //-----------------------------state------------------------------------------

    private final static int numberOfMetaDataInfoMethods__ = 108;
    private Object[] metaDataInfoCache_ = new Object[numberOfMetaDataInfoMethods__];
    private boolean metaDataInfoIsCached_ = false;

    public ProductLevel productLevel_;

    /** The JDBC major version supported by the server. */
    private final int serverJdbcMajorVersion;
    /** The JDBC minor version supported by the server. */
    private final int serverJdbcMinorVersion;

    public boolean useServerXAState_ = true;

    //---------------------constructors/finalizer---------------------------------

    protected DatabaseMetaData(Agent agent, Connection connection, ProductLevel productLevel) {
        agent_ = agent;
        connection_ = connection;
        productLevel_ = productLevel;
        computeFeatureSet_();
        if (connection.isXAConnection()) {
            connection.xaHostVersion_ = productLevel_.versionLevel_;
        }
        if (productLevel_.lessThan(10, 2, 0)) {
            serverJdbcMajorVersion = 3;
            serverJdbcMinorVersion = 0;
        } else {
            serverJdbcMajorVersion = 4;
            serverJdbcMinorVersion = 0;
        }
    }

    // ---------------------------jdbc 1------------------------------------------

    //----------------------------------------------------------------------
    // First, a variety of minor information about the target database.

    private final static int allProceduresAreCallable__ = 0;

    public boolean allProceduresAreCallable() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(allProceduresAreCallable__);
    }

    private final static int allTablesAreSelectable__ = 1;

    public boolean allTablesAreSelectable() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(allTablesAreSelectable__);
    }

    private final static int nullsAreSortedHigh__ = 2;

    public boolean nullsAreSortedHigh() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(nullsAreSortedHigh__);
    }

    private final static int nullsAreSortedLow__ = 3;

    public boolean nullsAreSortedLow() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(nullsAreSortedLow__);
    }

    private final static int nullsAreSortedAtStart__ = 4;

    public boolean nullsAreSortedAtStart() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(nullsAreSortedAtStart__);
    }

    private final static int nullsAreSortedAtEnd__ = 5;

    public boolean nullsAreSortedAtEnd() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(nullsAreSortedAtEnd__);
    }

    private final static int usesLocalFiles__ = 6;

    public boolean usesLocalFiles() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(usesLocalFiles__);
    }

    private final static int usesLocalFilePerTable__ = 7;

    public boolean usesLocalFilePerTable() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(usesLocalFilePerTable__);
    }

    private final static int storesUpperCaseIdentifiers__ = 8;

    public boolean storesUpperCaseIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesUpperCaseIdentifiers__);
    }


    private final static int storesLowerCaseIdentifiers__ = 9;

    public boolean storesLowerCaseIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesLowerCaseIdentifiers__);
    }

    private final static int storesMixedCaseIdentifiers__ = 10;

    public boolean storesMixedCaseIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesMixedCaseIdentifiers__);
    }

    private final static int storesUpperCaseQuotedIdentifiers__ = 11;

    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesUpperCaseQuotedIdentifiers__);
    }

    private final static int storesLowerCaseQuotedIdentifiers__ = 12;

    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesLowerCaseQuotedIdentifiers__);
    }

    private final static int storesMixedCaseQuotedIdentifiers__ = 13;

    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(storesMixedCaseQuotedIdentifiers__);
    }

    private final static int getSQLKeywords__ = 14;

    public String getSQLKeywords() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getSQLKeywords__);
    }

    private final static int getNumericFunctions__ = 15;

    public String getNumericFunctions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getNumericFunctions__);
    }

    private final static int getStringFunctions__ = 16;

    public String getStringFunctions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getStringFunctions__);
    }

    private final static int getSystemFunctions__ = 17;

    public String getSystemFunctions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getSystemFunctions__);
    }

    private final static int getTimeDateFunctions__ = 18;

    public String getTimeDateFunctions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getTimeDateFunctions__);
    }

    private final static int getSearchStringEscape__ = 19;

    public String getSearchStringEscape() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getSearchStringEscape__);
    }

    private final static int getExtraNameCharacters__ = 20;

    public String getExtraNameCharacters() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getExtraNameCharacters__);
    }

    private final static int supportsAlterTableWithAddColumn__ = 21;

    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsAlterTableWithAddColumn__);
    }

    private final static int supportsAlterTableWithDropColumn__ = 22;

    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsAlterTableWithDropColumn__);
    }

    private final static int supportsConvert__ = 23;

    public boolean supportsConvert() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsConvert__);
    }

    private final static int supportsConvertType__ = 24;

    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean_supportsConvert(supportsConvertType__, fromType, toType);
    }

    private final static int supportsDifferentTableCorrelationNames__ = 25;

    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsDifferentTableCorrelationNames__);
    }

    private final static int supportsExpressionsInOrderBy__ = 26;

    public boolean supportsExpressionsInOrderBy() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsExpressionsInOrderBy__);
    }

    private final static int supportsOrderByUnrelated__ = 27;

    public boolean supportsOrderByUnrelated() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOrderByUnrelated__);
    }

    private final static int supportsGroupBy__ = 28;

    public boolean supportsGroupBy() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsGroupBy__);
    }

    private final static int supportsGroupByUnrelated__ = 29;

    public boolean supportsGroupByUnrelated() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsGroupByUnrelated__);
    }

    private final static int supportsGroupByBeyondSelect__ = 30;

    public boolean supportsGroupByBeyondSelect() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsGroupByBeyondSelect__);
    }

    private final static int supportsMultipleResultSets__ = 31;

    public boolean supportsMultipleResultSets() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsMultipleResultSets__);
    }

    private final static int supportsMultipleTransactions__ = 32;

    public boolean supportsMultipleTransactions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsMultipleTransactions__);
    }

    private final static int supportsCoreSQLGrammar__ = 33;

    public boolean supportsCoreSQLGrammar() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCoreSQLGrammar__);
    }

    private final static int supportsExtendedSQLGrammar__ = 34;

    public boolean supportsExtendedSQLGrammar() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsExtendedSQLGrammar__);
    }

    private final static int supportsANSI92IntermediateSQL__ = 35;

    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsANSI92IntermediateSQL__);
    }

    private final static int supportsANSI92FullSQL__ = 36;

    public boolean supportsANSI92FullSQL() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsANSI92FullSQL__);
    }

    private final static int supportsIntegrityEnhancementFacility__ = 37;

    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsIntegrityEnhancementFacility__);
    }

    private final static int supportsOuterJoins__ = 38;

    public boolean supportsOuterJoins() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOuterJoins__);
    }

    private final static int supportsFullOuterJoins__ = 39;

    public boolean supportsFullOuterJoins() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsFullOuterJoins__);
    }

    private final static int supportsLimitedOuterJoins__ = 40;

    public boolean supportsLimitedOuterJoins() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsLimitedOuterJoins__);
    }

    private final static int getSchemaTerm__ = 41;

    public String getSchemaTerm() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getSchemaTerm__);
    }

    private final static int getProcedureTerm__ = 42;

    public String getProcedureTerm() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getProcedureTerm__);
    }

    private final static int getCatalogTerm__ = 43;

    public String getCatalogTerm() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getCatalogTerm__);
    }

    private final static int isCatalogAtStart__ = 44;

    public boolean isCatalogAtStart() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(isCatalogAtStart__);
    }

    private final static int getCatalogSeparator__ = 45;

    public String getCatalogSeparator() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoString(getCatalogSeparator__);
    }

    private final static int supportsSchemasInDataManipulation__ = 46;

    public boolean supportsSchemasInDataManipulation() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSchemasInDataManipulation__);
    }

    private final static int supportsSchemasInProcedureCalls__ = 47;

    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSchemasInProcedureCalls__);
    }

    private final static int supportsSchemasInTableDefinitions__ = 48;

    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSchemasInTableDefinitions__);
    }


    private final static int supportsSchemasInIndexDefinitions__ = 49;

    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSchemasInIndexDefinitions__);
    }

    private final static int supportsSchemasInPrivilegeDefinitions__ = 50;

    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSchemasInPrivilegeDefinitions__);
    }

    private final static int supportsCatalogsInDataManipulation__ = 51;

    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCatalogsInDataManipulation__);
    }

    private final static int supportsCatalogsInProcedureCalls__ = 52;

    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCatalogsInProcedureCalls__);
    }

    private final static int supportsCatalogsInTableDefinitions__ = 53;

    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCatalogsInTableDefinitions__);
    }

    private final static int supportsCatalogsInIndexDefinitions__ = 54;

    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCatalogsInIndexDefinitions__);
    }

    private final static int supportsCatalogsInPrivilegeDefinitions__ = 55;

    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsCatalogsInPrivilegeDefinitions__);
    }

    private final static int supportsPositionedDelete__ = 56;

    public boolean supportsPositionedDelete() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsPositionedDelete__);
    }


    private final static int supportsPositionedUpdate__ = 57;

    public boolean supportsPositionedUpdate() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsPositionedUpdate__);
    }

    private final static int supportsSelectForUpdate__ = 58;

    public boolean supportsSelectForUpdate() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSelectForUpdate__);
    }

    private final static int supportsStoredProcedures__ = 59;

    public boolean supportsStoredProcedures() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsStoredProcedures__);
    }

    private final static int supportsSubqueriesInComparisons__ = 60;

    public boolean supportsSubqueriesInComparisons() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsSubqueriesInComparisons__);
    }

    private final static int supportsUnion__ = 61;

    public boolean supportsUnion() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsUnion__);
    }

    private final static int supportsUnionAll__ = 62;

    public boolean supportsUnionAll() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsUnionAll__);

    }

    private final static int supportsOpenCursorsAcrossCommit__ = 63;

    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOpenCursorsAcrossCommit__);
    }

    private final static int supportsOpenCursorsAcrossRollback__ = 64;

    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOpenCursorsAcrossRollback__);
    }

    private final static int supportsOpenStatementsAcrossCommit__ = 65;

    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOpenStatementsAcrossCommit__);
    }


    private final static int supportsOpenStatementsAcrossRollback__ = 66;

    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsOpenStatementsAcrossRollback__);
    }

    //----------------------------------------------------------------------
    // The following group of methods exposes various limitations
    // based on the target database with the current driver.
    // Unless otherwise specified, a result of zero means there is no
    // limit, or the limit is not known.
    private final static int getMaxBinaryLiteralLength__ = 67;

    public int getMaxBinaryLiteralLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxBinaryLiteralLength__);
    }

    private final static int getMaxCharLiteralLength__ = 68;

    public int getMaxCharLiteralLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxCharLiteralLength__);
    }

    private final static int getMaxColumnNameLength__ = 69;

    public int getMaxColumnNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnNameLength__);
    }

    private final static int getMaxColumnsInGroupBy__ = 70;

    public int getMaxColumnsInGroupBy() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnsInGroupBy__);
    }

    private final static int getMaxColumnsInIndex__ = 71;

    public int getMaxColumnsInIndex() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnsInIndex__);
    }

    private final static int getMaxColumnsInOrderBy__ = 72;

    public int getMaxColumnsInOrderBy() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnsInOrderBy__);
    }

    private final static int getMaxColumnsInSelect__ = 73;

    public int getMaxColumnsInSelect() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnsInSelect__);
    }

    private final static int getMaxColumnsInTable__ = 74;

    public int getMaxColumnsInTable() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxColumnsInTable__);
    }

    private final static int getMaxConnections__ = 75;

    public int getMaxConnections() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxConnections__);
    }

    private final static int getMaxCursorNameLength__ = 76;

    public int getMaxCursorNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxCursorNameLength__);
    }

    private final static int getMaxIndexLength__ = 77;

    public int getMaxIndexLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxIndexLength__);
    }

    private final static int getMaxSchemaNameLength__ = 78;

    public int getMaxSchemaNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxSchemaNameLength__);
    }

    private final static int getMaxProcedureNameLength__ = 79;

    public int getMaxProcedureNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxProcedureNameLength__);
    }

    private final static int getMaxCatalogNameLength__ = 80;

    public int getMaxCatalogNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxCatalogNameLength__);
    }

    private final static int getMaxRowSize__ = 81;

    public int getMaxRowSize() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxRowSize__);
    }

    private final static int doesMaxRowSizeIncludeBlobs__ = 82;

    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(doesMaxRowSizeIncludeBlobs__);
    }

    private final static int getMaxStatementLength__ = 83;

    public int getMaxStatementLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxStatementLength__);
    }

    private final static int getMaxStatements__ = 84;

    public int getMaxStatements() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxStatements__);
    }

    private final static int getMaxTableNameLength__ = 85;

    public int getMaxTableNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxTableNameLength__);
    }

    private final static int getMaxTablesInSelect__ = 86;

    public int getMaxTablesInSelect() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxTablesInSelect__);
    }

    private final static int getMaxUserNameLength__ = 87;

    public int getMaxUserNameLength() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getMaxUserNameLength__);
    }

    private final static int getDefaultTransactionIsolation__ = 88;

    public int getDefaultTransactionIsolation() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt(getDefaultTransactionIsolation__);
    }

    private final static int supportsTransactions__ = 89;

    public boolean supportsTransactions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsTransactions__);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all supported levels
    private final static int supportsTransactionIsolationLevel__ = 90;

    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(supportsTransactionIsolationLevel__, level);
    }


    private final static int supportsDataDefinitionAndDataManipulationTransactions__ = 91;

    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsDataDefinitionAndDataManipulationTransactions__);
    }

    private final static int supportsDataManipulationTransactionsOnly__ = 92;

    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsDataManipulationTransactionsOnly__);
    }

    private final static int dataDefinitionCausesTransactionCommit__ = 93;

    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(dataDefinitionCausesTransactionCommit__);
    }

    private final static int dataDefinitionIgnoredInTransactions__ = 94;

    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(dataDefinitionIgnoredInTransactions__);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported resultSet types
    private final static int supportsResultSetType__ = 95;

    public boolean supportsResultSetType(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(supportsResultSetType__, type);
    }

    private final static int supportsResultSetConcurrency__ = 96;

    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoInt_SupportsResultSetConcurrency(supportsResultSetConcurrency__, type, concurrency);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int ownUpdatesAreVisible__ = 97;

    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(ownUpdatesAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int ownDeletesAreVisible__ = 98;

    public boolean ownDeletesAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(ownDeletesAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list all the supported result Set types
    private final static int ownInsertsAreVisible__ = 99;

    public boolean ownInsertsAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(ownInsertsAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int othersUpdatesAreVisible__ = 100;

    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(othersUpdatesAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int othersDeletesAreVisible__ = 101;

    public boolean othersDeletesAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(othersDeletesAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int othersInsertsAreVisible__ = 102;

    public boolean othersInsertsAreVisible(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(othersInsertsAreVisible__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int updatesAreDetected__ = 103;

    public boolean updatesAreDetected(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(updatesAreDetected__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int deletesAreDetected__ = 104;

    public boolean deletesAreDetected(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(deletesAreDetected__, type);
    }

    // Stored Procedure will return a String containing a
    // comma seperated list of all the supported result Set types
    private final static int insertsAreDetected__ = 105;

    public boolean insertsAreDetected(int type) throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBooleanWithType(insertsAreDetected__, type);
    }

    private final static int supportsBatchUpdates__ = 106;

    public boolean supportsBatchUpdates() throws SQLException {
        checkForClosedConnection();
        return getMetaDataInfoBoolean(supportsBatchUpdates__);
    }

    public boolean supportsSavepoints() throws SQLException {
        checkForClosedConnection();
        if (productLevel_.greaterThanOrEqualTo(5, 2, 0)) {
            return true;
        }

        return false;
    }

    // start tagging all abstract methods with an underscore like this !!
    abstract public String getURL_() throws SqlException;

    public String getURL() throws SQLException {
        try
        {
            checkForClosedConnection();
            return getURL_();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public String getUserName() throws SQLException {
        checkForClosedConnection();
        return connection_.user_;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public String getDatabaseProductName() throws SQLException {
        checkForClosedConnection();
        return productLevel_.databaseProductName_;
    }

    public String getDatabaseProductVersion() throws SQLException {
        checkForClosedConnection();
        return productLevel_.databaseProductVersion_;
    }

    public String getDriverName() throws SQLException {
        checkForClosedConnection();
        return Configuration.dncDriverName;
    }

    public String getDriverVersion() throws SQLException {
        checkForClosedConnection();
        return Version.getDriverVersion();
    }

    // JDBC signature also does not throw SqlException, so we don't check for closed connection.
    public int getDriverMajorVersion() {
        return Version.getMajorVersion();
    }

    // JDBC signature also does not throw SqlException, so we don't check for closed connection.
    public int getDriverMinorVersion() {
        return Version.getMinorVersion();
    }

    //All JDBC Drivers must return false for this method. For this reason we choose
    //to return FALSE
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public String getIdentifierQuoteString() throws SQLException {
        checkForClosedConnection();
        return "\"";
    }

    public boolean supportsColumnAliasing() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean nullPlusNonNullIsNull() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsTableCorrelationNames() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsLikeEscapeClause() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsNonNullableColumns() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsMinimumSQLGrammar() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsSubqueriesInExists() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsSubqueriesInIns() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsCorrelatedSubqueries() throws SQLException {
        checkForClosedConnection();
        return true;
    }


    //------------------------catalog query methods follow--------------------------------------------

    // call stored procedure SQLProcedures
    // SYSIBM.SQLProcedures(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              ProcName    varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getProcedures(String catalog,
                                            String schemaPattern,
                                            String procedureNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getProcedures", catalog, schemaPattern, procedureNamePattern);
                }
                return getProceduresX(catalog, schemaPattern, procedureNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }


    private ResultSet getProceduresX(String catalog,
                                     String schemaPattern,
                                     String procedureNamePattern) throws SqlException {
        checkForClosedConnectionX();

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLPROCEDURES(?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, procedureNamePattern);
        cs.setStringX(4, getOptions());
        return executeCatalogQuery(cs);
    }


    // call stored procedure SQLProcedureCols
    // SYSIBM.SQLProcedureCols(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              ProcName    varchar(128),
    //              ParamName   varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getProcedureColumns(String catalog,
                                                  String schemaPattern,
                                                  String procedureNamePattern,
                                                  String columnNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getProcedureColumns", catalog, schemaPattern, procedureNamePattern, columnNamePattern);
                }
                return getProcedureColumnsX(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
        
    }

    private ResultSet getProcedureColumnsX(String catalog,
                                           String schemaPattern,
                                           String procedureNamePattern,
                                           String columnNamePattern) throws SqlException {
        checkForClosedConnectionX();
        ;

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLPROCEDURECOLS(?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, procedureNamePattern);
        cs.setStringX(4, columnNamePattern);
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }

    /** 
     * Get the function names available in the database.  Calls stored
     * procedure <code>SYSIBM.SQLFunctions(CatalogName
     * varchar(128), SchemaName varchar(128), FuncName varchar(128),
     * Options varchar(4000))</code> on the server. This procedure
     * will in turn call
     * <code>EmbedDatabaseMetaData.getFunctions(String,String,String)</code><p>
     * Compatibility: Only available if both server and client version
     * > 10.1, and JDK version >= 1.6. Older clients will not have
     * this method available. Newer clients will be able to call this
     * method when connected to an older server, but this will be
     * trigger an exception in
     * <code>checkServerJdbcVersionX()</code>. <p>Upgrade:
     * <code>SYSIBM.SQLFunctions</code> is added in
     * <code>DataDictionaryImpl.create_10_2_system_procedures
     * (TransactionController,UUID)</code> so it will become available
     * in newly created databases and after <b>hard</b> upgrade.
     *
     * @param catalog limit search to this catalog
     * @param schemaPattern limit search to schemas matching this pattern
     * @param functionNamePattern limit search to functions matching this 
     * pattern
     * @return a <code>ResultSet</code> listing the fucntions
     * @exception SqlException if a database error occurs
     * @see #getFunctionsX(String, String, String)
     * @see org.apache.derby.impl.sql.catalog.DataDictionaryImpl#create_10_2_system_procedures(TransactionController,UUID)
     * @see org.apache.derby.impl.jdbc.EmbedDatabaseMetaData#getFunctions(String,String,String)
     */

    public java.sql.ResultSet getFunctions(String catalog,
                                           String schemaPattern,
                                           String functionNamePattern) 
        throws SQLException {
        try {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getFunctions", 
                                                 catalog, schemaPattern, 
                                                 functionNamePattern);
                }
                return getFunctionsX(catalog, schemaPattern, 
                                     functionNamePattern);
            }
        }
        catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /** 
     * Untraced version of <code>getFunctions(String, String, String)</code>.
     * @param catalog limit search to this catalog
     * @param schemaPattern limit search to schemas matching this pattern
     * @param functionNamePattern limit search to functions matching this 
     * pattern
     * @return a <code>ResultSet</code> listing the fucntions
     * @exception SqlException if a database error occurs
     * @see #getFunctions(String, String, String)
     */
    private ResultSet getFunctionsX(String catalog,
                                    String schemaPattern,
                                    String functionNamePattern) 
        throws SqlException {
        checkForClosedConnectionX();
        checkServerJdbcVersionX("getFunctions(String,String,String)", 4, 0); 

        PreparedStatement cs = 
            prepareMetaDataQuery("SYSIBM.SQLFUNCTIONS(?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, functionNamePattern);
        cs.setStringX(4, getOptions());
        return executeCatalogQuery(cs);
    }

    /** 
     * Get the function names available in the database.  Calls stored
     * procedure <code>SYSIBM.SQLFunctionParams(CatalogName
     * varchar(128), SchemaName varchar(128), FuncName varchar(128),
     * ParamName varchar(128), Options varchar(4000))</code> on the
     * server. This procedure will in turn call
     * <code>EmbedDatabaseMetaData.getFunctionParameters(String,String,
     * String,String)</code><p> Compatibility: Only available if both
     * server and client version > 10.1, and JDK version >= 1.6. Older
     * clients will not have this method available. Newer clients will
     * be able to call this method when connected to an older server,
     * but this will be trigger an exception in
     * <code>checkServerJdbcVersionX()</code>. <p>Upgrade:
     * <code>SYSIBM.SQLFunctionParams</code> is added in
     * <code>DataDictionaryImpl.create_10_2_system_procedures
     * (TransactionController,UUID)</code> so it will become available
     * in newly created databases and after <b>hard</b> upgrade.
     *
     * @param catalog limit search to this catalog
     * @param schemaPattern limit search to schemas matching this pattern
     * @param functionNamePattern limit search to functions matching this 
     * pattern
     * @return a <code>ResultSet</code> listing the fucntions
     * @exception SqlException if a database error occurs
     * @see #getFunctionParametersX(String, String, String,String)
     * @see org.apache.derby.impl.sql.catalog.DataDictionaryImpl#create_10_2_system_procedures(TransactionController,UUID)
     * @see org.apache.derby.impl.jdbc.EmbedDatabaseMetaData#getFunctions(String,String,String)
     */
    public java.sql.ResultSet 
        getFunctionParameters(String catalog,
                              String schemaPattern,
                              String functionNamePattern,
                              String parameterNamePattern) 
        throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.
                        traceEntry(this, 
                                   "getFunctionParameters", 
                                   catalog, schemaPattern, 
                                   functionNamePattern, parameterNamePattern);
                }
                return getFunctionParametersX(catalog, schemaPattern, 
                                              functionNamePattern, 
                                              parameterNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    /** 
     * Untraced version of <code>getFunctionParameters(String, String,
     * String, String)</code>.
     * @param catalog limit search to this catalog
     * @param schemaPattern limit search to schemas matching this pattern
     * @param functionNamePattern limit search to functions matching this 
     * pattern
     * @param parameterNamePattern limit search to parameters mathing
     * this pattern
     * @return a <code>ResultSet</code> listing the fucntions
     * @exception SqlException if a database error occurs
     * @see #getFunctionParameters(String, String, String, String)
     */
    private ResultSet getFunctionParametersX(String catalog,
                                             String schemaPattern,
                                             String functionNamePattern,
                                             String parameterNamePattern) 
        throws SqlException {
        checkForClosedConnectionX();
        checkServerJdbcVersionX("getFunctionParameters"+
                                "(String,String,String,String)", 4, 0);
 
        PreparedStatement cs = 
            prepareMetaDataQuery("SYSIBM.SQLFUNCTIONPARAMS(?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, functionNamePattern);
        cs.setStringX(4, parameterNamePattern);
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLTables
    // SYSIBM.SQLTables(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              TaleType    varchar(4000),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getTables(String catalog,
                                        String schemaPattern,
                                        String tableNamePattern,
                                        String types[]) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getTables", catalog, schemaPattern, tableNamePattern, types);
                }
                return getTablesX(catalog, schemaPattern, tableNamePattern, types);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getTablesX(String catalog,
                                 String schemaPattern,
                                 String tableNamePattern,
                                 String types[]) throws SqlException {
        try {
            checkForClosedConnection();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLTABLES(?,?,?,?,?)");

        if (catalog == null) {
            cs.setNullX(1, java.sql.Types.VARCHAR);
        } else {
            cs.setStringX(1, catalog);
        }

        if (schemaPattern == null) {
            cs.setNullX(2, java.sql.Types.VARCHAR);
        } else {
            cs.setStringX(2, schemaPattern);
        }

        if (tableNamePattern == null) {
            cs.setNullX(3, java.sql.Types.VARCHAR);
        } else {
            cs.setStringX(3, tableNamePattern);
        }

        String tableTypes = new String();
        int i = 0;
        if (types == null) {
            cs.setNullX(4, java.sql.Types.VARCHAR);
        } else if (types.length == 1 && (types[0].trim()).equals("%")) {
            cs.setStringX(4, types[0]);
        } else {
            while (i < types.length) {
                if (i > 0) {
                    tableTypes = tableTypes.concat(",");
                }
                tableTypes = tableTypes.concat("'" + types[i] + "'");
                i++;
            }
            cs.setStringX(4, tableTypes);
        }
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLTables
    // SYSIBM.SQLTables(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              TaleType    varchar(4000),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getSchemas() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getSchemas");
                }
                return getSchemasX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getSchemasX() throws SqlException {
        try {
            checkForClosedConnection();
        } catch ( SQLException se ) {
            throw new SqlException(se);
        }
        
        ;

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLTABLES('', '', '', '', 'GETSCHEMAS=1')");
        return (ResultSet) cs.executeQueryX();
    }


    // DERBY does not have the notion of a catalog, so we return a result set with no rows.
    public java.sql.ResultSet getCatalogs() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getCatalogs");
                }
                return getCatalogsX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getCatalogsX() throws SqlException {
        checkForClosedConnectionX();

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLTABLES('', '', '', '', 'GETCATALOGS=1')");
        return (ResultSet) cs.executeQueryX();
    }

    // call stored procedure SQLTables
    // SYSIBM.SQLTables(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              TableType   varchar(4000),
    //              Options     varchar(4000))
    public java.sql.ResultSet getTableTypes() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getTableTypes");
                }
                return getTableTypesX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getTableTypesX() throws SqlException {
        checkForClosedConnectionX();
        ;

        PreparedStatement cs = null;
        cs = prepareMetaDataQuery("SYSIBM.SQLTABLES(?,?,?,?,?)");

        cs.setStringX(1, "");
        cs.setStringX(2, "");
        cs.setStringX(3, "");
        cs.setStringX(4, "%");
        int cursorHold;
        if (connection_.holdability() == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {
            cursorHold = 1;
        } else {
            cursorHold = 0;
        }
        cs.setStringX(5, "DATATYPE='JDBC';GETTABLETYPES=1; CURSORHOLD=" + cursorHold);
        return executeCatalogQuery(cs);
    }


    // call stored procedure SQLColumns
    // SYSIBM.SQLColumns(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              ColumnName  varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getColumns(String catalog,
                                         String schemaPattern,
                                         String tableNamePattern,
                                         String columnNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getColumns", catalog, schemaPattern, tableNamePattern, columnNamePattern);
                }
                checkForClosedConnection();
                return getColumnsX(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getColumnsX(String catalog,
                                  String schemaPattern,
                                  String tableNamePattern,
                                  String columnNamePattern) throws SqlException {
        checkForClosedConnectionX();

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLCOLUMNS(?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, tableNamePattern);
        cs.setStringX(4, columnNamePattern); //Always null  for JDBC
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }


    // call stored procedure SQLColumnPrivileges
    // SYSIBM.SQLColPrivileges(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              ColumnName  varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getColumnPrivileges(String catalog,
                                                  String schema,
                                                  String table,
                                                  String columnNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getColumnPrivileges", catalog, schema, table, columnNamePattern);
                }
                return getColumnPrivilegesX(catalog, schema, table, columnNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getColumnPrivilegesX(String catalog,
                                           String schema,
                                           String table,
                                           String columnNamePattern) throws SqlException {
        checkForClosedConnectionX();
        // check input params, table and columnNamePattern cannot be null
        if (table == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.TABLE_NAME_CANNOT_BE_NULL)); 

        }

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLCOLPRIVILEGES(?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schema);
        cs.setStringX(3, table);
        cs.setStringX(4, columnNamePattern);
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }


    // call stored procedure SQLTablePrivileges
    // SYSIBM.SQLTablePrivileges(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getTablePrivileges(String catalog,
                                                 String schemaPattern,
                                                 String tableNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getTablePrivileges", catalog, schemaPattern, tableNamePattern);
                }
                return getTablePrivilegesX(catalog, schemaPattern, tableNamePattern);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getTablePrivilegesX(String catalog,
                                          String schemaPattern,
                                          String tableNamePattern) throws SqlException {
        checkForClosedConnectionX();
        ;

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLTABLEPRIVILEGES(?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, tableNamePattern);
        cs.setStringX(4, getOptions());
        return executeCatalogQuery(cs);
    }


    // call stored procedure
    // SYSIBM.SQLSPECIALCOLUMNS ( IN COLTYPE SMALLINT,
    //                            IN CATALOG_NAME VARCHAR(128),
    //                            IN SCHEMA_NAME  VARCHAR(128),
    //                            IN TABLE_NAME   VARCHAR(128),
    //                            IN SCOPE        SMALLINT,
    //                            IN NULLABLE     SMALLINT,
    //                            IN OPTIONS      VARCHAR(4000) )
    //
    public java.sql.ResultSet getBestRowIdentifier(String catalog,
                                                   String schema,
                                                   String table,
                                                   int scope,
                                                   boolean nullable) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getBestRowIdentifier", catalog, schema, table, scope, nullable);
                }
                return getBestRowIdentifierX(catalog, schema, table, scope, nullable);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getBestRowIdentifierX(String catalog,
                                            String schema,
                                            String table,
                                            int scope,
                                            boolean nullable) throws SqlException {
        checkForClosedConnectionX();
        ;

        // check input params
        //
        // validate input table, which can not be null
        if (table == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.TABLE_NAME_CANNOT_BE_NULL)); 

        }
        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLSPECIALCOLUMNS(?,?,?,?,?,?,?)");

        cs.setIntX(1, SQL_BEST_ROWID);
        cs.setStringX(2, catalog);
        cs.setStringX(3, schema);
        cs.setStringX(4, table);
        cs.setIntX(5, scope);
        if (nullable) {
            cs.setShortX(6, (short) 1);
        } else {
            cs.setShortX(6, (short) 0);
        }
        cs.setStringX(7, getOptions());
        return executeCatalogQuery(cs);
    }


    public java.sql.ResultSet getVersionColumns(String catalog,
                                                String schema,
                                                String table) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getVersionColumns", catalog, schema, table);
                }
                return getVersionColumnsX(catalog, schema, table);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getVersionColumnsX(String catalog,
                                         String schema,
                                         String table) throws SqlException {
        checkForClosedConnectionX();

        // validate input table, which can not be null
        if (table == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.TABLE_NAME_CANNOT_BE_NULL)); 

        }
        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLSPECIALCOLUMNS(?,?,?,?,?,?,?)");

        cs.setIntX(1, SQL_ROWVER);
        cs.setStringX(2, catalog);
        cs.setStringX(3, schema);
        cs.setStringX(4, table);
        cs.setIntX(5, 0);
        cs.setShortX(6, (short) 0);
        cs.setStringX(7, getOptions());

        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLPrimaryKeys
    // SYSIBM.SQLPrimaryKeys(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getPrimaryKeys(String catalog,
                                             String schema,
                                             String table) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getPrimaryKeys", catalog, schema, table);
                }
                return getPrimaryKeysX(catalog, schema, table);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getPrimaryKeysX(String catalog,
                                      String schema,
                                      String table) throws SqlException {
        checkForClosedConnectionX();
        ;

        // validate the input table name
        if (table == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.TABLE_NAME_CANNOT_BE_NULL)); 

        }
        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLPRIMARYKEYS(?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schema);
        cs.setStringX(3, table);
        cs.setStringX(4, getOptions());
        return executeCatalogQuery(cs);
    }


    // call storlastGetPrimaryKeysResultSet_ed procedure SQLForeignKeys
    // SYSIBM.SQLForeignKeys(
    //              PKCatalogName varchar(128),
    //              PKSchemaName  varchar(128),
    //              PKTableName   varchar(128),
    //              FKCatalogName varchar(128),
    //              FKSchemaName  varchar(128),
    //              FKTableName   varchar(128),
    //              Options       varchar(4000))
    //
    public java.sql.ResultSet getImportedKeys(String catalog,
                                              String schema,
                                              String table) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getImportedKeys", catalog, schema, table);
                }
                return getImportedKeysX(catalog, schema, table);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getImportedKeysX(String catalog,
                                       String schema,
                                       String table) throws SqlException {
        checkForClosedConnectionX();

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)");

        cs.setStringX(1, "");
        cs.setStringX(2, null);
        cs.setStringX(3, "");
        cs.setStringX(4, catalog);
        cs.setStringX(5, schema);
        cs.setStringX(6, table);
        // We're passing the keyword EXPORTEDKEY, but this support may not be in the GA version of SPs.
        // As a workaround in getCrossReference(), we'll just "select * where 0=1" when primaryTable==""
        if (connection_.holdability() == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {
            cs.setStringX(7, "DATATYPE='JDBC';IMPORTEDKEY=1; CURSORHOLD=1");
        } else {
            cs.setStringX(7, "DATATYPE='JDBC';IMPORTEDKEY=1; CURSORHOLD=0");
        }
        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLForeignKeys
    // SYSIBM.SQLForeignKeys(
    //              PKCatalogName varchar(128),
    //              PKSchemaName  varchar(128),
    //              PKTableName   varchar(128),
    //              FKCatalogName varchar(128),
    //              FKSchemaName  varchar(128),
    //              FKTableName   varchar(128),
    //              Options       varchar(4000))
    //
    public java.sql.ResultSet getExportedKeys(String catalog,
                                              String schema,
                                              String table) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getExportedKeys", catalog, schema, table);
                }
                return getExportedKeysX(catalog, schema, table);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getExportedKeysX(String catalog,
                                       String schema,
                                       String table) throws SqlException {
        checkForClosedConnectionX();
        ;

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schema);
        cs.setStringX(3, table);
        cs.setStringX(4, "");
        cs.setStringX(5, null);
        cs.setStringX(6, "");
        // We're passing the keyword EXPORTEDKEY, but this support may not be in the GA version of SPs.
        // As a workaround in getCrossReference(), we'll just "select * where 0=1" when foreignTable==""
        if (connection_.holdability() == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {
            cs.setStringX(7, "DATATYPE='JDBC';EXPORTEDKEY=1; CURSORHOLD=1");
        } else {
            cs.setStringX(7, "DATATYPE='JDBC';EXPORTEDKEY=1; CURSORHOLD=0");
        }
        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLForeignKeys
    // SYSIBM.SQLForeignKeys(
    //              PKCatalogName varchar(128),
    //              PKSchemaName  varchar(128),
    //              PKTableName   varchar(128),
    //              FKCatalogName varchar(128),
    //              FKSchemaName  varchar(128),
    //              FKTableName   varchar(128),
    //              Options       varchar(4000))
    //
    public java.sql.ResultSet getCrossReference(String primaryCatalog,
                                                String primarySchema,
                                                String primaryTable,
                                                String foreignCatalog,
                                                String foreignSchema,
                                                String foreignTable) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getCrossReference", primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable);
                }
                return getCrossReferenceX(primaryCatalog, primarySchema, primaryTable,
                        foreignCatalog, foreignSchema, foreignTable);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }


    private ResultSet getCrossReferenceX(String primaryCatalog,
                                         String primarySchema,
                                         String primaryTable,
                                         String foreignCatalog,
                                         String foreignSchema,
                                         String foreignTable) throws SqlException {
        checkForClosedConnectionX();
        ;

        // check input params, primaryTable and foreignTable cannot be null
        if (primaryTable == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.PRIMARY_TABLE_NAME_IS_NULL)); 

        }

        if (foreignTable == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.FOREIGN_TABLE_NAME_IS_NULL)); 

        }

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLFOREIGNKEYS(?,?,?,?,?,?,?)");

        cs.setStringX(1, primaryCatalog);
        cs.setStringX(2, primarySchema);
        cs.setStringX(3, primaryTable);
        cs.setStringX(4, foreignCatalog);
        cs.setStringX(5, foreignSchema);
        cs.setStringX(6, foreignTable);
        cs.setStringX(7, getOptions());
        return executeCatalogQuery(cs);
    }

    // call stored procedure SQLGetTypeInfo
    // SYSIBM.SQLGetTypeInfo (IN DATATYPE SMALLINT,
    //                        IN Options VARCHAR(4000))
    //
    //
    public java.sql.ResultSet getTypeInfo() throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getTypeInfo");
                }
                return getTypeInfoX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getTypeInfoX() throws SqlException {
        checkForClosedConnectionX();
        ;

        // check if the last call's resultset is closed or not.
        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLGETTYPEINFO(?,?)");

        cs.setShortX(1, (short) 0);
        cs.setStringX(2, getOptions());
        return executeCatalogQuery(cs);
    }


    // call stored procedure SQLStatistics
    // SYSIBM.SQLStatistics(
    //              CatalogName varchar(128),
    //              SchemaName  varchar(128),
    //              TableName   varchar(128),
    //              Unique      Smallint,
    //              Reserved    Smallint,
    //              Options     varchar(4000))
    //
    public java.sql.ResultSet getIndexInfo(String catalog,
                                           String schema,
                                           String table,
                                           boolean unique,
                                           boolean approximate) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getIndexInfo", catalog, schema, table, unique, approximate);
                }
                return getIndexInfoX(catalog, schema, table, unique, approximate);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getIndexInfoX(String catalog,
                                    String schema,
                                    String table,
                                    boolean unique,
                                    boolean approximate) throws SqlException {
        checkForClosedConnectionX();

        // validate the input table name
        if (table == null) {
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.TABLE_NAME_CANNOT_BE_NULL)); 
        }
        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLSTATISTICS(?,?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schema);
        cs.setStringX(3, table);

        if (unique) {
            cs.setShortX(4, SQL_INDEX_UNIQUE);
        } else {
            cs.setShortX(4, SQL_INDEX_ALL);
        }

        if (approximate) {
            cs.setShortX(5, (short) 1);
        } else {
            cs.setShortX(5, (short) 0);
        }

        cs.setStringX(6, getOptions());
        return executeCatalogQuery(cs);
    }


    //--------------------------JDBC 2.0-----------------------------

    public java.sql.ResultSet getUDTs(String catalog,
                                      String schemaPattern,
                                      String typeNamePattern,
                                      int[] types) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getUDTs", catalog, schemaPattern, typeNamePattern, types);
                }
                return getUDTsX(catalog, schemaPattern, typeNamePattern, types);
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getUDTsX(String catalog,
                               String schemaPattern,
                               String typeNamePattern,
                               int[] types) throws SqlException {
        checkForClosedConnectionX();
        ;

        PreparedStatement cs = prepareMetaDataQuery("SYSIBM.SQLUDTS(?,?,?,?,?)");

        cs.setStringX(1, catalog);
        cs.setStringX(2, schemaPattern);
        cs.setStringX(3, typeNamePattern);
        int i = 0;
        String udtTypes = new String();
        while (types != null && i < types.length) {
            if (i > 0) {
                udtTypes = udtTypes.concat(",");
            }
            udtTypes = udtTypes.concat(String.valueOf(types[i]));
            i++;
        }
        cs.setStringX(4, udtTypes);
        cs.setStringX(5, getOptions());
        return executeCatalogQuery(cs);
    }


    // helper method for the catalog queries only
    private String getOptions() {
        int cursorHold;
        if (connection_.holdability() == JDBC30Translation.HOLD_CURSORS_OVER_COMMIT) {
            cursorHold = 1;
        } else {
            cursorHold = 0;
        }
        return "DATATYPE='JDBC';DYNAMIC=0;REPORTPUBLICPRIVILEGES=1;CURSORHOLD=" + cursorHold;

    }

    // Derby uses a PreparedStatement argument rather than a callable statement
    private ResultSet executeCatalogQuery(PreparedStatement cs) 
        throws SqlException {
        try {
            return cs.executeQueryX();
        } catch (SqlException e) {
            if (e.getErrorCode() == -440) {
                SqlException newException = new SqlException(agent_.logWriter_,
                        new ClientMessageId(SQLState.STORED_PROC_NOT_INSTALLED));
                newException.setNextException(e);
                throw newException;
            } else if (e.getErrorCode() == -444) {
                SqlException newException = new SqlException(agent_.logWriter_,
                    new ClientMessageId(SQLState.STORED_PROC_LOAD_MODULE_NOT_FOUND));
                newException.setNextException(e);
                throw newException;
            } else {
                throw e;
            }
        }
    }

    public java.sql.Connection getConnection() throws SQLException {
        checkForClosedConnection();
        return connection_;
    }

    // ------------------- JDBC 3.0 -------------------------

    public boolean supportsNamedParameters() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    public boolean supportsMultipleOpenResults() throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public boolean supportsGetGeneratedKeys() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    public java.sql.ResultSet getSuperTypes(String catalog,
                                            String schemaPattern,
                                            String typeNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getSuperTypes", catalog, schemaPattern, typeNamePattern);
                }
                return getSuperTypesX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getSuperTypesX() throws SqlException {
        checkForClosedConnectionX();
        String sql = "SELECT CAST(NULL AS VARCHAR(128)) AS TYPE_CAT," +
                "CAST(NULL AS VARCHAR(128)) AS TYPE_SCHEM," +
                "VARCHAR('', 128) AS TYPE_NAME," +
                "CAST(NULL AS VARCHAR(128)) AS SUPERTYPE_CAT," +
                "CAST(NULL AS VARCHAR(128)) AS SUPERTYPE_SCHEM," +
                "VARCHAR('', 128) AS SUPERTYPE_NAME " +
                "FROM SYSIBM.SYSDUMMY1 WHERE 1=0 WITH UR ";
        PreparedStatement ps = connection_.prepareDynamicCatalogQuery(sql);
        return ps.executeQueryX();
    }

    public java.sql.ResultSet getSuperTables(String catalog,
                                             String schemaPattern,
                                             String tableNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getSuperTables", catalog, schemaPattern, tableNamePattern);
                }
                return getSuperTablesX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getSuperTablesX() throws SqlException {
        checkForClosedConnectionX();
        java.lang.String sql = "SELECT CAST(NULL AS VARCHAR(128)) AS TABLE_CAT," +
                "CAST(NULL AS VARCHAR(128)) AS TABLE_SCHEM," +
                "VARCHAR('', 128) AS TABLE_NAME," +
                "VARCHAR('', 128) AS SUPERTABLE_NAME FROM SYSIBM.SYSDUMMY1 " +
                "WHERE 1=0 WITH UR";
        PreparedStatement ps = connection_.prepareDynamicCatalogQuery(sql);
        return ps.executeQueryX();
    }


    public java.sql.ResultSet getAttributes(String catalog,
                                            String schemaPattern,
                                            String typeNamePattern,
                                            String attributeNamePattern) throws SQLException {
        try
        {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getAttributes", catalog, schemaPattern, typeNamePattern, attributeNamePattern);
                }
                return getAttributesX();
            }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private ResultSet getAttributesX() throws SqlException {
        checkForClosedConnectionX();
        java.lang.String sql = "SELECT CAST(NULL AS VARCHAR(128)) AS TYPE_CAT," +
                "CAST(NULL AS VARCHAR(128)) AS TYPE_SCHEM," +
                "VARCHAR('', 128) AS TYPE_NAME," +
                "VARCHAR('',128) AS ATTR_NAME," +
                "SMALLINT(0) AS DATA_TYPE," +
                "VARCHAR('',129) AS ATTR_TYPE_NAME," +
                "0 AS ATTR_SIZE," +
                "0 AS DECIMAL_DIGITS," +
                "0 AS NUM_PREC_RADIX," +
                "2 AS NULLABLE," +
                "CAST(NULL AS VARCHAR(254)) AS REMARKS," +
                "CAST(NULL AS VARCHAR(128)) AS ATTR_DEF," +
                "0 AS SQL_DATA_TYPE," +
                "0 AS SQL_DATETIME_SUB," +
                "0 AS CHAR_OCTET_LENGTH," +
                "0 AS ORDINAL_POSITION," +
                "VARCHAR('',128) AS IS_NULLABLE," +
                "CAST(NULL AS VARCHAR(128)) AS SCOPE_CATALOG," +
                "CAST(NULL AS VARCHAR(128)) AS SCOPE_SCHEMA," +
                "CAST(NULL AS VARCHAR(128)) AS SCOPE_TABLE," +
                "CAST(NULL AS SMALLINT) AS SOURCE_DATA_TYPE " +
                "FROM SYSIBM.SYSDUMMY1 WHERE 1=0 WITH UR";
        PreparedStatement ps = connection_.prepareDynamicCatalogQuery(sql);
        return ps.executeQueryX();
    }

    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        checkForClosedConnection();
        return true;
    }

    public int getResultSetHoldability() throws SQLException {
        checkForClosedConnection();
        return java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    public int getDatabaseMajorVersion() throws SQLException {
        checkForClosedConnection();
        return productLevel_.versionLevel_;
    }

    public int getDatabaseMinorVersion() throws SQLException {
        checkForClosedConnection();
        return productLevel_.releaseLevel_;
    }

    public int getJDBCMajorVersion() throws SQLException {
        checkForClosedConnection();
        return 3;
    }

    public int getJDBCMinorVersion() throws SQLException {
        checkForClosedConnection();
        return 0;
    }

    public int getSQLStateType() throws SQLException {
        checkForClosedConnection();
        return sqlStateSQL99;
    }

    public boolean locatorsUpdateCopy() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    public boolean supportsStatementPooling() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    //--------------------Abstract material layer call-down methods-----------------

    // Compute feature set based on release
    abstract protected void computeFeatureSet_();

    //------------helper methods for meta data info call methods------------------


    private boolean getMetaDataInfoBoolean(int infoCallIndex) throws SQLException {
        try
        {
            if ( !metaDataInfoIsCached_) { metaDataInfoCall(); }

            // account for fact that after 10.2 booleans are really booleans, not ints.
            Object	metadataScrap = metaDataInfoCache_[infoCallIndex];

            if ( metadataScrap instanceof Boolean ) { return ((Boolean) metadataScrap).booleanValue(); }
            else { return ((Integer) metadataScrap).intValue() != 0; }
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

	private int getMetaDataInfoInt(int infoCallIndex) throws SQLException {
        try
        {
            if (metaDataInfoIsCached_) {
                return ((Integer) metaDataInfoCache_[infoCallIndex]).intValue();
            }
            metaDataInfoCall();
            return ((Integer) metaDataInfoCache_[infoCallIndex]).intValue();
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
            
    }

    private String getMetaDataInfoString(int infoCallIndex) throws SQLException {
        try
        {
            if (metaDataInfoIsCached_) {
                return (String) metaDataInfoCache_[infoCallIndex];
            }
            metaDataInfoCall();
            return (String) metaDataInfoCache_[infoCallIndex];
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean getMetaDataInfoBooleanWithType(int infoCallIndex, int type) throws SQLException {
        // Stored Procedure will return a String containing a
        // comma seperated list of all the supported result Set types
        // not throwing any exception right now even if the the type is wrong as per the spec
        try
        {
            String returnedFromSP = null;
            if (metaDataInfoIsCached_) {
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            } else {
                metaDataInfoCall();
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            }
            java.util.StringTokenizer st = new java.util.StringTokenizer(returnedFromSP, ",");
            while (st.hasMoreTokens()) {
                if ((new Integer(st.nextToken())).intValue() == type) {
                    return true;
                }
            }
            return false;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    private boolean getMetaDataInfoInt_SupportsResultSetConcurrency(int infoCallIndex, int type, int concurrency) throws SQLException {
        // The stored procured will return a String containing a list
        // of lists: For each result set type in the outer list, an
        // inner list gives the allowed concurrencies for that type:
	// The encoding syntax is reproduced here from the server file
	// 'metadata_net.properties (please keep in synch!):  
	//
        // String syntax:  
	// <type> { "," <concurrency>}* { ";" <type> { "," <concurrency>}* }}*
	//
	// <type> ::= <the integer value for that type from interface java.sql.Resultset
	//             i.e. TYPE_FORWARD_ONLY is 1003>
	// <concurrency> ::= <the integer value for that concurrency
	//                    from interface java.sql.Resultset, i.e.
	//                    CONCUR_UPDATABLE is 1008>
        try
        {
            String returnedFromSP = null;
            if (metaDataInfoIsCached_) {
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            } else {
                metaDataInfoCall();
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            }
            java.util.StringTokenizer st = new java.util.StringTokenizer(returnedFromSP, ";");
            while (st.hasMoreTokens()) {
                java.util.StringTokenizer stForConc = 
		    new java.util.StringTokenizer(st.nextToken(), ",");
                if ((new Integer(stForConc.nextToken())).intValue() == type) {
                    while (stForConc.hasMoreTokens()) {
                        if ((new Integer(stForConc.nextToken())).intValue() == concurrency) {
                            return true;
                        }
                    }
                    return false;
                }
            }
            return false;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }            
    }

    private boolean getMetaDataInfoBoolean_supportsConvert(int infoCallIndex, int fromType, int toType) throws SQLException {
        // The Stored procedure will return a String contain a list of all the valid conversions it support
        // For eg. If the database conversion from char(1) to date(91), time(92) and
        // Decimal(3) to char(1) ,double(8)
        // then StoredProcedure string will return "1,91,92;3,1,8"
        // see how fromTypes are seperated by ";"
        try
        {
            String returnedFromSP = null;
            if (metaDataInfoIsCached_) {
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            } else {
                metaDataInfoCall();
                returnedFromSP = (String) metaDataInfoCache_[infoCallIndex];
            }
            java.util.StringTokenizer st = new java.util.StringTokenizer(returnedFromSP, ";");
            while (st.hasMoreTokens()) {
                java.util.StringTokenizer stForType = new java.util.StringTokenizer(st.nextToken(), ",");
                if ((new Integer(stForType.nextToken())).intValue() == fromType) {
                    while (st.hasMoreTokens()) {
                        if ((new Integer(st.nextToken())).intValue() == toType) {
                            return true;
                        }
                    }
                    return false;
                }
            }
            return false;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    // We synchronize at this level so that we don't have to synchronize all
    // the meta data info methods.  If we just return hardwired answers we don't
    // need to synchronize at the higher level.
    private void metaDataInfoCall() throws SqlException {
        synchronized (connection_) {
            ResultSet rs;

            // These remote calls return a result set containing a single row.
            // Each column in the row corresponds to a particular get meta data info
            // method.
            PreparedStatement ps = prepareMetaDataQuery("SYSIBM.MetaData()");
            rs = (ResultSet) ps.executeQueryX();
            rs.nextX();
            int ColumnCount;
            try {
                ColumnCount = ((ColumnMetaData) rs.getMetaDataX()).getColumnCount();
            } catch ( SQLException se ) {
                throw new SqlException(se);
            }
            for (int infoCallIndex = 0;
                 (infoCallIndex < ColumnCount && infoCallIndex < metaDataInfoCache_.length);
                 infoCallIndex++) {
                metaDataInfoCache_[infoCallIndex] = rs.getObjectX(infoCallIndex + 1);
            }
            metaDataInfoIsCached_ = true;
            rs.closeX();
        }
    }

    // ------------------- JDBC 4.0 -------------------------

    /**
     * Retrieves whether this database supports invoking user-defined
     * or vendor functions using the stored procedure escape syntax.
     *
     * @return <code>true</code>, since Derby supports the escape syntax
     * @exception SQLException if a database access error occurs
     */
    public final boolean supportsStoredFunctionsUsingCallSyntax()
        throws SQLException
    {
        checkForClosedConnection();
        return true;
    }

    /**
     * Retrieves whether an <code>SQLException</code> will cause all
     * open <code>ResultSet</code>s to be closed when auto-commit is
     * <code>true</code>.
     *
     * @return <code>false</code>, since Derby does not close all open
     * result sets when an error occurs
     * @exception SQLException if a database access error occurs
     */
    public final boolean autoCommitFailureClosesAllResultSets()
        throws SQLException
    {
        checkForClosedConnection();
        return false;
    }

    /**
     * Retrieves whether this JDBC driver provides its own
     * <code>QueryObjectGenerator</code>.
     *
     * @return <code>false</code>, since Derby does not provide its
     * own generator
     * @exception SQLException if a database access error occurs
     */
    public final boolean providesQueryObjectGenerator() throws SQLException {
        checkForClosedConnection();
        return false;
    }

    /**
     * Get the schema names available in this database. The results
     * are ordered by schema name.
     *
     * <p>The schema columns are:
     *  <ol>
     *  <li><strong>TABLE_SCHEM</strong> String =&gt; schema name</li>
     *  <li><strong>TABLE_CATALOG</strong> String =&gt; catalog name
     *  (may be <code>null</code>)</li>
     *  </ol>
     *
     * @param catalog catalog name used to narrow down the search; ""
     * means no catalog, <code>null</code> means any catalog
     * @param schemaPattern schema name used to narrow down the
     * search, <code>null</code> means schema name should not be used
     * to narrow down search
     * @return a <code>ResultSet</code> object in which each row is a
     * schema description
     * @exception SQLException if a database error occurs
     */
    public ResultSet getSchemas(String catalog, String schemaPattern)
        throws SQLException
    {
        try {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this, "getSchemas");
                }
                return getSchemasX(catalog, schemaPattern);
            }
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Untraced version of <code>getSchemas(String, String)</code>.
     *
     * @param catalog catalog name
     * @param schemaPattern pattern for schema name
     * @return a <code>ResultSet</code> value
     * @exception SqlException if a database error occurs
     * @see #getSchemas(String, String)
     */
    private ResultSet getSchemasX(String catalog, String schemaPattern)
        throws SqlException
    {
        checkForClosedConnectionX();

        // If the server has not implemented support for JDBC 4.0,
        // SYSIBM.SQLTABLES does not recognize the GETSCHEMAS=2
        // option, and it will call getTables() instead of
        // getSchemas(). Therefore, check server version and throw an
        // exception if the server does not support JDBC 4.0.
        checkServerJdbcVersionX("getSchemas(String, String)", 4, 0);

        String call = "SYSIBM.SQLTABLES(?, ?, '', '', 'GETSCHEMAS=2')";
        PreparedStatement cs = prepareMetaDataQuery(call);
        if (catalog == null) {
            cs.setNullX(1, java.sql.Types.VARCHAR);
        } else {
            cs.setStringX(1, catalog);
        }
        if (schemaPattern == null) {
            cs.setNullX(2, java.sql.Types.VARCHAR);
        } else {
            cs.setStringX(2, schemaPattern);
        }
        return cs.executeQueryX();
    }

    /**
     * Returns a list of the client info properties supported by the
     * driver. The result set contains the following columns:
     *
     * <p>
     * <ol>
     *  <li>NAME String=&gt; The name of the client info property.</li>
     *  <li>MAX_LEN int=&gt; The maximum length of the value for the
     *      property.</li>
     *  <li>DEFAULT_VALUE String=&gt; The default value of the property.</li>
     *  <li>DESCRIPTION String=&gt; A description of the property.</li>
     * </ol>
     *
     * <p>The <code>ResultSet</code> is sorted by the NAME column.
     *
     * @return A <code>ResultSet</code> object; each row is a
     * supported client info property
     * @exception SQLException if an error occurs
     */
    public ResultSet getClientInfoProperties() throws SQLException {
        try {
            synchronized (connection_) {
                if (agent_.loggingEnabled()) {
                    agent_.logWriter_.traceEntry(this,
                                                 "getClientInfoProperties");
                }
                return getClientInfoPropertiesX();
            }
        } catch (SqlException se) {
            throw se.getSQLException();
        }
    }

    /**
     * Untraced version of <code>getClientInfoProperties()</code>.
     * Returns an empty <code>ResultSet</code> with the correct column
     * names.
     *
     * @return a <code>ResultSet</code> value
     * @exception SqlException if a database error occurs
     * @see #getClientInfoProperties
     */
    private ResultSet getClientInfoPropertiesX() throws SqlException {
        checkForClosedConnectionX();
        final String sql =
            "SELECT CAST(NULL AS VARCHAR(128)) AS NAME, " +
            "CAST(NULL AS INT) AS MAX_LEN, " +
            "CAST(NULL AS VARCHAR(128)) AS DEFAULT_VALUE, " +
            "CAST(NULL AS VARCHAR(128)) AS DESCRIPTION " +
            "FROM SYSIBM.SYSDUMMY1 WHERE 1=0 WITH UR";
        PreparedStatement ps = connection_.prepareDynamicCatalogQuery(sql);
        return ps.executeQueryX();
    }

    //----------------------------helper methods----------------------------------


    private PreparedStatement prepareMetaDataQuery(String cmd) throws SqlException {
        PreparedStatement ps;

        ps = (org.apache.derby.client.am.PreparedStatement)
                connection_.prepareStatementX("CALL " + cmd,
                        java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_READ_ONLY,
                        connection_.holdability(),
                        java.sql.Statement.NO_GENERATED_KEYS,
                        null);
        return ps;
    }

    /** 
     * A "public" version of checkForClosedConnection() that throws
     * SQLException instead of SqlException.  In particular this is used
     * by all the DatabaseMetadata methods
     */
    protected void checkForClosedConnection() throws SQLException
    {
        try {
            checkForClosedConnectionX();
        } catch ( SqlException se ) {
            throw se.getSQLException();
        }
    }
    
    private void checkForClosedConnectionX() throws SqlException {
        if (connection_.isClosedX()) {
            agent_.checkForDeferredExceptions();
            throw new SqlException(agent_.logWriter_,
                new ClientMessageId(SQLState.NO_CURRENT_CONNECTION)); 

        } else {
            agent_.checkForDeferredExceptions();
        }
    }

    /**
     * Checks whether the server supports a JDBC version. If the
     * server does not support the JDBC version, an exception is
     * thrown.
     *
     * @param method name of the method for which support is needed on
     * the server (used in exception message)
     * @param major minimum JDBC major version
     * @param minor minimum JDBC minor version if major version matches
     * @exception SqlException if the server does not support the
     * specified JDBC version
     */
    protected void checkServerJdbcVersionX(String method, int major, int minor)
        throws SqlException
    {
        if (serverJdbcMajorVersion < major ||
            (serverJdbcMajorVersion == major &&
             serverJdbcMinorVersion < minor)) {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.JDBC_METHOD_NOT_SUPPORTED_BY_SERVER), method);
        }
    }
}
