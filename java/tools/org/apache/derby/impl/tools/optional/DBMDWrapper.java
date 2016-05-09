/*

   Derby - Class org.apache.derby.impl.tools.optional.DBMDWrapper

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

package org.apache.derby.impl.tools.optional;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.iapi.sql.dictionary.OptionalTool;

/**
 * <p>
 * OptionalTool to create wrapper functions which allow you to invoke DatabaseMetaData methods
 * via SQL. The wrapper functions slightly change the signature of the metadata
 * methods as follows:
 * </p>
 *
 * <ul>
 * <li>Arguments of type int[] and String[] have been eliminated--they are
 * automatically wildcarded.</li>
 * <li>The method getRowIdLifetime() has been commented out--Derby does not
 * support object types.</li>
 * <li>The method getSchemas() has been
 * commented out--it can be uncommented when the registration logic is made
 * smarter to handle the dropping of different overloads.</li>
 * <li>The method supportsConvert() has been
 * commented out because Derby only allows one function by a given name and
 * the supportsConvert( int, int ) overload is more general.</li>
 * </ul>
 *
 * <p>
 * Methods which return ResultSet are mapped to table functions. You can join
 * the metadata table functions like this:
 * </p>
 *
 * <pre>
 * -- list all of the columns in the connected Derby database
 * select t.table_schem, t.table_name, c.column_name, c.type_name
 * from table( getTables( null, null, null ) ) t,
 *         table( getColumns( null, null, null, null ) ) c
 * where c.table_schem = t.table_schem
 * and c.table_name = t.table_name
 * and t.table_type = 'TABLE'
 * ;
 * 
 * 
 * -- now list metadata in a foreign database
 * call setDatabaseURL( 'com.mysql.jdbc.Driver', 'jdbc:mysql://localhost/world?user=root&password=' );
 * 
 * select t.table_schem, t.table_name, c.column_name, c.type_name
 * from table( getTables( 'WORLD', null, null ) ) t,
 *         table( getColumns( 'WORLD', null, null, null) ) c
 * where c.table_name = t.table_name
 * and t.table_type = 'TABLE'
 * ;
 * 
 * -- release the foreign connection
 * call setDatabaseURL( '', '' );
 * </pre>
 */
public  class   DBMDWrapper implements OptionalTool
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   int DEFAULT_PRECISION = 128;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor to satisfy the OptionalTool contract */
    public  DBMDWrapper() {}
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        register( true );
    }

    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        register( false );
    }

    /**
     * <p>
     * Workhorse to register or unregister all public static methods as
     * Derby routines.
     * </p>
     */
    private  void    register( boolean register )
        throws SQLException
    {
        Connection  conn = getDefaultConnection();

        Method[]    methods = getClass().getDeclaredMethods();
        int             count = methods.length;

        for ( int midx = 0; midx < count; midx++ )
        {
            Method  method = methods[ midx ];
            int         modifiers = method.getModifiers();

            if (
                isSet( modifiers, Modifier.PUBLIC ) &&
                isSet( modifiers, Modifier.STATIC )
                )
            {
                if ( register ) { registerFunction( conn,  method ); }
                else { unregisterFunction( conn, method ); }
            }
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // WRAPPER FUNCTIONS WHICH ARE REGISTERED WITH DERBY
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  boolean 	allProceduresAreCallable() throws SQLException
    { return getDBMD().allProceduresAreCallable(); }
    
    public  static  boolean 	allTablesAreSelectable() throws SQLException
    { return getDBMD().allTablesAreSelectable(); }
    
    public  static  boolean 	autoCommitFailureClosesAllResultSets() throws SQLException
    { return getDBMD().autoCommitFailureClosesAllResultSets(); }
    
    public  static  boolean 	dataDefinitionCausesTransactionCommit() throws SQLException
    { return getDBMD().dataDefinitionCausesTransactionCommit(); }
    
    public  static  boolean 	dataDefinitionIgnoredInTransactions() throws SQLException
    { return getDBMD().dataDefinitionIgnoredInTransactions(); }
    
    public  static  boolean 	deletesAreDetected(int type) throws SQLException
    { return getDBMD().deletesAreDetected( type); }
    
    public  static  boolean 	doesMaxRowSizeIncludeBlobs() throws SQLException
    { return getDBMD().doesMaxRowSizeIncludeBlobs(); }
    
    public  static  ResultSet 	getAttributes( String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException
    { return getDBMD().getAttributes( catalog,  schemaPattern,  typeNamePattern,  attributeNamePattern); }
    

    public  static  ResultSet 	getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable ) throws SQLException
    { return getDBMD().getBestRowIdentifier( catalog,  schema,  table,  scope,  nullable ); }
    

    public  static  ResultSet 	getCatalogs() throws SQLException
    { return getDBMD().getCatalogs(); }
    
    public  static  String 	getCatalogSeparator() throws SQLException
    { return getDBMD().getCatalogSeparator(); }
    
    public  static  String 	getCatalogTerm() throws SQLException
    { return getDBMD().getCatalogTerm(); }
    
    public  static  ResultSet 	getClientInfoProperties() throws SQLException
    { return getDBMD().getClientInfoProperties(); }
    
    public  static  ResultSet 	getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException
    { return getDBMD().getColumnPrivileges( catalog,  schema,  table,  columnNamePattern); }
    
    public  static  ResultSet 	getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException
    { return getDBMD().getColumns( catalog,  schemaPattern,  tableNamePattern,  columnNamePattern); }
    
    public  static  ResultSet 	getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException
    { return getDBMD().getCrossReference( parentCatalog,  parentSchema,  parentTable,  foreignCatalog,  foreignSchema,  foreignTable); }
    
    public  static  int 	getDatabaseMajorVersion() throws SQLException
    { return getDBMD().getDatabaseMajorVersion(); }
    
    public  static  int 	getDatabaseMinorVersion() throws SQLException
    { return getDBMD().getDatabaseMinorVersion(); }
    
    public  static  String 	getDatabaseProductName() throws SQLException
    { return getDBMD().getDatabaseProductName(); }
    
    public  static  String 	getDatabaseProductVersion() throws SQLException
    { return getDBMD().getDatabaseProductVersion(); }
    
    public  static  int 	getDefaultTransactionIsolation() throws SQLException
    { return getDBMD().getDefaultTransactionIsolation(); }
    
    public  static  int 	getDriverMajorVersion() throws SQLException
    { return getDBMD().getDriverMajorVersion(); }
    
    public  static  int 	getDriverMinorVersion() throws SQLException
    { return getDBMD().getDriverMinorVersion(); }
    
    public  static  String 	getDriverName() throws SQLException
    { return getDBMD().getDriverName(); }
    
    public  static  String 	getDriverVersion() throws SQLException
    { return getDBMD().getDriverVersion(); }
    
    public  static  ResultSet 	getExportedKeys(String catalog, String schema, String table) throws SQLException
    { return getDBMD().getExportedKeys( catalog,  schema,  table); }
    
    public  static  String 	getExtraNameCharacters() throws SQLException
    { return getDBMD().getExtraNameCharacters(); }
    
    public  static  ResultSet 	getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException
    { return getDBMD().getFunctionColumns( catalog,  schemaPattern,  functionNamePattern,  columnNamePattern); }
    
    public  static  ResultSet 	getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException
    { return getDBMD().getFunctions( catalog,  schemaPattern,  functionNamePattern); }
    
    public  static  String 	getIdentifierQuoteString() throws SQLException
    { return getDBMD().getIdentifierQuoteString(); }
    
    public  static  ResultSet 	getImportedKeys(String catalog, String schema, String table) throws SQLException
    { return getDBMD().getImportedKeys( catalog,  schema,  table); }
    

    public  static  ResultSet 	getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException
    { return getDBMD().getIndexInfo( catalog,  schema,  table,  unique,  approximate ); }
    

    public  static  int 	getJDBCMajorVersion() throws SQLException
    { return getDBMD().getJDBCMajorVersion(); }
    
    public  static  int 	getJDBCMinorVersion() throws SQLException
    { return getDBMD().getJDBCMinorVersion(); }
    
    public  static  int 	getMaxBinaryLiteralLength() throws SQLException
    { return getDBMD().getMaxBinaryLiteralLength(); }
    
    public  static  int 	getMaxCatalogNameLength() throws SQLException
    { return getDBMD().getMaxCatalogNameLength(); }
    
    public  static  int 	getMaxCharLiteralLength() throws SQLException
    { return getDBMD().getMaxCharLiteralLength(); }
    
    public  static  int 	getMaxColumnNameLength() throws SQLException
    { return getDBMD().getMaxColumnNameLength(); }
    
    public  static  int 	getMaxColumnsInGroupBy() throws SQLException
    { return getDBMD().getMaxColumnsInGroupBy(); }
    
    public  static  int 	getMaxColumnsInIndex() throws SQLException
    { return getDBMD().getMaxColumnsInIndex(); }
    
    public  static  int 	getMaxColumnsInOrderBy() throws SQLException
    { return getDBMD().getMaxColumnsInOrderBy(); }
    
    public  static  int 	getMaxColumnsInSelect() throws SQLException
    { return getDBMD().getMaxColumnsInSelect(); }
    
    public  static  int 	getMaxColumnsInTable() throws SQLException
    { return getDBMD().getMaxColumnsInTable(); }
    
    public  static  int 	getMaxConnections() throws SQLException
    { return getDBMD().getMaxConnections(); }
    
    public  static  int 	getMaxCursorNameLength() throws SQLException
    { return getDBMD().getMaxCursorNameLength(); }
    
    public  static  int 	getMaxIndexLength() throws SQLException
    { return getDBMD().getMaxIndexLength(); }
    
    public  static  int 	getMaxProcedureNameLength() throws SQLException
    { return getDBMD().getMaxProcedureNameLength(); }
    
    public  static  int 	getMaxRowSize() throws SQLException
    { return getDBMD().getMaxRowSize(); }
    
    public  static  int 	getMaxSchemaNameLength() throws SQLException
    { return getDBMD().getMaxSchemaNameLength(); }
    
    public  static  int 	getMaxStatementLength() throws SQLException
    { return getDBMD().getMaxStatementLength(); }
    
    public  static  int 	getMaxStatements() throws SQLException
    { return getDBMD().getMaxStatements(); }
    
    public  static  int 	getMaxTableNameLength() throws SQLException
    { return getDBMD().getMaxTableNameLength(); }
    
    public  static  int 	getMaxTablesInSelect() throws SQLException
    { return getDBMD().getMaxTablesInSelect(); }
    
    public  static  int 	getMaxUserNameLength() throws SQLException
    { return getDBMD().getMaxUserNameLength(); }
    
    public  static  String 	getNumericFunctions() throws SQLException
    { return getDBMD().getNumericFunctions(); }
    
    public  static  ResultSet 	getPrimaryKeys(String catalog, String schema, String table) throws SQLException
    { return getDBMD().getPrimaryKeys( catalog,  schema,  table); }
    
    public  static  ResultSet 	getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException
    { return getDBMD().getProcedureColumns( catalog,  schemaPattern,  procedureNamePattern,  columnNamePattern); }
    
    public  static  ResultSet 	getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException
    { return getDBMD().getProcedures( catalog,  schemaPattern,  procedureNamePattern); }
    
    public  static  String 	getProcedureTerm() throws SQLException
    { return getDBMD().getProcedureTerm(); }
    
    public  static  int 	getResultSetHoldability() throws SQLException
    { return getDBMD().getResultSetHoldability(); }

    // Comment out this method because we don't support this datatype
    //public  static  RowIdLifetime 	getRowIdLifetime() throws SQLException
    //{ return getDBMD().getRowIdLifetime(); }
    
    // Comment out this method so that we don't drop the following method
    //public  static  ResultSet 	getSchemas() throws SQLException
    //{ return getDBMD().getSchemas(); }

    public  static  ResultSet 	getSchemas(String catalog, String schemaPattern) throws SQLException
    { return getDBMD().getSchemas( catalog,  schemaPattern); }
    
    public  static  String 	getSchemaTerm() throws SQLException
    { return getDBMD().getSchemaTerm(); }
    
    public  static  String 	getSearchStringEscape() throws SQLException
    { return getDBMD().getSearchStringEscape(); }
    
    public  static  String 	getSQLKeywords() throws SQLException
    { return getDBMD().getSQLKeywords(); }
    
    public  static  int 	getSQLStateType() throws SQLException
    { return getDBMD().getSQLStateType(); }
    
    public  static  String 	getStringFunctions() throws SQLException
    { return getDBMD().getStringFunctions(); }
    
    public  static  ResultSet 	getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
    { return getDBMD().getSuperTables( catalog,  schemaPattern,  tableNamePattern); }
    
    public  static  ResultSet 	getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException
    { return getDBMD().getSuperTypes( catalog,  schemaPattern,  typeNamePattern); }
    
    public  static  String 	getSystemFunctions() throws SQLException
    { return getDBMD().getSystemFunctions(); }
    
    public  static  ResultSet 	getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException
    { return getDBMD().getTablePrivileges( catalog,  schemaPattern,  tableNamePattern); }

    // Needs to cast String[] to something else
    public  static  ResultSet 	getTables(String catalog, String schemaPattern, String tableNamePattern ) throws SQLException
    { return getDBMD().getTables( catalog,  schemaPattern,  tableNamePattern, (String[]) null ); }
    
    public  static  ResultSet 	getTableTypes() throws SQLException
    { return getDBMD().getTableTypes(); }
    
    public  static  String 	getTimeDateFunctions() throws SQLException
    { return getDBMD().getTimeDateFunctions(); }
    
    public  static  ResultSet 	getTypeInfo() throws SQLException
    { return getDBMD().getTypeInfo(); }
     
    // Eliminated the final "int[] types" argument
    public  static  ResultSet 	getUDTs(String catalog, String schemaPattern, String typeNamePattern) throws SQLException
    { return getDBMD().getUDTs( catalog,  schemaPattern,  typeNamePattern, (int[]) null ); }
    
    public  static  String 	getURL() throws SQLException
    { return getDBMD().getURL(); }
    
    public  static  String 	getUserName() throws SQLException
    { return getDBMD().getUserName(); }
    
    public  static  ResultSet 	getVersionColumns(String catalog, String schema, String table) throws SQLException
    { return getDBMD().getVersionColumns( catalog,  schema,  table); }
    
    public  static  boolean 	insertsAreDetected(int type) throws SQLException
    { return getDBMD().insertsAreDetected(type); }
    
    public  static  boolean 	isCatalogAtStart() throws SQLException
    { return getDBMD().isCatalogAtStart(); }
    
    public  static  boolean 	isReadOnly() throws SQLException
    { return getDBMD().isReadOnly(); }
    
    public  static  boolean 	locatorsUpdateCopy() throws SQLException
    { return getDBMD().locatorsUpdateCopy(); }
    
    public  static  boolean 	nullPlusNonNullIsNull() throws SQLException
    { return getDBMD().nullPlusNonNullIsNull(); }
    
    public  static  boolean 	nullsAreSortedAtEnd() throws SQLException
    { return getDBMD().nullsAreSortedAtEnd(); }
    
    public  static  boolean 	nullsAreSortedAtStart() throws SQLException
    { return getDBMD().nullsAreSortedAtStart(); }
    
    public  static  boolean 	nullsAreSortedHigh() throws SQLException
    { return getDBMD().nullsAreSortedHigh(); }
    
    public  static  boolean 	nullsAreSortedLow() throws SQLException
    { return getDBMD().nullsAreSortedLow(); }
    
    public  static  boolean 	othersDeletesAreVisible(int type) throws SQLException
    { return getDBMD().othersDeletesAreVisible( type); }
    
    public  static  boolean 	othersInsertsAreVisible(int type) throws SQLException
    { return getDBMD().othersInsertsAreVisible(type); }
    
    public  static  boolean 	othersUpdatesAreVisible(int type) throws SQLException
    { return getDBMD().othersUpdatesAreVisible( type); }
    
    public  static  boolean 	ownDeletesAreVisible(int type) throws SQLException
    { return getDBMD().ownDeletesAreVisible(type); }
    
    public  static  boolean 	ownInsertsAreVisible(int type) throws SQLException
    { return getDBMD().ownInsertsAreVisible(type); }
    
    public  static  boolean 	ownUpdatesAreVisible(int type) throws SQLException
    { return getDBMD().ownUpdatesAreVisible(type); }
    
    public  static  boolean 	storesLowerCaseIdentifiers() throws SQLException
    { return getDBMD().storesLowerCaseIdentifiers(); }
    
    public  static  boolean 	storesLowerCaseQuotedIdentifiers() throws SQLException
    { return getDBMD().storesLowerCaseQuotedIdentifiers(); }
    
    public  static  boolean 	storesMixedCaseIdentifiers() throws SQLException
    { return getDBMD().storesMixedCaseIdentifiers(); }
    
    public  static  boolean 	storesMixedCaseQuotedIdentifiers() throws SQLException
    { return getDBMD().storesMixedCaseQuotedIdentifiers(); }
    
    public  static  boolean 	storesUpperCaseIdentifiers() throws SQLException
    { return getDBMD().storesUpperCaseIdentifiers(); }
    
    public  static  boolean 	storesUpperCaseQuotedIdentifiers() throws SQLException
    { return getDBMD().storesUpperCaseQuotedIdentifiers(); }
    
    public  static  boolean 	supportsAlterTableWithAddColumn() throws SQLException
    { return getDBMD().supportsAlterTableWithAddColumn(); }
    
    public  static  boolean 	supportsAlterTableWithDropColumn() throws SQLException
    { return getDBMD().supportsAlterTableWithDropColumn(); }
    
    public  static  boolean 	supportsANSI92EntryLevelSQL() throws SQLException
    { return getDBMD().supportsANSI92EntryLevelSQL(); }
    
    public  static  boolean 	supportsANSI92FullSQL() throws SQLException
    { return getDBMD().supportsANSI92FullSQL(); }
    
    public  static  boolean 	supportsANSI92IntermediateSQL() throws SQLException
    { return getDBMD().supportsANSI92IntermediateSQL(); }
    
    public  static  boolean 	supportsBatchUpdates() throws SQLException
    { return getDBMD().supportsBatchUpdates(); }
    
    public  static  boolean 	supportsCatalogsInDataManipulation() throws SQLException
    { return getDBMD().supportsCatalogsInDataManipulation(); }
    
    public  static  boolean 	supportsCatalogsInIndexDefinitions() throws SQLException
    { return getDBMD().supportsCatalogsInIndexDefinitions(); }
    
    public  static  boolean 	supportsCatalogsInPrivilegeDefinitions() throws SQLException
    { return getDBMD().supportsCatalogsInPrivilegeDefinitions(); }
    
    public  static  boolean 	supportsCatalogsInProcedureCalls() throws SQLException
    { return getDBMD().supportsCatalogsInProcedureCalls(); }
    
    public  static  boolean 	supportsCatalogsInTableDefinitions() throws SQLException
    { return getDBMD().supportsCatalogsInTableDefinitions(); }
    
    public  static  boolean 	supportsColumnAliasing() throws SQLException
    { return getDBMD().supportsColumnAliasing(); }

    // Comment this out in favor of the more general overload which follows.
    // Derby only allows one function by a given name in a given schema.
    //public  static  boolean 	supportsConvert() throws SQLException
    //{ return getDBMD().supportsConvert(); }
    
    public  static  boolean 	supportsConvert(int fromType, int toType) throws SQLException
    { return getDBMD().supportsConvert( fromType,  toType); }
    
    public  static  boolean 	supportsCoreSQLGrammar() throws SQLException
    { return getDBMD().supportsCoreSQLGrammar(); }
    
    public  static  boolean 	supportsCorrelatedSubqueries() throws SQLException
    { return getDBMD().supportsCorrelatedSubqueries(); }
    
    public  static  boolean 	supportsDataDefinitionAndDataManipulationTransactions() throws SQLException
    { return getDBMD().supportsDataDefinitionAndDataManipulationTransactions(); }
    
    public  static  boolean 	supportsDataManipulationTransactionsOnly() throws SQLException
    { return getDBMD().supportsDataManipulationTransactionsOnly(); }
    
    public  static  boolean 	supportsDifferentTableCorrelationNames() throws SQLException
    { return getDBMD().supportsDifferentTableCorrelationNames(); }
    
    public  static  boolean 	supportsExpressionsInOrderBy() throws SQLException
    { return getDBMD().supportsExpressionsInOrderBy(); }
    
    public  static  boolean 	supportsExtendedSQLGrammar() throws SQLException
    { return getDBMD().supportsExtendedSQLGrammar(); }
    
    public  static  boolean 	supportsFullOuterJoins() throws SQLException
    { return getDBMD().supportsFullOuterJoins(); }
    
    public  static  boolean 	supportsGetGeneratedKeys() throws SQLException
    { return getDBMD().supportsGetGeneratedKeys(); }
    
    public  static  boolean 	supportsGroupBy() throws SQLException
    { return getDBMD().supportsGroupBy(); }
    
    public  static  boolean 	supportsGroupByBeyondSelect() throws SQLException
    { return getDBMD().supportsGroupByBeyondSelect(); }
    
    public  static  boolean 	supportsGroupByUnrelated() throws SQLException
    { return getDBMD().supportsGroupByUnrelated(); }
    
    public  static  boolean 	supportsIntegrityEnhancementFacility() throws SQLException
    { return getDBMD().supportsIntegrityEnhancementFacility(); }
    
    public  static  boolean 	supportsLikeEscapeClause() throws SQLException
    { return getDBMD().supportsLikeEscapeClause(); }
    
    public  static  boolean 	supportsLimitedOuterJoins() throws SQLException
    { return getDBMD().supportsLimitedOuterJoins(); }
    
    public  static  boolean 	supportsMinimumSQLGrammar() throws SQLException
    { return getDBMD().supportsMinimumSQLGrammar(); }
    
    public  static  boolean 	supportsMixedCaseIdentifiers() throws SQLException
    { return getDBMD().supportsMixedCaseIdentifiers(); }
    
    public  static  boolean 	supportsMixedCaseQuotedIdentifiers() throws SQLException
    { return getDBMD().supportsMixedCaseQuotedIdentifiers(); }
    
    public  static  boolean 	supportsMultipleOpenResults() throws SQLException
    { return getDBMD().supportsMultipleOpenResults(); }
    
    public  static  boolean 	supportsMultipleResultSets() throws SQLException
    { return getDBMD().supportsMultipleResultSets(); }
    
    public  static  boolean 	supportsMultipleTransactions() throws SQLException
    { return getDBMD().supportsMultipleTransactions(); }
    
    public  static  boolean 	supportsNamedParameters() throws SQLException
    { return getDBMD().supportsNamedParameters(); }
    
    public  static  boolean 	supportsNonNullableColumns() throws SQLException
    { return getDBMD().supportsNonNullableColumns(); }
    
    public  static  boolean 	supportsOpenCursorsAcrossCommit() throws SQLException
    { return getDBMD().supportsOpenCursorsAcrossCommit(); }
    
    public  static  boolean 	supportsOpenCursorsAcrossRollback() throws SQLException
    { return getDBMD().supportsOpenCursorsAcrossRollback(); }
    
    public  static  boolean 	supportsOpenStatementsAcrossCommit() throws SQLException
    { return getDBMD().supportsOpenStatementsAcrossCommit(); }
    
    public  static  boolean 	supportsOpenStatementsAcrossRollback() throws SQLException
    { return getDBMD().supportsOpenStatementsAcrossRollback(); }
    
    public  static  boolean 	supportsOrderByUnrelated() throws SQLException
    { return getDBMD().supportsOrderByUnrelated(); }
    
    public  static  boolean 	supportsOuterJoins() throws SQLException
    { return getDBMD().supportsOuterJoins(); }
    
    public  static  boolean 	supportsPositionedDelete() throws SQLException
    { return getDBMD().supportsPositionedDelete(); }
    
    public  static  boolean 	supportsPositionedUpdate() throws SQLException
    { return getDBMD().supportsPositionedUpdate(); }
    
    public  static  boolean 	supportsResultSetConcurrency(int type, int concurrency) throws SQLException
    { return getDBMD().supportsResultSetConcurrency( type,  concurrency); }
    
    public  static  boolean 	supportsResultSetHoldability(int holdability) throws SQLException
    { return getDBMD().supportsResultSetHoldability( holdability); }
    
    public  static  boolean 	supportsResultSetType(int type) throws SQLException
    { return getDBMD().supportsResultSetType( type); }
    
    public  static  boolean 	supportsSavepoints() throws SQLException
    { return getDBMD().supportsSavepoints(); }
    
    public  static  boolean 	supportsSchemasInDataManipulation() throws SQLException
    { return getDBMD().supportsSchemasInDataManipulation(); }
    
    public  static  boolean 	supportsSchemasInIndexDefinitions() throws SQLException
    { return getDBMD().supportsSchemasInIndexDefinitions(); }
    
    public  static  boolean 	supportsSchemasInPrivilegeDefinitions() throws SQLException
    { return getDBMD().supportsSchemasInPrivilegeDefinitions(); }
    
    public  static  boolean 	supportsSchemasInProcedureCalls() throws SQLException
    { return getDBMD().supportsSchemasInProcedureCalls(); }
    
    public  static  boolean 	supportsSchemasInTableDefinitions() throws SQLException
    { return getDBMD().supportsSchemasInTableDefinitions(); }
    
    public  static  boolean 	supportsSelectForUpdate() throws SQLException
    { return getDBMD().supportsSelectForUpdate(); }
    
    public  static  boolean 	supportsStatementPooling() throws SQLException
    { return getDBMD().supportsStatementPooling(); }
    
    public  static  boolean 	supportsStoredFunctionsUsingCallSyntax() throws SQLException
    { return getDBMD().supportsStoredFunctionsUsingCallSyntax(); }
    
    public  static  boolean 	supportsStoredProcedures() throws SQLException
    { return getDBMD().supportsStoredProcedures(); }
    
    public  static  boolean 	supportsSubqueriesInComparisons() throws SQLException
    { return getDBMD().supportsSubqueriesInComparisons(); }
    
    public  static  boolean 	supportsSubqueriesInExists() throws SQLException
    { return getDBMD().supportsSubqueriesInExists(); }
    
    public  static  boolean 	supportsSubqueriesInIns() throws SQLException
    { return getDBMD().supportsSubqueriesInIns(); }
    
    public  static  boolean 	supportsSubqueriesInQuantifieds() throws SQLException
    { return getDBMD().supportsSubqueriesInQuantifieds(); }
    
    public  static  boolean 	supportsTableCorrelationNames() throws SQLException
    { return getDBMD().supportsTableCorrelationNames(); }
    
    public  static  boolean 	supportsTransactionIsolationLevel(int level) throws SQLException
    { return getDBMD().supportsTransactionIsolationLevel(level); }
    
    public  static  boolean 	supportsTransactions() throws SQLException
    { return getDBMD().supportsTransactions(); }
    
    public  static  boolean 	supportsUnion() throws SQLException
    { return getDBMD().supportsUnion(); }
    
    public  static  boolean 	supportsUnionAll() throws SQLException
    { return getDBMD().supportsUnionAll(); }
    
    public  static  boolean 	updatesAreDetected(int type) throws SQLException
    { return getDBMD().updatesAreDetected(type); }
    
    public  static  boolean 	usesLocalFilePerTable() throws SQLException
    { return getDBMD().usesLocalFilePerTable(); }
    
    public  static  boolean 	usesLocalFiles() throws SQLException
    { return getDBMD().usesLocalFiles(); }
    
       


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // REGISTRATION MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** <p>Return true if the requested modifer is set</p> */
    private boolean isSet( int allModifiers, int requestedModifier )
    {
        return ( ( allModifiers & requestedModifier ) != 0 );
    }

    /** <p>Drop the function with this method name.</p> */
    private void    unregisterFunction( Connection conn, Method method )
        throws SQLException
    {
        // maybe the method doesn't exist. swallow the exception.
        try {
            executeDDL( conn, "drop function " + method.getName() );
        } catch (SQLException se) {}
    }
    
    /** <p>Register the method as a Derby function.</p> */
    private void    registerFunction( Connection conn, Method method )
        throws SQLException
    {
        StringBuffer   buffer = new StringBuffer();
        String              name = method.getName();
        boolean         isTableFunction = isTableFunction( method );

        buffer.append( "create function " + name + "\n(" );
        appendArgs( buffer, method );
        buffer.append( "\n)\n" );
        buffer.append( "returns " );
        appendReturnType( buffer, conn, method );
        buffer.append( "\nlanguage java\nreads sql data\nparameter style " );
        if ( isTableFunction ) { buffer.append( "DERBY_JDBC_RESULT_SET" ); }
        else { buffer.append( "java" ); }
        buffer.append( "\nexternal name '" + getClass().getName() + "." + name + "'" );

        executeDDL( conn, buffer.toString() );
    }

    /** <p>Return true if the method describes a table function.</p> */
    private boolean isTableFunction( Method method )
    {
        Class   returnType = method.getReturnType();

        return ( returnType == java.sql.ResultSet.class );
    }

    /** <p>Append function arguments to an evolving ddl text buffer.</p> */
    private void    appendArgs( StringBuffer buffer, Method method )
        throws SQLException
    {
        Class[] parameterTypes = method.getParameterTypes();
        int         count = parameterTypes.length;
        String  paramStub = "a_";

        for ( int pidx = 0; pidx < count; pidx++ )
        {
            Class paramType = parameterTypes[ pidx ];

            if ( pidx > 0 ) { buffer.append( "," ); }
            buffer.append( "\n\t" );
            buffer.append( paramStub + pidx );
            buffer.append( ' ' );
            buffer.append( mapJavaToSQLType( paramType ) );
        }
    }

    /** <p>Append return type to an evolving ddl text buffer</p> */
    private void    appendReturnType( StringBuffer buffer, Connection conn, Method method )
        throws SQLException
    {
        Class   returnType = method.getReturnType();

        if ( java.sql.ResultSet.class == returnType ) { appendTableFunctionSignature( buffer, conn, method ); }
        else { buffer.append( mapJavaToSQLType( returnType ) ); }
    }

    /** <p>Append the signature of a table function to an evolving ddl text buffer</p> */
    private void    appendTableFunctionSignature( StringBuffer buffer, Connection conn, Method method )
        throws SQLException
    {
        buffer.append( "table\n(" );

        Class[]                     parameterTypes = method.getParameterTypes();
        int                         argCount = parameterTypes.length;
        Object[]                argValues = new Object[ argCount ];
        for ( int i = 0; i < argCount; i++ ) { argValues[ i ] = getDummyValue( parameterTypes[ i ] ); }
        ResultSet               returnValue;

        try {
            returnValue = (ResultSet) method.invoke( null, argValues );
        }
        catch (IllegalAccessException iae) { throw wrap( iae ); }
        catch (InvocationTargetException ite) { throw wrap( ite ); }

        ResultSetMetaData   rsmd = returnValue.getMetaData();
        int                         columnCount = rsmd.getColumnCount();

        for ( int i = 0; i < columnCount; i++ )
        {
            int columnNumber = i + 1;
            
            if ( i > 0 ) { buffer.append( "," ); }
            buffer.append( "\n\t" );
            buffer.append( rsmd.getColumnName( columnNumber ) );
            buffer.append( "\t" );
            stringifyJDBCType(  buffer, rsmd, columnNumber );
        }
        
        buffer.append( "\n)" );
    }

    /** <p>Get a dummy value for an argument to a DBMD method.</p> */
    private Object  getDummyValue( Class type )
    {
        if ( String.class == type ) { return ""; }
        else if ( Integer.TYPE == type ) { return 1; }
        else if ( Short.TYPE == type ) { return (short) 1; }
        else if ( Boolean.TYPE == type ) { return Boolean.TRUE; }
        else { return null; }
    }
    
    /** <p>Append the name of a SQL type to an evolving ddl text buffer</p> */
    private void    stringifyJDBCType( StringBuffer buffer, ResultSetMetaData rsmd, int columnNumber )
        throws SQLException
    {
        switch ( rsmd.getColumnType( columnNumber ) )
        {
        case Types.CHAR:
        case Types.VARCHAR:
            buffer.append( rsmd.getColumnTypeName( columnNumber ) );
            buffer.append( "( " );
            int precision = rsmd.getPrecision( columnNumber );
            if ( precision <= 0 ) { precision = DEFAULT_PRECISION; }
            buffer.append( precision );
            buffer.append( " )" );
            break;
        default:
            buffer.append( rsmd.getColumnTypeName( columnNumber ) );
            break;
        }
    }
    
    /**<p>Get the SQL type which corresponds to a Java type.</p> */
    private String  mapJavaToSQLType( Class javaType )
        throws SQLException
    {
        if ( Short.TYPE == javaType ) { return "smallint"; }
        else if ( Integer.TYPE == javaType ) { return "int"; }
        else if ( Boolean.TYPE == javaType ) { return "boolean"; }
        else if ( String.class == javaType ) { return "varchar( 32672 )"; }
        else { throw new SQLException( "Unsupported type: " + javaType.getName() ); }
    }

    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // GENERAL MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the current session's database metadata.
     * </p>
     */
    private static DatabaseMetaData getDBMD()
        throws SQLException
    {
        return getDefaultConnection().getMetaData();
    }
    
    /**
     * <p>
     * Get the default connection, called from inside the database engine.
     * </p>
     */
    private static Connection getDefaultConnection()
        throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

    /**<p>Execute a DDL statement.</p> */
	private	static	void	executeDDL( Connection conn, String text )
		throws SQLException
	{
		PreparedStatement	ps = null;

		try {
			ps = prepareStatement( conn, text );

			ps.execute();
		}
		finally { if ( ps != null ) { ps.close(); } }
	}
	
    /**
     *<p>
     * Prepare a statement and print out the text.
     * </p>
     */
	private	static	PreparedStatement	prepareStatement( Connection conn, String text )
		throws SQLException
	{
		PreparedStatement	ps = conn.prepareStatement( text );

		return ps;
	}

    /**
     * <p>
     * Wrap an exception in a SQLException.
     * </p>
     */
    private static  SQLException    wrap( Throwable t )
    {
        return new SQLException( t.getMessage(), t );
    }
    
}

