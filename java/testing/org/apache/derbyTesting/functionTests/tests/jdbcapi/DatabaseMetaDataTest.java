/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.DatabaseMetaDataTest

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the DatabaseMetaData api.
 * Work in progress.
 *
 */
public class DatabaseMetaDataTest extends BaseJDBCTestCase {

    public DatabaseMetaDataTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        return TestConfiguration.defaultSuite(DatabaseMetaDataTest.class);
     }
    
    private final DatabaseMetaData getDMD() throws SQLException
    {
        return getConnection().getMetaData();
    }

    /**
     * Test the methods that indicate if a feature
     * is supported or not. Methods start with
     * 'support'. See secton 7.3 in JDBC 3.0 specification.
     * 
     * Work in progress.
     * @throws SQLException 
     *
     */
    public void testDetermineFeatureSupport() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertTrue(dmd.supportsAlterTableWithAddColumn());
        assertTrue(dmd.supportsAlterTableWithDropColumn());
        
        // Bug DERBY-2243 - return value is indicating support
        // level of the SQL engine, so should be consistent.
        if (usingEmbedded())
            assertFalse(dmd.supportsANSI92EntryLevelSQL());
        else
            assertTrue(dmd.supportsANSI92EntryLevelSQL());
              
        assertFalse(dmd.supportsANSI92FullSQL());
        assertFalse(dmd.supportsANSI92IntermediateSQL());
        
        assertTrue(dmd.supportsBatchUpdates());
        
        assertFalse(dmd.supportsCatalogsInDataManipulation());
        assertFalse(dmd.supportsCatalogsInIndexDefinitions());
        assertFalse(dmd.supportsCatalogsInPrivilegeDefinitions());
        assertFalse(dmd.supportsCatalogsInProcedureCalls());
        assertFalse(dmd.supportsCatalogsInTableDefinitions());
        
        assertTrue(dmd.supportsColumnAliasing());
        
        // Bug DERBY-462 should return false.
        assertTrue(dmd.supportsConvert());
        // Simple check since convert is not supported.
        // A comprehensive test should be added when convert
        // is supported, though most likely in a test class
        // specific to convert.
        assertFalse(dmd.supportsConvert(Types.INTEGER, Types.SMALLINT));
        
        //supportsCoreSQLGrammar()
        assertTrue(dmd.supportsCorrelatedSubqueries());
        
        assertTrue(dmd.supportsDataDefinitionAndDataManipulationTransactions());
        assertFalse(dmd.supportsDataManipulationTransactionsOnly());
        //supportsDifferentTableCorrelationNames()
        
        // Bug DERBY-2244, order by with expressions was added by DERBY-134
        assertFalse(dmd.supportsExpressionsInOrderBy());
        
        //supportsExtendedSQLGrammar()
        //supportsFullOuterJoins()
        //supportsGetGeneratedKeys()
        
        assertTrue(dmd.supportsGroupBy());
        //supportsGroupByBeyondSelect()
        //supportsGroupByUnrelated()
        
        //supportsIntegrityEnhancementFacility()
        assertTrue(dmd.supportsLikeEscapeClause());
        //supportsLimitedOuterJoins()
       //supportsMinimumSQLGrammar()
        
        assertFalse(dmd.supportsMixedCaseIdentifiers());
        assertTrue(dmd.supportsMixedCaseQuotedIdentifiers());
        
        assertTrue(dmd.supportsMultipleOpenResults());
        assertTrue(dmd.supportsMultipleResultSets());
        assertTrue(dmd.supportsMultipleTransactions());
        
        assertFalse(dmd.supportsNamedParameters());
        assertTrue(dmd.supportsNonNullableColumns());
        
        // Open cursors are not supported across global
        // (XA) transactions so the driver returns false.
        assertFalse(dmd.supportsOpenCursorsAcrossCommit());
        
        assertFalse(dmd.supportsOpenCursorsAcrossRollback());
        assertTrue(dmd.supportsOpenStatementsAcrossCommit());
        
        assertFalse(dmd.supportsOpenStatementsAcrossRollback());
        
        assertFalse(dmd.supportsOrderByUnrelated());
        
        assertTrue(dmd.supportsOuterJoins());
        
        assertTrue(dmd.supportsPositionedDelete());
        assertTrue(dmd.supportsPositionedUpdate());
        
       //supportsResultSetConcurrency(int type, int concurrency)
        
       //supportsResultSetHoldability(int holdability)
        
       //supportsResultSetType(int type)
        
        assertTrue(dmd.supportsSavepoints());
        assertTrue(dmd.supportsSchemasInDataManipulation());
        assertTrue(dmd.supportsSchemasInIndexDefinitions());
        assertTrue(dmd.supportsSchemasInPrivilegeDefinitions());
        assertTrue(dmd.supportsSchemasInProcedureCalls());
        assertTrue(dmd.supportsSchemasInTableDefinitions());
        assertTrue(dmd.supportsSelectForUpdate());
        
       //supportsStatementPooling()
       //supportsStoredProcedures()
       //supportsSubqueriesInComparisons()
       //supportsSubqueriesInExists()
       //supportsSubqueriesInIns()
       //supportsSubqueriesInQuantifieds()
        assertTrue(dmd.supportsTableCorrelationNames());
        
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_READ_COMMITTED));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_READ_UNCOMMITTED));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_REPEATABLE_READ));
        assertTrue(dmd.supportsTransactionIsolationLevel(
                Connection.TRANSACTION_SERIALIZABLE));
        
        assertTrue(dmd.supportsTransactions());
        assertTrue(dmd.supportsUnion());
        assertTrue(dmd.supportsUnionAll());         
    }
}
