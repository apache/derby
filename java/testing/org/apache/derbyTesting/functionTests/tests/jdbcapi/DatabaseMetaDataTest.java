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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.StringTokenizer;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test the DatabaseMetaData api.
 * Work in progress.
 *
 */
public class DatabaseMetaDataTest extends BaseJDBCTestCase {
    /*
    ** Escaped function testing
    */
    private static final String[][] NUMERIC_FUNCTIONS =
    {
        // Section C.1 JDBC 3.0 spec.
        { "ABS", "-25.67" },
        { "ACOS", "0.0707" },
        { "ASIN", "0.997" },
        { "ATAN", "14.10" },
        { "ATAN2", "0.56", "1.2" },
        { "CEILING", "3.45" },
        { "COS", "1.2" },
        { "COT", "3.4" },
        { "DEGREES", "2.1" },
        { "EXP", "2.3" },
        { "FLOOR", "3.22" },
        { "LOG", "34.1" },
        { "LOG10", "18.7" },
        { "MOD", "124", "7" },
        { "PI" },
        { "POWER", "2", "3" },
        { "RADIANS", "54" },
        { "RAND", "17" }, 
        { "ROUND", "345.345", "1" }, 
        { "SIGN", "-34" },
        { "SIN", "0.32" },
        { "SQRT", "6.22" },
        { "TAN", "0.57", },
        { "TRUNCATE", "345.395", "1" }
    };
    
    private static final String[][] TIMEDATE_FUNCTIONS =
    {   
        // Section C.3 JDBC 3.0 spec.
        { "CURDATE" },
        { "CURTIME" },
        { "DAYNAME", "{d '1995-12-19'h}" },
        { "DAYOFMONTH", "{d '1995-12-19'}" },
        { "DAYOFWEEK", "{d '1995-12-19'}" },
        { "DAYOFYEAR", "{d '1995-12-19'}" },
        { "HOUR", "{t '16:13:03'}" },
        { "MINUTE", "{t '16:13:03'}" },
        { "MONTH", "{d '1995-12-19'}" },
        { "MONTHNAME", "{d '1995-12-19'}" },
        { "NOW" },
        { "QUARTER", "{d '1995-12-19'}" },
        { "SECOND", "{t '16:13:03'}" },
        { "TIMESTAMPADD", "SQL_TSI_DAY", "7", "{ts '1995-12-19 12:15:54'}" },
        { "TIMESTAMPDIFF", "SQL_TSI_DAY", "{ts '1995-12-19 12:15:54'}", "{ts '1997-11-02 00:15:23'}" },
        { "WEEK", "{d '1995-12-19'}" },
        { "YEAR", "{d '1995-12-19'}" },
        
    };

    private static final String[][] SYSTEM_FUNCTIONS =
    {   
        // Section C.4 JDBC 3.0 spec.
        { "DATABASE" },
        { "IFNULL", "'this'", "'that'" },
        { "USER"},
        };  
    
    private static final String[][] STRING_FUNCTIONS =
    {   
        // Section C.2 JDBC 3.0 spec.
        { "ASCII" , "'Yellow'" },
        { "CHAR", "65" },
        { "CONCAT", "'hello'", "'there'" },
        { "DIFFERENCE", "'Pires'", "'Piers'" },
        { "INSERT", "'Bill Clinton'", "4", "'William'" },
        { "LCASE", "'Fernando Alonso'" },
        { "LEFT", "'Bonjour'", "3" },
        { "LENGTH", "'four    '" } ,
        { "LOCATE", "'jour'", "'Bonjour'" },
        { "LTRIM", "'   left trim   '"},
        { "REPEAT", "'echo'", "3" },
        { "REPLACE", "'to be or not to be'", "'be'", "'England'" },
        { "RTRIM", "'  right trim   '"},
        { "SOUNDEX", "'Derby'" },
        { "SPACE", "12"},
        { "SUBSTRING", "'Ruby the Rubicon Jeep'", "10", "7", },
        { "UCASE", "'Fernando Alonso'" }
        };

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
        
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertTrue(dmd.supportsResultSetConcurrency(
            ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertFalse(dmd.supportsResultSetConcurrency(
             ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        assertFalse(dmd.supportsResultSetConcurrency(
             ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertTrue(dmd.supportsResultSetHoldability(
                ResultSet.CLOSE_CURSORS_AT_COMMIT));
        assertTrue(dmd.supportsResultSetHoldability(
                ResultSet.HOLD_CURSORS_OVER_COMMIT));
        
        assertTrue(dmd.supportsResultSetType(
                ResultSet.TYPE_FORWARD_ONLY));
        assertTrue(dmd.supportsResultSetType(
                ResultSet.TYPE_SCROLL_INSENSITIVE));
        assertFalse(dmd.supportsResultSetType(
                ResultSet.TYPE_SCROLL_SENSITIVE));
        
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
    
    /**
     * Test group of methods provides the limits imposed by a given data source
     *  Methods start with
     * 'getMax'. See section 7.4 in JDBC 3.0 specification.
     * 
     * Note a return of zero from one of these functions means
     * no limit or limit not known.
     * 
     * Work in progress.
     * @throws SQLException 
     *
     */
    public void testDataSourceLimits() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        
        assertEquals(0, dmd.getMaxBinaryLiteralLength());
        
        // Catalogs not supported.
        assertEquals(0, dmd.getMaxCatalogNameLength());
        
        assertEquals(0, dmd.getMaxCharLiteralLength());
        
        assertEquals(128, dmd.getMaxColumnNameLength());
        assertEquals(0, dmd.getMaxColumnsInGroupBy());
        assertEquals(0, dmd.getMaxColumnsInIndex());
        assertEquals(0, dmd.getMaxColumnsInOrderBy());
        assertEquals(0, dmd.getMaxColumnsInSelect());
        assertEquals(0, dmd.getMaxColumnsInTable());
        assertEquals(0, dmd.getMaxConnections());
        assertEquals(128, dmd.getMaxCursorNameLength());
        assertEquals(0, dmd.getMaxIndexLength());
        assertEquals(128, dmd.getMaxProcedureNameLength());
        assertEquals(0, dmd.getMaxRowSize());
        assertEquals(128, dmd.getMaxSchemaNameLength());
        assertEquals(0, dmd.getMaxStatementLength());
        assertEquals(0, dmd.getMaxStatements());
        assertEquals(128, dmd.getMaxTableNameLength());
        assertEquals(0, dmd.getMaxTablesInSelect());
        assertEquals(30, dmd.getMaxUserNameLength());
    }
    
    /**
     * JDBC escaped numeric functions - JDBC 3.0 C.1
     * @throws SQLException
     */
    public void testNumericFunctions() throws SQLException
    {
        escapedFunctions(NUMERIC_FUNCTIONS,
                getDMD().getNumericFunctions());
    }
    /**
     * JDBC escaped string functions - JDBC 3.0 C.2
     * @throws SQLException
     */
    public void testStringFunctions() throws SQLException
    {
        escapedFunctions(STRING_FUNCTIONS,
                getDMD().getStringFunctions());
    }    
    /**
     * JDBC escaped date time functions - JDBC 3.0 C.3
     * @throws SQLException
     */
    public void testTimeDataFunctions() throws SQLException
    {
        escapedFunctions(TIMEDATE_FUNCTIONS,
                getDMD().getTimeDateFunctions());
    }    
    /**
     * JDBC escaped system functions - JDBC 3.0 C.4
     * @throws SQLException
     */
    public void testSystemFunctions() throws SQLException
    {
        escapedFunctions(SYSTEM_FUNCTIONS,
                getDMD().getSystemFunctions());
    }           
    
    /**
     * Check that the list of escaped functions provided by
     * the driver is a strict subet of the specified set,
     * the list does not contain duplicates, all the functions
     * listed can be executed and that if a function is not
     * in the list but is specified it cannot be executed.
     */
    private void escapedFunctions(String[][] specList, String metaDataList)
    throws SQLException
    {
        boolean[] seenFunction = new boolean[specList.length];
        
        StringTokenizer st = new StringTokenizer(metaDataList, ",");
        while (st.hasMoreTokens())
        {
            String function = st.nextToken();
            
            // find this function in the list
            boolean isSpecFunction = false;
            for (int f = 0; f < specList.length; f++)
            {
                String[] specDetails = specList[f];
                if (function.equals(specDetails[0]))
                {
                    // Matched spec.
                    if (seenFunction[f])
                        fail("Function in list twice: " + function);
                    seenFunction[f] = true;
                    isSpecFunction = true;
                    executeEscaped(specDetails);
                    break;
                }
            }
            
            if (!isSpecFunction)
            {
               fail("Non-JDBC spec function in list: " + function);
            }
        }
        
        // Now see if any speced functions are not in the metadata list
        for (int f = 0; f < specList.length; f++)
        {
            if (seenFunction[f])
                continue;
            String[] specDetails = specList[f];
            
            // bug DERBY-723 CHAR maps to wrong function
            if ("CHAR".equals(specDetails[0]))
                continue;
            try {
                executeEscaped(specDetails);
                fail("function works but not declared in list: " + specDetails[0]);
            } catch (SQLException e) {
                assertSQLState("42X01", e);
            }
        }
    }
    
    /**
     * Test we can execute a function listed as a supported
     * JDBC escaped function. We don't care about the actual
     * return value, that should be tested elsewhere in
     * the specific test of a function.
     */
    private void executeEscaped(String[] specDetails)
        throws SQLException
    {
        
        String sql = "VALUES { fn " + specDetails[0] + "(";
        
        for (int p = 0; p < specDetails.length - 1; p++)
        {
            if (p != 0)
                sql = sql + ", ";
            
            sql = sql + specDetails[p + 1];
        }
        
        sql = sql + ") }";       
        
        PreparedStatement ps = prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        JDBC.assertDrainResults(rs);
        rs.close();
        ps.close();
    }
}
