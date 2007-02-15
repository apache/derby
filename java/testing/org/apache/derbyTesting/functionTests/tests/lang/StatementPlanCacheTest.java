/*
*
* Derby - Class org.apache.derbyTesting.functionTests.lang.StatementPlanCacheTest
*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, 
* software distributed under the License is distributed on an 
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
* either express or implied. See the License for the specific 
* language governing permissions and limitations under the License.
*/
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;

/**
 * Tests statement plan caching.
 * <BR>
 * Size of the cache can be configured by derby.language.statementCacheSize.
 * derby.language.statementCacheSize.
 * <BR>
 * The statement cache can be viewed using the diagnostic table
 * SYSCS_DIAG.STATEMENT_CACHE
 * <BR>
 * The test also uses the fact that toString() for a Derby embedded
 * prepared statement returns a string identifier that matches the
 * ID column of SYSCS_DIAG.STATEMENT_CACHE.
 *
 */
public class StatementPlanCacheTest extends BaseJDBCTestCase {
    
    private static int CACHE_SIZE;
    
    private int statmentCacheSize;
    private PreparedStatement cacheInPlanPS_ID;
    private PreparedStatement cacheInPlanPS_TEXT;
  
    public StatementPlanCacheTest(String name) {
        super(name);
        statmentCacheSize = CACHE_SIZE;
    }
    
    /**
     * Runs in embedded only since it's testing the server side cache.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("StatementPlanCacheTest");
        
        CACHE_SIZE = 100; // default cache size
        suite.addTest(baseSuite("default"));
        suite.addTest(suiteWithSizeSet(5));
        suite.addTest(suiteWithSizeSet(140));
        
        // no caching
        suite.addTest(DatabasePropertyTestSetup.singleProperty(
                new StatementPlanCacheTest("noCachingTest"),
                "derby.language.statementCacheSize", "0", true));
        
        
        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("CREATE PROCEDURE EMPTY_STATEMENT_CACHE() " +
                        "LANGUAGE JAVA PARAMETER STYLE JAVA " +
                        "NO SQL " +
                        "EXTERNAL NAME 'org.apache.derby.diag.StatementCache.emptyCache'");
            }
            
        };
    }
    
    private static Test suiteWithSizeSet(int cacheSize)
    {
        // Sets up the cache size picked up by the constructor of this class
        CACHE_SIZE = cacheSize;
        String cs = Integer.toString(cacheSize);
        return DatabasePropertyTestSetup.singleProperty(
                baseSuite(cs),
                "derby.language.statementCacheSize", cs, true);
    }
    
    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("StatementPlanCacheTest:derby.language.statementCacheSize=" + name);
        suite.addTestSuite(StatementPlanCacheTest.class);
        return suite;
    }
    
    /**
     * Prepare the statement that sees if a statement given its
     * ID is in the cache. We hold onto it so that the statement
     * cache can be emptied and executing this will not alter
     * the state of cache.
     * Empty the statement cache so that each fixture starts
     * out with an empty cache.
     */
    protected void setUp() throws SQLException  {
        cacheInPlanPS_ID = prepareStatement(
                "SELECT COUNT(ID) FROM SYSCS_DIAG.STATEMENT_CACHE WHERE ID = ?");
        cacheInPlanPS_TEXT = prepareStatement(
                "SELECT COUNT(ID) FROM SYSCS_DIAG.STATEMENT_CACHE WHERE " +
                "SCHEMANAME = ? AND SQL_TEXT = ?");
        Statement s = createStatement();
        s.execute("CALL EMPTY_STATEMENT_CACHE()");
        s.close();
       
    }
    
    protected void tearDown() throws Exception {
        cacheInPlanPS_ID.close();
        cacheInPlanPS_TEXT.close();
        super.tearDown();
    }
    
    /**
     * Check that when the cache size is set to zero that
     * no caching takes place. Tests with Statement, PreparedStatement
     * and CallableStatement.
     */
    public void noCachingTest() throws SQLException
    {
        String schema = this.getTestConfiguration().getUserName();
        
        String sql = "VALUES 1";
        Statement s = createStatement();
        s.executeQuery(sql).close();
        s.close();
        
        assertFalse(sql, isPlanInCache(schema, sql));
        
        prepareStatement(sql).close();
        assertFalse(sql, isPlanInCache(schema, sql));
        
        sql = "CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)";
        prepareCall(sql).close();
        assertFalse(sql, isPlanInCache(schema, sql));
    }
    
    /**
     * Check that the same plan can be shared across
     * Statement, PreparedStatement and CallableStatement.
     */
    public void testAcrossStatementObjects() throws SQLException
    {
        String schema = this.getTestConfiguration().getUserName();
        
        String sql = sql = "CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)";
        Statement s = createStatement();
        s.execute(sql);
        s.close();
        
        assertTrue(sql, isPlanInCache(schema, sql));
        
        PreparedStatement ps = prepareStatement(sql);
        assertTrue(sql, isPlanInCache(ps));
               
        CallableStatement cs = prepareCall(sql);      
        assertTrue(sql, isPlanInCache(cs));
             
        // Check the prepared statement matches the callable
        assertEquals(ps.toString(), cs.toString());
        
        ps.close();
        cs.close();
    }
    
    /**
     * Test that statements that fail to compile do not end up in the cache.
     */
    public void testCompileFailuresNotInCache() throws SQLException
    {
        String schema = this.getTestConfiguration().getUserName();
        
        // Parse error
        String sql = "TO BE OR NOT TO BE";
        assertCompileError("42X01", sql);
        assertFalse(sql, isPlanInCache(schema, sql));
        
        // Valid tokens but missing elements
        sql = "CREATE PROCEDURE BAD_PROC() EXTERNAL NAME 'lll' LANGUAGE JAVA";
        assertCompileError("42X01", sql);
        assertFalse(sql, isPlanInCache(schema, sql));
        
        // Bind error
        sql = "SELECT * FROM NO_SUCH_TABLE_EXISTS";
        assertCompileError("42X05", sql);
        assertFalse(sql, isPlanInCache(schema, sql));  
    } 
    
    /**
     * Test statement caching according to the size of the cache
     * using PreparedStatement.
     * 
     */
    public void testPreparedStatementPlanCaching() throws SQLException
    {
        checkPreparedPlanInCache(statmentCacheSize, "VALUES &");
    }
    
    /**
     * Test statement caching according to the size of the cache
     * using CallableStatement.
     * 
     */
    public void testCallableStatementPlanCaching() throws SQLException
    {
        checkPreparedPlanInCache(statmentCacheSize,
                "CALL SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(&)");
    }    
    
   
    /**
     * Compile a number of statements based upon the sqlbase
     * replacing the & with a number increasing from 0 to number - 1
     * 
     * Uses PreparedStatement unless sqlbase starts with CALL then
     * CallableStatement is used.
     * 
     * Asserts that the statements appear in the cache and that recompiling
     * it picks up the same plan.
     */
    private void checkPreparedPlanInCache(int number,
            String sqlBase) throws SQLException
    {
        boolean isCall = sqlBase.startsWith("CALL ");
        
        String[] sql = new String[number];
        String[] firstCompileID = new String[sql.length];
        
        for (int i = 0; i < firstCompileID.length; i++ )
        {
           
            sql[i] = getSQL(sqlBase, i);
            PreparedStatement ps = isCall ?
                    prepareCall(sql[i]) : prepareStatement(sql[i]);
            assertTrue(sql[i], isPlanInCache(ps));
            firstCompileID[i] = ps.toString();
            ps.close();
        }
        
        
        for (int i = 0; i < firstCompileID.length; i++ )
        {
            // Check caching is across statements
            PreparedStatement ps = isCall ?
                    prepareCall(sql[i]) : prepareStatement(sql[i]);
            PreparedStatement ps2 = isCall ?
                    prepareCall(sql[i]) : prepareStatement(sql[i]);
            assertTrue(sql[i], isPlanInCache(ps));
            assertEquals(sql[i], firstCompileID[i], ps.toString());
            assertEquals(sql[i], ps.toString(), ps2.toString());
            ps.close();
            ps2.close();
            
            // Check the caching is across connections
            Connection c2 = openDefaultConnection();
            PreparedStatement psD = isCall ?
                    c2.prepareCall(sql[i]) : c2.prepareStatement(sql[i]);
            
            assertEquals(sql[i], firstCompileID[i], psD.toString());
            psD.close();
            c2.close();
            
        }
        
        String schema = this.getTestConfiguration().getUserName();
        
        // Now check that futher statements throw out existing cache entries.
        for (int n = firstCompileID.length; n < firstCompileID.length*2; n++)
        {
            // Compile a new statement, ends up in cache.
            String sqlN = getSQL(sqlBase, n);
            PreparedStatement ps = isCall ?
                    prepareCall(sqlN) : prepareStatement(sqlN);
            assertTrue(sqlN, isPlanInCache(ps));
            
            ps.close();
        }
        
        // Can only assume some of the original statements will
        // have been thrown out.
        boolean thrownOut = false;
        for (int i = 0; i < sql.length; i++)
        {
            if (isPlanInCache(schema, sql[i]))
                continue;
            
           thrownOut = true;
           break;             
        }
        
        

        assertTrue("Expect a plan to thrown out", thrownOut);
    }
    
    private static String getSQL(String sqlBase, int i)
    {
        StringBuffer sb = new StringBuffer();
        int rp = sqlBase.indexOf('&');
        sb.append(sqlBase.substring(0, rp));
        sb.append(i);
        if (rp+1 < sqlBase.length())
            sb.append(sqlBase.substring(rp+1));
        
        
        return sb.toString();       
    }
    
    private boolean isPlanInCache(PreparedStatement ps) throws SQLException {
        cacheInPlanPS_ID.setString(1, ps.toString());
        ResultSet rs = cacheInPlanPS_ID.executeQuery();
        rs.next();
        int count = rs.getInt(1);
        rs.close();
        assertTrue("Statement in cache multiple times ", count <= 1);
        return count == 1;
    }
    private boolean isPlanInCache(String schema, String sql) throws SQLException {
        cacheInPlanPS_TEXT.setString(1, schema);
        cacheInPlanPS_TEXT.setString(2, sql);
        ResultSet rs = cacheInPlanPS_TEXT.executeQuery();
        rs.next();
        int count = rs.getInt(1);
        rs.close();
        assertTrue("Statement in cache multiple times ", count <= 1);
        cacheInPlanPS_TEXT.clearParameters();
        return count == 1;
    }
}
