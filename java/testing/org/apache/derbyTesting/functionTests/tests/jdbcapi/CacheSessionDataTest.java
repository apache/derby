
/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.CacheSessionDataTest
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at
  
     http://www.apache.org/licenses/LICENSE-2.0
  
   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */


package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.sql.ResultSet;
import java.util.Arrays;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Utility class for representing isolation levels. Provides a convenient way 
 * to track the JDBC constant, the JDBC constant name and the SQL name of an
 * isolation level. By overriding equals and toString it becomes convenient to
 * use this class in JUnit's assertEquals methods. 
 */
final class IsoLevel {
    private final int isoLevel_;
    private final String jdbcName_;
    private final String sqlName_;
    
    /**
     * Constructs an IsoLevel object from a ResultSet. The ResultSet must be 
     * equivalent to 'SELECT * FROM ISOLATION_NAMES'. Calls next() on the 
     * ResultSet, so the caller must position the ResultSet on the row before
     * the row that is to be used to create the IsoLevel object.
     * @param rs ResultSet holding isolation level descriptions
     * @throws java.sql.SQLException
     */
    public IsoLevel(ResultSet rs) throws SQLException {
        rs.next();
        isoLevel_ = rs.getInt("ISOLEVEL");
        jdbcName_ = rs.getString("JDBCNAME");
        sqlName_ = rs.getString("SQLNAME");
    }

    public int getIsoLevel() { return isoLevel_; }
    public String getJdbcName() { return jdbcName_; }
    public String getSqlName() { return sqlName_; }
    public String toString() {
        return "(" + jdbcName_ + ", " + sqlName_ + ")";
    }
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof IsoLevel) {
            return (isoLevel_ == ((IsoLevel) that).isoLevel_);
        }
        return false;
    }
    public int hashCode() { return isoLevel_; }
}

/**
 * This is a test for DERBY-3192 (https://issues.apache.org/jira/browse/DERBY-3192) 
 * which tries to avoid unecessary roundtrips by piggybacking session 
 * information on the messages going back to the client. The goal is that
 * whenever a user requests session information from the client driver, the 
 * correct information should already be available and no special roundtrip
 * be required. 
 * So far the test only checks caching of the isolation level, but other 
 * session attributes can be added later. The test attempts to "fool" 
 * the caching mechanism by
 * modifying the isolation level without going through the client's 
 * Connection.setTransactionIsolation method. 
 * The effect of modifying the isolation level in and 
 * out of XA transactions is covered by the XA tests and not tested here.
 */
public class CacheSessionDataTest extends BaseJDBCTestCase {
    
    public CacheSessionDataTest(String name) {
        super(name);
    }

    /**
     * Adds both the embedded and client-server versions of the baseSuite to
     * the Test. An empty TestSuite is returned unless we have JDBC3 support, because
     * all test cases call verifyCachedIsolation() which in turn 
     * makes use of getTransactionIsolationJDBC()
     * (GET_TRANSACTION_ISOLATION_JDBC) which uses DriverManager to access the 
     * default connection.
     * @return the resulting Test object
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("CacheSessionDataTest");
        if (JDBC.vmSupportsJDBC3()) {
            suite.addTest(baseSuite("CacheSessionDataTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                    baseSuite("CacheSessionDataTest:client")));
        }
        return suite;
    }
    
    /**
     * Creates a new TestSuite with all the tests, and wraps it in a 
     * CleanDatabaseSetup with a custom decorator.
     * @param name TestSuite name
     * @return wrapped TestSuite
     */
    private static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(CacheSessionDataTest.class);
        
        return new CleanDatabaseTestSetup(suite) {
            /**
            * Creates the tables, stored procedures, and functions 
            * shared by all test cases.
            * @throws SQLException 
            */
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("CREATE TABLE ISOLATION_NAMES(ISOLEVEL INT, JDBCNAME " +
                        "VARCHAR(30), SQLNAME VARCHAR(2))");
                PreparedStatement insert = s.getConnection().prepareStatement(
                        "INSERT INTO ISOLATION_NAMES VALUES (?, ?, ?)");
                
                insert.setInt(1, Connection.TRANSACTION_NONE);
                insert.setString(2, "TRANSACTION_NONE"); 
                insert.setNull(3, Types.VARCHAR);
                insert.execute();
                
                insert.setInt(1, Connection.TRANSACTION_READ_UNCOMMITTED);
                insert.setString(2, "TRANSACTION_READ_UNCOMMITTED");
                insert.setString(3, "UR");
                insert.execute();
                
                insert.setInt(1, Connection.TRANSACTION_READ_COMMITTED);
                insert.setString(2, "TRANSACTION_READ_COMMITTED");
                insert.setString(3, "CS");
                insert.execute();
                
                insert.setInt(1, Connection.TRANSACTION_REPEATABLE_READ);
                insert.setString(2, "TRANSACTION_REPEATABLE_READ");
                insert.setString(3, "RS");
                insert.execute();
                
                insert.setInt(1, Connection.TRANSACTION_SERIALIZABLE);
                insert.setString(2, "TRANSACTION_SERIALIZABLE");
                insert.setString(3, "RR");
                insert.execute();
                insert.close();
                
                s.execute("CREATE TABLE BIG(C1 VARCHAR(32672), " +
                        "C2 VARCHAR(32672), C3 VARCHAR(32672), C4 VARCHAR(32672))");                
                s.execute("CREATE PROCEDURE INSERTDATA1(IN A INT) LANGUAGE JAVA " +
                        "PARAMETER STYLE JAVA EXTERNAL NAME " +
                        "'org.apache.derbyTesting.functionTests.util." +
                        "ProcedureTest.bigTestData'");
                CallableStatement cs = s.getConnection().prepareCall("CALL INSERTDATA1(?)");
                cs.setInt(1,9);
                for (int i = 0; i < 10; ++i) {
                    cs.execute();
                }
                ResultSet x = s.executeQuery("SELECT COUNT(*) FROM BIG");
                x.next();
                println("BIG has "+x.getInt(1)+" rows");
                
                // Create procedures
                s.execute("CREATE PROCEDURE SET_ISOLATION_JDBC" +
                        " (ISO INT) NO SQL LANGUAGE JAVA PARAMETER STYLE " +
                        "JAVA EXTERNAL NAME '" + 
                        CacheSessionDataTest.class.getName() + 
                        ".setIsolationJDBC'");
                
                s.execute("CREATE PROCEDURE SET_ISOLATION_SQL " +
                        "(SQLNAME VARCHAR(2)) MODIFIES SQL DATA LANGUAGE JAVA PARAMETER STYLE " +
                        "JAVA EXTERNAL NAME '" +
                        CacheSessionDataTest.class.getName() + 
                        ".setIsolationSQL'");
                
                // Create functions
                s.execute("CREATE FUNCTION GET_TRANSACTION_ISOLATION_JDBC " +
                        "() RETURNS INT NO SQL LANGUAGE JAVA " +
                        "PARAMETER STYLE JAVA EXTERNAL NAME '" +
                        CacheSessionDataTest.class.getName() + 
                        ".getTransactionIsolationJDBC'");        
                        
                s.execute("CREATE FUNCTION GET_CYCLE_ISOLATION_JDBC " +
                        "() RETURNS INT NO SQL LANGUAGE JAVA " +
                        "PARAMETER STYLE JAVA EXTERNAL NAME '" +
                        CacheSessionDataTest.class.getName() + 
                        ".getCycleIsolationJDBC'");
 
                s.execute("CREATE FUNCTION GET_CYCLE_ISOLATION_SQL " +
                        "() RETURNS VARCHAR(2) READS SQL DATA LANGUAGE JAVA " +
                        "PARAMETER STYLE JAVA EXTERNAL NAME '" +
                        CacheSessionDataTest.class.getName() + 
                        ".getCycleIsolationSQL'");

                // Schema testing
                s.execute("CREATE SCHEMA FOO");
                String unicodeschema = "\u00bbMY\u20ac\u00ab";
                s.execute("CREATE SCHEMA \"" + unicodeschema + "\"");

                s.execute("CREATE PROCEDURE APP.SET_SCHEMA (SCHEMANAME " +
                        "VARCHAR(128)) MODIFIES SQL DATA LANGUAGE JAVA " +
                        "PARAMETER STYLE JAVA EXTERNAL NAME '" +
                        CacheSessionDataTest.class.getName() + ".setSchema'");

                s.execute("CREATE FUNCTION APP.GET_SCHEMA_TRANSITION " +
                        "(SCHEMANAME VARCHAR(128)) RETURNS VARCHAR(128) READS " +
                        "SQL DATA LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL " +
                        "NAME '" + CacheSessionDataTest.class.getName() +
                        ".getSchemaTransition'");

                s.execute("CREATE TABLE APP.LARGE(X VARCHAR(32000), " +
                        "SCHEMANAME VARCHAR(128), Y VARCHAR(32000))");

                char[] carray = new char[32000];
                Arrays.fill(carray, 'x');
                String xs = new String(carray);
                Arrays.fill(carray, 'y');
                String ys = new String(carray);

                s.execute("INSERT INTO APP.LARGE (SELECT '" + xs + "', " +
                        "SCHEMANAME, " + " '" + ys + "' FROM SYS.SYSSCHEMAS)");
            }
        };
    } // End baseSuite
    
    /**
     * Turns off auto commit on the default connection and verifies that the 
     * isolation level is read committed. Initailizes the array 'isoLevels' 
     * with the 4 standard isolation levels if this has not already been done.
     * @throws java.sql.SQLException
     */
    public void setUp() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(false);
        assertEquals(Connection.TRANSACTION_READ_COMMITTED, c.getTransactionIsolation());
        if (isoLevels == null) {
            Statement s = createStatement();
            ResultSet rs = s.executeQuery(
                    "SELECT * FROM ISOLATION_NAMES WHERE ISOLEVEL > 0 " +
                    "ORDER BY ISOLEVEL");
            isoLevels = new IsoLevel[4];
            
            for (int i = 0; i < 4; ++i) {
                isoLevels[i] = new IsoLevel(rs);
                println(isoLevels[i].toString()); 
            }
            assertFalse(rs.next());
            rs.close();
            s.close();
        }
        assertNotNull(isoLevels[0]);
        assertNotNull(isoLevels[1]);
        assertNotNull(isoLevels[2]);
        assertNotNull(isoLevels[3]);
    }
    /**
     * Removes all tables in schema APP which has the prefix 'T', before calling
     * super.tearDown().
     * @throws java.lang.Exception
     */
    public void tearDown() throws Exception {
        DatabaseMetaData meta = getConnection().getMetaData();
        ResultSet tables = meta.getTables(null, "APP", "T%", null);
        Statement s = createStatement();
        while (tables.next()) {
            s.execute("DROP TABLE " + tables.getString("TABLE_NAME"));
        }
        tables.close();
        s.close();
        commit();
        super.tearDown();
    }
    
    /**
     * Implementation of the stored procedure SET_ISOLATION_JDBC.
     * Sets the the isolation level given as argument on the default connection
     * using Connection.setTransactionIasolation.
     * @param isolation JDBC isolation level constant representing the 
     * new isolation level
     * @throws java.sql.SQLException
     */
    public static void setIsolationJDBC(int isolation)
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        c.setTransactionIsolation(isolation);
    }
    /**
     * Implementation of the SQL function SET_ISOLATION_SQL.
     * Sets the the isolation level given as argument on the default connection
     * using SQL.
     * @param sqlName SQL string representing the new isolation level
     * @throws java.sql.SQLException
     */
    public static void setIsolationSQL(String sqlName)
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        s.execute("SET ISOLATION " + sqlName);
        s.close();
    }
    /**
     * Implementation of the SQL function GET_TRANSACTION_ISOLATION_JDBC.
     * Returns the isolation level reported by the default EmbedConnection 
     * on the server. Used to verify that the isolation level reported by 
     * the client is correct.
     * @return JDBC isolation level constant reported by the embedded driver
     * @throws java.sql.SQLException
     */
    public static int getTransactionIsolationJDBC()
            throws SQLException {
        return DriverManager.getConnection("jdbc:default:connection").
                getTransactionIsolation();
    }
    /**
     * Implementation of the SQL function GET_CYCLE_ISOLATION_JDBC.
     * Cycles the isolation level on the default Connection.
     * @return the new JDBC isolation level constant
     * @throws java.sql.SQLException
     */
    public static int getCycleIsolationJDBC()
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        c.setTransactionIsolation(cycleIsolation().getIsoLevel());
        println("getCycleIsolationJDBC() -> "+c.getTransactionIsolation());
        return c.getTransactionIsolation();
    }
    /**
     * Implementation of the SQL function GET_CYCLE_ISOLATION_SQL.
     * Cycles the isolation level on the default Connection.
     * @return the SQL name of the new isolation level
     * @throws java.sql.SQLException
     */
    public static String getCycleIsolationSQL()
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        s.execute("SET ISOLATION "+cycleIsolation().getSqlName());
        ResultSet rs = s.executeQuery("VALUES CURRENT ISOLATION");
        rs.next();
        String sqlName = rs.getString(1);
        rs.close();
        s.close();
        println("getCycleIsolationSQL() -> "+sqlName);
        return sqlName;
    }

    /**
     * Implementation of the SQL procedure SET_SCHEMA.
     * Sets a different schema on the default Connection.
     * @param schemaName name of the new schema
     * @throws java.sql.SQLException
     */
    public static void setSchema(String schemaName)
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        s.execute("SET SCHEMA " + schemaName);
        s.close();
    }

    /**
     * Implementation of the SQL function GET_SCHEMA_TRANSITION.
     * Sets the current schema to the name given as argument and returns the
     * schema transition.
     * @param nextSchema schema to transition to
     * @return a string of the form oldSchema->newSchema
     * @throws java.sql.SQLException
     */
    public static String getSchemaTransition(String nextSchema)
            throws SQLException {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("VALUES CURRENT SCHEMA");
        rs.next();
        String prevSchema = rs.getString(1);
        rs.close();
        s.execute("SET SCHEMA \"" + nextSchema + "\"");
        s.close();
        return (prevSchema + "->" + nextSchema);
    }

    // Utilities
    private static IsoLevel[] isoLevels;    
    private static int isolationIndex = -1;
    
    /**
     * Utility that cycles through the legal isolation levels in the following
     * order: read uncommitted -> read committed -> repeatable read -> 
     * serializable -> read uncommitted -> ...
     * @return IsoLevel object representing the isolation level.
     */
    private static IsoLevel cycleIsolation() {
        ++isolationIndex;
        isolationIndex %= 4;
        return isoLevels[isolationIndex];
    }

    /**
     * Utility that verifies that the isolation level reported by the client 
     * is the same as evaluating 'VALUES CURRENT ISOLATION' and getting the
     * isolation level from the EmbedConnection on the server.
     * @param c Connection to check
     * @throws java.sql.SQLException
     */
    private void verifyCachedIsolation(Connection c) throws SQLException {
        final int clientInt = c.getTransactionIsolation();
        
        Statement s = createStatement();
        final IsoLevel serverSql = new IsoLevel(s.executeQuery(
                "SELECT * FROM ISOLATION_NAMES " +
                "WHERE SQLNAME = (VALUES CURRENT ISOLATION)"));
        
        final IsoLevel serverJdbc = new IsoLevel(s.executeQuery(
                "SELECT * FROM ISOLATION_NAMES " +
                "WHERE ISOLEVEL = GET_TRANSACTION_ISOLATION_JDBC()"));
        
        final IsoLevel client = new IsoLevel(s.executeQuery("SELECT * FROM " +
                "ISOLATION_NAMES WHERE ISOLEVEL = "+clientInt));
        s.getResultSet().close();
        s.close();
        assertEquals(serverSql, client);
        assertEquals(serverJdbc, client);
    }
    
    private void verifyCachedSchema(Connection c) throws SQLException {
        if (c instanceof org.apache.derby.client.am.Connection) {
            String cached =
                    ((org.apache.derby.client.am.Connection) c).
                    getCurrentSchemaName();
            Statement s = c.createStatement();
            ResultSet rs = s.executeQuery("VALUES CURRENT SCHEMA");
            rs.next();
            String reported = rs.getString(1);
            assertEquals(reported, cached);
        } else {
            println("Cannot verify cached schema for "+c.getClass());
        }
    }

    // Test cases (fixtures) 
    // Change the isolation level using SQL
    public void testChangeIsoLevelStatementSQL() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        for (int i = 0; i < 4; ++i) {
            s.execute("SET ISOLATION "+isoLevels[i].getSqlName());
            verifyCachedIsolation(c);
        }
        s.close();
    }
    public void testChangeIsoLevelPreparedStatementSQL() throws SQLException {
        Connection c = getConnection();
        for (int i = 0; i < 4; ++i) {
            PreparedStatement ps = prepareStatement("SET ISOLATION " + 
                    isoLevels[i].getSqlName());
            ps.execute();
            verifyCachedIsolation(c);
            ps.close();
        }
    }
    
    // Change the isolation level using a function
    public void testChangeIsoLevelFunctionJDBC() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(true);
        Statement s = createStatement();
        s.execute("CREATE TABLE T1(ISOLEVEL INT)");
        for (int i = 0; i < 4; ++i) {
            s.execute("INSERT INTO T1 VALUES GET_CYCLE_ISOLATION_JDBC()");
            verifyCachedIsolation(c);
        }
        s.close();
    }
    public void testChangeIsoLevelFunctionSQL() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(true);
        Statement s = createStatement();
        s.execute("CREATE TABLE T1(SQLNAME VARCHAR(2))");
        for (int i = 0; i < 4; ++i) {
            s.executeUpdate("INSERT INTO T1 VALUES GET_CYCLE_ISOLATION_SQL()");
            verifyCachedIsolation(c);
        }
        s.close();
    }
    public void testChangeIsoLevelPreparedFunctionJDBC() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(true);
        Statement s = createStatement();
        s.execute("CREATE TABLE T1(ISOLEVEL INT)");
        PreparedStatement ps = prepareStatement("INSERT INTO T1 VALUES " +
                "GET_CYCLE_ISOLATION_JDBC()");
        for (int i = 0; i < 4; ++i) {
            ps.executeUpdate();
            verifyCachedIsolation(c);
        }
        ps.close();
    }
    public void testChangeIsoLevelPreparedFunctionSQL() throws SQLException {
        Connection c = getConnection();
        c.setAutoCommit(true);
        Statement s = createStatement();
        s.execute("CREATE TABLE T1(SQLNAME VARCHAR(2))");
        PreparedStatement ps = prepareStatement("INSERT INTO T1 VALUES " +
                "GET_CYCLE_ISOLATION_SQL()");
        for (int i = 0; i < 4; ++i) {
            ps.executeUpdate();
            verifyCachedIsolation(c);
        }
        ps.close();
    }
 
    // Change isolation level from a stored procedure
    public void testChangeIsoLevelProcedureJDBC() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        for (int i = 0; i < 4; ++i) {
            s.execute("CALL SET_ISOLATION_JDBC(" + isoLevels[i].getIsoLevel() + ")");
            verifyCachedIsolation(c);
        }
        s.close();
    }
    public void testChangeIsoLevelProcedureSQL() throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement();
        for (int i = 0; i < 4; ++i) {
            s.execute("CALL SET_ISOLATION_SQL('" + isoLevels[i].getSqlName() + "')");
            verifyCachedIsolation(c);
        }
        s.close();
    }

    // Changing isolation level from a stored procedure invoked from a 
    // callable statement
    public void testChangeIsoLevelCallableStatementJDBC() throws SQLException {
        Connection c = getConnection();
        CallableStatement cs = prepareCall("CALL SET_ISOLATION_JDBC(?)");
        for (int i = 0; i < 4; ++i) {
            cs.setInt(1, isoLevels[i].getIsoLevel());
            cs.execute();
            verifyCachedIsolation(c);
        }
        cs.close();
    }
    public void testChangeIsoLevelCallableStatementSQL() throws SQLException {
        Connection c = getConnection();
        CallableStatement cs = prepareCall("CALL SET_ISOLATION_SQL(?)");
        for (int i = 0; i < 4; ++i) {
            cs.setString(1, isoLevels[i].getSqlName());
            cs.execute();
            verifyCachedIsolation(c);
        }
        cs.close();
    }
    
    // Changing isolation level from executeBatch()
    public void testChangeIsoLevelSQLInBatch() throws SQLException {
        Statement s = createStatement();
        for (int i = 0; i < isoLevels.length; ++i) {
            s.addBatch("SET ISOLATION " + isoLevels[i].getSqlName());
        }

        try {
            s.executeBatch();
        } catch (SQLException e) {
            SQLException prev = e;
            while (e != null) {
                prev = e;
                e = e.getNextException();
            }
            throw prev;
        }
        verifyCachedIsolation(s.getConnection());
        s.close();
    }
    
    public void testChangeIsoLevelProcedureJdbcBatch() throws SQLException {
        Statement s = createStatement();
        for (int i = 0; i < isoLevels.length; ++i) {
            s.addBatch("CALL SET_ISOLATION_JDBC(" + isoLevels[i].getIsoLevel() + ")");
        }
        try {
            s.executeBatch();
        } catch (SQLException e) {
            SQLException prev = e;
            while (e != null) {
                prev = e;
                e = e.getNextException();
            }
            throw prev;
        }
        verifyCachedIsolation(s.getConnection());
        s.close();
    }
    public void testChangeIsoLevelProcedureSqlBatch() throws SQLException {
        Statement s = createStatement();
        for (int i = 0; i < isoLevels.length; ++i) {
            s.addBatch("CALL SET_ISOLATION_SQL('" + isoLevels[i].getSqlName() + "')");
        }
        
        try {
            s.executeBatch();
        } catch (SQLException e) {
            SQLException prev = e;
            while (e != null) {
                prev = e;
                e = e.getNextException();
            }
            throw prev;
        }
        verifyCachedIsolation(s.getConnection());
        s.close();
    }
    
    public void testChangeIsoLevelProcedureJdbcCallableBatch() throws SQLException {
        CallableStatement cs = prepareCall("CALL SET_ISOLATION_JDBC(?)");
        for (int i = 0; i < isoLevels.length; ++i) {
            cs.setInt(1, isoLevels[i].getIsoLevel());
            cs.addBatch();
        }
        try {
            cs.executeBatch();
        } catch (SQLException e) {
            SQLException prev = e;
            while (e != null) {
                prev = e;
                e = e.getNextException();
            }
            throw prev;
        }
        verifyCachedIsolation(cs.getConnection());
        cs.close();
    }
    public void testChangeIsoLevelProcedureSqlCallableBatch() throws SQLException {
        CallableStatement cs = prepareCall("CALL SET_ISOLATION_SQL(?)");
        for (int i = 0; i < isoLevels.length; ++i) {
            cs.setString(1, isoLevels[i].getSqlName());
            cs.addBatch();
        }
        try {
            cs.executeBatch();
        } catch (SQLException e) {
            SQLException prev = e;
            while (e != null) {
                prev = e;
                e = e.getNextException();
            }
            throw prev;
        }
        verifyCachedIsolation(cs.getConnection());
        cs.close();
    }

    
    /**
     * Utility method for testing Statements that return different 
     * types of ResultSets to check that piggybacking doesn't cause problems.
     * @param table table to select from
     * @param type type of ResultSet
     * @param concur concurrency of ResultSet
     * @throws java.sql.SQLException
     */
    private void cursorTest(String table, int type, int concur) 
            throws SQLException {
        Connection c = getConnection();
        Statement s = createStatement(type, concur);
        ResultSet rs = s.executeQuery("SELECT * FROM "+table);
        verifyCachedIsolation(c);
        while (rs.next()) {
            verifyCachedIsolation(c);
        }
        rs.close();
        s.close();
    }
    
    /**
     * Utility method for testing PreparedStatements that return different 
     * types of ResultSets to check that piggybacking doesn't cause problems.
     * 
     * @param table table to select from
     * @param type type of ResultSet
     * @param concur concurrency of ResultSet
     * @throws java.sql.SQLException
     */
    private void preparedCursorTest(String table, int type, int concur)
            throws SQLException {
        Connection c = getConnection();
        PreparedStatement ps = c.prepareStatement("SELECT * FROM " + table, 
                type, concur);
        ResultSet rs = ps.executeQuery();
        verifyCachedIsolation(c);
        while (rs.next()) {
            verifyCachedIsolation(c);
        }
        rs.close();
        ps.close();
    }

    public void testSmallForwardOnlyReadOnly() throws SQLException {
        cursorTest("ISOLATION_NAMES", ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testSmallScrollInsensitiveReadOnly() throws SQLException {
        cursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_INSENSITIVE,                
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testSmallScrollSensitiveReadOnly() throws SQLException {
        cursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testSmallForwardOnlyUpdatable() throws SQLException {
        cursorTest("ISOLATION_NAMES", ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testSmallScrollInsensitiveUpdatable() throws SQLException {
        cursorTest("ISOLATION_NAMES",ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testSmallScrollSensitiveUpdatable() throws SQLException {
        cursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }
   

    public void testSmallPreparedForwardOnlyReadOnly() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);
    }    
    public void testSmallPreparedScrollSensitiveReadOnly() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }     
    public void testSmallPreparedScrollInsensitiveReadOnly() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testSmallPreparedForwardOnlyUpdatable() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testSmallPreparedScrollSensitiveUpdatable() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testSmallPreparedScrollInsensitiveUpdatable() throws SQLException {
        preparedCursorTest("ISOLATION_NAMES", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }
    
    
    public void testLargeForwardOnlyReadOnly() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);
    }    
    public void testLargeScrollSensitiveReadOnly() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_SCROLL_SENSITIVE, 
                ResultSet.CONCUR_READ_ONLY);
    }    
    public void testLargeScrollInsensitiveReadOnly() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testLargeForwardOnlyUpdatable() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testLargeScrollSensitiveUpdatable() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_SCROLL_SENSITIVE, 
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testLargeScrollInsensitiveUpdatable() throws SQLException {
        cursorTest("BIG", ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_UPDATABLE);
    }


    public void testLargePreparedForwardOnlyReadOnly() throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
    }     
    public void testLargePreparedScrollSensitiveReadOnly() throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testLargePreparedScrollInsensitiveReadOnly()
            throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }
    public void testLargePreparedForwardOnlyUpdatable() throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testLargePreparedScrollSensitiveUpdatable()
            throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }
    public void testLargePreparedScrollInsensitiveUpdatable()
            throws SQLException {
        preparedCursorTest("BIG", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_UPDATABLE);
    }

    // Test that the current schema is piggy-backed correctly
    public void testSetSchema() throws SQLException {
        Statement s = createStatement();
        s.execute("SET SCHEMA FOO");
        verifyCachedSchema(getConnection());
        s.execute("SET SCHEMA \"\u00bbMY\u20ac\u00ab\"");
        verifyCachedSchema(getConnection());
    }
    public void testPreparedSetSchema() throws SQLException {
        PreparedStatement ps = prepareStatement("SET SCHEMA ?");
        ps.setString(1, "FOO");
        ps.execute();
        verifyCachedSchema(getConnection());
        ps.setString(1, "\u00bbMY\u20ac\u00ab");
        ps.execute();
        verifyCachedSchema(getConnection());
    }
    public void testSetSchemaProcedure() throws SQLException {
        Statement s = createStatement();
        s.execute("CALL APP.SET_SCHEMA('FOO')");
        verifyCachedSchema(getConnection());
        s.execute("CALL APP.SET_SCHEMA('\"\u00bbMY\u20ac\u00ab\"')");
        verifyCachedSchema(getConnection());
    }
    public void testPreparedSetSchemaProcedure() throws SQLException {
        CallableStatement cs = prepareCall("CALL APP.SET_SCHEMA(?)");
        cs.setString(1, "FOO");
        cs.execute();
        verifyCachedSchema(getConnection());
        cs.setString(1, "\"\u00bbMY\u20ac\u00ab\"");
        cs.execute();
        verifyCachedSchema(getConnection());
    }

    public void testSetSchemaFunction() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT " +
                "APP.GET_SCHEMA_TRANSITION(SCHEMANAME) FROM SYS.SYSSCHEMAS");
        while (rs.next()) {
            assertTrue(rs.getString(1).length() > 2);
            verifyCachedSchema(getConnection());
        }
    }

    public void testPreparedSetSchemaFunction() throws SQLException {
        PreparedStatement ps = prepareStatement("SELECT " +
                "APP.GET_SCHEMA_TRANSITION(SCHEMANAME) FROM SYS.SYSSCHEMAS");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertTrue(rs.getString(1).length() > 2);
            verifyCachedSchema(getConnection());
        }
    }

    public void testSetSchemaFunctionLarge() throws SQLException {
        Statement s = createStatement();
        ResultSet rs = s.executeQuery("SELECT X, " +
                "APP.GET_SCHEMA_TRANSITION(SCHEMANAME), " +
                "Y FROM APP.LARGE");
        while (rs.next()) {
            assertTrue(rs.getString(2).length() > 2);
            verifyCachedSchema(getConnection());
        }
    }

    public void testPreparedSetSchemaFunctionLarge() throws SQLException {
        PreparedStatement ps = prepareStatement("SELECT X, " +
                "APP.GET_SCHEMA_TRANSITION(SCHEMANAME), " +
                "Y FROM APP.LARGE");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            assertTrue(rs.getString(2).length() > 2);
            verifyCachedSchema(getConnection());
        }
    }
}
