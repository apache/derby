/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SqlExceptionTest
 
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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This is used for testing the SqlException class.  This test can be added
 * to.  My itch right now is to verify that exception chaining is working
 * correctly.
 *
 * This test also verifies that a SQLException object generated out of the
 * derby network client driver can be serialized (DERBY-790).
 */

public class SqlExceptionTest extends BaseJDBCTestCase
{    
    public SqlExceptionTest(String name)
    {
        super(name);
    }
    
    /**
     * Makes sure exception chaining works correctly (DERBY-1117)
     */
    public void testChainedException() {
        IOException ioe = new IOException("Test exception");
        SqlException sqle = new SqlException(null,
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            ioe);
        SQLException javae = sqle.getSQLException();
        
        // The underlying SqlException is the first cause; the IOException
        // should be the second cause        
        assertEquals(sqle, javae.getCause());
        assertEquals(ioe, javae.getCause().getCause());
        assertNull(sqle.getNextException());
    }
    
    /**
     * Make sure a SQLException is chained as a nextSQLException()
     * and as a chained exception.
     */
    public void testNextException() {
        SQLException nexte = new SQLException("test");
        SqlException sqle = new SqlException(null,
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            nexte);
        SQLException javae = sqle.getSQLException();
        
        assertEquals(sqle, javae.getCause());
//IC see: https://issues.apache.org/jira/browse/DERBY-2692
        assertEquals(nexte, javae.getCause().getCause());
        assertEquals(nexte, javae.getNextException());
        
        // Make sure exception chaining works with Derby's SqlException
        // just as well as java.sql.SQLException
        SqlException internalException = 
            new SqlException(null, 
                new ClientMessageId("08000"));
        
        javae = new SqlException(null, 
            new ClientMessageId(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION),
            internalException).getSQLException();
        
        assertNotNull(javae.getNextException());
        assertEquals(javae.getNextException().getSQLState(), "08000");
//IC see: https://issues.apache.org/jira/browse/DERBY-2692
        assertEquals(internalException, javae.getCause().getCause());
    }

    public void testSQLStateInRootException() throws SQLException {
        String expectedSQLState = "22018";
        Statement s = createStatement();
        try {
            s.execute("values cast('hello' as int)");
            fail();
        } catch (SQLDataException sqle) {
            assertSQLState(expectedSQLState, sqle);

            // Check message of the root cause (a StandardException on embedded
            // and an SqlException on the client). Client didn't include
            // the SQLState before DERBY-6484.
            Throwable cause = sqle;
            while (cause instanceof SQLException) {
                cause = cause.getCause();
            }
            String toString = cause.toString();
            assertTrue("Message should start with the SQLState, found: "
                            + toString,
                       toString.startsWith("ERROR " + expectedSQLState + ":"));
        }
    }

    /**
     * Verify that a SQLException generated by the derby network client
     * driver can be serialized (DERBY-790).
     */
    public void testSerializedException() throws Exception {
        // DERBY-62; verify an exception using table name can be serialized.
        try {
            createStatement().execute("DROP TABLE APP.DERBY62_DAIN_SUNDSTROM");
            fail("should've received an error");
        } catch (SQLException sqle) {
            SQLException se_ser = recreateSQLException(sqle);
            // and that the original and serialized exceptions are equals
            assertSQLState("Unexpected SQL State", sqle.getSQLState(), se_ser);
            assertSQLExceptionEquals(sqle, se_ser);
        }
        
        try {
            Connection conn = getConnection();
            Statement stmt = conn.createStatement();
            // generate some exception by inserting some duplicate
            // primary keys in the same batch
            // This will generate some chained / nested transactions
            // as well
            String insertData = "INSERT INTO tableWithPK values " +
                "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)";
            stmt.addBatch(insertData);
            stmt.addBatch(insertData);
            stmt.addBatch(insertData);
            stmt.executeBatch();

            // In case the statement completes successfully which is not
            // expected
            fail("Unexpected: SQL statement should have failed");
        } catch (SQLException se) {
            // Verify the SQLException can be serialized (DERBY-790)
            SQLException se_ser = recreateSQLException(se);
            // and that the original and serialized exceptions are equals
            assertSQLState("Unexpected SQL State", se.getSQLState(), se_ser);
            assertSQLExceptionEquals(se, se_ser);
        }
    }
    
    /**
     * Verify that an SQLException thrown by a function can be returned
     * (DERBY-790).
     */
    public void testDerby3390() throws Exception {
        setAutoCommit(false);
        Statement stmt = createStatement();

        // with client/server we prefetch, so the error comes earlier
        try {
            if (usingDerbyNetClient())
            {
                stmt.execute("values badFunction1()");
                fail("expected an error");
            }
            else 
            {
                stmt.execute("values badFunction1()");
                ResultSet rs = stmt.getResultSet();
                rs.next();
                fail("expected an error");
            }
        } catch (SQLException e) {
            setAutoCommit(true);
            // if DERBY-3390 occurs, at this point, with networkserver/client, 
            // we'd get a 08006. In the server's derby.log you'd see a 
            // ClassCastException
            assertSQLState("38000", e);
            assertTrue(e.getMessage().indexOf("I refuse to return an int") > 1);
        }

        // as above, but this time the function uses the constructor for
        // SQLException with SQLState.
        try {
            if (usingDerbyNetClient())
            {
                stmt.execute("values badFunction2()");
                fail("expected an error");
            }
            else 
            {
                stmt.execute("values badFunction2()");
                ResultSet rs = stmt.getResultSet();
                rs.next();
                fail("expected an error");
            }
        } catch (SQLException e) {
            setAutoCommit(true);
            // if DERBY-3390 occurs, at this point, with networkserver/client, 
            // we'd get a 08006. In the server's derby.log you'd see a 
            // ClassCastException
            assertSQLState("38000", e);
            assertSQLState("50000", e);
            assertTrue(e.getMessage().indexOf("I refuse to return an int") > 1);
        }

        // test an Exception gets thrown for good measure
        try {
            if (usingDerbyNetClient())
            {
                stmt.execute("values badFunction3()");
                fail("expected an error");
            }
            else 
            {
                stmt.execute("values badFunction3()");
                ResultSet rs = stmt.getResultSet();
                rs.next();
                fail("expected an error");
            }
        } catch (SQLException e) {
            setAutoCommit(true);
            assertSQLState("38000", e);
            assertTrue(e.getMessage().indexOf("The exception 'java.lang.Exception: I refuse to return an int!'")==0);
        }
        
        stmt.close();
        rollback();
        setAutoCommit(true);
    }    

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws Exception {
        Connection conn = getConnection();
        String createTableWithPK = "CREATE TABLE tableWithPK (" +
                "c1 int primary key," +
                "c2 int)";
        Statement stmt = conn.createStatement();
        stmt.execute(createTableWithPK);
        stmt.execute("create function badFunction1() returns int language java"
                + " parameter style java no sql external name '" +
                SqlExceptionTest.class.getName() + ".badFunction1'");
        stmt.execute("create function badFunction2() returns int language java"
                + " parameter style java no sql external name '" +
                SqlExceptionTest.class.getName() + ".badFunction2'");
        stmt.execute("create function badFunction3() returns int language java"
                + " parameter style java no sql external name '" +
                SqlExceptionTest.class.getName() + ".badFunction3'");
        stmt.close();
        conn.close();
    }

    /**
     * Drop the table
     */
    public void tearDown() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("DROP TABLE tableWithPK");
        stmt.executeUpdate("drop function badfunction1");
        stmt.executeUpdate("drop function badfunction2");
        stmt.executeUpdate("drop function badfunction3");
        stmt.close();
        conn.close();
        super.tearDown();
    }

    /**
     * Recreate a SQLException by serializing the passed-in one and
     * deserializing it into a new one that we're returning.
     */
    private SQLException recreateSQLException(SQLException se)
    throws Exception
    {
        SQLException recreatedDS = null;

        // Serialize and recreate (deserialize) the passed-in Exception
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(se);
        oos.flush();
        oos.close();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        recreatedDS = (SQLException) ois.readObject();
        ois.close();
        assertNotNull(recreatedDS);

        return recreatedDS;
    }

    public static Test suite() {
    	if ( JDBC.vmSupportsJSR169())
    		// see DERBY-2157 for details
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
                        return new BaseTestSuite(
                "empty SqlExceptionTest - client not supported on JSR169");
    	else
        {
            Test test = TestConfiguration.defaultSuite(SqlExceptionTest.class);
            return test;
        }
    }
    
    /* <p> 
     * For testing DERBY-3390
     * This function just throws a SQLException, without SQLState 
     * </p> 
     */ 
    public static int badFunction1() 
        throws SQLException 
    { 
        throw new SQLException( "I refuse to return an int!" );
    }

    /* <p> 
     * For testing DERBY-3390
     * This function just throws a SQLException, with SQLState 
     * </p> 
     */ 
    public static int badFunction2() 
        throws SQLException 
    { 
        throw new SQLException( "I refuse to return an int!", "50000" );
    }
    
    /* <p> 
     * For testing DERBY-3390
     * This function just throws an Exception 
     * </p> 
     */ 
    public static int badFunction3() 
        throws Exception 
    { 
        throw new Exception( "I refuse to return an int!" );
    }

}
