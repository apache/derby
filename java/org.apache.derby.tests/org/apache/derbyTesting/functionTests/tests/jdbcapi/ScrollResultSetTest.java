/*
 *
 * Derby - Class ScrollResultSetTest
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Tests scrollable result sets
 *
 *
 * Tests:
 * - testNextOnLastRowForwardOnly: tests that the result set is closed when all
 * rows have been retrieved and next has been called from the last row,
 * autocommit = true, the result set is not holdable and type forward 
 * only. (DERBY-1295)
 * - testNextOnLastRowScrollable: tests that the result set is not closed when 
 * next is called while the result set is positioned in the last row, 
 * autocommit = true, the result set is not holdable type scrollable 
 * insensitive. (DERBY-1295)
 * - testDerby6737: tests that a LOB can be accessed after using an absolute
 * positioning method to move the position to the row on which the result set
 * is currently positioned. (DERBY-6737)
 *
 */
public class ScrollResultSetTest extends BaseJDBCTestCase {
    
    /** Creates a new instance of ScrollResultSetTest */
    public ScrollResultSetTest(String name) {
        super(name);
    }
    
    public static Test suite() {
                
        // Requires holdability
        if (JDBC.vmSupportsJDBC3() || JDBC.vmSupportsJSR169()) {
            // Run embedded and client
        	return TestConfiguration.defaultSuite(ScrollResultSetTest.class);
        }
        
        // empty suite, no holdability supported.
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        return new BaseTestSuite(
                "Empty ScrollResultSetTest suite, no support for holdability");
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws  Exception {       
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        Connection con = getConnection();
        con.setAutoCommit(true);

        String createTableWithPK = "CREATE TABLE tableWithPK (" +
                "c1 int primary key," +
                "c2 int)";
        String insertData = "INSERT INTO tableWithPK values " +
                "(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)";
        Statement stmt = con.createStatement();
        stmt.execute(createTableWithPK);
        
        stmt.execute(insertData);
//IC see: https://issues.apache.org/jira/browse/DERBY-1701
        stmt.close();
    }
    
    /**
     * Drop the table
     */
    public void tearDown() throws Exception {
        println("TearDown");
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        Statement s = getConnection().createStatement();
        try { 
            
            s.executeUpdate("DROP TABLE tableWithPK");
         } catch (SQLException e) {
            printStackTrace(e);
        }    
        s.close();
        super.tearDown();
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
//IC see: https://issues.apache.org/jira/browse/DERBY-1701

    }
    
    /**
     * Test that moving to next row after positioned at the last row on a 
     * forward only result set will close the result set
     */
    public void testNextOnLastRowForwardOnly()  throws SQLException{

        Connection con = getConnection();
        con.setAutoCommit(true);
        con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Statement roStmt = con.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = roStmt.executeQuery("SELECT c1 FROM tableWithPK");

        // call next until positioned after last
        while (rs.next());
        
        try {
            // forward only result set should be closed now, an exception will
            // be thrown
            rs.next();
            assertTrue("Excepted exception to be thrown - result set is closed", 
                       false);
        } catch (SQLException se) {
                assertSQLState("Unexpected SQL State",
                               SQLStateConstants.RESULT_SET_IS_CLOSED, se);
        }
    }

    /**
     * Test that moving to next row after positioned at the last row on a 
     * scrollable result set will not close the result set
     */
    public void testNextOnLastRowScrollable()  throws SQLException{

//IC see: https://issues.apache.org/jira/browse/DERBY-1555
//IC see: https://issues.apache.org/jira/browse/DERBY-1555
        Connection con = getConnection();
        con.setAutoCommit(true);
        con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        Statement roStmt = con.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, 
                ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = roStmt.executeQuery("SELECT c1 FROM tableWithPK");
        // move to last position and then call next
        rs.last();
        rs.next();
        
        // scrollable result set should still be open and not throw no 
        // exception will be thrown
        assertFalse("Calling next while positioned after last returns " +
                "false", rs.next());
        assertTrue("Moving to absolute(2) returns true", rs.absolute(2));
        rs.close();

    }

    /**
     * <p>
     * Test that it is possible to access LOBs after moving the position of
     * a scrollable result set from one row to the same row. Before DERBY-6737
     * the following sequence of calls
     * </p>
     *
     * <pre>
     * rs.last();
     * rs.first();
     * rs.getClob(1).length();
     * </pre>
     *
     * <p>
     * would fail with 'invalid locator' on the client, if the result set
     * contained only one row.
     * </p>
     */
    public void testDerby6737() throws SQLException {
        Statement s = createStatement();
        s.execute("create table d6737(c clob)");
        s.execute("insert into d6737 values 'abc'");

        PreparedStatement ps = prepareStatement("select * from d6737",
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = ps.executeQuery();
        assertTrue(rs.last());
        assertTrue(rs.first());
        assertEquals(3, rs.getClob(1).length()); // used to fail on client
        rs.close();

        rs = ps.executeQuery();
        assertTrue(rs.next());
        assertTrue(rs.first());
        assertEquals(3, rs.getClob(1).length()); // used to fail on client
        rs.close();

        rs = ps.executeQuery();
        assertTrue(rs.first());
        assertTrue(rs.last());
        assertEquals(3, rs.getClob(1).length()); // used to fail on client
        rs.close();

        rs = ps.executeQuery();
        assertTrue(rs.last());
        assertTrue(rs.absolute(1));
        assertEquals(3, rs.getClob(1).length()); // used to fail on client
        rs.close();

        // This case, where the CLOB had been accessed, but not explicitly
        // freed before the result set was repositioned, passed even before
        // the fix.
        rs = ps.executeQuery();
        assertTrue(rs.last());
        assertEquals(3, rs.getClob(1).length());
        assertTrue(rs.first());
        assertEquals(3, rs.getClob(1).length());
        rs.close();

        // But it failed if the CLOB was explicitly freed before repositioning.
        rs = ps.executeQuery();
        assertTrue(rs.last());
        rs.getClob(1).free();
        assertTrue(rs.first());
        assertEquals(3, rs.getClob(1).length()); // used to fail on client
        rs.close();
    }
}
