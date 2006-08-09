/*
 *
 * Derby - Class URCoveringIndexTest
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
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import junit.framework.*;
import java.sql.*;

/**
 * Tests updatable result sets when there is a index that includes all data for 
 * the query (covering index).
 *
 * DERBY-1087
 *
 * @author Fernanda Pizzorno
 */
public class URCoveringIndexTest extends BaseJDBCTestCase {
    
    /** Creates a new instance of SURBaseTest */
    public URCoveringIndexTest(String name) {
        super(name);
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws  Exception {       
        con = getConnection();
        con.setAutoCommit(false);

        String createTableWithPK = "CREATE TABLE tableWithPK (" +
                "c1 int primary key," +
                "c2 int)";
        String insertData = "INSERT INTO tableWithPK values (1, 1)";
        Statement stmt = con.createStatement();
        stmt.execute(createTableWithPK);
        
        stmt.execute(insertData);
    }
    
    /**
     * Rollback the transaction
     */
    public void tearDown() throws Exception {
        println("TearDown");
        try { 
            con.rollback();
            con.close();
        } catch (SQLException e) {
            printStackTrace(e);
        }      
    }
    
    private void testUpdateUpdatedTupleWithCoveringIndex(
            boolean scroll,
            boolean usePositionedUpdate) throws SQLException{
        
        SQLWarning w = null;
        int resultsetType = scroll ? ResultSet.TYPE_SCROLL_INSENSITIVE :
                ResultSet.TYPE_FORWARD_ONLY;
        
        if (!(con.getMetaData().supportsResultSetConcurrency(resultsetType,
                ResultSet.CONCUR_UPDATABLE))) {
            return;
        }

            
        Statement updStmt = con.createStatement(resultsetType, 
                ResultSet.CONCUR_UPDATABLE);
        Statement roStmt = con.createStatement();
        
        ResultSet rs = updStmt.executeQuery("SELECT c1 FROM tableWithPK");
        rs.next();
        int orig_c1 = rs.getInt(1);
        roStmt.executeUpdate("UPDATE tableWithPK SET c1 = " + 
                (orig_c1 + 10) + "WHERE c1 = " + rs.getInt(1));
        rs.clearWarnings();
        if (usePositionedUpdate) {
            roStmt.executeUpdate("UPDATE tableWithPK set c1 = " + 
                    (orig_c1 + 20) + "WHERE CURRENT OF " + 
                    rs.getCursorName());
            w = roStmt.getWarnings();
        } else {
            rs.updateInt(1, (orig_c1 + 20));
            rs.updateRow();
            w = rs.getWarnings();
        }
        assertTrue("Update should not produce any warnings ", w == null);
        rs.close();
        
        rs = roStmt.executeQuery("SELECT c1 FROM tableWithPK");
        rs.next();
        assertEquals("Expecting c1 to be " + orig_c1 + " + 20", 
                rs.getInt(1), (orig_c1 + 20));
        rs.close();
        roStmt.close();
        updStmt.close();

    }

    /**
     * Updates a previously updated row with a covering index using positioned
     * updates and scrollable result sets.
     */
    public void testUpdateUpdatedTupleScrollPostitioned()  throws SQLException{
        testUpdateUpdatedTupleWithCoveringIndex(true, true);
    }

    /**
     * Updates a previously updated row with a covering index using updateRow
     * and scrollable result sets.
     */
    public void testUpdateUpdatedTupleScrollUpdateRow()  throws SQLException{
        testUpdateUpdatedTupleWithCoveringIndex(true, false);
    }

    /**
     * Updates a previously updated row with a covering index using positioned
     * updates and forward only result sets.
     */
    public void testUpdateUpdatedTupleFOPositioned()  throws SQLException{
        testUpdateUpdatedTupleWithCoveringIndex(false, true);
    }

    /**
     * Updates a previously updated row with a covering index using updateRow
     * and forward only result sets.
     */
    public void testUpdateUpdatedTupleFOUpdateRow()  throws SQLException{
        testUpdateUpdatedTupleWithCoveringIndex(false, false);
    }


    protected Connection con = null; // Connection established in setUp()
       
}
