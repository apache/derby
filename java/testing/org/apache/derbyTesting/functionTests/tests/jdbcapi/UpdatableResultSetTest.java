/*
 *
 * Derby - Class UpdatableResultSetTest
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
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.*;
import java.sql.*;

/**
 * Tests updatable result sets.
 *
 * DERBY-1767 - Test that the deleteRow, insertRow and updateRow methods 
 * with column/table/schema/cursor names containing quotes.
 *
 */
public class UpdatableResultSetTest extends BaseJDBCTestCase {
    
    /** Creates a new instance of UpdatableResultSetTest */
    public UpdatableResultSetTest(String name) {
        super(name);
    }

    private Connection conn = null;
    
    protected void setUp() throws SQLException {
        conn = getConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        
        // Quoted table
        stmt.executeUpdate("create table \"my \"\"quoted\"\" table\" (x int)");
        stmt.executeUpdate("insert into \"my \"\"quoted\"\" table\" (x) " +
                "values (1), (2), (3)");
        
        // Quoted columns
        stmt.executeUpdate("create table \"my quoted columns\" " +
                "(\"my \"\"quoted\"\" column\" int)");
        stmt.executeUpdate("insert into \"my quoted columns\" " +
                "values (1), (2), (3) ");
        
        // Quoted schema
        stmt.executeUpdate("create table \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\" (x int)");
        stmt.executeUpdate("insert into \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\" values (1), (2), (3) ");
        
        // No quotes, use with quoted cursor
        stmt.executeUpdate("create table \"my table\" (x int)");
        stmt.executeUpdate("insert into \"my table\" values (1), (2), (3) ");
        
        
        
        stmt.close();
    }

    protected void tearDown() throws SQLException {
        conn.rollback();
        conn.close();
    }
    
    
    /**
     * Tests insertRow with table name containing quotes
     */
    public void testInsertRowOnQuotedTable() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\"");
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt(1, 4);
        rs.insertRow();
        rs.moveToCurrentRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\" " +
                "order by x");
        for (int i=1; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests updateRow with table name containing quotes
     */
    public void testUpdateRowOnQuotedTable() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\"");
        rs.next();
        rs.updateInt(1, 4);
        rs.updateRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\" " +
                "order by x");
        for (int i=2; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();        
    }

    /**
     * Tests deleteRow with table name containing quotes
     */
    public void testDeleteRowOnQuotedTable() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\"");
        rs.next();
        rs.deleteRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" table\" " +
                "order by x");
        for (int i=2; i<=3; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();                
    }

    /**
     * Tests insertRow with column name containing quotes
     */    
    public void testInsertRowOnQuotedColumn() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my quoted columns\"");
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt(1, 4);
        rs.insertRow();
        rs.moveToCurrentRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my quoted columns\" " +
                "order by \"my \"\"quoted\"\" column\"");
        for (int i=1; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests updateRow with column name containing quotes
     */    
    public void testUpdateRowOnQuotedColumn() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my quoted columns\"");
        rs.next();
        rs.updateInt(1, 4);
        rs.updateRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my quoted columns\" " +
                "order by \"my \"\"quoted\"\" column\"");
        for (int i=2; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();        
    }

    /**
     * Tests deleteRow with column name containing quotes
     */    
    public void testDeleteRowOnQuotedColumn() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my quoted columns\"");
        rs.next();
        rs.deleteRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my quoted columns\" " +
                "order by \"my \"\"quoted\"\" column\"");
        for (int i=2; i<=3; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();                
    }

    /**
     * Tests insertRow with schema name containing quotes
     */    
    public void testInsertRowOnQuotedSchema() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\"");
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt(1, 4);
        rs.insertRow();
        rs.moveToCurrentRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\" order by x");
        for (int i=1; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests updateRow with schema name containing quotes
     */    
    public void testUpdateRowOnQuotedSchema() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\"");
        rs.next();
        rs.updateInt(1, 4);
        rs.updateRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\" order by x");
        for (int i=2; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();        
    }

    /**
     * Tests deleteRow with schema name containing quotes
     */    
    public void testDeleteRowOnQuotedSchema() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\"");
        rs.next();
        rs.deleteRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my \"\"quoted\"\" schema\"." +
                "\"my quoted schema\" order by x");
        for (int i=2; i<=3; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();                
    }

    /**
     * Tests insertRow with cursor name containing quotes
     */    
    public void testInsertRowOnQuotedCursor() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        stmt.setCursorName("my \"\"\"\"quoted\"\"\"\" cursor\"\"");
        rs = stmt.executeQuery("select * from \"my table\"");
        rs.next();
        rs.moveToInsertRow();
        rs.updateInt(1, 4);
        rs.insertRow();
        rs.moveToCurrentRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my table\" order by x");
        for (int i=1; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();
    }

    /**
     * Tests updateRow with cursor name containing quotes
     */    
    public void testUpdateRowOnQuotedCursor() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        stmt.setCursorName("\"\"my quoted cursor");
        rs = stmt.executeQuery("select * from \"my table\"");
        rs.next();
        rs.updateInt(1, 4);
        rs.updateRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my table\" order by x");
        for (int i=2; i<=4; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();        
    }

    /**
     * Tests deleteRow with cursor name containing quotes
     */    
    public void testDeleteRowOnQuotedCursor() throws SQLException {
        ResultSet rs = null;
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                ResultSet.CONCUR_UPDATABLE);
        stmt.setCursorName("\"\"my quoted cursor\"\"");
        rs = stmt.executeQuery("select * from \"my table\"");
        rs.next();
        rs.deleteRow();
        rs.close();
        
        rs = stmt.executeQuery("select * from \"my table\" order by x");
        for (int i=2; i<=3; i++) {
            assertTrue("there is a row", rs.next());
            assertEquals("row contains correct value", i, rs.getInt(1));
        }
        rs.close();
        stmt.close();                
    }
}
