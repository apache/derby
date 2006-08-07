/*
 *
 * Derby - Class UpdateXXXTest
 *
 * Copyright 2006 The Apache Software Foundation or its
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.JDBC;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Connection;

import java.math.BigDecimal;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * Tests updateXXX() methods on updatable resultsets.
 * This is done by creating a table which has n columns with
 * different SQL types. Then there is one testcase for each
 * updateXXX method, which calls updateXXX on all columns.
 */
final public class UpdateXXXTest extends BaseJDBCTestCase
{
    /**
     * Constructor
     * @param name name of testcase. Should be the name of test method.
     */
    public UpdateXXXTest(final String name) {
        super(name);
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite();
        
        suite.addTest(new UpdateXXXTest("testUpdateShort"));
        suite.addTest(new UpdateXXXTest("testUpdateInt"));
        suite.addTest(new UpdateXXXTest("testUpdateLong"));
        suite.addTest(new UpdateXXXTest("testUpdateFloat"));
        suite.addTest(new UpdateXXXTest("testUpdateDouble"));
        suite.addTest(new UpdateXXXTest("testUpdateNull"));
        suite.addTest(new UpdateXXXTest("testUpdateObjectWithNull"));
        suite.addTest(new UpdateXXXTest("testUpdateString"));
        
        // requires java.math.BigDecimal
        if (JDBC.vmSupportsJDBC2())
            suite.addTest(new UpdateXXXTest("testUpdateBigDecimal"));
                      
        return suite;
    }
 

    /**
     * The setup creates a Connection to the database, and also
     * creates a table with one row. Then it creates an updatable
     * ResultSet which is positioned on the row.
     * @exception Exception any exception will cause test to fail with error.
     */
    public void setUp()
        throws Exception
    {
        try {
            con = getConnection();
            con.setAutoCommit(false);
            
            Statement stmt = con.createStatement();
            String createTableString = "CREATE TABLE " + TABLE_NAME + " (" +
                "F01 SMALLINT," + 
                "F02 INTEGER," +
                "F03 BIGINT," + 
                "F04 REAL," +
                "F05 FLOAT," +
                "F06 DOUBLE," +
                "F07 DECIMAL," +
                "F08 NUMERIC," +
                "F09 CHAR(100)," +
                "F10 VARCHAR(256) )";
            println(createTableString);
            stmt.executeUpdate(createTableString);
            PreparedStatement ps = con.prepareStatement
                ("insert into " + TABLE_NAME + " values(?,?,?,?,?,?,?,?,?,?)");
            
            ps.setShort(1, (short) 1);
            ps.setInt(2, 1);
            ps.setLong(3, 1L);
            ps.setFloat(4, 1.0f);
            ps.setDouble(5, 1.0);
            ps.setDouble(6, 1.0);
            
            // Use setString instead of setBigDecimal to
            // allow most of the test cases to run under J2ME
            ps.setString(7, "1");
            ps.setString(8, "1");
            
            ps.setString(9, "1");
            ps.setString(10, "1");
            ps.executeUpdate();
            
            rs = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                     ResultSet.CONCUR_UPDATABLE).
                executeQuery(SELECT_STMT);
            rs.next();
        } catch (SQLException e) {
            con.rollback();
            throw e;
        }
    }

    /**
     * Teardown test.
     * Rollback connection and close it.
     * @exception Exceptions causes the test to fail with error
     */
    public void tearDown() 
        throws Exception 
    {
        try { 
            con.rollback();
            con.close();
        } catch (SQLException e) {
            printStackTrace(e);
        }      
    }
        
    /**
     * Tests calling updateString on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateString() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateString(i, "2");
            assertEquals("Expected rs.getDouble(" + i + 
                         ") to match updated value", 2, (int) rs.getDouble(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
        
    }

    /**
     * Tests calling updateInt on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateInt() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateInt(i, 2);
            assertEquals("Expected rs.getInt(" + i + 
                         ") to match updated value", 2, rs.getInt(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }

    /**
     * Tests calling updateLong on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateLong() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateLong(i, 2L);
            assertEquals("Expected rs.getLong(" + i + 
                         ") to match updated value", 2L, rs.getLong(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }

    /**
     * Tests calling updateShort on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateShort() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateShort(i, (short) 2);
            assertEquals("Expected rs.getShort(" + i + 
                         ") to match updated value", 2, (int) rs.getShort(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }
    
    /**
     * Tests calling updateFloat on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateFloat() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateFloat(i, 2.0f);
            assertEquals("Expected rs.getFloat(" + i + 
                         ") to match updated value", 2, (int) rs.getFloat(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }
    
    /**
     * Tests calling updateDouble on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateDouble() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateDouble(i, 2.0);
            assertEquals("Expected rs.getDouble(" + i + 
                         ") to match updated value", 2, (int) rs.getDouble(i));
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }

    /**
     * Tests calling update on all columns of the row.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateBigDecimal() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateBigDecimal(i, BigDecimal.valueOf(2L));
            assertEquals("Expected rs.getBigDecimal(" + i + 
                         ") to match updated value", 2, 
                         rs.getBigDecimal(i).intValue());
        }
        rs.updateRow();
        checkColumnsAreUpdated();
    }
    
    /**
     * Tests calling updateObject with a null value on all columns.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateObjectWithNull() 
        throws SQLException
    {
        Object value = null;
        
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateObject(i, value);
            assertNull("Expected rs.getObject(" + i + ") to be null", 
                       rs.getObject(i));
            assertTrue("Expected rs.wasNull() to return true",
                       rs.wasNull());
        }
        rs.updateRow();
        checkColumnsAreNull();
    }

    /**
     * Tests calling setNull on all columns
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    public void testUpdateNull() 
        throws SQLException
    {
        for (int i = 1; i <= COLUMNS; i++) {
            rs.updateNull(i);
            assertNull("Expected rs.getObject(" + i + ") to be null", 
                       rs.getObject(i));
            assertTrue("Expected rs.wasNull() to return true",
                       rs.wasNull());
        }
        rs.updateRow();
        checkColumnsAreNull();
    }

    /**
     * Checks that the columns in the row are all SQL null.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    private void checkColumnsAreNull() 
        throws SQLException
    {
        rs.close();
        
        rs = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                 ResultSet.CONCUR_READ_ONLY).
            executeQuery(SELECT_STMT);
        
        rs.next();
        
        for (int i = 1; i <= COLUMNS; i++) {
            assertNull("Expected column " + i + " to be null", 
                       rs.getObject(i));
            assertTrue("Expected wasNull() after reading column " + i +
                       " to be true when data is SQL Null on column", 
                       rs.wasNull());
        }
    }

    /**
     * Checks that the columns in the row are updated in the database.
     * Using a new ResultSet to do this check.
     * @exception SQLException database access error. Causes test to 
     *                         fail with an error.
     */
    private void checkColumnsAreUpdated() 
        throws SQLException
    {
        rs.close();
        
        rs = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
                                 ResultSet.CONCUR_READ_ONLY).
            executeQuery(SELECT_STMT);
        
        rs.next();
        for (int i = 1; i <= COLUMNS; i++) {
            int expectedVal = 2;
            
            // Since rs.getInt(i) on CHAR/VARCHAR columns with value 2.0 gives:
            // "ERROR 22018: Invalid character string format for type int"
            // we use getDouble(i). We cast it to int, because there is not
            // assertEquals(..) methods which takes double.
            int actualVal = (int) rs.getDouble(i); 
            assertEquals("Unexpected value from rs.getDouble( + " + i + ")",
                         expectedVal, actualVal);
        }
    }
    
    /* Updatable ResultSet */
    private ResultSet rs = null;
    
    /* Connection */
    private Connection con = null;
    
    /* Table name */
    private static final String TABLE_NAME = "MultiTypeTable";

    /* SQL String for the SELECT statement */
    private static final  String SELECT_STMT = 
        "SELECT * FROM " + TABLE_NAME;
                             
    /* Number of columns in table */
    private static final int COLUMNS = 10;
}
