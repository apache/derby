/*
 
   Derby - Class CallableStatementTestSetup
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.Assert;
import junit.framework.Test;
import junit.extensions.TestSetup;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;

import java.io.UnsupportedEncodingException;
import java.sql.*;

/**
 * Create the necessary tables, function and procedures for running the 
 * CallableStatement tests under JDK 1.6.
 * Java methods used as functions and procedures are also implemented here,
 * along with helper methods that returns CallableStatements for the various
 * functions and procedures.
 */
public class CallableStatementTestSetup
    extends BaseJDBCTestSetup {

    private static final String SOURCECLASS = "org.apache.derbyTesting." +
        "functionTests.tests.jdbc4.CallableStatementTestSetup.";
    
    /** List of tables to drop on tear-down */
    private static final String[] TABLE_DROPS = new String[] {
        "CSDATA"};
    /** List of functions to drop on tear-down. */
    private static final String[] FUNCTION_DROPS = new String[] {
        "INT_TO_STRING", "GET_BINARY_DB", "GET_VARCHAR_DB"};
    /** List of procedures to drop on tear-down. */
    private static final String[] PROCEDURE_DROPS = new String[] {
        "GET_BINARY_DIRECT"};

    /** Id for row with byte representation of a string. */
    public static final int STRING_BYTES_ID = 1;
    /** 
     * String converted to bytes in UTF-16BE representation. 
     * Note that the charset used matters, and for Derby it must be UTF-16BE.
     */
    public static final String STRING_BYTES =
        "This is a string, converted to bytes and inserted into the database";

    /** Id for row with SQL NULL values. */
    public static final int SQL_NULL_ID = 2;
    
    /**
     * Create a new test setup for the CallableStatementTest.
     *
     * @param test the test/suite to provide setup for.
     */
    public CallableStatementTestSetup(Test test) {
        super(test);
    }

    protected void setUp()
        throws SQLException {
        Connection con = getConnection();
        // Create the tables, functions and procedures we need.
        Statement stmt = con.createStatement();
        // Create table CSDATA and populate
        stmt.execute("CREATE TABLE CSDATA (ID INT PRIMARY KEY," +
                "BINARYDATA VARCHAR(256) FOR BIT DATA, " +
                "CHARDATA VARCHAR(256))");
        PreparedStatement pStmt = 
            con.prepareStatement("INSERT INTO CSDATA VALUES (?,?,?)");
        pStmt.setInt(1, STRING_BYTES_ID);
        try {
            pStmt.setBytes(2, STRING_BYTES.getBytes("UTF-16BE"));
        } catch (UnsupportedEncodingException uee) {
           SQLException sqle = new SQLException(uee.getMessage());
           sqle.initCause(uee);
           throw sqle;
        }
        pStmt.setString(3, STRING_BYTES);
        pStmt.execute();
        pStmt.setInt(1, SQL_NULL_ID);
        pStmt.setNull(2, Types.VARBINARY);
        pStmt.setNull(3, Types.VARCHAR);
        pStmt.execute();
        pStmt.close();

        // Create function INT_TO_STRING
        stmt.execute("CREATE FUNCTION INT_TO_STRING(INTNUM INT) " +
                "RETURNS VARCHAR(10) " +
                "PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA " +
                "EXTERNAL NAME 'java.lang.Integer.toString'");
        // Create procedure GET_BINARY_DIRECT
        stmt.execute("CREATE PROCEDURE GET_BINARY_DIRECT(IN INSTRING " +
                "VARCHAR(40), OUT OUTBYTES VARCHAR(160) FOR BIT DATA) " +
                "DYNAMIC RESULT SETS 0 " +
                "PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA " +
                "EXTERNAL NAME '" + SOURCECLASS + "getBinaryDirect'");
        // Create function GET_BINARY_DB
        stmt.execute("CREATE FUNCTION GET_BINARY_DB(ID INT) " +
                "RETURNS VARCHAR(256) FOR BIT DATA " +
                "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA " +
                "EXTERNAL NAME '" + SOURCECLASS + "getBinaryFromDb'");
        // Create function GET_VARCHAR_DB
        stmt.execute("CREATE FUNCTION GET_VARCHAR_DB(ID INT) " +
                "RETURNS VARCHAR(256) " +
                "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA " +
                "EXTERNAL NAME '" + SOURCECLASS + "getVarcharFromDb'");
        stmt.close();
    }

    protected void tearDown()
        throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        // Drop functions
        for (String function : FUNCTION_DROPS) {
            stmt.execute("DROP FUNCTION "  + function);
        }
        // Drop procedures
        for (String procedure : PROCEDURE_DROPS) {
            stmt.execute("DROP PROCEDURE "  + procedure);
        }
        // Drop tables
        for (String table : TABLE_DROPS) {
            stmt.execute("DROP TABLE "  + table);
        }
        stmt.close();
        super.tearDown();
    }

    // Methods for getting CallableStatements

    /**
     * Return function converting an integer to a string.
     * Parameter 1: output - String/VARCHAR
     * Parameter 2: input  - int/INT
     */
    public static CallableStatement getIntToStringFunction(Connection con)
        throws SQLException {
        Assert.assertNotNull("Connection cannot be null", con);
        CallableStatement cStmt = con.prepareCall("?= CALL INT_TO_STRING(?)");
        cStmt.registerOutParameter(1, Types.VARCHAR);
        return cStmt;
    }
    
    /**
     * Return statement for calling procedure that converts a string to a 
     * byte array (UTF-16BE charset).
     * Parameter 1: input  - String/VARCHAR(40)
     * Parameter 2: output - byte[]/VARCHAR(160) FOR BIT DATA
     */
    public static CallableStatement getBinaryDirectProcedure(Connection con)
        throws SQLException {
        Assert.assertNotNull("Connection cannot be null", con);
        CallableStatement cStmt = 
            con.prepareCall("CALL GET_BINARY_DIRECT(?,?)");
        cStmt.registerOutParameter(2, Types.VARBINARY);
        return cStmt;
    }

    /**
     * Return statement for calling getBinaryFromDb function.
     * Parameter 1: return/output - byte[]/VARCHAR FOR BINARY - data from db
     * Parameter 2: input         - int/INT - id for row to fetch
     *
     * @param con database connection.
     * @return statement for executing getBinaryFromDb function.
     */
    public static CallableStatement getBinaryFromDbFunction(Connection con)
        throws SQLException {
        Assert.assertNotNull("Connection cannot be null", con);
        CallableStatement cStmt =
            con.prepareCall("?= CALL GET_BINARY_DB(?)");
        cStmt.registerOutParameter(1, Types.VARBINARY);
        return cStmt;
    }

    /**
     * Return statement for calling getVarcharFromDb function.
     * Parameter 1: return/output - String/VARCHAR - data from db
     * Parameter 2: input         - int/INT - id for row to fetch
     *
     * @param con database connection.
     * @return statement for executing getVarcharFromDb function.
     */
    public static CallableStatement getVarcharFromDbFunction(Connection con)
        throws SQLException {
        Assert.assertNotNull("Connection cannot be null", con);
        CallableStatement cStmt =
            con.prepareCall("?= CALL GET_VARCHAR_DB(?)");
        cStmt.registerOutParameter(1, Types.VARCHAR);
        return cStmt;
    }

    // Methods used as functions and procedures in the db
    
    /**
     * Procedure creating a byte representation of a string.
     *
     * @param inputString a string.
     * @param outputByte string returned as UTF-16BE byte representation.
     */
    public static void getBinaryDirect(String inputString, byte[][] outputByte) {
        try {
            outputByte[0] = inputString.getBytes("UTF-16BE");
        } catch (java.io.UnsupportedEncodingException uee) {
            outputByte[0] = new byte[0];
        }
    }
    
    /**
     * Function fetching binary data from the database.
     *
     * @param id id of row to fetch.
     * @return a byte array.
     */
    public static byte[] getBinaryFromDb(int id) 
        throws Exception {
        Connection con = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT BINARYDATA FROM CSDATA " +
                "WHERE ID = " + id);
        rs.next();
        byte[] bytes = rs.getBytes(1);
        rs.close();
        stmt.close();
        con.close();
        return bytes;
    }
    
    /**
     * Function fetching character data from the database.
     * 
     * @param id id of row to fetch.
     * @return a string.
     */
    public static String getVarcharFromDb(int id) 
        throws Exception {
        Connection con = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT CHARDATA FROM CSDATA " +
                "WHERE ID = " + id);
        rs.next();
        String chardata = rs.getString(1);
        rs.close();
        stmt.close();
        con.close();
        return chardata;
    }

}
