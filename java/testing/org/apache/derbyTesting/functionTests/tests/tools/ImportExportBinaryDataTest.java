/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.
                                         tools.ImportExportBinaryDataTest

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
package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.JDBC;

/**
 * This class tests import/export of  a table with simple binary data types 
 * CHAR FOR BIT DATA, VARCHAR FOR BIT DATA,  LONG VARCHAR FOR BIT DATA.
 */

public class ImportExportBinaryDataTest extends ImportExportBaseTest {

    String fileName; // file used to perform import/export.

    public ImportExportBinaryDataTest(String name) {
        super(name);
        // set the file that is used by the import/export. 
        fileName = 
            (SupportFilesSetup.getReadWrite("bin_tab.del")).getPath();
    }

    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite(ImportExportBinaryDataTest.class);
        suite.addTest(TestConfiguration.clientServerSuite(
                      ImportExportBinaryDataTest.class));
        Test test = suite;
        test = new SupportFilesSetup(test);
        return new CleanDatabaseTestSetup(test) {
                protected void decorateSQL(Statement s) throws SQLException {
                    // table used to test  export.
                    s.execute("CREATE TABLE BIN_TAB (id int," +
                              "C_BD CHAR(4) FOR BIT DATA," + 
                              "C_VBD VARCHAR(10) FOR BIT DATA, " +
                              "C_LVBD LONG VARCHAR FOR BIT DATA)");
                    // load some data into the above table. 
                    loadData(s);
                    // table used to test import. 
                    s.execute("CREATE TABLE BIN_TAB_IMP(id int," +
                              "C_BD CHAR(4) FOR BIT DATA," + 
                              "C_VBD VARCHAR(10) FOR BIT DATA, " +
                              "C_LVBD LONG VARCHAR FOR BIT DATA)");
                    // Create a table that holds some invalid hex strings. 
                    s.execute("CREATE TABLE hex_tab(id int," +
                              "C1 varchar(20)," + 
                              "C2 varchar(20)," +
                              "C3 varchar(20))");
                }
            };
    }

    
    /**
     * Simple set up, just empty the import table.
     * @throws SQLException 
     */
    protected void setUp() throws SQLException
    {
        Statement s  = createStatement();
        // delete the rows from the import table.
        s.executeUpdate("DELETE FROM BIN_TAB_IMP");
        s.close();
    }
    

    /**
     * Test import/export of a table, using 
     * SYSCS_EXPORT_TABLE and SYSCS_IMPORT_TABLE procedures.
     */
    public void testImportTableExportTable()  
        throws SQLException, IOException
    {
        doExportTable("APP", "BIN_TAB", fileName, null, null , null);
	    doImportTable("APP", "BIN_TAB_IMP", fileName, null, null, null, 0);
        verifyData(" * ");
    }

    
    /*
     * Test import/export of all the columns using 
     * SYSCS_EXPORT_QUERY and SYSCS_IMPORT_DATA procedures.  
     */
    public void testImportDataExportQuery() 
        throws SQLException, IOException
    {
        doExportQuery("select * from BIN_TAB", fileName,
                      null, null , null);
	    doImportData(null, "BIN_TAB_IMP", null, null, fileName, 
                     null, null, null, 0);
        verifyData(" * ");

        // perform import with column names specified in random order.
        doImportData(null, "BIN_TAB_IMP", "C_LVBD, C_VBD, C_BD, ID", 
                     "4, 3, 2, 1",  fileName, null, null, null, 1);
        verifyData("C_LVBD, C_VBD, C_BD, ID");

        // test with  non-default delimiters. 
        doExportQuery("select * from BIN_TAB", fileName,
                      ";", "%" , null);
	    doImportData(null, "BIN_TAB_IMP", null, null, fileName, 
                     ";", "%", null, 1);

    }


    /*
     * Test import of only some columns of the table 
     * using  SYSCS_EXPOR_QUERY and IMPORT_DATA procedures.  
     */
    public void testImportDataExportQueryWithFewColumns() 
        throws SQLException, IOException
    {
        doExportQuery("select id, c_bd, c_vbd, c_lvbd from BIN_TAB",  
                      fileName,  null, null, null);
        doImportData(null, "BIN_TAB_IMP", "ID,C_LVBD", "1 , 4",
                     fileName, null, null, null, 0);
        verifyData("ID,C_LVBD");
        doImportData(null, "BIN_TAB_IMP", "ID, C_LVBD, C_BD", "1, 4, 2",
                     fileName, null, null, null, 1);
        verifyData("ID, C_LVBD, C_BD");
        doImportData(null, "BIN_TAB_IMP", "ID, C_VBD, C_BD", "1, 3, 2",
                     fileName, null, null, null, 1);
        verifyData("ID, C_VBD, C_BD");

        // test with  non-default delimiters. 
        doExportQuery("select id, c_bd, c_vbd, c_lvbd from BIN_TAB",  
                      fileName,  "$", "!" , null);
        doImportData(null, "BIN_TAB_IMP", "ID,C_LVBD", "1 , 4",
                     fileName, "$", "!", null, 0);
    }


    /* 
     *  Tests import/export procedures with invalid
     *  hex decimal characters (0-9, a-f, A-F)  as delimiters. 
     */
    public void testImportExportInvalideDelimiters() 
         throws SQLException, IOException   
    {
        try {
            doExportTable("APP", "BIN_TAB", fileName, null, "9" , null);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }

        try {
            doExportQuery("select * from BIN_TAB", fileName,
                          "|", "f", null);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }

        try {
            doExportTable("APP", "BIN_TAB", fileName, "B", null , null);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }

        doExportTable("APP", "BIN_TAB", fileName, null, null , null);

        /* Currently BaseJDBCTestCase.assertSQLState() is unable
         * to find nested SQLSTATEs with 1.6 JVMs, so we have to
         * check for the top-level SQLSTATE in that case.  When
         * that changes the "JDBC.vmSupportsJDBC4()" call can be
         * removed from the following assertSQLState() calls.
         * (DERBY-1440)
         */

        try {
            doImportTable("APP", "BIN_TAB_IMP", fileName, "2", null, null, 0);
        } catch (SQLException e) {
             assertSQLState(JDBC.vmSupportsJDBC4() ? "38000": "XIE0J", e);
        }

        try {
            doImportData(null, "BIN_TAB_IMP", null, 
                         null,  fileName, null, "c", null, 1);
        } catch (SQLException e) {
            assertSQLState(JDBC.vmSupportsJDBC4() ? "38000": "XIE0J", e);
        }
    }


    /* 
     * Tests import procedures with invalid hex strings in 
     * the import file. 
     */
    public void testImportWitgInvalidHexStrings() 
        throws SQLException   
    {
        Statement s = createStatement();
        // Insert row with correctly formed hex strings.  
        s.executeUpdate("insert into hex_tab " + 
                        "values(1, 'A121', '3122A1F20B', 'B1C201DA')");

        // Insert row with an invalid hex string, because 
        // it's length is not a multiple of 2 (B1C201A) , 
        s.executeUpdate("insert into hex_tab " + 
                        "values(2, 'A121', '3122A1F20B', 'B1C201A')");

        // Insert row with an invalid hex string that contains 
        // a non-hex character (3122A1F20Z). 
        s.executeUpdate("insert into hex_tab " + 
                        "values(3, '', '3122A1F20Z', 'B1C201DA')");

        // Insert row with an invalid hex string that contains 
        // a delimiter character (A1\"21). 
        s.executeUpdate("insert into hex_tab " + 
                        "values(3, 'A1\"21', '3122A1F20Z', 'B1C201DA')");
        s.close();

        // export the invalid hex strings from the table to a file. 
        doExportTable("APP", "HEX_TAB", fileName, null, null , null);


        // attempt to import the invalid hex string data into a table 
        // with binary columns. It should fail.
 
        try {
            // import should fail because of invalied hex string length
            doImportTable("APP", "BIN_TAB_IMP", fileName, null, null, null, 0);
            fail("import did not fail on data with invalid hex string");
        } catch (SQLException e) {
             assertSQLState(JDBC.vmSupportsJDBC4() ? "XIE0R": "XIE0N", e);
        }

        try {
            // import should fail because hex string contains invalid 
            // hex chatacters.
            doImportData(null, "BIN_TAB_IMP", "ID, C_VBD", "1,3",
                         fileName, null, null, null, 1);
            fail("import did not fail on data with invalid hex strings");
        } catch (SQLException e) {
            assertSQLState(JDBC.vmSupportsJDBC4() ? "XIE0R": "XIE0N", e);
        }
        
        try {
            // import should fail because hex string contains invalid 
            // hex chatacters.
            doImportData(null, "BIN_TAB_IMP", "ID, C_VBD", "1,2",
                         fileName, null, null, null, 1);
            fail("import did not fail on data with invalid hex strings");
        } catch (SQLException e) {
            assertSQLState(JDBC.vmSupportsJDBC4() ? "XIE0R": "XIE0N", e);
        }
    }


    /* 
     * Verifies data in the import test table (BIN_TAB_IMP) is same 
     * as the test table from which the data was exported earlier(BIN_TAB). 
     * @param cols  imported columns , if all then " * ", otherwise 
     *              comma separated column list. 
     * @exception SQLException  if the data does match or if 
     *                          any other error during comparision.  
     */
    private void verifyData(String cols)  
        throws SQLException, IOException
    {
        Statement s1 = createStatement();
        ResultSet rsExport = s1.executeQuery("SELECT " + cols  +  
                                             " FROM BIN_TAB order by id");
        Statement s2 = createStatement();
        ResultSet rsImport = s2.executeQuery("SELECT " + cols  +  
                                             " FROM BIN_TAB_IMP order by id");
        JDBC.assertSameContents(rsExport, rsImport);
        
        s1.close();
        s2.close();
    }
    
    
    /*
     * Insert data to the into the table, whose data will be exported.
     */
    private static void loadData(Statement s) throws SQLException {
        s.executeUpdate("insert into bin_tab values " + 
                        "(1, X'31', X'3241510B',  X'3743640ADE12337610')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(2, X'33', X'3341610B',  X'3843640ADE12337610')");
        // rows with empty strings. 
        s.executeUpdate("insert into bin_tab values " + 
                        "(4, X'41', X'42',  X'')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(5, X'41', X'', X'42')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(6, X'', X'42',  X'3233445578990122558820')");
        
        // rows with a null
        s.executeUpdate("insert into bin_tab values " + 
                        "(7, null, X'3341610B',  X'3843640ADE12337610')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(8,  X'3341610B', null,  X'3843640ADE12337610')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(9,  X'3341610B',  X'3843640ADE' , null)");

        s.executeUpdate("insert into bin_tab values " + 
                        "(10, X'', null,  X'3843640ADE12')");
        s.executeUpdate("insert into bin_tab values " + 
                        "(11, X'66', null,  X'')");
        
        // insert data that contains some delimiter characters 
        // ( "(x22) ,(x2C) %(x25) ;(x3B) , tab(9) LF(A) )
        s.executeUpdate("insert into bin_tab values " + 
                        "(12, X'2C313B09', X'224122',  X'222C23B90A')");
        // !(x21) $(24)
        s.executeUpdate("insert into bin_tab values " + 
                        "(13, X'212C3B24', X'2422412221', " + 
                        "  X'212421222C23B90A2124')");
    }
}
