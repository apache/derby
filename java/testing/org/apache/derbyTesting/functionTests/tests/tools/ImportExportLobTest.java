/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.
                                         tools.ImportExportLobTest

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
import java.sql.Connection;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.JDBC;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;


/**
 * This class tests import/export of a table with data types clob and blob.
 */

public class ImportExportLobTest extends ImportExportBaseTest
{

    String fileName; // main file used to perform import/export.
    String lobsFileName; // file name used to store lobs.

    public ImportExportLobTest(String name) throws SQLException {
        super(name);
        // set the file that is used by the import/export test cases.
        fileName = 
            (SupportFilesSetup.getReadWrite("books.del")).getPath();
        lobsFileName = 
            (SupportFilesSetup.getReadWrite("books_lobs.dat")).getPath();
    }

    /**
     * Runs the tests in the default embedded configuration and then
     * the client server configuration.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite(ImportExportLobTest.class);
        suite.addTest(TestConfiguration.clientServerSuite(
                             ImportExportLobTest.class));
        Test test = suite;
        test = new SupportFilesSetup(test);
        return new CleanDatabaseTestSetup(test) {
                protected void decorateSQL(Statement s) throws SQLException {
                    // table used to test  export.
                    s.execute("CREATE TABLE BOOKS(id int," +
                              "name varchar(30)," + 
                              "content clob, " + 
                              "pic blob )");
                    // load some data into the above table. 
                    loadData(s);
                    // table used to test import. 
                    s.execute("CREATE TABLE BOOKS_IMP(id int," +
                              "name varchar(30)," + 
                              "content clob," +  
                              "pic blob )");
                    // table that holds some invalid hex strings. 
                    s.execute("CREATE TABLE hex_tab(id int," +
                              "C1 varchar(20)," + 
                              "C2 varchar(20)," +
                              "C3 varchar(20))");
		    s.execute("CREATE TABLE derby_2925_lob(id int," +
			      "name varchar(30), content clob," +
			      "pic blob)");
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
        s.executeUpdate("DELETE FROM BOOKS_IMP");
        s.close();
        // delete the export files. 
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);
    }

    /**
     * Test import/export of a table, using 
     * SYSCS_EXPORT_TABLE and SYSCS_IMPORT_TABLE procedures.
     */
    public void testImportTableExportTable()  
        throws SQLException, IOException
    {
        doExportTable("APP", "BOOKS", fileName, null, null , null);
	    doImportTable("APP", "BOOKS_IMP", fileName, null, null, null, 0);
        verifyData(" * ");
    }

    
    /*
     * Test import/export of all the columns using 
     * SYSCS_EXPORT_QUERY and SYSCS_IMPORT_DATA procedures.  
     */
    public void testImportDataExportQuery() 
        throws SQLException, IOException
    {
        doExportQuery("select * from BOOKS", fileName,
                      null, null , null);
	    doImportData(null, "BOOKS_IMP", null, null, fileName, 
                     null, null, null, 0);
        verifyData(" * ");

        // perform import with column names specified in random order.
        doImportData(null, "BOOKS_IMP", "PIC, CONTENT, NAME, ID", 
                     "4, 3, 2, 1",  fileName, null, null, null, 1);
        verifyData("PIC, CONTENT, NAME, ID");
	
	//DERBY-2925: need to delete export files first
	SupportFilesSetup.deleteFile(fileName);

        // test with  non-default delimiters. 
        doExportQuery("select * from BOOKS_IMP", fileName,
                      ";", "%" , null);
	    doImportData(null, "BOOKS_IMP", null, null, fileName, 
                     ";", "%", null, 1);

    }


    /*
     * Test import of only some columns of the table 
     * using  SYSCS_EXPOR_QUERY and IMPORT_DATA procedures.  
     */
    public void testImportDataExportQueryWithFewColumns() 
        throws SQLException, IOException
    {
        doExportQuery("select id, name, content, pic from BOOKS",  
                      fileName,  null, null, null);
        doImportData(null, "BOOKS_IMP", "ID,PIC", "1 , 4",
                     fileName, null, null, null, 0);
        verifyData("ID,PIC");
        doImportData(null, "BOOKS_IMP", "ID, PIC, NAME", "1, 4, 2",
                     fileName, null, null, null, 1);
        verifyData("ID, PIC, NAME");
        doImportData(null, "BOOKS_IMP", "ID, CONTENT, NAME", "1, 3, 2",
                     fileName, null, null, null, 1);
        verifyData("ID, CONTENT, NAME");

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);

        // test with  non-default delimiters. 
        doExportQuery("select id, name, content, pic from BOOKS",  
                      fileName,  "$", "!" , null);
        doImportData(null, "BOOKS_IMP", "ID,PIC", "1 , 4",
                     fileName, "$", "!", null, 0);
    }




    /* 
     * Test import procedures with invalid hex strings in 
     * the import file for the blob column. 
     */
    public void testImportWithInvalidHexStrings() 
        throws SQLException   
    {
        Statement s = createStatement();
        // Insert row with correctly formed hex strings.  
        s.executeUpdate("insert into hex_tab " + 
                        "values(1, 'row 1', 'clob 1', 'B1C201DA')");

        // Insert row with an invalid hex string, because 
        // it's length is not a multiple of 2 (B1C201A) , 
        s.executeUpdate("insert into hex_tab " + 
                        "values(2, 'row 2', 'clob2 ', 'B1C201A')");

        // Insert row with an invalid hex string that contains 
        // a non-hex character (3122A1F20Z). 
        s.executeUpdate("insert into hex_tab " + 
                        "values(3, '', 'clobs 3', '3122A1F20Z')");

        // Insert row with an invalid hex string that contains 
        // a delimiter character (B1C2\"01DA). 
        s.executeUpdate("insert into hex_tab " + 
                        "values(4, 'row \"4', '3122A1F20Z', 'B1C2\"01DA')");
        s.close();

        // export the invalid hex strings from the table to a file. 
        doExportTable("APP", "HEX_TAB", fileName, null, null , null);

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);

        // attempt to import the invalid hex string data into a table 
        // with binary columns. It should fail.
 
        try {
            
            doExportQuery("select * from hex_tab where id <= 2",  
                          fileName,  null, null, null);
            // import should fail because of invalied hex string length
            doImportTable("APP", "BOOKS_IMP", fileName, null, null, null, 0);
            fail("import did not fail on data with invalid hex string");
        } catch (SQLException e) {
             assertSQLState("XIE0N", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);

        try {
            doExportQuery("select * from hex_tab where id = 3",  
                          fileName,  null, null, null);
            // import should fail because hex string contains invalid 
            // hex chatacters.
            doImportData(null, "BOOKS_IMP", "ID, PIC", "1,4",
                         fileName, null, null, null, 1);
            fail("import did not fail on data with invalid hex strings");
        } catch (SQLException e) {
            assertSQLState("XIE0N", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);

        try {
            doExportQuery("select * from hex_tab where id = 4",  
                          fileName,  null, null, null);
            // import should fail because hex string contains invalid 
            // hex chatacters.
            doImportData(null, "BOOKS_IMP", "ID, PIC", "1,4",
                         fileName, null, null, null, 1);
            fail("import did not fail on data with invalid hex strings");
        } catch (SQLException e) {
            assertSQLState("XIE0N", e);
        }
    }


    /**
     * Test import/export of a table, using 
     * SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE and 
     * SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE  procedures.
     */
    public void testImportTableExportTableLobsInExtFile()  
        throws SQLException, IOException
    {
        doExportTableLobsToExtFile("APP", "BOOKS", fileName, 
                                   null, null , null, lobsFileName);
	    doImportTableLobsFromExtFile("APP", "BOOKS_IMP", fileName, 
                                     null, null, null, 0);
        verifyData(" * ");
    }

    /**
     * Test import/export of a table, using 
     * SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE and 
     * SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE  procedures, 
     * with an unqualified lob data file name as parameter
     * for the export procedure.
     */
    public void testImportTableExportTableLobsInUnqalifiedExtFile()  
        throws SQLException, IOException
    {
        // test export procedure with unqulified lob data  file name
        // lob data file should get crated at the same location, where
        // the main export file is created. And also perform import/export
        // using "UTF-16" code set.
        
        doExportTableLobsToExtFile("APP", "BOOKS", fileName, 
                                    "\t", "|", "UTF-16", 
                                   "unql_books_lobs.dat");
        // DERBY-2546 - with JSR this hits a JVM issue
        if (JDBC.vmSupportsJDBC3()) 
        {
            doImportTableLobsFromExtFile("APP", "BOOKS_IMP", fileName, 
                "\t", "|", "UTF-16", 0);
            verifyData(" * ");
        }
        
    }


    
    /*
     * Test import/export of all the columns using 
     * SYSCS_EXPORT_QUERY_LOBS_TO_EXTFILE and 
     * SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE procedures.  
     */
    public void testImportDataExportQueryLobsInExtFile() 
        throws SQLException, IOException
    {
        doExportQueryLobsToExtFile("select * from BOOKS", fileName,
                                  null, null, "8859_1", lobsFileName);
	    doImportDataLobsFromExtFile(null, "BOOKS_IMP", null, null, fileName, 
                                   null, null , "8859_1", 0);
        verifyData(" * ");

        // perform import with column names specified in random order.
        doImportDataLobsFromExtFile(null, "BOOKS_IMP", "PIC, CONTENT, NAME, ID", 
                                  "4, 3, 2, 1", fileName, null, null, null, 1);
        verifyData("PIC, CONTENT, NAME, ID");

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);

        // test with  non-default delimiters. 
        doExportQueryLobsToExtFile("select * from BOOKS_IMP", fileName,
                                   ";", "%" , null, lobsFileName);
	    doImportDataLobsFromExtFile(null, "BOOKS_IMP", null, null, fileName, 
                                  ";", "%", null, 1);

    }


    /*
     * Test import of only some columns of the table 
     * using  SYSCS_EXPOR_QUERY_LOBS_TO_EXTFILE and 
     * SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE procedures.  
     */
    public void testImportDataExportQueryWithFewColsLobsInExtFile() 
        throws SQLException, IOException
    {
        doExportQueryLobsToExtFile("select id, name, content, pic from BOOKS",
                                   fileName,  null, null, null, lobsFileName);
        doImportDataLobsFromExtFile(null, "BOOKS_IMP", "ID,PIC", "1 , 4",
                                    fileName, null, null, null, 0);
        verifyData("ID,PIC");
        doImportDataLobsFromExtFile(null, "BOOKS_IMP", "ID, PIC, NAME", "1, 4, 2",
                                  fileName, null, null, null, 1);
        verifyData("ID, PIC, NAME");
        doImportDataLobsFromExtFile(null, "BOOKS_IMP", "ID, CONTENT, NAME", 
                                  "1, 3, 2", fileName, null, null, null, 1);
        verifyData("ID, CONTENT, NAME");

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);

        // test with  non-default delimiters. 
        doExportQueryLobsToExtFile("select id, name, content, pic from BOOKS",  
                                   fileName,  "$", "!" , null, lobsFileName);
        doImportDataLobsFromExtFile(null, "BOOKS_IMP", "ID,PIC", "1 , 4",
                                  fileName, "$", "!", null, 0);
    }


    /* 
     *  Test lobs in exteranl file import/export procedures 
     *  with invalid delimiters. 
     */
    public void testImportExportInvalideDelimiters() 
         throws SQLException, IOException   
    {
        try {
            doExportTableLobsToExtFile("APP", "BOOKS", fileName, 
                                       null, "9" , null, lobsFileName);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);

        try {
            doExportQueryLobsToExtFile("select * from BOOKS", fileName,
                                       "|", "f", null, lobsFileName);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);

        doExportQueryLobsToExtFile("select * from BOOKS where id < 10", 
                                   fileName, null, null, null, lobsFileName);

        try {
            doImportTableLobsFromExtFile("APP", "BOOKS_IMP", fileName, "2", 
                                         null, null, 0);
        } catch (SQLException e) {
             assertSQLState("XIE0J", e);
        }

        try {
            doImportDataLobsFromExtFile(null, "BOOKS_IMP", null, 
                                      null,  fileName, null, "c", null, 1);
        } catch (SQLException e) {
            assertSQLState("XIE0J", e);
        }
    }



    /**
     * Test import/export of a table, using 
     * SYSCS_EXPORT_TABLE_LOBS_TO_EXTFILE and 
     * SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE  procedures, with an unqualified
     * lobs file name as an argument value.
     */
    public void testImportTableExportWithInvalidLobFileName()  
        throws SQLException, IOException
    {
        // test export of lob data with lob file name parameter 
        // value as null,  it should fail.
        try {
            doExportTableLobsToExtFile("APP", "BOOKS", fileName, 
                                       null, null , null, 
                                       null);
        }catch (SQLException e) {
            assertSQLState("XIE0Q", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);

        // export of lob data into an external file.
        doExportTableLobsToExtFile("APP", "BOOKS", fileName, 
                                   null, null , null, 
                                   lobsFileName);
        // delete the lob data file, and then perfom the import.
        // import should fail with lob data file not found error. 
        SupportFilesSetup.deleteFile(lobsFileName);
        try {
            // missing lob file, refered by the main import file.
            doImportTableLobsFromExtFile("APP", "BOOKS_IMP", fileName, 
                                         null, null, null, 0);
        }catch (SQLException e) {
            assertSQLState("XIE0P", e);
        }
    }

    public void testDerby2955ExportQueryLobs()
	throws SQLException
    {
	doExportTableLobsToExtFile("APP", "DERBY_2925_LOB", fileName,
                                   "\t", "|", "UTF-16",
                                   lobsFileName);
	try {
       	    doExportTableLobsToExtFile("APP", "DERBY_2925_LOB", fileName,
                                   "\t", "|", "UTF-16",
                                   lobsFileName);
	    fail("export should have failed as the data file exists.");
	}
	catch (SQLException e) {
            assertSQLState("XIE0S", e);
        }

	//DERBY-2925: need to delete export files first
        SupportFilesSetup.deleteFile(fileName);
        SupportFilesSetup.deleteFile(lobsFileName);

	doExportTableLobsToExtFile("APP", "DERBY_2925_LOB", fileName,
                                   "\t", "|", "UTF-16",
                                   lobsFileName);
        // delete the data file, and then perform export
	// export should fail with lob file already exists error. 
	SupportFilesSetup.deleteFile(fileName);

        try {
            doExportTableLobsToExtFile("APP", "DERBY_2925_LOB", fileName,
                                   "\t", "|", "UTF-16",
                                   lobsFileName);
            fail("export should have failed as the data file exists.");
        }
        catch (SQLException e) {
            assertSQLState("XIE0T", e);
        }
    }





    /* 
     * Verifies data in the import test table (BOOKS_IMP) is same 
     * as the test table from which the data was exported earlier(BOOKS). 
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
                                             " FROM BOOKS order by id");
        Statement s2 = createStatement();
        ResultSet rsImport = s2.executeQuery("SELECT " + cols  +  
                                             " FROM BOOKS_IMP order by id");
        JDBC.assertSameContents(rsExport, rsImport);
        
        s1.close();
        s2.close();
    }
    

    /*
     * Insert data to the into the table, whose data will be exported.
     */
    private static void loadData(Statement s) throws SQLException {

        s.executeUpdate("insert into books values " + 
                        "(1, 'book 1', 'clob 1'," +  
                        "cast(X'3743640ADE12337610' as blob))");
        // rows with empty strings. 
        s.executeUpdate("insert into books values " + 
                        "(2, 'book 2', 'clob 2',  cast (X'' as blob))");
        s.executeUpdate("insert into books values " + 
                        "(3, 'book 3', '', cast(X'42' as blob))");
        s.executeUpdate("insert into books values " + 
                        "(4, 'book 4', 'clob 4',  " + 
                        "cast (X'3233445578990122558820' as blob))");
        
        // rows with a null
        s.executeUpdate("insert into books values " + 
                        "(5, null, 'clob 5'," +  
                        "cast(X'3843640ADE12337610' as blob))");
        s.executeUpdate("insert into books values " + 
                        "(6,  'book  6', null,  " + 
                        "cast(X'3843640ADE12337610' as blob))");
        s.executeUpdate("insert into books values " + 
                        "(7,  'book  7',  'clob 7' , null)");
        s.executeUpdate("insert into books values " + 
                        "(8, '', null,  cast (X'3843640ADE12' as blob))");
        s.executeUpdate("insert into books values " + 
                        "(9, 'book  9', null,  cast (X'' as blob))");
        
        // insert data that contains some delimiter characters 
        // ( "(x22) ,(x2C) %(x25) ;(x3B) , tab(9) LF(A) )
        s.executeUpdate("insert into books values " + 
                        "(10, 'book ;10', '%asdadasdasd'," + 
                        " cast (X'222C23B90A' as blob))");
        // !(x21) $(24)
        s.executeUpdate("insert into books values " + 
                        "(11, '212C3B24', '2422412221', " + 
                        "  cast (X'212421222C23B90A2124' as blob))");
        // insert some clob data with default char delimiter inside 
        // the data. It should get exported in double-delimiter format
        // when exporting to the main export file. 
        s.executeUpdate("insert into books values" +
                        "(12, 'Transaction Processing' , " +
                        "'This books covers \"Transaction\" \"processing\" concepts'"+
                        ",cast (X'144594322143423214ab35f2e54e' as blob))");
        s.executeUpdate("insert into books values" + 
                        "(13, 'effective java' ," +  
                        "'describes how to write \" quality java \" code', " +
                        "cast (X'124594322143423214ab35f2e34c' as blob))");

        // insert some more randomly genrated data.
        Connection conn = s.getConnection();
        String sql = "insert into books values(? , ? , ? , ?)";
        PreparedStatement ps = conn.prepareStatement(sql);
        int blobSize = 0;
        int id = 14;
        for (int i = 0 ; i < 17 ; i++) {
            ps.setInt(1 , id++);
            ps.setString(2 , "book" +i);
            blobSize +=  1024 * i;
            int clobSize = 1024 * i;
            Reader reader = new LoopingAlphabetReader(clobSize);
            ps.setCharacterStream(3, reader, clobSize);
            InputStream stream = new LoopingAlphabetStream(blobSize);
            ps.setBinaryStream(4, stream, blobSize);
            ps.executeUpdate();

            if ((i % 10) == 0) 
                conn.commit();
        }

        ps.close();
        conn.commit();
    }
}
