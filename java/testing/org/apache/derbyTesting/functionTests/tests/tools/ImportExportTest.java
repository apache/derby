/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ImportExportTest

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
import java.sql.Statement;
import java.sql.SQLException;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test import and export procedures 
 */
public class ImportExportTest extends BaseJDBCTestCase {

	public ImportExportTest(String name) {
		super(name);
	}
	
	public static Test suite() {
        TestSuite suite = new TestSuite("ImportExportTest");

        // disabled on weme6.1 due at the moment due 
        // to problems with security exceptions.
        if (JDBC.vmSupportsJSR169())
        {
        	return new TestSuite();
        }
        suite.addTest(baseSuite("ImportExportTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("ImportExportTest:client")));    
        return suite;
	}
	
	public static Test baseSuite(String name) {
		TestSuite suite = new TestSuite(ImportExportTest.class, name);
		Test test = new SupportFilesSetup(suite, new String[] {"functionTests/testData/ImportExport/TwoLineBadEOF.dat"} );
		return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {

                s.execute("CREATE TABLE T1 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
						   "COLUMN3 SMALLINT , COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
						   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
						   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
						   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1))");
                s.execute("CREATE TABLE T2 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
						   "COLUMN3 SMALLINT, COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
						   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
						   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
						   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1))");
                s.execute("create table T4 (   Account int,    Name   char(30), Jobdesc char(40), " +
                           "Company varchar(35), Address1 varchar(40), Address2 varchar(40), " +
                           "City    varchar(20), State   char(5), Zip char(10), Country char(10), " +
                           "Phone1  char(20), Phone2  char(20), email   char(30), web     char(30), " +
                           "Fname   char(30), Lname   char(30), Comment char(30), AccDate char(30), " +
                           "Payment decimal(8,2), Balance decimal(8,2))");
                }
        };
	}
	
	public void testImportFromNonExistantFile() {
		try {
            doImport("Z", null, "T1", null, null, null, 0);
            fail();
		} catch (SQLException e) {
			assertSQLState("XIE04", e);
		}
	}
	
	public void testNullDataFile() {
		try {
            doImport(null, null, "T1", null, null, null, 0);
            fail();
		} catch (SQLException e) {
			assertSQLState("XIE05", e);
		}
	}
	
	public void testEmptyTable() throws SQLException {
        doImportAndExport(null, "T1", null, null, null);
	}

	public void testEmptyTableWithDelimitedFormat() throws SQLException {
        doImportAndExport(null, "T1", null, null, "8859_1");
	}

	public void testEmptyTableWithFieldCharDelimiters() throws SQLException {
        doImportAndExport(null, "T1", "\t", "|", "8859_1");
	}
	
	public void testWithDefaultOptions() throws Exception {
		resetTables();
        doImportAndExport(null, "T1", null, null, null);
	}
	
	public void testWithCodeset() throws Exception {
		resetTables();
        doImportAndExport(null, "T1", null, null, "8859_1");
	}

	public void testDelimiterAndCodeset() throws Exception {
		resetTables();
        doImportAndExport(null, "T1", "\t", "|", "8859_1");
	}
	
	public void testSpecialDelimitersAndCodeset() throws Exception {
		resetTables();
        doImportAndExport(null, "T1", "%", "&", "Cp1252");
	}

	public void testSpecialDelimitersAndUTF16() throws Exception {
		resetTables();
        doImportAndExport(null, "T1", "%", "&", "UTF-16");
	}
	
	public void testInvalidEncoding() throws Exception {
		resetTables();
		try {
            doImportAndExport(null, "T1", "^", "#", "INAVALID ENCODING");
            fail();
		} catch (SQLException e) {
			assertSQLState("XIE0I", e);
		}
	}
	
	public void testEarlyEndOfFile() throws Exception {
		try {
            doImportFromFile("extin/TwoLineBadEOF.dat", null, "T4",
                             null, null, "US-ASCII", 0);
            fail();
		} catch (SQLException e) {
			assertSQLState("XIE0E", e);
		}
	}

    /**
     * Test that quotes in the arguments to the export and import procedures
     * are handled properly (DERBY-4042).
     */
    public void testQuotesInArguments() throws Exception {
        resetTables();

        // Create schema names and table names containing both single quotes
        // and double quotes to expose bugs both for internally generated
        // string literals (enclosed in single quotes) and SQL identifiers
        // (enclosed in double quotes). Both single and double quotes used
        // to cause problems.
        final String schema = "s'\"";
        final String table = "t'\"";
        final String escapedName = JDBC.escape(schema, table);

        Statement s = createStatement();
        s.execute("create table " + escapedName +
                  " as select * from T1 with no data");
        s.execute("insert into " + escapedName + " select * from t1");

        // Quotes in the delimiters didn't use to be a problem, but test
        // it anyway
        final String colDel = "'";
        final String charDel = "\"";
        final String encoding = "US-ASCII";

        // Single quotes in file name used to cause syntax errors
        final String fileName = SupportFilesSetup.
                getReadWrite("please don't fail.dat").getPath();

        // Export used to fail with a syntax error
        doExportToFile(fileName, schema, table, colDel, charDel, encoding);

        // Empty the table so that we can see that it was imported later
        int rowsInTable = s.executeUpdate("delete from " + escapedName);

        // Import used to fail with a syntax error
        doImportFromFile(fileName, schema, table, colDel, charDel, encoding, 0);

        // Verify that the table was imported
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select count(*) from " + escapedName),
                Integer.toString(rowsInTable));
        JDBC.assertEmpty(s.executeQuery(
                "select * from " + escapedName +
                " except all select * from T1"));
    }

    private void doImport(String fromTable, String toSchema, String toTable,
			 String colDel, String charDel , 
			 String codeset, int replace) throws SQLException 
    {
        String fileName = (fromTable == null) ?
            null : SupportFilesSetup.getReadWrite(fromTable + ".dat").getPath();
        doImportFromFile(fileName, toSchema, toTable,
                colDel, charDel, codeset, replace);
    }
	
    private void doImportFromFile(
             String fileName, String toSchema, String toTable,
			 String colDel, String charDel , 
			 String codeset, int replace) throws SQLException
    {
		String impsql = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (? , ? , ? , ?, ? , ?, ?)";
        PreparedStatement ps = prepareStatement(impsql);
        ps.setString(1, toSchema);
		ps.setString(2, toTable);
		ps.setString(3, fileName);
		ps.setString(4 , colDel);
		ps.setString(5 , charDel);
		ps.setString(6 , codeset);
		ps.setInt(7, replace);
		ps.execute();
		ps.close();

    }

	private void doImportAndExport(
              String fromSchema, String fromTable, String colDel,
			  String charDel, 
			  String codeset) throws SQLException 
    {
        doExport(fromSchema, fromTable, colDel, charDel, codeset);
        doImportAndVerify(fromSchema, fromTable, colDel, charDel, codeset, 0);
        // also test with replace
        doImportAndVerify(fromSchema, fromTable, colDel, charDel, codeset, 1);
    }
	
    private void doExport(String fromSchema, String fromTable, String colDel,
			 String charDel,
			 String codeset) throws SQLException
	{
        String fileName = (fromTable == null) ?
            null : SupportFilesSetup.getReadWrite(fromTable + ".dat").getPath();
        doExportToFile(
                fileName, fromSchema, fromTable, colDel, charDel, codeset);
    }

    private void doExportToFile(
            String fileName, String fromSchema, String fromTable,
            String colDel, String charDel, String codeset) throws SQLException
    {
		 //DERBY-2925: need to delete existing files first.
         if (fileName != null) {
             SupportFilesSetup.deleteFile(fileName);
         }
		 String expsql = "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (? , ? , ? , ?, ? , ?)";
         PreparedStatement ps = prepareStatement(expsql);
         ps.setString(1, fromSchema);
		 ps.setString(2, fromTable);
         ps.setString(3, fileName);
		 ps.setString(4 , colDel);
		 ps.setString(5 , charDel);
		 ps.setString(6 , codeset);
		 ps.execute();
		 ps.close();
    }
	
	/**
	 * doImportAndVerify checks that data which has been imported and
	 * then exported is identical. It imports the requested data, 
	 * which has been exported from T1. Row counts are compared, and
	 * then the data in T2 is again exported. A bytewise comparison 
	 * of the two files is then made to verify that the data has been
	 * gone through the import/export process intact.
	 */
	private void doImportAndVerify(
              String fromSchema, String fromTable, String colDel,
			  String charDel , String codeset, 
			  int replace) throws SQLException 
    {

        doImport(fromTable, null, "T2", colDel, charDel, codeset, replace);

        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " +
                ((fromSchema == null) ?
                    JDBC.escape(fromTable) :
                    JDBC.escape(fromSchema, fromTable)));
		rs.next();
		int numberOfRowsInT1 = rs.getInt(1);
		rs.close();
		rs = stmt.executeQuery("SELECT COUNT(*) FROM t2");
		rs.next();
		int numberOfRowsInT2 = rs.getInt(1);
		rs.close();
		stmt.close();
		assertEquals(numberOfRowsInT1, numberOfRowsInT2);

		doExport(null, "T2" , colDel , charDel , codeset);

        //check whether the  exported files from T1 and T2  are same now.
		assertEquals(SupportFilesSetup.getReadWrite(fromTable + ".dat"),
				     SupportFilesSetup.getReadWrite("T2.dat"));
    }
	
	/**
	 * Called from each fixture that verifies data in the table.
	 * Ensures that the import and export operate on a consistent
	 * set of data.
	 */
	private void resetTables() throws Exception {
		runSQLCommands("delete from t1");
		runSQLCommands("delete from t2");
		runSQLCommands("INSERT INTO T1 VALUES (null,'aa',1,'a',DATE('1998-06-30'),"+
		               "1,1,1,1,1,1,1,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),1,'a')");
        runSQLCommands("INSERT INTO T1 VALUES (null,'bb',1,'b',DATE('1998-06-30'),"+
					   "2,2,2,2,2,2,2,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),2,'b')");
        runSQLCommands("INSERT INTO T1 VALUES (null,'cc',1,'c',DATE('1998-06-30'),"+
					   "3,3,3,3,3,3,3,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),3,'c')");
        runSQLCommands("INSERT INTO T1 VALUES (null,'dd',1,'d',DATE('1998-06-30'),"+
					   "4,4,4,4,4,4,4,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),4,'d')");
	}

}
