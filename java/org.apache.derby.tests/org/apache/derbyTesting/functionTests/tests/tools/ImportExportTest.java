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
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test import and export procedures 
 */
public class ImportExportTest extends BaseJDBCTestCase {

	public ImportExportTest(String name) {
		super(name);
	}
	
	public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("ImportExportTest");

        // disabled on weme6.1 due at the moment due 
        // to problems with security exceptions.
        if (JDBC.vmSupportsJSR169())
        {
            return new BaseTestSuite();
        }
        suite.addTest(baseSuite("ImportExportTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                        baseSuite("ImportExportTest:client")));    
        return suite;
	}
	
	public static Test baseSuite(String name) {
        BaseTestSuite suite = new BaseTestSuite(ImportExportTest.class, name);
		Test test = new SupportFilesSetup(suite, new String[] {"functionTests/testData/ImportExport/TwoLineBadEOF.dat"} );
		return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {

                s.execute( "create type Price external name 'org.apache.derbyTesting.functionTests.tests.lang.Price' language java" );
                s.execute( "create type hashmap external name 'java.util.HashMap' language java" );

                s.execute( "create function makePrice( ) returns price " +
                           "language java parameter style java no sql " +
                           "external name 'org.apache.derbyTesting.functionTests.tests.lang.Price.makePrice'" );

                s.execute( "create function makeHashMap( ) returns hashmap " +
                           "language java parameter style java no sql " +
                           "external name 'org.apache.derbyTesting.functionTests.tests.lang.UDTTest.makeHashMap'" );

                s.execute("CREATE TABLE T1 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
						   "COLUMN3 SMALLINT , COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
						   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
						   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
						   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1), COLUMN17 PRICE)");
                s.execute("CREATE TABLE T2 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
						   "COLUMN3 SMALLINT, COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
						   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
						   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
						   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1), COLUMN17 PRICE)");
                s.execute("create table T4 (   Account int,    Name   char(30), Jobdesc char(40), " +
                           "Company varchar(35), Address1 varchar(40), Address2 varchar(40), " +
                           "City    varchar(20), State   char(5), Zip char(10), Country char(10), " +
                           "Phone1  char(20), Phone2  char(20), email   char(30), web     char(30), " +
                           "Fname   char(30), Lname   char(30), Comment char(30), AccDate char(30), " +
                           "Payment decimal(8,2), Balance decimal(8,2))");

                s.execute( "create table t5( a int, b price )" );
                s.execute( "create table t6( a int, b hashmap )" );
                }
        };
	}

    /**
     * Set up the test environment.
     */
    protected void setUp() throws Exception {
        resetTables();
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
        doImportAndExport(null, "T1", null, null, null);
	}
	
	public void testWithCodeset() throws Exception {
        doImportAndExport(null, "T1", null, null, "8859_1");
	}

	public void testDelimiterAndCodeset() throws Exception {
        doImportAndExport(null, "T1", "\t", "|", "8859_1");
	}
	
	public void testSpecialDelimitersAndCodeset() throws Exception {
        doImportAndExport(null, "T1", "%", "&", "Cp1252");
	}

	public void testSpecialDelimitersAndUTF16() throws Exception {
        doImportAndExport(null, "T1", "%", "&", "UTF-16");
	}
	
	public void testInvalidEncoding() throws Exception {
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
     * Test that import to a table in the default schema works if a table
     * with the same name exists in a different schema (DERBY-3296).
     */
    public void testImportWithSameNameInDifferentSchema() throws Exception {
        doExport(null, "T1", null, null, null);
        Statement s = createStatement();
        s.executeUpdate("create table otherschema.t2(x int)");
        // toSchema must be null to trigger the bug. The bug is not exposed if
        // the schema is explicit.
        doImport("T1", null, "T2", null, null, null, 0);
        // Check that the rows were imported to the correct table (APP.T2)
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select count(*) from app.t2"), "4");
        setAutoCommit(false); // requirement for dropSchema()
        JDBC.dropSchema(getConnection().getMetaData(), "OTHERSCHEMA");
    }

    /**
     * Test that quotes in the arguments to the export and import procedures
     * are handled properly (DERBY-4042).
     */
    public void testQuotesInArguments() throws Exception {
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
    }

    /**
     * Test that we can successfully export from and import to tables that
     * have columns with special characters in their names (single and double
     * quotes, spaces, mixed case). Regression test case for DERBY-4828.
     */
    public void testQuotesInColumnNames() throws Exception {
        Statement s = createStatement();

        // Create a source table with column names that contain special
        // characters.
        s.execute("create table table_with_funny_cols_src ("
                // simple column name
                + "x int, "
                // column name with single and double quotes, mixed case
                // and spaces
                + "\"Let's try this! \"\" :)\" int)");
        s.execute("insert into table_with_funny_cols_src values (1,2), (3,4)");

        // Export the table to a file.
        doExport(null, "TABLE_WITH_FUNNY_COLS_SRC", null, null, null);

        // Create an empty destination table with the same schema as the
        // source table.
        s.execute("create table table_with_funny_cols_dest as "
                + "select * from table_with_funny_cols_src with no data");

        // Import into the destination table.
        doImport("TABLE_WITH_FUNNY_COLS_SRC",
                 null, "TABLE_WITH_FUNNY_COLS_DEST",
                 null, null, null, 0);

        // Verify that the rows were successfully imported.
        JDBC.assertFullResultSet(
                s.executeQuery(
                    "select * from table_with_funny_cols_dest order by x"),
                new String[][] { {"1", "2"}, {"3", "4"} });
    }

    /**
     * Test that you can't import the wrong type of object into a UDT column.
     */
    public void testCastingProblem() throws Exception
    {
        final String fileName = SupportFilesSetup.
                getReadWrite("castCheck.dat").getPath();

        // export table which has a HashMap column
        doExportToFile( fileName, null, "T6", null, null, null );

        // try to import the HashMap into a Price column
        try {
            doImportFromFile( fileName, null, "T5", null, null, null, 0 );
            fail();
		} catch (SQLException e) {
			assertSQLState("XJ001", e);
		}
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
	 * Called from {@link #setUp()}.
	 * Ensures that the import and export operate on a consistent
	 * set of data.
	 */
	private void resetTables() throws Exception {
		runSQLCommands("delete from t1");
		runSQLCommands("delete from t2");
		runSQLCommands("delete from t5");
		runSQLCommands("delete from t6");
		runSQLCommands("INSERT INTO T1 VALUES (null,'aa',1,'a',DATE('1998-06-30'),"+
		               "1,1,1,1,1,1,1,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),1,'a', makePrice() )");
        runSQLCommands("INSERT INTO T1 VALUES (null,'bb',1,'b',DATE('1998-06-30'),"+
					   "2,2,2,2,2,2,2,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),2,'b', makePrice() )");
        runSQLCommands("INSERT INTO T1 VALUES (null,'cc',1,'c',DATE('1998-06-30'),"+
					   "3,3,3,3,3,3,3,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),3,'c', makePrice())");
        runSQLCommands("INSERT INTO T1 VALUES (null,'dd',1,'d',DATE('1998-06-30'),"+
					   "4,4,4,4,4,4,4,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),4,'d', makePrice())");
        runSQLCommands( "insert into t6 values( 1, makeHashMap() )" );
	}

}
