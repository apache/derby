/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.importExport

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Properties;
import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
  This tests import and export utilties. It first creates
  a temp table T1 and inserts data into it. Then it calls
  export to export data out from it into a temp file. Then
  it calls import to read data from the temp file just
  created. The program goes through the resultset of import
  and inserts one row at a time into another temp table T2
  which has same number of columns as T1. Then it compares
  number of rows in T1 and T2. If the number of rows are same
  as in T1 then part of the test succedded.

  The second part imports data out from T2 into second temp
  file and then we compare both the temp files to see if the
  2 files exactly match

  @author Mamta, Suresht
 */


public class importExport {
    

	private static Connection conn;
    private static String currentVersion;
	private static boolean passed = false;

	public static void main(String[] args) {
		System.out.println("Test importExport starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(true);
			setup(true);


			//try to import from a file which doesn't exist
			try {
				System.out.println("testing non-existing data file");
				doImport("Z" , "T1" , null , null , null, 0);
			} catch (Exception ex) {
				printExceptionMessage(ex);
			}

			//try to import from a null file
			try {
				System.out.println("testing null data file");
				doImport(null , "T1" , null , null, null, 0);
			} catch (Exception ex) {
				printExceptionMessage(ex);
			}


			System.out.println("testing empty table");
			doImportAndExport("T1",null, null , null);

			System.out.println("testing empty table with Delimited format");
			doImportAndExport("T1", null, null , "8859_1");

			System.out.println("testing empty table import Field/Char Delimiters");
			doImportAndExport("T1", "\t", "|" , "8859_1");

			cleanupBeforeNextRun();
			addDummyRows();
			System.out.println("testing import/export with default options");
			doImportAndExport("T1",null, null, null);

			cleanupBeforeNextRun();
			System.out.println("testing IE with code set 8859_1");
			doImportAndExport("T1", null, null , "8859_1");
			
			cleanupBeforeNextRun();
			System.out.println("testing IE with delimiter and codeset");
			doImportAndExport("T1", "\t", "|", "8859_1");

			cleanupBeforeNextRun();
			System.out.println("testing IE with delimiters(%, &) and Cp1252");
			doImportAndExport("T1", "%", "&", "Cp1252");

			cleanupBeforeNextRun();
			System.out.println("testing IE with delimiters(%, &) and UTF-16");
			doImportAndExport("T1", "%", "&", "UTF-16");

			cleanupBeforeNextRun();

			System.out.println("testing IE with delimiters(^, #) and WRONG ENCODEINGH");
			try{
				doImportAndExport("T1", "^", "#", "INAVALID ENCODING");
			} catch (Exception ex) {
				printExceptionMessage(ex) ;
			}


			System.out.println("testing datatypes that does not have Export Supprt");
			try {
				doExport("T3", null, null , null);
			} catch (Exception ex) {
				printExceptionMessage(ex);
			}

			try {
				doImport("T1" , "T3" , null , null , null, 0);
			} catch (Exception ex) {
				printExceptionMessage(ex);
			}
				
			//test less data case on the seconds line of input that 
			//should throw end of file exception. 
			try{
				doImportFromFile("extin/EndOfFile.txt" , "T4" , null , null , null, 0);
			}catch (Exception ex) {
				printExceptionMessage(ex);
			}

			System.out.println("PASS: finished testing import and export");
			teardown();
			System.out.println("PASS: finished cleaning up the temporary objects from database");

			conn.close();

			passed = true;

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			passed = false;
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		if (passed)
			System.out.println("PASS");
		System.out.println("Test importExport finished");
	}

	static void cleanupBeforeNextRun() throws Exception {
		Statement stmt = conn.createStatement();
		stmt.execute("delete from t2");
		stmt.close();
	}

	static void doImportAndExport(String fromTable, String colDel , 
								  String charDel, 
								  String codeset) throws  Exception 
	{


		doExport(fromTable , colDel , charDel , codeset);
		doImportAndVerify(fromTable, colDel , charDel, codeset,  0);
		//test with replace
		doImportAndVerify(fromTable, colDel , charDel, codeset,  1);

	}

	private static void doExport(String fromTable, String colDel , 
					 String charDel, 
					 String codeset) throws  Exception 
	{
		
		String expsql = "call SYSCS_UTIL.SYSCS_EXPORT_TABLE (? , ? , ? , ?, ? , ?)";
		PreparedStatement ps = conn.prepareStatement(expsql);
		ps.setString(1 , "APP");
		ps.setString(2, fromTable);
		ps.setString(3, (fromTable==null ?  fromTable : "extinout/" + fromTable + ".dat" ));
		ps.setString(4 , colDel);
		ps.setString(5 , charDel);
		ps.setString(6 , codeset);

		//perform export
		ps.execute();
		ps.close();

	}

	private static void doImport(String fromTable, String toTable, 
								 String colDel, String charDel , 
								 String codeset, int replace) throws Exception 
	{

				
		String impsql = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (? , ? , ? , ?, ? , ?, ?)";
		PreparedStatement ps = conn.prepareStatement(impsql);
		ps.setString(1 , "APP");
		ps.setString(2, toTable);
		ps.setString(3, (fromTable==null ?  fromTable : "extinout/" + fromTable + ".dat" ));
		ps.setString(4 , colDel);
		ps.setString(5 , charDel);
		ps.setString(6 , codeset);
		ps.setInt(7, replace);

		//perform export
		ps.execute();
		ps.close();

	}
	
	private static void doImportFromFile(String fileName, String toTable, 
								 String colDel, String charDel , 
								 String codeset, int replace) throws Exception 
	{

				
		String impsql = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (? , ? , ? , ?, ? , ?, ?)";
		PreparedStatement ps = conn.prepareStatement(impsql);
		ps.setString(1 , "APP");
		ps.setString(2, toTable);
		ps.setString(3, fileName);
		ps.setString(4 , colDel);
		ps.setString(5 , charDel);
		ps.setString(6 , codeset);
		ps.setInt(7, replace);

		//perform export
		ps.execute();
		ps.close();

	}
	
	static void doImportAndVerify(String fromTable,  String colDel, 
								  String charDel , String codeset, 
								  int replace) throws Exception {

		doImport(fromTable , "T2" , colDel , charDel , codeset , replace);

		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + fromTable);
		rs.next();
		int numberOfRowsInT1 = rs.getInt(1);
		rs.close();
		rs = stmt.executeQuery("SELECT COUNT(*) FROM t2");
		rs.next();
		int numberOfRowsInT2 = rs.getInt(1);
		rs.close();
		stmt.close();
		if (numberOfRowsInT1 != numberOfRowsInT2)
		{
			System.out.println("FAIL: Expected " + numberOfRowsInT1 + " got " + numberOfRowsInT2 + " rows after import");
			throw new SQLException("Wrong number of rows returned");
		}

		doExport("T2" , colDel , charDel , codeset);

		//check whether the  exported files from T1 and T2  are same now.
		if (diffTwoFiles( "extinout/"+fromTable + ".dat", "extinout/"+"T2.dat")) {
			throw new SQLException("Export from  " + fromTable + " and T2 don't match.");

		}

	}

	static boolean diffTwoFiles(String file1, String file2) throws Exception {

		InputStream f1 = new BufferedInputStream(new FileInputStream(file1));
		InputStream f2 = new BufferedInputStream(new FileInputStream(file2));

		int lineNo=1;
		int o=1;
		String lineSep = "\n";

		boolean diffed = false;
		boolean notDone = true;

		int b1, b2;

		while (notDone) {
			b1 = f1.read();
			b2 = f2.read();
			if ((b1 != b2) && (b1 != -1) && (b2 != -1)) {
				diffed = true;
				System.out.println(file1 + " " + file2 + " differ: byte " + o + ", line " + lineNo);
				notDone = false;
			} else {
				if (b1 == b2) {
					if (b1 == -1) {
						notDone = false;
					} else
						if (b1 == (int)lineSep.charAt(0)) {
							lineNo++;
						}
				} else
					if (b1 == -1) {
						diffed = true;
						System.out.println(file1 + " " + file2 + " differ: EOF on " + file1);
						notDone = false;
					} else if (b2 == -1) {
						diffed = true;
						System.out.println(file1 + " " + file2 + " differ: EOF on " + file2);
						notDone = false;
					}

			}
			o++;
		}
		return diffed;
	}

	static void setup(boolean first) throws Exception {
		Statement stmt = conn.createStatement();

		if (first) {
			verifyCount(
						stmt.executeUpdate("CREATE TABLE T1 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
										   "COLUMN3 SMALLINT , COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
										   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
										   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
										   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1))"), 0);
			verifyCount(
						stmt.executeUpdate("CREATE TABLE T2 (COLUMN1 VARCHAR(5) , COLUMN2 VARCHAR(8) , " +
										   "COLUMN3 SMALLINT, COLUMN4 CHAR(11) , COLUMN5 DATE , COLUMN6 DECIMAL(5,1) , " +
										   "COLUMN7 DOUBLE PRECISION , COLUMN8 INT , COLUMN9 BIGINT , COLUMN10 NUMERIC , " +
										   "COLUMN11 REAL , COLUMN12 SMALLINT , COLUMN13 TIME , COLUMN14 TIMESTAMP , "+
										   "COLUMN15 SMALLINT , COLUMN16 VARCHAR(1))"), 0);
			verifyCount(
						stmt.executeUpdate("CREATE TABLE T3 (C1 BLOB)"), 0);
			verifyCount(
						stmt.executeUpdate("create table T4 (   Account int,    Fname   char(30),"+
                        "Lname   char(30), Company varchar(35), Address varchar(40), City    varchar(20),"+
 					   "State   char(5), Zip     char(10), Payment decimal(8,2), Balance decimal(8,2))"),0);
			
		} else {
			verifyBoolean( stmt.execute("DELETE FROM t1"), false);
		}
		stmt.close();
	}

	static void addDummyRows() throws Exception {
		Statement stmt = conn.createStatement();

		verifyCount(
					stmt.executeUpdate("INSERT INTO T1 VALUES (null,'aa',1,'a',DATE('1998-06-30'),"+
									   "1,1,1,1,1,1,1,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),1,'a')"),1);

		verifyCount(
					stmt.executeUpdate("INSERT INTO T1 VALUES (null,'bb',1,'b',DATE('1998-06-30'),"+
									   "2,2,2,2,2,2,2,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),2,'b')"),1);

		verifyCount(
					stmt.executeUpdate("INSERT INTO T1 VALUES (null,'cc',1,'c',DATE('1998-06-30'),"+
									   "3,3,3,3,3,3,3,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),3,'c')"),1);

		verifyCount(
					stmt.executeUpdate("INSERT INTO T1 VALUES (null,'dd',1,'d',DATE('1998-06-30'),"+
									   "4,4,4,4,4,4,4,TIME('12:00:00'),TIMESTAMP('1998-06-30 12:00:00.0'),4,'d')"),1);

		System.out.println("PASS: setup complete");
	}


	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount(
					stmt.executeUpdate("DROP TABLE t1"),
					0);

		verifyCount(
					stmt.executeUpdate("DROP TABLE t2"),
					0);

		stmt.close();

		System.out.println("PASS: teardown complete");
	}

	static void verifyCount(int count, int expect) throws SQLException {
		if (count!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+count+" rows");
			throw new SQLException("Wrong number of rows returned");
		}
	}

	static void verifyBoolean(boolean got, boolean expect) throws SQLException {
		if (got!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+got);
			throw new SQLException("Wrong boolean returned");
		}
	}

	static void printExceptionMessage(Exception ex) throws Exception 
	{
		if (ex instanceof SQLException) {
			SQLException ie_ex = ((SQLException)ex);
			
			while(ie_ex.getNextException() != null)
			{
				ie_ex = ie_ex.getNextException();
			}
			System.out.println(ie_ex.getMessage());
		}
		else
			throw ex;
	}
}







