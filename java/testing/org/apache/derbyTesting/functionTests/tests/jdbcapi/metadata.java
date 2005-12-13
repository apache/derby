/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.Time;
import java.sql.Date;
import java.math.BigDecimal;

import java.util.Properties;

import org.apache.derby.tools.ij;

/**
 * Test of database meta-data.  This program simply calls each of the meta-data
 * methods, one by one, and prints the results.  The test passes if the printed
 * results match a previously stored "master".  Thus this test cannot actually
 * discern whether it passes or not.
 *
 * @author alan
 */

public class metadata extends metadata_test {

	public metadata() {
	}

	/**
	 * Constructor:
	 * Just intializes the Connection and Statement fields
	 * to be used through the test.
	 */
	public metadata(String [] args) {

		try {

			ij.getPropertyArg(args);
			con = ij.startJBMS();
			s = con.createStatement();

		} catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

	}

	/**
	 * Makes a call to the "runTest" method in metadata_test.java,
	 * which will in turn call back here for implementations of
	 * the abstract methods.
	 */
	public static void main(String[] args) throws Exception {

		new metadata(args).runTest();

	}

	/**
	 * This method is responsible for executing a metadata query and returning
	 * a result set that complies with the JDBC specification.
	 */
	protected ResultSet getMetaDataRS(DatabaseMetaData dmd, int procId,
		String [] sArgs, String [] argArray, int [] iArgs, boolean [] bArgs)
		throws SQLException
	{

		switch (procId) {

			case GET_PROCEDURES:
				return dmd.getProcedures(sArgs[0], sArgs[1], sArgs[2]);

			case GET_PROCEDURE_COLUMNS:
				return dmd.getProcedureColumns(sArgs[0], sArgs[1], sArgs[2], sArgs[3]);

			case GET_TABLES:
				return dmd.getTables(sArgs[0], sArgs[1], sArgs[2], argArray);

			case GET_COLUMNS:
				return dmd.getColumns(sArgs[0], sArgs[1], sArgs[2], sArgs[3]);

			case GET_COLUMN_PRIVILEGES:
				return dmd.getColumnPrivileges(sArgs[0], sArgs[1], sArgs[2], sArgs[3]);

			case GET_TABLE_PRIVILEGES:
				return dmd.getTablePrivileges(sArgs[0], sArgs[1], sArgs[2]);

			case GET_BEST_ROW_IDENTIFIER:
				return dmd.getBestRowIdentifier(sArgs[0], sArgs[1], sArgs[2],
					iArgs[0], bArgs[0]);

			case GET_VERSION_COLUMNS:
				return dmd.getVersionColumns(sArgs[0], sArgs[1], sArgs[2]);

			case GET_PRIMARY_KEYS:
				return dmd.getPrimaryKeys(sArgs[0], sArgs[1], sArgs[2]);

			case GET_IMPORTED_KEYS:
				return dmd.getImportedKeys(sArgs[0], sArgs[1], sArgs[2]);

			case GET_EXPORTED_KEYS:
				return dmd.getExportedKeys(sArgs[0], sArgs[1], sArgs[2]);

			case GET_CROSS_REFERENCE:
				return dmd.getCrossReference(sArgs[0], sArgs[1], sArgs[2],
					sArgs[3], sArgs[4], sArgs[5]);

			case GET_TYPE_INFO:
				return dmd.getTypeInfo();

			case GET_INDEX_INFO:
				return dmd.getIndexInfo(sArgs[0], sArgs[1], sArgs[2],
					bArgs[0], bArgs[1]);

			default:
			// shouldn't get here.

				System.out.println("*** UNEXPECTED PROCEDURE ID ENCOUNTERED: " + procId + ".");
				return null;
		}

	}

	protected void dumpRS(int procId, ResultSet s) throws SQLException {

		ResultSetMetaData rsmd = s.getMetaData ();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount ();
		String[] headers = new String[numCols];
		if (numCols <= 0) {
			System.out.println("(no columns!)");
			return;
		}
		
		// Display column headings, and include column types
		// as part of those headings.
		for (int i=1; i<=numCols; i++) {
			if (i > 1) System.out.print(",");
			headers[i-1] = rsmd.getColumnLabel(i);
			System.out.print(headers[i-1]);
			System.out.print("[" + rsmd.getColumnTypeName(i) + "]");

		}
		System.out.println();
	
		// Display data, fetching until end of the result set
		StringBuffer errorColumns;
		while (s.next()) {
			// Loop through each column, getting the
			// column data and displaying
			errorColumns = new StringBuffer();
			String value;
			for (int i=1; i<=numCols; i++) {
				if (i > 1) System.out.print(",");
				value = s.getString(i);
				if (headers[i-1].equals("DATA_TYPE"))
				{
					if (((org.apache.derbyTesting.functionTests.util.TestUtil.getJDBCMajorVersion(s.getStatement().getConnection()) >= 3) &&
						 (Integer.valueOf(value).intValue() == 16)) ||
						(Integer.valueOf(value).intValue() == -7))
						System.out.print("**BOOLEAN_TYPE for VM**");
					else
						System.out.print(value);
				}
				else
					System.out.print(value);

			}
			System.out.println();
		}
		s.close();
	}

    /** dummy method to test getProcedureColumns
     */
    public static byte[] getpc(String a, BigDecimal b, short c, byte d, short e, int f, long g, float h, double i, byte[] j, Date k, Time l, Timestamp T)
    {
        return j;
    }

	/** overload getpc to further test getProcedureColumns
	*/
	public static void getpc(int a, long[] b)
	{
	}

    /** overload getpc to further test getProcedureColumns
	 *  private method shouldn't be returned with alias, ok with procedure
     */
    private static void getpc(int a, long b)
    {
    }

	// instance method 
	// with method alias, this should not be returned by getProcedureColumns
	// but DB2 returns this with a java procedure
	public void getpc(String a, String b) {
	}

	// this method should notbe seen by getProcedureColumns as
	// it has no parameters and no return value.
	public static void getpc4a() {
	}

	// check a method with no paramters and a return value works
	// for getProcedureColumns.
	public static int getpc4b() {
		return 4;
	}

	// check for nested connection working ok
	public static void isro() throws SQLException {
		DriverManager.getConnection("jdbc:default:connection").getMetaData().isReadOnly();
	}

}

