/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.metadataJdbc20

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.StringTokenizer;

import org.apache.derby.tools.ij;

/**
 * Test of database meta-data for new methods in jdbc 20. This program simply calls
 * each of the new meta-data methods in jdbc20, one by one, and prints the results.
 *
 * @author mamta
 */

public class metadataJdbc20 { 
	public static void main(String[] args) {
		DatabaseMetaData met;
		Connection con;
		Statement  s;

		System.out.println("Test metadataJdbc20 starting");
    
		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();
			con.setAutoCommit(true); // make sure it is true

			s = con.createStatement();

			met = con.getMetaData();

			System.out.println("JDBC Driver '" + met.getDriverName() +
							   "', version " + met.getDriverMajorVersion() +
							   "." + met.getDriverMinorVersion() +
							   " (" + met.getDriverVersion() + ")");


			try {
				System.out.println("The URL is: " + met.getURL());
			} catch (NoSuchMethodError msme)
			{
				System.out.println("DatabaseMetaData.getURL not present - correct for JSR169");
			}

			System.out.println();
			System.out.println("getUDTs() with user-named types null :");
 			dumpRS(met.getUDTs(null, null, null, null));
      
			System.out.println("getUDTs() with user-named types in ('JAVA_OBJECT') :");
 			int[] userNamedTypes = new int[1];
 			userNamedTypes[0] = java.sql.Types.JAVA_OBJECT;
 			dumpRS(met.getUDTs("a", null, null, userNamedTypes));

 			System.out.println("getUDTs() with user-named types in ('STRUCT') :");
 			userNamedTypes[0] = java.sql.Types.STRUCT;
 			dumpRS(met.getUDTs("b", null, null, userNamedTypes));

 			System.out.println("getUDTs() with user-named types in ('DISTINCT') :");
 			userNamedTypes[0] = java.sql.Types.DISTINCT;
 			dumpRS(met.getUDTs("c", null, null, userNamedTypes));

			System.out.println("getUDTs() with user-named types in ('JAVA_OBJECT', 'STRUCT') :");
 			userNamedTypes = new int[2];
 			userNamedTypes[0] = java.sql.Types.JAVA_OBJECT;
 			userNamedTypes[1] = java.sql.Types.STRUCT;
 			dumpRS(met.getUDTs("a", null, null, userNamedTypes));

			System.out.println("Test the metadata calls related to visibility of changes made by others for different resultset types");
			System.out.println("Since Derby materializes a forward only ResultSet incrementally, it is possible to see changes");
			System.out.println("made by others and hence following 3 metadata calls will return true for forward only ResultSets.");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersDeletesAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY)? " + met.othersInsertsAreVisible(ResultSet.TYPE_FORWARD_ONLY));
			System.out.println("Scroll insensitive ResultSet by their definition do not see changes made by others and hence following metadata calls return false");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE)? " + met.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_INSENSITIVE));
			System.out.println("Derby does not yet implement scroll sensitive resultsets and hence following metadata calls return false");
			System.out.println("othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersUpdatesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
			System.out.println("othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersDeletesAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));
			System.out.println("othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE)? " + met.othersInsertsAreVisible(ResultSet.TYPE_SCROLL_SENSITIVE));

			s.close();
			
			System.out.println("Test escaped numeric functions - JDBC 3.0 C.1");
			testEscapedFunctions(con, NUMERIC_FUNCTIONS, met.getNumericFunctions());
			
			System.out.println("Test escaped string functions - JDBC 3.0 C.2");
			testEscapedFunctions(con, STRING_FUNCTIONS, met.getStringFunctions());
			
			System.out.println("Test escaped system functions - JDBC 3.0 C.4");
			testEscapedFunctions(con, SYSTEM_FUNCTIONS, met.getSystemFunctions());

			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: " + e.getMessage());
			e.printStackTrace(System.out);
		}

		System.out.println("Test metadataJdbc20 finished");
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception");
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se.printStackTrace(System.out);
			se = se.getNextException();
		}
	}
	static private void showSQLExceptions (SQLException se) {
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): " + se.getMessage());
			se = se.getNextException();
		}
	}
	
	static void dumpRS(ResultSet s) throws SQLException {
		ResultSetMetaData rsmd = s.getMetaData ();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount ();

		if (numCols <= 0) {
			System.out.println("(no columns!)");
			return;
		}
		
		// Display column headings
		for (int i=1; i<=numCols; i++) {
			if (i > 1) System.out.print(",");
			System.out.print(rsmd.getColumnLabel(i));
		}
		System.out.println();
	
		// Display data, fetching until end of the result set
		while (s.next()) {
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++) {
				if (i > 1) System.out.print(",");
				System.out.print(s.getString(i));
			}
			System.out.println();
		}
		s.close();
	}
	
	/*
	** Escaped function testing
	*/
	private static final String[][] NUMERIC_FUNCTIONS =
	{
		// Section C.1 JDBC 3.0 spec.
		{ "ABS", "-25.67" },
		{ "ACOS", "1.34" },
		{ "ASIN", "1.21" },
		{ "ATAN", "0.34" },
		{ "ATAN2", "0.56", "1.2" },
		{ "CEILING", "3.45" },
		{ "COS", "1.2" },
		{ "COT", "3.4" },
		{ "DEGREES", "2.1" },
		{ "EXP", "2.3" },
		{ "FLOOR", "3.22" },
		{ "LOG", "34.1" },
		{ "LOG10", "18.7" },
		{ "MOD", "124", "7" },
		{ "PI" },
		{ "POWER", "2", "3" },
		{ "RADIANS", "54" },
		{ "RAND", "17" }, 
		{ "ROUND", "345.345", "1" }, 
		{ "SIGN", "-34" },
		{ "SIN", "0.32" },
		{ "SQRT", "6.22" },
		{ "TAN", "0.57", },
		{ "TRUNCATE", "345.395", "1" }
	};
	
	private static final String[][] STRING_FUNCTIONS =
	{	
		// Section C.2 JDBC 3.0 spec.
		{ "ASCII" , "'Yellow'" },
		{ "CHAR", "65" },
		{ "CONCAT", "'hello'", "'there'" },
		{ "DIFFERENCE", "'Pires'", "'Piers'" },
		{ "INSERT", "'Bill Clinton'", "4", "'William'" },
		{ "LCASE", "'Fernando Alonso'" },
		{ "LEFT", "'Bonjour'", "3" },
		{ "LENGTH", "'four    '" } ,
		{ "LOCATE", "'jour'", "'Bonjour'" },
		{ "LTRIM", "'   left trim   '"},
		{ "REPEAT", "'echo'", "3" },
		{ "REPLACE", "'to be or not to be'", "'be'", "'England'" },
		{ "RTRIM", "'  right trim   '"},
		{ "SOUNDEX", "'Derby'" },
		{ "SPACE", "12"},
		{ "SUBSTRING", "'Ruby the Rubicon Jeep'", "10", "7", },
		{ "UCASE", "'Fernando Alonso'" }
		};
	
	private static final String[][] SYSTEM_FUNCTIONS =
	{	
		// Section C.4 JDBC 3.0 spec.
		{ "DATABASE" },
		{ "IFNULL", "'this'", "'that'" },
		{ "USER"},
		};	
	

	/**
	 * Test escaped functions. Working from the list of escaped functions defined
	 * by JDBC, compared to the list returned by the driver.
	 * <OL>
	 * <LI> See that all functions defined by the driver are in the spec list
	 * and that they work.
	 * <LI> See that only functions defined by the spec are in the driver's list.
	 * <LI> See that any functions defined by the spec that work are in the driver's list.
	 * </OL>
	 * FAIL will be printed for any issues.
	 * @param conn
	 * @param specList
	 * @param metaDataList
	 * @throws SQLException
	 */
	private static void testEscapedFunctions(Connection conn, String[][] specList, String metaDataList)
	throws SQLException
	{
		boolean[] seenFunction = new boolean[specList.length];
		
		System.out.println("TEST FUNCTIONS DECLARED IN DATABASEMETADATA LIST");
		StringTokenizer st = new StringTokenizer(metaDataList, ",");
		while (st.hasMoreTokens())
		{
			String function = st.nextToken();
			
			// find this function in the list
			boolean isSpecFunction = false;
			for (int f = 0; f < specList.length; f++)
			{
				String[] specDetails = specList[f];
				if (function.equals(specDetails[0]))
				{
					// Matched spec.
					if (seenFunction[f])
						System.out.println("FAIL Function in list twice: " + function);
					seenFunction[f] = true;
					isSpecFunction = true;
					
					if (!executeEscaped(conn, specDetails))
						System.out.println("FAIL Function failed to execute "+ function);
					break;
				}
			}
			
			if (!isSpecFunction)
			{
				System.out.println("FAIL Non-JDBC spec function in list: " + function);
			}
		}
		
		// Now see if any speced functions are not in the metadata list
		System.out.println("TEST FUNCTIONS NOT DECLARED IN DATABASEMETADATA LIST");
		for (int f = 0; f < specList.length; f++)
		{
			if (seenFunction[f])
				continue;
			String[] specDetails = specList[f];
			if (executeEscaped(conn, specDetails))
				System.out.println("FAIL function works but not declared in list: " + specDetails[0]);
			
		}
	}
	
	private static boolean executeEscaped(Connection conn, String[] specDetails)
	{
		
		String sql = "VALUES { fn " + specDetails[0] + "(";
		
		for (int p = 0; p < specDetails.length - 1; p++)
		{
			if (p != 0)
				sql = sql + ", ";
			
			sql = sql + specDetails[p + 1];
		}
		
		sql = sql + ") }";
		
		System.out.print("Executing " + sql + " -- ");
			
		try {
			PreparedStatement ps = conn.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			
			while (rs.next())
			{
				// truncate numbers to avoid multiple master files
				// with double values.
				String res = rs.getString(1);
				
				switch (rs.getMetaData().getColumnType(1))
				{
				case Types.DOUBLE:
				case Types.REAL:
				case Types.FLOAT:
					if (res.length() > 4)
						res = res.substring(0, 4);
					break;
				default:
					break;
				}
				System.out.print("  = >" + res + "< ");
			}
			rs.close();
			ps.close();
			System.out.println(" << ");
			return true;
		} catch (SQLException e) {
			System.out.println("");
			showSQLExceptions(e);
			return false;
		}
		
	}
	
	
}
