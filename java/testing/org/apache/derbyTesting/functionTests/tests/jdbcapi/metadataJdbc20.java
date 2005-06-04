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
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
}
