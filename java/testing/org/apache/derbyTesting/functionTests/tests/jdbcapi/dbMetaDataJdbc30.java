/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.dbMetaDataJdbc30

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.iapi.reference.JDBC30Translation;

import org.apache.derby.tools.ij;

/**
 * Test of database meta-data for new methods in jdbc 30. This program simply calls
 * each of the new meta-data methods in jdbc30, one by one, and prints the results.
 *
 * @author mamta
 */

public class dbMetaDataJdbc30 { 

	public static void main(String[] args) {
		DatabaseMetaData met;
		Connection con;
		Statement  s;


		System.out.println("Test dbMetaDataJdbc30 starting");
    
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

			System.out.println("The URL is: " + met.getURL());

			System.out.println();
			System.out.println("supportsSavepoints() : " + met.supportsSavepoints());

			System.out.println();
			System.out.println("supportsNamedParameters() : " + met.supportsNamedParameters());

			System.out.println();
			System.out.println("supportsMultipleOpenResults() : " + met.supportsMultipleOpenResults());

			System.out.println();
			System.out.println("supportsGetGeneratedKeys() : " + met.supportsGetGeneratedKeys());

      System.out.println();
      System.out.println("supportsResultSetHoldability(HOLD_CURSORS_OVER_COMMIT) : " +
      met.supportsResultSetHoldability(JDBC30Translation.HOLD_CURSORS_OVER_COMMIT));

			System.out.println();
			System.out.println("supportsResultSetHoldability(CLOSE_CURSORS_AT_COMMIT) : " +
        met.supportsResultSetHoldability(JDBC30Translation.CLOSE_CURSORS_AT_COMMIT));

			System.out.println();
			System.out.println("getJDBCMajorVersion() : " + met.getJDBCMajorVersion());

			System.out.println();
			System.out.println("getJDBCMinorVersion() : " + met.getJDBCMinorVersion());

			System.out.println();
			System.out.println("getSQLStateType() : " + met.getSQLStateType());

			System.out.println();
			System.out.println("getResultSetHoldability() : " + met.getResultSetHoldability());

			System.out.println();
			System.out.println("getDatabaseMajorVersion() : " + met.getDatabaseMajorVersion());

			System.out.println();
			System.out.println("getDatabaseMinorVersion() : " + met.getDatabaseMinorVersion());

			System.out.println();
			System.out.println("supportsStatementPooling() : " + met.supportsStatementPooling());

			System.out.println("getMaxColumnNameLength() = "+met.getMaxColumnNameLength());
			System.out.println("getMaxCursorNameLength() = "+met.getMaxCursorNameLength());
			System.out.println("getMaxSchemaNameLength() = "+met.getMaxSchemaNameLength());
			System.out.println("getMaxProcedureNameLength() = "+met.getMaxProcedureNameLength());
			System.out.println("getMaxTableNameLength() = "+met.getMaxTableNameLength());
			System.out.println("getMaxUserNameLength() = "+met.getMaxUserNameLength());

			//following will give not implemented exceptions.
			// JCC will return an empty result set, so either a
			// result set with no rows or an exception will pass
			try {
			  System.out.println();
			  System.out.println("getSuperTypes() with null :");
			  checkEmptyRSOrNotImplemented(met.getSuperTypes(null,null,null),null);
 			} catch (SQLException ex) {
				checkEmptyRSOrNotImplemented(null,ex);
 			}

			try {
			  System.out.println();
			  System.out.println("getSuperTables() with null :");
			  checkEmptyRSOrNotImplemented(met.getSuperTables(null,null,null),null);
 			} catch (SQLException ex) {
				checkEmptyRSOrNotImplemented(null,ex);
 			}

			try {
			  System.out.println();
			  System.out.println("getAttributes() with null :");

 			  checkEmptyRSOrNotImplemented(met.getAttributes(null, null, null,
															 null), null);
 			} catch (SQLException ex) {
				checkEmptyRSOrNotImplemented(null,ex);
 			}

			try {
			  System.out.println();
			  System.out.println("locatorsUpdateCopy(): ");
			  // JCC doesn't throw exception, Embedded driver does.
			  System.out.println("Returned: " + met.locatorsUpdateCopy());
 			} catch (SQLException ex) {
			  System.out.println("Expected : " + ex.getMessage());
 			}
        
			s.close();

			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:");
			e.printStackTrace(System.out);
		}

		System.out.println("Test dbMetaDataJdbc30 finished");
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

	/**
	 * JCC returns an empty resultset instead of throwing an exception
	 * for some methods.
	 * Checks for either a ResultSet with no rows or a NotImplemented  
	 * exception.  Usually either the rs or se are null.
	 *
	 */
	static void checkEmptyRSOrNotImplemented(ResultSet rs, SQLException se)
	{		
		boolean passed = false;

		try {
			
			if (rs != null)
			{
				int numrows = 0;
				while (rs.next())
					numrows++;
				// Zero rows is what we want.
				if (numrows == 0)
					passed = true;			
			}
			else if (se != null)
			{
				// Not implemented exception is OK too.
				String sqlState =se.getSQLState();
				if (sqlState != null && sqlState.startsWith("0A"))
				passed = true;
			}
		}
		catch (SQLException e)
		{
			System.out.println("Unexpected SQL Exception" + 
							   e.getMessage());
			e.printStackTrace();
		}
		finally 
		{
			if (passed)
				System.out.println("EXPECTED: Not Implemented Exception or empty  ResultSet");
			else 
				System.out.println("FAIL:  Should have gotten Not Implemented Exception or empty ResultSet");
		}
	}
}
