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
import org.apache.derby.iapi.services.info.JVMInfo;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

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

			boolean pass = false;
			try {
				pass = TestUtil.compareURL(met.getURL());				 
			}catch (NoSuchMethodError msme) {
				// DatabaseMetaData.getURL not present - correct for JSR169
				if(!TestUtil.HAVE_DRIVER_CLASS)
					pass = true;
			} catch (Throwable err) {
			    System.out.println("%%getURL() gave the exception: " + err);
			}
			
			if(pass)
				System.out.println("DatabaseMetaData.getURL test passed");
			else
				System.out.println("FAIL: DatabaseMetaData.getURL test failed");
			
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
			checkJDBCVersion(met);

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
			System.out.println();
			System.out.println("getSuperTypes() with null :");
			checkEmptyRS(met.getSuperTypes(null,null,null));

			System.out.println();
			System.out.println("getSuperTables() with null :");
			checkEmptyRS(met.getSuperTables(null,null,null));

            System.out.println();
            System.out.println("getAttributes() with null :");

 			checkEmptyRS(met.getAttributes(null, null, null, null));

            System.out.println();
			System.out.println("locatorsUpdateCopy(): ");
			System.out.println("Returned: " + met.locatorsUpdateCopy());
        
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

	public static void dumpRS(ResultSet s) throws SQLException {
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
	 * Check whether <code>getJDBCMajorVersion()</code> and
	 * <code>getJDBCMinorVersion()</code> return the expected version numbers.
	 * @param met the <code>DatabaseMetaData</code> object to test
	 * @exception SQLException if a database error occurs
	 */
	private static void checkJDBCVersion(DatabaseMetaData met)
		throws SQLException
	{
		final int major, minor;
		if (TestUtil.isJCCFramework()) {
			major = 3;
			minor = 0;
		} else if (JVMInfo.JDK_ID < JVMInfo.J2SE_16) {
			major = 3;
			minor = 0;
		} else {
			major = 4;
			minor = 0;
		}
		System.out.print("getJDBCMajorVersion()/getJDBCMinorVersion() : ");
		int maj = met.getJDBCMajorVersion();
		int min = met.getJDBCMinorVersion();
		if (major == maj && minor == min) {
			System.out.println("AS EXPECTED");
		} else {
			System.out.println("GOT " + maj + "." + min +
							   ", EXPECTED " + major + "." + minor);
		}
	}

	/**
  	 * In order to be JDBC compliant, all metadata calls must return valid
     * results, even if it's an empty result set.  It should be considered
     * a failure if we throw an exception
	 */
	public static void checkEmptyRS(ResultSet rs)
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
				System.out.println("EXPECTED: Empty ResultSet");
			else 
				System.out.println("FAIL: Should have gotten empty ResultSet");
		}
	}
}
