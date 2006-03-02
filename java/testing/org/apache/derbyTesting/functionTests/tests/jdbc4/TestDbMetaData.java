/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc.TestDbMetaData

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.RowIdLifetime;

import org.apache.derby.tools.ij;

/**
 * Test of database meta-data for new methods in jdbc 30. This program simply calls
 * each of the new meta-data methods in jdbc30, one by one, and prints the results.
 *
 * @author mamta
 */

public class TestDbMetaData { 

	public static void main(String[] args) {
		DatabaseMetaData met;
		Connection con;
		Statement  s;
    
		try
		{
            // Using this for now instead of ij because ij.startJBMS()
            // returns null for classes built against JDK 1.6
            con = new TestConnection().createEmbeddedConnection();

			con.setAutoCommit(true); // make sure it is true

			s = con.createStatement();
            
			met = con.getMetaData();

            if ( ! met.supportsStoredFunctionsUsingCallSyntax() ) {
                throw new Exception("FAIL: supportsStoredFunctionsUsingCallSyntax() " +
                    "should return true");
            }
            
            if ( met.autoCommitFailureClosesAllResultSets() ) {
                throw new Exception("FAIL: autoCommitFailureClosesAllResultSets() " +
                    "should return false");
            }
            
            if ( met.providesQueryObjectGenerator() ) {
                throw new Exception("FAIL: providesQueryObjectGenerator() should " +
                    "return false");
            }
            
            RowIdLifetime lifetime = met.getRowIdLifetime();
            if ( lifetime != RowIdLifetime.ROWID_UNSUPPORTED ) {
                throw new Exception("FAIL: getRowIdLifetime() should return " +
                    "ROWID_UNSUPPORTED, but got " + lifetime );
            }

			checkEmptyRS(met.getClientInfoProperties());

			// Create some functions in the default schema (app) to make the output
			// from getFunctions() and getFunctionParameters more interesting
			s.execute("CREATE FUNCTION DUMMY1 ( X SMALLINT ) RETURNS SMALLINT "+
					  "PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL "+
					  "NAME 'java.some.func'");
			s.execute("CREATE FUNCTION DUMMY2 ( X INTEGER, Y SMALLINT ) RETURNS"+
					  " INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA "+
					  "EXTERNAL NAME 'java.some.func'");
			s.execute("CREATE FUNCTION DUMMY3 ( X VARCHAR(16), Y INTEGER ) "+
					  "RETURNS VARCHAR(16) PARAMETER STYLE JAVA NO SQL LANGUAGE"+
					  " JAVA EXTERNAL NAME 'java.some.func'");
			s.execute("CREATE FUNCTION DUMMY4 ( X VARCHAR(128), Y INTEGER ) "+
					  "RETURNS INTEGER PARAMETER STYLE JAVA NO SQL LANGUAGE "+
					  "JAVA EXTERNAL NAME 'java.some.func'");

			checkEmptyRS(met.getFunctionParameters(null,null,null,null));

			// Any function in any schema in any catalog
			dumpRS(met.getFunctions(null, null, null));
			// Any function in any schema in "Dummy
			// Catalog". Same as above since the catalog
			// argument is ignored (is always null)
			dumpRS(met.getFunctions("Dummy Catalog", null, null));
			// Any function in a schema starting with "SYS"
			dumpRS(met.getFunctions(null, "SYS%", null));
			// All functions containing "GET" in any schema 
			// (and any catalog)
			dumpRS(met.getFunctions(null, null, "%GET%"));
			// Any function that belongs to NO schema and 
			// NO catalog (none)
			checkEmptyRS(met.getFunctions("", "", null));
            
            // 
            // Test the new getSchemas() with no schema qualifiers
            //
            dumpRS(met.getSchemas(null, null));
            
            //
            // Test the new getSchemas() with a schema wildcard qualifier
            // 
            dumpRS(met.getSchemas(null, "SYS%"));
            
            // 
            // Test the new getSchemas() with an exact match
            //
            dumpRS(met.getSchemas(null, "APP"));
            
            //
            // Make sure that getSchemas() returns an empty result
            // set when a schema is passed with no match
            //
            checkEmptyRS(met.getSchemas(null, "BLAH"));
        
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
	 * Checks for a ResultSet with no rows.
	 *
	 */
	static void checkEmptyRS(ResultSet rs) throws Exception
	{		
		boolean passed = false;

		try {
			if ( rs == null )
            {
                throw new Exception("Metadata result set can not be null");
            }
            int numrows = 0;
            while (rs.next())
                numrows++;
            // Zero rows is what we want.
            if (numrows != 0) {
                throw new Exception("Result set is not empty");
            }
		}
		catch (SQLException e)
		{
			throw new Exception("Unexpected SQL Exception: " + e.getMessage(), e);
		}
	}
}
