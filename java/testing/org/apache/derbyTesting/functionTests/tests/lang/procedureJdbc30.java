/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.procedureJdbc30

   Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;
import java.sql.*;

import org.apache.derby.tools.ij;
import java.io.PrintStream;
import java.math.BigInteger;
import java.math.BigDecimal;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.parameterMetaDataJdbc30;
import org.apache.derbyTesting.functionTests.util.TestUtil;
public class procedureJdbc30
{ 

	static private boolean isDerbyNet = false;

	public static void main (String[] argv) throws Throwable
	{
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();
		isDerbyNet = TestUtil.isNetFramework();

        runTests( conn);
    }

    public static void runTests( Connection conn) throws Throwable
    {
		try {
			testMoreResults(conn);
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
		
	}

	private static void testMoreResults(Connection conn) throws SQLException {

		Statement s = conn.createStatement();

		s.executeUpdate("create table MRS.FIVERS(i integer)");
		PreparedStatement ps = conn.prepareStatement("insert into MRS.FIVERS values (?)");
		for (int i = 1; i <= 20; i++) {
			ps.setInt(1, i);
			ps.executeUpdate();
		}
		ps.close();

		// create a procedure that returns 5 result sets.
			
		s.executeUpdate("create procedure MRS.FIVEJP() parameter style JAVA READS SQL DATA dynamic result sets 5 language java external name 'org.apache.derbyTesting.functionTests.util.ProcedureTest.fivejp'");


		CallableStatement cs = conn.prepareCall("CALL MRS.FIVEJP()");
		ResultSet[] allRS = new ResultSet[5];

		// execute the procedure that returns 5 result sets and then use the various
		// options of getMoreResults().




		System.out.println("\n\nFetching result sets with getMoreResults()");
		int pass = 0;
		cs.execute();
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything except the current result set to be closed.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults());
		// check last one got closed
		showResultSetStatus(allRS);
		java.util.Arrays.fill(allRS, null);

		System.out.println("\n\nFetching result sets with getMoreResults(Statement.CLOSE_CURRENT_RESULT)");
		pass = 0;
		cs.execute();
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything except the current result set to be closed.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
		// check last one got closed
		showResultSetStatus(allRS);
		java.util.Arrays.fill(allRS, null);

		System.out.println("\n\nFetching result sets with getMoreResults(Statement.CLOSE_ALL_RESULTS)");
		pass = 0; 
		cs.execute();
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything except the current result set to be closed.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults(Statement.CLOSE_ALL_RESULTS));
		// check last one got closed
		showResultSetStatus(allRS);
		java.util.Arrays.fill(allRS, null);

		System.out.println("\n\nFetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT)");
		pass = 0;
		cs.execute();
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything to stay open.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
		// All should still be open.
		showResultSetStatus(allRS);
		// now close them all.
		for (int i = 0; i < allRS.length; i++) {
			allRS[i].close();
		}
		java.util.Arrays.fill(allRS, null);

		System.out.println("\n\nFetching result sets with getMoreResults(<mixture>)");
		cs.execute();

		System.out.println(" first two with KEEP_CURRENT_RESULT");
		allRS[0] = cs.getResultSet();
		boolean moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		if (!moreRS)
			System.out.println("FAIL - no second result set");
		allRS[1] = cs.getResultSet();
		// two open
		showResultSetStatus(allRS);
		
		System.out.println(" third with CLOSE_CURRENT_RESULT");
		moreRS = cs.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
		if (!moreRS)
			System.out.println("FAIL - no third result set");
		allRS[2] = cs.getResultSet();
		// first and third open, second closed
		showResultSetStatus(allRS);

		
		System.out.println(" fourth with KEEP_CURRENT_RESULT");
		moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		if (!moreRS)
			System.out.println("FAIL - no fourth result set");
		allRS[3] = cs.getResultSet();
		// first, third and fourth open, second closed
		showResultSetStatus(allRS);

		System.out.println(" fifth with CLOSE_ALL_RESULTS");
		moreRS = cs.getMoreResults(Statement.CLOSE_ALL_RESULTS);
		if (!moreRS)
			System.out.println("FAIL - no fifth result set");
		allRS[4] = cs.getResultSet();
		// only fifth open
		showResultSetStatus(allRS);

		System.out.println(" no more results with with KEEP_CURRENT_RESULT");
		moreRS = cs.getMoreResults(Statement.KEEP_CURRENT_RESULT);
		if (moreRS)
			System.out.println("FAIL - too many result sets");
		// only fifth open
		showResultSetStatus(allRS);
		allRS[4].close();
		java.util.Arrays.fill(allRS, null);

		System.out.println("\n\nFetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.execute() closes them");
		pass = 0;
		cs.execute();
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything to stay open.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
		System.out.println(" fetched all results");
		// All should still be open.
		showResultSetStatus(allRS);
		System.out.println(" executing statement");
		cs.execute();
		// all should be closed.
		showResultSetStatus(allRS);
		java.util.Arrays.fill(allRS, null);


		System.out.println("\n\nFetching result sets with getMoreResults(Statement.KEEP_CURRENT_RESULT) and checking that cs.close() closes them");
		pass = 0;
		// using execute from above.
		do {

			allRS[pass++] = cs.getResultSet();
			System.out.println("  PASS " + pass + " got result set " + (allRS[pass -1] != null));
			// expect everything to stay open.
			showResultSetStatus(allRS);

		} while (cs.getMoreResults(Statement.KEEP_CURRENT_RESULT));
		System.out.println(" fetched all results");
		// All should still be open.
		showResultSetStatus(allRS);
		System.out.println(" closing statement");
		cs.close();
		// all should be closed.
		showResultSetStatus(allRS);
		java.util.Arrays.fill(allRS, null);

	}

	private static void showResultSetStatus(ResultSet[] allRS) {
		for (int i = 0; i < allRS.length; i++) {
			try {
				ResultSet rs = allRS[i];
				if (rs == null)
					continue;
				rs.next();
				System.out.println("     RS (" + (i + 1) + ") val " + rs.getInt(1));
			} catch (SQLException sqle) {
				System.out.println("     Exception - " + sqle.getMessage());
			}
		}
	}
}
