/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.dbManagerLimits

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derbyTesting.functionTests.util.Formatters;

/**
  Test various data manager limits like in db2 here.
 */
public class dbManagerLimits
{

	public static void main (String[] argv) throws Throwable
	{
		ij.getPropertyArg(argv);
		Connection conn = ij.startJBMS();

		testStringAndHexConstants(conn);
		testMostColumnsInTable(conn);
		testMostColumnsInView(conn);
		testMostElementsInSelectList(conn);
		testMostElementsInOrderBy(conn);
		testMostParametersInStoredProcedures(conn);

		//not running Group By test because it gets out of memory error
		//testMostElementsInGroupBy(conn);

		//not running indexes test because it doesn't finish even after running for over 2 hours
		//ALSO, IF WE EVER ENABLE THIS TEST IN FUTURE, WE NEED TO REWRITE THE TEST SO THAT WE TRY TO CREATE OVER
		//32767 *DIFFERENT* INDEXES. AS PART OF DB2 COMPATIBILITY WORK, BUG - 5685 DISALLOWS CREATION OF AN INDEX
		//ON A COLUMN THAT ALREADY HAS A PRIMARY KEY OR UNIQUE CONSTRAINT ON IT.
		//testMostIndexesOnTable(conn);
	}

	public static void testStringAndHexConstants( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - maximum length of character constant is 32672 and that of hex constant is 16336");
			String stringConstant32671 = Formatters.repeatChar("a",32671);
			String hexConstant16334 = Formatters.repeatChar("a",16334);
			Statement s = conn.createStatement();
			s.executeUpdate("create table t1 (c11 long varchar, c12 long varchar for bit data)");

			System.out.println("First testing less than maximum constant lengths through insert statement");
			s.executeUpdate("insert into t1(c11) values ('" +  stringConstant32671 + "')");
			s.executeUpdate("insert into t1(c12) values (X'" +  hexConstant16334 + "')");
      
			System.out.println("Next testing less than maximum constant lengths through values");
			s.execute("values ('" +  stringConstant32671 + "')");
			s.execute("values (X'" +  hexConstant16334 + "')");

			System.out.println("Next testing maximum constant lengths through insert statement");
			s.executeUpdate("insert into t1(c11) values ('" +  stringConstant32671 + "a')");
			s.executeUpdate("insert into t1(c12) values (X'" +  hexConstant16334 + "ab')");
      
			System.out.println("Next testing maximum constant lengths through values");
			s.execute("values ('" +  stringConstant32671 + "a')");
			s.execute("values (X'" +  hexConstant16334 + "ab')");

			System.out.println("Next testing maximum constant lengths + 1 through insert statement");
			try {
				s.executeUpdate("insert into t1(c11) values ('" +  stringConstant32671 + "ab')");
				System.out.println("FAIL - should have gotten string constant too long error for this insert statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("insert into t1(c12) values (X'" +  hexConstant16334 + "abcd')");
				System.out.println("FAIL - should have gotten string constant too long error for this insert statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Next testing maximum constant lengths + 1 through values");
			try {
				s.executeUpdate("values ('" +  stringConstant32671 + "ab')");
				System.out.println("FAIL - should have gotten string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("values (X'" +  hexConstant16334 + "abcd')");
				System.out.println("FAIL - should have gotten string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Next testing maximum constant lengths + n through insert statement");
			try {
				s.executeUpdate("insert into t1(c11) values ('" +  stringConstant32671 + "bcdef')");
				System.out.println("FAIL - should have gotten string constant too long error for this insert statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("insert into t1(c12) values (X'" +  hexConstant16334 + "abcdef')");
				System.out.println("FAIL - should have gotten string constant too long error for this insert statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Next testing maximum constant lengths + n through values");
			try {
				s.executeUpdate("values ('" +  stringConstant32671 + "bcdef')");
				System.out.println("FAIL - should have gotten string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			try {
				s.executeUpdate("values (X'" +  hexConstant16334 + "abcdef')");
				System.out.println("FAIL - should have gotten string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54002"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Next testing odd number of hex digits in a hex constant through insert statement");
			try {
				s.executeUpdate("insert into t1(c12) values (X'" +  hexConstant16334 + "a')");
				System.out.println("FAIL - should have gotten hex constant invalid string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42606"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			System.out.println("And finally testing odd number of hex digits in a hex constant through values statement");
			try {
				s.executeUpdate("values (X'" +  hexConstant16334 + "a')");
				System.out.println("FAIL - should have gotten string constant too long error for this values statement");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("42606"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table t1");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testMostColumnsInTable( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most columns allowed in a table");

			StringBuffer sbTableElements = new StringBuffer();
			String tempString = new String();
			int i = 0;
			sbTableElements.append("create table t1 (");
			for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_TABLE-2; i++)
				sbTableElements.append("c" + i +" int, ");

			Statement s = conn.createStatement();
			System.out.println("First create a table with one column less than maximum allowed number of columns");
			tempString = (sbTableElements.toString()).concat("c" + i + " int)");
			s.executeUpdate(tempString);
			System.out.println("  Try alter table on it to have table with maximum allowed number of columns");
			s.executeUpdate("alter table t1 add column c" + (i+1) + " int");
			System.out.println("  Try another alter table to have table with one column more than maximum allowed number of columns");
			try {
				s.executeUpdate("alter table t1 add column c" + (i+2) + " int");
				System.out.println("FAIL - The alter table should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			s.executeUpdate("drop table t1");

			System.out.println("Next create a table with maximum allowed number of columns");
			tempString = (sbTableElements.toString()).concat("c" + i +" int, c" + (i+1) + " int)");
			s.executeUpdate(tempString);
			System.out.println("  Try alter table to have table with more columns than maximum allowed number of columns");
			try {
				s.executeUpdate("alter table t1 add column c" + (i+2) + " int");
				System.out.println("FAIL - The alter table should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			//just some basic sanity check 
			DatabaseMetaData met = conn.getMetaData();
			getCount(met.getColumns("", "APP", "T1", null));
			s.executeUpdate("insert into t1(c1, c2) values (1,1)");
			s.executeUpdate("drop table t1");

			System.out.println("Next create a table with one column more than maximum allowed number of columns");
			tempString = (sbTableElements.toString()).concat("c" + i +" int, c" + (i+1) + " int, c" + (i+2) + " int)");
			try {
				s.executeUpdate(tempString);
				System.out.println("FAIL - The create table should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Finally, create a table with 2 columns more than maximum allowed number of columns");
			tempString = (sbTableElements.toString()).concat("c" + i +" int, c" + (i+1) + " int, c" + (i+2) + " int, c" + (i+3) + " int)");
			try {
				s.executeUpdate(tempString);
				System.out.println("FAIL - The create table should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	private static void getCount( ResultSet s) throws Throwable
	{
		int counter = 0; // Display data, fetching until end of the result set
		while (s.next())
			counter++;
		System.out.println("Found " + counter + " columns/parameters through meta data");
	}

	public static void testMostColumnsInView( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most columns allowed in a view");

			StringBuffer sbValuesClause = new StringBuffer();
			StringBuffer sbViewColumnNames = new StringBuffer();
			String tempString = new String();
			int i = 0;
			for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_VIEW-2; i++) {
				sbValuesClause.append(1 + ", ");
				sbViewColumnNames.append("c" + i + ", ");
			}

			Statement s = conn.createStatement();
			System.out.println("First create a view with one column less than maximum allowed number of columns");
			tempString = "create view v1(" + sbViewColumnNames.toString() + "c" + i + ") as values (" + sbValuesClause.toString() + "1)";
			s.executeUpdate(tempString);
			s.executeUpdate("drop view v1");

			System.out.println("Next create a view with maximum allowed number of columns");
			tempString = "create view v1(" + sbViewColumnNames.toString() + "c" + i + ", c" + (i+1)+ ") as values (" + sbValuesClause.toString() + "1,1)";
			s.executeUpdate(tempString);
			//just some basic sanity check 
			DatabaseMetaData met = conn.getMetaData();
			getCount(met.getColumns("", "APP", "V1", null));
			s.executeUpdate("drop view v1");

			System.out.println("Next create a view with one column more than that maximum allowed number of columns");
			tempString = "create view v1(" + sbViewColumnNames.toString() + "c" + i + ", c" + (i+1) + ", c" + (i+2) + ") as values (" + sbValuesClause.toString() + "1,1,1)";
			try {
				s.executeUpdate(tempString);
				System.out.println("FAIL - The create view should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("And finally create a view with 2 columns that maximum allowed number of columns");
			tempString = "create view v1(" + sbViewColumnNames.toString() + "c" + i + ", c" + (i+1) + ", c" + (i+2) + ", c" + (i+3) +") as values (" + sbValuesClause.toString() + "1,1,1,1)";
			try {
				s.executeUpdate(tempString);
				System.out.println("FAIL - The create view should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testMostElementsInSelectList( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most elements allowed in a select list");

			StringBuffer sb = new StringBuffer();
			String tempString = new String();
			int i = 0;
			sb.append("create table t1 (");
			for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_TABLE-2; i++)
				sb.append("c" + i +" int, ");

			Statement s = conn.createStatement();
			tempString = (sb.toString()).concat("c" + i + " int)");
			s.executeUpdate(tempString);

			System.out.println("First try a select with one column less than maximum allowed number of columns");
			s.execute("select * from t1");

			System.out.println("Next try a select with maximum allowed number of columns");
			s.execute("select t1.*,1 from t1");

			System.out.println("Next try a select with one column more than maximum allowed number of columns");
			try {
				s.execute("select t1.*,1,2 from t1");
				System.out.println("FAIL - select should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54004"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("Next try a select with 2 more columns than maximum allowed number of columns");
			try {
				s.execute("select t1.*,1,2,3 from t1");
				System.out.println("FAIL - select should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54004"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table t1");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testMostElementsInOrderBy( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most columns allowed in a ORDER BY clause");

			StringBuffer sbOrderBy = new StringBuffer();
			String tempString = new String();
			int i = 0;
			sbOrderBy.append("select * from t1 order by ");
			for (i = 0; i < DB2Limit.DB2_MAX_ELEMENTS_IN_ORDER_BY-2; i++)
				sbOrderBy.append("c1, ");

			Statement s = conn.createStatement();
			s.executeUpdate("create table t1 (c1 int not null, c2 int)");
      
			System.out.println("First try order by with one column less than maximum allowed number of columns");
			tempString = (sbOrderBy.toString()).concat("c2");
			s.execute(tempString);

			System.out.println("Next try an order by with maximum allowed number of columns");
			tempString = (sbOrderBy.toString()).concat("c1, c2");
			s.execute(tempString);

			System.out.println("Next try an order by with one column more than maximum allowed number of columns");
			tempString = (sbOrderBy.toString()).concat("c1, c2, c1");
			try {
				s.execute(tempString);
				System.out.println("FAIL - order by should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54004"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("And finally try an order by with 2 more columns than maximum allowed number of columns");
			tempString = (sbOrderBy.toString()).concat("c1, c2, c1");
			try {
				s.execute(tempString);
				System.out.println("FAIL - order by should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54004"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop table t1");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testMostElementsInGroupBy( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most columns allowed in a GROUP BY clause");
			Statement s = conn.createStatement();
			StringBuffer sbGroupBy = new StringBuffer("select 1 from v1, v2, v3, v4, v5, v6, v7 group by ");
			StringBuffer sbValuesClause = new StringBuffer();
			StringBuffer sbViewColumnNames = new StringBuffer();
			String tempString = new String();

			//first create 7 views with 5000 columns each
			int i = 0;
			for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_VIEW-1; i++)
				sbValuesClause.append(1 + ", ");

			for (int j = 1; j < 8; j++) {
				for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_VIEW-1; i++) {
					sbViewColumnNames.append("c" + j + "" + i + ", ");
				}
				tempString = "create view v" + j + "(" + sbViewColumnNames.toString() + "c" + j + "" + i + ") as values (" + sbValuesClause.toString() + "1)";
				s.executeUpdate(tempString);
				sbViewColumnNames = new StringBuffer();
			}
      
			for (int j = 1; j < 7; j++) {
				for (i = 0; i < DB2Limit.DB2_MAX_COLUMNS_IN_VIEW; i++)
					sbGroupBy.append("c" + j + "" + i + ", ");
			}
			for (i = 0; i < 2675; i++)
				sbGroupBy.append("c7" + i + ", ");

			System.out.println("First try group by with one column less than maximum allowed number of columns");
			tempString = (sbGroupBy.toString()).concat("c72675");
			s.execute(tempString);

			System.out.println("Next try an group by with maximum allowed number of columns");
			tempString = (sbGroupBy.toString()).concat("c72675, c72675");
			s.execute(tempString);

			System.out.println("And finally try an group by with more columns that maximum allowed number of columns");
			tempString = (sbGroupBy.toString()).concat("c72675, c72676, c72677");
			try {
				s.execute(tempString);
				System.out.println("FAIL - group by should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54004"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			s.executeUpdate("drop view v1");
			s.executeUpdate("drop view v2");
			s.executeUpdate("drop view v3");
			s.executeUpdate("drop view v4");
			s.executeUpdate("drop view v5");
			s.executeUpdate("drop view v6");
			s.executeUpdate("drop view v7");

			s.execute("select 1 from v1 group by c1,c2");
			s.executeUpdate("drop table t1");
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	public static void testMostParametersInStoredProcedures( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most parameters allowed for a stored procedure");
			Statement s = conn.createStatement();
			StringBuffer sbCreateProcParams = new StringBuffer();
			StringBuffer sbExecuteProcParams = new StringBuffer();
			String tempString = new String();
			int i = 0;

			for (i = 0; i < DB2Limit.DB2_MAX_PARAMS_IN_STORED_PROCEDURE-2; i++) {
				sbCreateProcParams.append("i" + i + " int, ");
				sbExecuteProcParams.append("1, ");
			}

			System.out.println("First create a procedure with one parameter less than maximum allowed number of parameters");
			tempString = "create procedure P1(" + sbCreateProcParams.toString() + "i" + i +
        " int) parameter style java language java external name \'org.apache.derbyTesting.functionTests.util.ProcedureTest.lessThanMaxParams\' NO SQL";
			s.executeUpdate(tempString);

			System.out.println("Next create a procedure with maximum allowed number of parameters");
			tempString = "create procedure P2(" + sbCreateProcParams.toString() + "i" + i +
        " int, i" + (i+1) + " int) parameter style java language java external name \'org.apache.derbyTesting.functionTests.util.ProcedureTest.maxAllowedParams\' NO SQL";
			s.executeUpdate(tempString);
			//just some basic sanity check 
			DatabaseMetaData met = conn.getMetaData();
			getCount(met.getProcedureColumns("", "APP", "P2", null));

			System.out.println("And finally create a procedure with more parameters that maximum allowed number of parameters");
			tempString = "create procedure P3(" + sbCreateProcParams.toString() + "i" + i +
        " int, i" + (i+1) + " int, i" + (i+2) + " int) parameter style java language java external name \'org.apache.derbyTesting.functionTests.util.ProcedureTest.moreThanMaxAllowedParams\' NO SQL";
			try {
				s.executeUpdate(tempString);
				System.out.println("FAIL - create procedure should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54023"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

		//not running indexes test because it doesn't finish even after running for over 2 hours
		//ALSO, IF WE EVER ENABLE THIS TEST IN FUTURE, WE NEED TO REWRITE THE TEST SO THAT WE TRY TO CREATE OVER
		//32767 *DIFFERENT* INDEXES. AS PART OF DB2 COMPATIBILITY WORK, BUG - 5685 DISALLOWS CREATION OF AN INDEX
		//ON A COLUMN THAT ALREADY HAS A PRIMARY KEY OR UNIQUE CONSTRAINT ON IT.
	public static void testMostIndexesOnTable( Connection conn) throws Throwable
	{
    try {
			System.out.println("Test - most indexes allowed on a table");
			conn.setAutoCommit(false);
			Statement s = conn.createStatement();
			int i = 0;

			s.executeUpdate("create table t1 (c1 int not null, c2 int, primary key(c1))");
			System.out.println("First create one index less than maximum allowed number of indexes");
			for (i = 0; i < DB2Limit.DB2_MAX_INDEXES_ON_TABLE-2; i++) {
				s.executeUpdate("create index i" + i + " on t1(c1,c2)");
			System.out.println("   create index" + i);
			}

			System.out.println("Next create maximum allowed number of indexes");
			s.executeUpdate("create index i" + (i+1) + " on t1(c1,c2)");

			System.out.println("And finally create one index more than maximum allowed number of indexes");
			try {
				s.executeUpdate("create index i" + (i+2) + " on t1(c1,c2)");
				System.out.println("FAIL - create index should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}

			System.out.println("And finally try maximum allowed number of indexes violation using add constraint");
			try {
				s.executeUpdate("alter table t1 add constraint i" + (i+2) + " unique (c1,c2)");
				System.out.println("FAIL - create index should have failed");
			}
			catch (SQLException e) {
				if (e.getSQLState().equals("54011"))
					System.out.println("expected exception " + e.getMessage());
				else
					dumpSQLExceptions(e);
			}
			s.executeUpdate("drop table t1");
			conn.setAutoCommit(true);
		} catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
			sqle.printStackTrace(System.out);
		}
	}

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}

}
