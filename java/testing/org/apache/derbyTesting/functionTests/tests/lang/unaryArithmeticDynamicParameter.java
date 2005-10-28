/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.unaryArithmeticDynamicParameter

Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;


/**
  This tests unary minus and unary plus as dynamic parameters in PreparedStatements.
  Derby-582
 */
public class unaryArithmeticDynamicParameter { 

	public static void main (String[] argv) throws Throwable
	{
		System.out.println("Test using parameters for unary minus and unary plus");
		ij.getPropertyArg(argv);
		Connection conn = ij.startJBMS();
		Statement s = conn.createStatement();
		s.executeUpdate("create table t1 (c11 int, c12 smallint, c13 double, c14 char(3))");
		s.execute("create table t2 (c21 int)");
		s.execute("insert into t2 values (-1),(-2)");

		System.out.println("insert into t1 values(?,+?,-?,?)");
		PreparedStatement ps = conn.prepareStatement("insert into t1 values(?,+?,-?,?)");
		ps.setInt(1,1);
		ps.setInt(2,1);
		ps.setDouble(3,-1.1);
		ps.setString(4,"abc");
		ParameterMetaData pmd = ps.getParameterMetaData();
		System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
 		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(3));
		ps.executeUpdate();
		ps.setInt(1,-1);
		ps.setInt(2,-1);
		ps.setDouble(3,1.0);
		ps.setString(4,"def");
		ps.executeUpdate();

		System.out.println("select * from t1 where -? in (select c21 from t2)");
		ps = conn.prepareStatement("select * from t1 where -? in (select c21 from t2)");
		ps.setInt(1,1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where c11 = -? and c12 = +? and c13 = ?");
		ps = conn.prepareStatement("select * from t1 where c11 = -? and c12 = +? and c13 = ?");
		ps.setInt(1,-1);
		ps.setInt(2,1);
		ps.setDouble(3,1.1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
 		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
		System.out.println("? parameter type is " + pmd.getParameterTypeName(3));
		dumpRS(ps.executeQuery());
		ps.setShort(1,(short) 1);
		ps.setInt(2,-1);
		ps.setInt(3,-1);
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where -? = ABS_FUNCT(+?)");
		s.execute("CREATE FUNCTION ABS_FUNCT(P1 INT) RETURNS INT CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.Math.abs' LANGUAGE JAVA PARAMETER STYLE JAVA");
		ps = conn.prepareStatement("select * from t1 where -? = abs_funct(+?)");
		ps.setInt(1,-1);
		ps.setInt(2,1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where -? = max_cni(-5,-1)");
		s.execute("CREATE FUNCTION MAX_CNI(P1 INT, P2 INT) RETURNS INT CALLED ON NULL INPUT EXTERNAL NAME 'java.lang.Math.max' LANGUAGE JAVA PARAMETER STYLE JAVA");
		ps = conn.prepareStatement("select * from t1 where -? = max_cni(-5,-1)");
		ps.setInt(1,1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where -? = max_cni(-?,+?)");
		ps = conn.prepareStatement("select * from t1 where -? = max_cni(-?,+?)");
		ps.setInt(1,-1);
		ps.setInt(2,1);
		ps.setInt(3,1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(3));
		dumpRS(ps.executeQuery());

		System.out.println("Try the function again. But use, use sqrt(+?) & abs(-?) functions to send params");
		System.out.println("select * from t1 where -? = max_cni(abs(-?), sqrt(+?))");
		ps = conn.prepareStatement("select * from t1 where -? = max_cni(abs(-?), sqrt(+?))");
		ps.setInt(1,-2);
		ps.setInt(2,1);
		ps.setInt(3,4);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(3));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where c11 between -? and +?");
		ps = conn.prepareStatement("select * from t1 where c11 between -? and +?");
		ps.setInt(1,-1);
		ps.setInt(2,1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where +? not in (-?, +?, 2, ?)");
		ps = conn.prepareStatement("select * from t1 where +? not in (-?, +?, 2, ?)");
		ps.setInt(1,-11);
		ps.setInt(2,1);
		ps.setInt(3,1);
		ps.setInt(4,4);
		pmd = ps.getParameterMetaData();
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(3));
		System.out.println("? parameter type is " + pmd.getParameterTypeName(4));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where +? < c12");
		ps = conn.prepareStatement("select * from t1 where +? < c12");
		ps.setInt(1,0);
		pmd = ps.getParameterMetaData();
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where -? = c11 + ?");
		ps = conn.prepareStatement("select * from t1 where -? = c11 + ?");
		ps.setInt(1,2);
		ps.setInt(2,-1);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where c11 + ? = -?");
		ps = conn.prepareStatement("select * from t1 where c11 + ? = -?");
		ps.setInt(1,-1);
		ps.setInt(2,2);
		pmd = ps.getParameterMetaData();
		System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where c11 + c12 = -?");
		ps = conn.prepareStatement("select * from t1 where c11 + c12 = -?");
		ps.setInt(1,2);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where -? not in (select c21+? from t2)");
		ps = conn.prepareStatement("select * from t1 where -? not in (select c21+? from t2)");
		ps.setInt(1,1);
		ps.setInt(2,2);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select cast(-? as smallint), cast(+? as int) from t1");
		ps = conn.prepareStatement("select cast(-? as smallint), cast(+? as int) from t1");
		ps.setInt(1,2);
		ps.setInt(2,2);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
		dumpRS(ps.executeQuery());

		System.out.println("select nullif(-?,c11) from t1");
		ps = conn.prepareStatement("select nullif(-?,c11) from t1");
		ps.setInt(1,22);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select sqrt(-?) from t1");
		ps = conn.prepareStatement("select sqrt(-?) from t1");
		ps.setInt(1,-64);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		dumpRS(ps.executeQuery());

		System.out.println("select * from t1 where c11 = any (select -? from t2)");
		try {
			ps = conn.prepareStatement("select * from t1 where c11 = any (select -? from t2)");
			ps.setInt(1,1);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -?/+? at the beginning and/ at the end of where clause");
		System.out.println("select * from t1 where -? and c11=c11 or +?");
		try {
			ps = conn.prepareStatement("select * from t1 where -? and c11=c11 or +?");
			ps.setString(1,"SYS%");
			ps.setString(2,"");
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -?/+? in like escape function");
		System.out.println("select * from sys.systables where tablename like -? escape +?");
		try {
			ps = conn.prepareStatement("select * from sys.systables where tablename like -? escape +?");
			ps.setString(1,"SYS%");
			ps.setString(2,"");
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			System.out.println("unary +? parameter type is " + pmd.getParameterTypeName(2));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -?/+? in binary timestamp function");
		System.out.println("select timestamp(-?,+?) from t1");
		try {
			ps = conn.prepareStatement("select timestamp(-?,+?) from t1");
			ps.setInt(1,22);
			ps.setInt(2,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -? in unary timestamp function");
		System.out.println("select timestamp(-?) from t1");
		try {
			ps = conn.prepareStatement("select timestamp(-?) from t1");
			ps.setInt(1,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -? in views");
		System.out.println("create view v1 as select * from t1 where c11 = -?");
		try {
			ps = conn.prepareStatement("create view v1 as select * from t1 where c11 = -?");
			ps.setInt(1,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -? in inner join");
		System.out.println("select * from t1 inner join t1 as t333 on -?");
		try {
			ps = conn.prepareStatement("select * from t1 inner join t1 as t333 on -?");
			ps.setInt(1,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -? by itself in where clause");
		System.out.println("select * from t1 where -?");
		try {
			ps = conn.prepareStatement("select * from t1 where -?");
			ps.setInt(1,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - -? is null not allowed because is null allowed on char types only");
		System.out.println("select * from t1 where -? is null");
		try {
			ps = conn.prepareStatement("select * from t1 where -? is null");
			ps.setInt(1,22);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("select case when -?=c11 then -? else c12 end from t1");
		ps = conn.prepareStatement("select case when -?=c11 then -? else c12 end from t1");
		ps.setInt(1,1);
		ps.setInt(2,22);
		pmd = ps.getParameterMetaData();
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
		System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
		dumpRS(ps.executeQuery());

		System.out.println("Negative test - unary plus parameters on both sides of / operator");
		System.out.println("select * from t1 where c11 = ?/-?");
		try {
			ps = conn.prepareStatement("select * from t1 where c11 = ?/-?");
			ps.setInt(1,0);
			ps.setInt(2,0);
			pmd = ps.getParameterMetaData();
			System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - unary plus in || operation");
		System.out.println("select c11 || +? from t1");
		try {
			ps = conn.prepareStatement("select c11 || +? from t1");
			ps.setInt(1,0);
			pmd = ps.getParameterMetaData();
			System.out.println("? parameter type is " + pmd.getParameterTypeName(1));
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(2));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - unary minus for char column");
		System.out.println("select * from t1 where c14 = -?");
		try {
			ps = conn.prepareStatement("select * from t1 where c14 = -?");
			ps.setInt(1,-1);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}

		System.out.println("Negative test - unary plus for char column");
		System.out.println("select * from t1 where c14 like +?");
		try {
			ps = conn.prepareStatement("select * from t1 where c14 like +?");
			ps.setInt(1,-1);
			pmd = ps.getParameterMetaData();
			System.out.println("unary -? parameter type is " + pmd.getParameterTypeName(1));
			dumpRS(ps.executeQuery());
			System.out.println("FAIL-test should have failed");
		}
		catch (SQLException e) {
			System.out.println("SQL State : " + e.getSQLState());
			System.out.println("Got expected exception " + e.getMessage());
		}
	};
	private static void dumpRS(ResultSet s) throws SQLException
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		java.sql.ResultSetMetaData rsmd = s.getMetaData();

		// Get the number of columns in the result set
		int numCols = rsmd.getColumnCount();

		if (numCols <= 0) 
		{
			System.out.println("(no columns!)");
			return;
		}

		StringBuffer heading = new StringBuffer("\t ");
		StringBuffer underline = new StringBuffer("\t ");

		int len;
		// Display column headings
		for (int i=1; i<=numCols; i++) 
		{
			if (i > 1) 
			{
				heading.append(",");
				underline.append(" ");
			}
			len = heading.length();
			heading.append(rsmd.getColumnLabel(i));
			len = heading.length() - len;
			for (int j = len; j > 0; j--)
			{
				underline.append("-");
			}
		}
		System.out.println(heading.toString());
		System.out.println(underline.toString());
		
	
		StringBuffer row = new StringBuffer();
		// Display data, fetching until end of the result set
		while (s.next()) 
		{
			row.append("\t{");
			// Loop through each column, getting the
			// column data and displaying
			for (int i=1; i<=numCols; i++) 
			{
				if (i > 1) row.append(",");
				row.append(s.getString(i));
			}
			row.append("}\n");
		}
		System.out.println(row.toString());
		s.close();
	}
}

