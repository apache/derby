/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.tests.lang
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
	Test execution of JDBC method, isAutoincrement.

	@author manish
 */
public class AIjdbc
{

	public static void main(String[] args) {
		System.out.println("Test AIjdbc starting");
		boolean passed = true;

		try 
		{
			Connection conn;

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();
			
			// create the table first.
			passed = createTable(conn) && passed;

			// do a select from a base table.
			passed = testSelect(conn) && passed;

			// do a select from a view.
			passed = testSelectView(conn) && passed;
		} 
		catch (Throwable e) 
		{
			passed = false;
			System.out.println("FAIL -- unexpected exception:");
			JDBCDisplayUtil.ShowException(System.out, e);
		}

		if (passed)
			System.out.println("PASS");
		else 
			System.out.println("FAIL");
		
		System.out.println("Test AIjdbc finished");
	}
	
	private static boolean createTable(Connection conn) throws SQLException
	{
		Statement s;
		boolean passed = true;

		System.out.println("Test AIjdbc:creating objects");

		try
		{
			s = conn.createStatement();
			s.execute("create table tab1 (x int, y int generated always as identity,z char(2))");
			s.execute("create view tab1_view (a,b) as select y,y+1 from tab1");
		}
		catch (SQLException se)
		{
			passed = false;
			JDBCDisplayUtil.ShowSQLException(System.out,se);
		}
		
		return passed;
	}

	private static boolean testSelect(Connection conn)
	{
		Statement s;
		boolean passed = true;

		System.out.println("Test AIjdbc:select from base table");

		try 
		{
			s = conn.createStatement();
			ResultSet rs = s.executeQuery("select x,z from tab1");
			ResultSetMetaData rsmd = rs.getMetaData();
			
			if (rsmd.getColumnCount() != 2)
				throw new SQLException("column count doesn't match");
			if (rsmd.isAutoIncrement(1))
				throw new SQLException("column 1 is NOT ai!");
			if (rsmd.isAutoIncrement(2))
				throw new SQLException("column 2 is NOT ai!");
			rs.close();

			rs = s.executeQuery("select y, x,z from tab1");
			rsmd = rs.getMetaData();
			if (rsmd.getColumnCount() != 3)
				throw new SQLException("column count doesn't match");
			if (!rsmd.isAutoIncrement(1))
				throw new SQLException("column 1 IS ai!");
			if (rsmd.isAutoIncrement(2))
				throw new SQLException("column 2 is NOT ai!");
			if (rsmd.isAutoIncrement(3))
				throw new SQLException("column 2 is NOT ai!");
			rs.close();
		}
		catch (SQLException se)
		{
			passed = false;
			JDBCDisplayUtil.ShowSQLException(System.out,se);

		}
		return passed;
	}

	private static boolean testSelectView(Connection conn)
	{
		boolean passed = true;
		System.out.println("Test AIjdbc:select from view");

		try 
		{
			Statement s;
			s = conn.createStatement();
			ResultSet rs = s.executeQuery("select * from tab1_view");
			ResultSetMetaData rsmd = rs.getMetaData();
			
			if (rsmd.getColumnCount() != 2)
				throw new SQLException("column count doesn't match");
			if (!rsmd.isAutoIncrement(1))
				throw new SQLException("column 1 IS ai!");
			if (rsmd.isAutoIncrement(2))
				throw new SQLException("column 1 is NOT ai!");
		}
		catch (SQLException sqle)
		{
			passed = false;
			JDBCDisplayUtil.ShowSQLException(System.out,sqle);
		}
		return passed;
	}
}
