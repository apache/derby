/*

   Derby - Class org.apache.derbyTesting.functionTests.util.Triggers

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.db.*;
import java.sql.*;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Methods for testing triggers
 */
public class Triggers
{
	private Triggers()
	{
	}

	public static String triggerFiresMinimal(String string) throws Throwable
	{
		System.out.println("TRIGGER: " + "<"+string+">");
		return "";
	}

	public static String triggerFires(String string) throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println("TRIGGER: " + "<"+string+"> on statement "+tec.getEventStatementText());
		printTriggerChanges();
		return "";
	}

	public static int doNothingInt() throws Throwable
	{
		return 1;
	}

	public static void doNothing() throws Throwable
	{}

	public static int doConnCommitInt() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
		return 1;
	}

	public static void doConnCommit() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.commit();
	}
			
	public static void doConnRollback() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.rollback();
	}

	public static void doConnectionSetIsolation() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setTransactionIsolation(conn.TRANSACTION_SERIALIZABLE);
	}
			
	public static int doConnStmtIntNoRS(String text) throws Throwable
	{
		doConnStmtNoRS(text);
		return 1;
	}
	public static void doConnStmtNoRS(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		stmt.execute(text);
	}

	public static int doConnStmtInt(String text) throws Throwable
	{
		doConnStmt(text);
		return 1;
	}
	public static void doConnStmt(String text) throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		if (stmt.execute(text))
		{
			ResultSet rs = stmt.getResultSet();
			while (rs.next())
			{}
			rs.close();
		}
		stmt.close();
		conn.close();
	}

	public static void getConnection() throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.close();
		System.out.println("getConnection() called");
	}
	// used for performance numbers
	static void zipThroughRs(ResultSet s) throws SQLException
	{
		if (s == null)
			return;
		
		while (s.next()) ;
	}

	private static void printTriggerChanges() throws Throwable
	{
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		System.out.println("BEFORE RESULT SET");
		dumpRS(tec.getOldRowSet());
		System.out.println("\nAFTER RESULT SET");
		dumpRS(tec.getNewRowSet());
	}

	// lifted from the metadata test	
	private static void dumpRS(ResultSet s) throws SQLException 
	{
		if (s == null)
		{
			System.out.println("<NULL>");
			return;
		}

		ResultSetMetaData rsmd = s.getMetaData();

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

	public static long returnPrimLong(long  x)
	{
		return x;
	}

	public static Long returnLong(Long x)
	{
		return x;
	}


}