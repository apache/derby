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

import java.sql.*;

/**
 * Methods for testing triggers
 */
public class Triggers
{
    private static final String RESULT_SET_NOT_OPEN = "XCL16";

    private Triggers()
	{
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6516
            try {
                while (rs.next()) {}
            } catch (SQLException e) {
                if (RESULT_SET_NOT_OPEN.equals(e.getSQLState())) {
                    // Some side effect (stored proc?) made the rs close,
                    // bail out
                } else {
                    throw e;
                }
            } finally {
                rs.close();
            }
		}
		stmt.close();
		conn.close();
	}

	public static void getConnection() throws Throwable
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-551
//IC see: https://issues.apache.org/jira/browse/DERBY-1261
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


	public static long returnPrimLong(long  x)
	{
		return x;
	}

	public static Long returnLong(Long x)
	{
		return x;
	}


}
