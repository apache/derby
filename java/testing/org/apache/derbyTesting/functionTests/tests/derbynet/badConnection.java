/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.tests.derbynet
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.*;
import java.util.Vector;
import java.util.Properties;
import java.io.File;

import java.io.BufferedOutputStream;
import org.apache.derbyTesting.functionTests.harness.TimedProcess;

/**
	This tests various bad connection states
		- non-existant database
*/

public class badConnection
{
	
	private static Properties properties = new java.util.Properties();

	private static String dbNotFoundUrl =
		"jdbc:derby:net://localhost:1527/notthere";
	private static String  invalidAttrUrl = "jdbc:derby:net://localhost:1527/testbase;upgrade=notValidValue";
	private static String  derbynetUrl = "jdbc:derby:net://localhost:1527/testbase";


	private static Connection newConn(String databaseURL,Properties properties) throws Exception
	{
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(databaseURL, properties); 
			if (conn == null)
				System.out.println("create connection didn't work");
			else
				System.out.println("Connection made\n");

		}
		catch (SQLException se)
		{
			showSQLException(se);
		}

		return conn;
	}

	private static void showSQLException(SQLException e)
	{
		System.out.println("passed SQLException all the way to client, then thrown by client...");
		System.out.println("SQLState is: "+e.getSQLState());
		System.out.println("vendorCode is: "+e.getErrorCode());
		System.out.println("nextException is: "+e.getNextException());
		System.out.println("reason is: "+e.getMessage() +"\n\n");
	}

	public static void main (String args[]) throws Exception
	{
		
		try
		{
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");

			System.out.println("No user/password  (Client error)");
			Connection conn1 = newConn(derbynetUrl, properties);

			System.out.println("Database not Found  (RDBNFNRM)");
			properties.put ("user", "admin");
			properties.put ("password", "admin");
			conn1 = newConn(dbNotFoundUrl, properties);
			if (conn1 != null)
				conn1.close();

			System.out.println("Invalid Attribute  value (RDBAFLRM)");
			conn1 = newConn(invalidAttrUrl, properties);
			if (conn1 != null)
				conn1.close();
		
		}
		catch (SQLException se)
		{
			showSQLException(se);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


}
