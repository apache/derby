/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.checkSecMgr

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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.derbyTesting.functionTests.util.JDBCTestDisplayUtil;
import org.apache.derby.impl.tools.ij.util;

/**
	This tests to see if the security manager is running.
*/

public class checkSecMgr
{

	public static void main (String args[])
	{
		try
		{
			Connection conn = null;
			util.getPropertyArg(args);
			conn = util.startJBMS();
			// bug 6021
			// testIllegalDBCreate();
			testIllegalPropertySet(conn);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void testIllegalDBCreate() throws Exception
	{
			System.out.println("Security Manager Test Starts");
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection conn = null;

			// This tries to create a database that is not allowed. 
			// To repro bug 6021 change to some disallowed file system.
			// There are two problems with this test.
			// 1) if set to a different file system than the test runs,
			//    (e.g. D:/wombat), a null pointer is thrown.
			// 2) If just set to a disallowed directory on the same file system.
			//    We seem to be able to create the database.
			// Ideally this test should attempt to create the database
			// ../wombat;create=true and get the security exception.
			String databaseURL = "jdbc:derby:net://localhost/" + 
				"\"D:/wombat;create=true\"";
			System.out.println(databaseURL);
			java.util.Properties properties = new java.util.Properties();
			properties.put ("user", "cs");
			properties.put ("password", "cs");

			try {
				conn = DriverManager.getConnection(databaseURL, properties);
				System.out.println("FAILED: Expected Security Exception");
			}
			catch (SQLException se) {
				System.out.println("Expected Security Exception");
				JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);			
			}
	}

	
	/** Try to set a property in a stored procedure for which there is not
	 *  adequate permissions in the policy file
	 */
	public static void testIllegalPropertySet(Connection conn)
	{
		System.out.println("testIllegalPropertySet");
		try {
			String createproc = "CREATE PROCEDURE setIllegalPropertyProc() DYNAMIC RESULT SETS 0 LANGUAGE JAVA EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.derbynet.checkSecMgr.setIllegalPropertyProc' PARAMETER STYLE JAVA";
			PreparedStatement pstmt = conn.prepareStatement(createproc);
			pstmt.executeUpdate();
			CallableStatement cstmt = conn.prepareCall("{call setIllegalPropertyProc()}");
			System.out.println("execute the procedure setting illegal property");
			cstmt.executeUpdate();
			System.out.println("FAILED: Should have gotten security Exception");
		} catch (SQLException se)
		{
			System.out.println("Expected Security Exception");
			JDBCTestDisplayUtil.ShowCommonSQLException(System.out, se);
		}
	}

	public static void setIllegalPropertyProc()
	{
		System.setProperty("notAllowed", "somevalue");
	}
}
