/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.executeUpdate

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
/**
	This test tests the JDBC Statement executeUpdate method. Since IJ will eventually
	just use execute rather then executeUpdate, I want to make sure that executeUpdate
	is minimally covered.
*/

class executeUpdate
{

	public static void main (String args[])
	{
		try
		{
			System.out.println("executeUpdate Test Starts");
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			Connection conn = null;

			String databaseURL = "jdbc:derby:net://localhost/wombat;create=true";
			java.util.Properties properties = new java.util.Properties();
			properties.put ("user", "judy");
			properties.put ("password", "judy");
			conn = DriverManager.getConnection(databaseURL, properties);
			
			if (conn == null)
			{
				System.out.println("conn didn't work");
				return;
			}
			Statement stmt = conn.createStatement();
			int rowCount = stmt.executeUpdate("create table exup(a int)");
			if (rowCount != 0)
				System.out.println("FAIL - non zero return count on create table");
			else
				System.out.println("PASS - create table");
			rowCount = stmt.executeUpdate("insert into exup values(1)");
			if (rowCount != 1)
				System.out.println("FAIL - expected row count 1, got " + rowCount);
			else
				System.out.println("PASS - insert 1 row");
			rowCount = stmt.executeUpdate("insert into exup values(2),(3),(4)");
			if (rowCount != 3)
				System.out.println("FAIL - expected row count 3, got " + rowCount);
			else
				System.out.println("PASS - insert 3 rows");
			System.out.println("Rows in table should be 1,2,3,4");
			ResultSet rs = stmt.executeQuery("select * from exup");
			int i = 1;
			boolean fail = false;
			int val;
			while (rs.next())
			{
				if (i++ != (val = rs.getInt(1)))
				{
					System.out.println("FAIL - expecting " + i + " got " + val);
					fail = true;
				}
			}
			if (i != 5)
				System.out.println("FAIL - too many rows in table");
			else if (!fail)
				System.out.println("PASS - correct rows in table");
			rs.close();
			rowCount = stmt.executeUpdate("drop table exup");
			if (rowCount != 0)
				System.out.println("FAIL - non zero return count on drop table");
			else
				System.out.println("PASS - drop table");
			stmt.close();
			System.out.println("executeUpdate Test ends");

        }
        catch (java.sql.SQLException e) {
				e.printStackTrace();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
