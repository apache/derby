/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.bootLock1

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 *Just make a connection to wombat , 
 * Used by bootLock.java to invoke a different jvm and make a connection to wombat 
 * @author suresht
 */

public class bootLock1 { 
	public static void main(String[] args) {
		Connection con;
		Statement stmt;

		try
		{

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
			con = DriverManager.getConnection("jdbc:derby:wombat");
			//infinite loop until it gets killed.
			for(;;)
			{
				Thread.sleep(30000);
			}
		}		
		catch (SQLException e) {
			//			System.out.println("FAIL -- unexpected exception");
			//dumpSQLExceptions(e);
			//e.printStackTrace();
		}
		catch (Throwable e) {
			//System.out.println("FAIL -- unexpected exception: "+e);
			//e.printStackTrace();
		}

    }

	static private void dumpSQLExceptions (SQLException se) {
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): ");
			se = se.getNextException();
		}
	}
}




