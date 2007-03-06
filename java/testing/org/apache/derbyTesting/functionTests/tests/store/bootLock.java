/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.bootLock

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;


import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import java.util.Properties;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 *Testing for FileLocks that prevent Derby Double Boot.
 */

public class bootLock { 
	public static void main(String[] args) {
		Connection con;
		Statement stmt;

		try
		{

			System.out.println("Test BootLock Starting");
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			String derbyHome = System.getProperty("derby.system.home");
			con = ij.startJBMS();

			stmt = con.createStatement();
			stmt.execute("create table t1 (a int)");
			stmt.close();
			con.close();
			try{
				TestUtil.shutdownUsingDataSource("wombat");
			}catch(Exception e)
			{
				//Shutdown will throw exception , just ignore it.	
			}

			//Invoke anothe jvm that makes a connection to database wombat

			String cmd = "java org.apache.derbyTesting.functionTests.tests.store.bootLock1";
			Runtime rtime = Runtime.getRuntime();
			Process p1 = rtime.exec(cmd, (String[])null, new File(derbyHome));
			
			//sleep for some with the hope that other jvm has made the
			//connection.

			Thread.sleep(30000);

			//Now if we try to boot , we should get an multiple 
			//instance exception
			try{
				Properties prop = new Properties();
				prop.setProperty("databaseName", "wombat");
				TestUtil.getDataSourceConnection(prop);
			}catch(SQLException e) {
				System.out.println("expected exception");
				dumpSQLExceptions(e);
			}

			//kill the sub process
			p1.destroy();

		}		
		catch (SQLException e) {
			System.out.println("FAIL -- unexpected exception");
			dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception: "+e);
			e.printStackTrace();
		}

		System.out.println("Test BootLock finished");
    }

	static private void dumpSQLExceptions (SQLException se) {
		while (se != null) {
			System.out.println("SQLSTATE("+se.getSQLState()+"): ");
			se = se.getNextException();
		}
	}
}



