/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.runtimeinfo

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
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.net.InetAddress;
import java.sql.*;
import java.util.Vector;
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.ExecProcUtil;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
	This tests the runtimeinfo command
*/

public class runtimeinfo
{

	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
	private static BufferedOutputStream bos;
	private static String[] RuntimeinfoCmd = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"runtimeinfo"};
	private static String[] RuntimeinfoLocaleCmd = new String[] {"-Duser.language=err",
		"-Duser.country=DE", "org.apache.derby.drda.NetworkServerControl", "runtimeinfo"};
    


	public static void main (String args[]) throws Exception
	{
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
			jvm = jvm.getJvm("j9_13");
		else
			jvm = jvm.getJvm("currentjvm");		// ensure compatibility
		vCmd = jvm.getCommandLine();
		try
		{
			ij.getPropertyArg(args);
			Connection conn1 = ij.startJBMS();
            bos = new BufferedOutputStream(System.out, 1024);

			
			/************************************************************
			 *  Test runtimeinfo
			 ************************************************************/
			System.out.println("Testing Runtimeinfo");
			ExecProcUtil.execCmdDumpResults(RuntimeinfoCmd,vCmd,bos);	
			System.out.println("End test");
			
			// Now get a couple of connections with some prepared statements
			Connection conn2 = ij.startJBMS();
			PreparedStatement ps = prepareAndExecuteQuery(conn1,"SELECT count(*) from sys.systables");
			PreparedStatement ps2 = prepareAndExecuteQuery(conn1,"VALUES(1)");

			Connection conn3 = ij.startJBMS();
			PreparedStatement ps3 = prepareAndExecuteQuery(conn2,"SELECT count(*) from sys.systables");
			PreparedStatement ps4 = prepareAndExecuteQuery(conn2,"VALUES(2)");


			/************************************************************
			 *  Test runtimeinfo w/ foreign (non-English) locale
			 ************************************************************/
			System.out.println("Testing Runtimeinfo (locale)");
			ExecProcUtil.execCmdDumpResults(RuntimeinfoLocaleCmd,vCmd,bos);	
			System.out.println("End test (locale)");
			ps.close();
			ps2.close();
			ps3.close();
			ps4.close();
			conn1.close();
			conn2.close();
			conn3.close();
			/** once more after closing the connections 
			 * - by calling NetworkServerControl.getRuntimeInfo 
			 */
			System.out.println("Testing Runtimeinfo after closing connectiosn");
			// give the network server a second to clean up (DERBY-1455)
			Thread.sleep(1000);
			NetworkServerControl derbyServer = 
				new NetworkServerControl( InetAddress.getByName("localhost"),
										NetworkServerControl.DEFAULT_PORTNUMBER);
			System.out.println(derbyServer.getRuntimeInfo());	
			System.out.println("End test");

			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}


	public static PreparedStatement prepareAndExecuteQuery(Connection conn, 
														   String sql)
		throws SQLException
	{
		PreparedStatement ps  = conn.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		rs.next();
		return ps;
	}
}










