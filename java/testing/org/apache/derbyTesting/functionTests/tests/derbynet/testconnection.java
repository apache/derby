/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testconnection

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

import java.sql.*;
import java.util.Vector;
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.harness.ProcessStreamResult;

/**
	This tests the testconnection command
*/

public class testconnection
{

	private static String databaseURL = "jdbc:derby:net://localhost:1527/wombat;create=true";
	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
	private static String[] TestConnectionCmd1 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping"};
	private static String[] TestConnectionCmd2 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost", "-p", "1527"};
	private static String[] TestConnectionCmd3 = new String[] {"org.apache.derby.drda.NetworkServerControl",
				    "ping", "-h", "notthere", "-p", "1527"};
	private static String[] TestConnectionCmd3a = new String[] {"org.apache.derby.drda.NetworkServerControl",																		"ping", "-h", "ihave-inmyname.com", "-p", "1527"};
	private static String[] TestConnectionCmd4 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost"};
	private static String[] TestConnectionCmd5 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost", "-p", "9393"};
	private static String[] TestConnectionCmd6 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-p", "1527"};
	private static String[] TestConnectionCmd7 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-p", "9393"};

    private static  BufferedOutputStream bos = null;
	/**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
	 * @exception Exception
	 */

	private static void execCmdDumpResults (String[] args) throws Exception
	{
        // We need the process inputstream and errorstream
        ProcessStreamResult prout = null;
        ProcessStreamResult prerr = null;
            
        StringBuffer sb = new StringBuffer();
            
        for (int i = 0; i < args.length; i++)
        {
            sb.append(args[i] + " ");                    
        }
        System.out.println(sb.toString());
		int totalSize = vCmd.size() + args.length;
		String serverCmd[] = new String[totalSize];
		int i;
		for (i = 0; i < vCmd.size(); i++)
		{
			serverCmd[i] = (String)vCmd.elementAt(i);
		//	System.out.println("serverCmd["+i+"]: "+serverCmd[i]);
		}
		int j = 0;
		for (; i < totalSize; i++)
		{
			serverCmd[i] = args[j++];
		//	System.out.println("serverCmd["+i+"]: "+serverCmd[i]);
		}
 
		// Start a process to run the command
		Process pr = Runtime.getRuntime().exec(serverCmd);
        prout = new ProcessStreamResult(pr.getInputStream(), bos, null);
        prerr = new ProcessStreamResult(pr.getErrorStream(), bos, null);

		// wait until all the results have been processed
		prout.Wait();
		prerr.Wait();

	}

	private static Connection newConn() throws Exception
	{
		Connection conn = DriverManager.getConnection(databaseURL, properties); 
		if (conn == null)
			System.out.println("create connection didn't work");
		return conn;
	}

	public static void main (String args[]) throws Exception
	{
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
			jvm = jvm.getJvm("j9_13");
		else
			jvm = jvm.getJvm("currentjvm");		// ensure compatibility
		vCmd = jvm.getCommandLine();
		try
		{
			// Initialize JavaCommonClient Driver.
			Class.forName("com.ibm.db2.jcc.DB2Driver");
			properties.put ("user", "admin");
			properties.put ("password", "admin");

			Connection conn1 = newConn();
        	bos = new BufferedOutputStream(System.out, 1024);

			/************************************************************
			 *  Test testconnection
			 ************************************************************/
			System.out.println("Testing testconnection");
			//test connection - no parameters
			execCmdDumpResults(TestConnectionCmd1);	
			//test connection - specifying host and port
			execCmdDumpResults(TestConnectionCmd2);	
			//test connection - specifying non-existant host and port
			execCmdDumpResults(TestConnectionCmd3);	
			//test connection - specifying non-existant host with '-' in the name
			execCmdDumpResults(TestConnectionCmd3a);	
			//test connection - specifying host but no port
			execCmdDumpResults(TestConnectionCmd4);	
			//test connection - specifying host and invalid port
			execCmdDumpResults(TestConnectionCmd5);	
			//test connection - specifying no host and valid port
			execCmdDumpResults(TestConnectionCmd6);	
			//test connection - specifying no host and invalid port
			execCmdDumpResults(TestConnectionCmd7);	

			System.out.println("End test");
			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
