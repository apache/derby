/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.maxthreads

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

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.harness.ProcessStreamResult;

/**
	This tests the maxthreads command
*/

public class maxthreads
{
	private static String databaseURL = "jdbc:derby:net://localhost:1527/wombat;create=true";
	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
	private static String[] maxthreadsCmd1 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"maxthreads", "0"};
	private static String[] maxthreadsCmd2 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"maxthreads","-1", "-h", "localhost", "-p", "1527"};
	private static String[] maxthreadsCmd3 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"maxthreads", "-12"};
	private static String[] maxthreadsCmd4 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"maxthreads", "2147483647"};
	private static String[] maxthreadsCmd5 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"maxthreads", "9000"};
    private static  BufferedOutputStream bos = null;
	private static  NetworkServerControl server;
	private static String host = "localhost";
	private static int port = 1527;
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
	private static void checkMaxThreads( int value)
		throws Exception
	{
		int maxValue = server.getMaxThreads();
		if (maxValue == value)
			System.out.println("PASS - max threads value, "+ value +" is correct");
		else
			System.out.println("FAIL - max threads value is " + maxValue + " should be "
				+ value);
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

			server = new NetworkServerControl();
			/************************************************************
			 *  Test max threads
			 ************************************************************/
			System.out.println("Testing maxthreads");
			//test maxthreads 0
			execCmdDumpResults(maxthreadsCmd1);	
			checkMaxThreads(0);
			//test maxthreads -1 
			execCmdDumpResults(maxthreadsCmd2);	
			checkMaxThreads(0);	//default is currently 0
			//test maxthreads -12 - should error
			execCmdDumpResults(maxthreadsCmd3);	
			checkMaxThreads(0);
			//test maxthreads 2147483647 - should work
			execCmdDumpResults(maxthreadsCmd4);	
			checkMaxThreads(2147483647);
			//test maxthreads 9000 - should work
			execCmdDumpResults(maxthreadsCmd5);	
			checkMaxThreads(9000);
			// try the same values using the callable interface
			//test maxthreads 0
			server.setMaxThreads(0);
			checkMaxThreads(0);
			//test maxthreads -1 
			server.setMaxThreads(-1);
			checkMaxThreads(0);
			//test maxthreads -2 - should error
			try {
				server.setMaxThreads(-2);
			} catch (Exception e) {
				System.out.println (e.getMessage());
			}
			//test maxthreads 2147483647 - should work
			server.setMaxThreads(2147483647);
			checkMaxThreads(2147483647);
			//test maxthreads 9000 - should work
			server.setMaxThreads(9000);
			checkMaxThreads(9000);
			System.out.println("End test");
			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
