/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.timeslice

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
	This tests the timeslice command
*/

public class timeslice
{

	private static String databaseURL = "jdbc:derby:net://localhost:1527/wombat;create=true";
	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
	private static String[] timesliceCmd1 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"timeslice", "0"};
	private static String[] timesliceCmd2 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"timeslice","-1", "-h", "localhost", "-p", "1527"};
	private static String[] timesliceCmd3 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"timeslice", "-12"};
	private static String[] timesliceCmd4 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"timeslice", "2147483647"};
	private static String[] timesliceCmd5 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"timeslice", "9000"};
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
	private static void checkTimeSlice( int value)
		throws Exception
	{
		int timeSliceValue = server.getTimeSlice();
		if (timeSliceValue == value)
			System.out.println("PASS - time slice value, "+value+" is correct");
		else
			System.out.println("FAIL - time slice value is " + timeSliceValue + " should be "
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
			 *  Test timeslice
			 ************************************************************/
			System.out.println("Testing timeslice");
			//test timeslice 0
			execCmdDumpResults(timesliceCmd1);	
			checkTimeSlice(0);
			//test timeslice -1 
			execCmdDumpResults(timesliceCmd2);	
			checkTimeSlice(0);	//default is currently 0
			//test timeslice -12 - should error
			execCmdDumpResults(timesliceCmd3);	
			checkTimeSlice(0);
			//test timeslice 2147483647 - should work
			execCmdDumpResults(timesliceCmd4);	
			checkTimeSlice(2147483647);
			//test timeslice 9000 - should work
			execCmdDumpResults(timesliceCmd5);	
			checkTimeSlice(9000);
			//test callable interface
			//test timeslice 0
			server.setTimeSlice(0);
			checkTimeSlice(0);
			//test timeslice -1 
			server.setTimeSlice(-1);
			checkTimeSlice(0);	//default is currently 0
			//test timeslice -2 - should error
			try {
				server.setTimeSlice(-2);
			} catch (Exception e) {
				System.out.println ("Expecting exception:" + e.getMessage());
			}
			checkTimeSlice(0);
			//test timeslice 2147483647 - should work
			server.setTimeSlice(2147483647);
			checkTimeSlice(2147483647);
			//test timeslice 9000 - should work
			server.setTimeSlice(9000);
			checkTimeSlice(9000);
			System.out.println("End test");
			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
