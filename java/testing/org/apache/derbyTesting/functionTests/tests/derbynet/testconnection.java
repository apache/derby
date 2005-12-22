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
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derby.tools.ij;

/**
	This tests the testconnection command
*/

public class testconnection
{

	private static jvm jvm;
	private static Vector vCmd;
	private static String hostName;
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
		"ping", "-h", "localhost", "-p", "1527"};
	private static String[] TestConnectionCmd6b = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost", "-p", "1527"};
	private static String[] TestConnectionCmd7 = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost", "-p", "9393"};
	private static String[] TestConnectionCmd7b = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"ping", "-h", "localhost", "-p", "9393"};

    private static  BufferedOutputStream bos = null;
    
    /**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
     * @param printcmd to printout the executing command or not
	 * @exception Exception
	 */
    private static void execCmdDumpResults (String[] args, boolean printcmd)
        throws Exception
    {
        execCmdDumpResults(args, 0, printcmd);
    }

    /**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
	 * @exception Exception
	 */
    private static void execCmdDumpResults (String[] args)
        throws Exception
    {
        execCmdDumpResults(args, 0, true);
    }


	/**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
     * @param expectedExitCode the exit code that we expect from running this
     */
	private static void execCmdDumpResults (String[] args, int expectedExitCode)
        throws Exception
	{
        execCmdDumpResults(args, expectedExitCode, true);
    }

	/**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
     * @param expectedExitCode the exit code that we expect from running this
     * @param printcmd to indicate if the executing command should get printed
	 * @exception Exception
	 */
	private static void execCmdDumpResults (String[] args, int expectedExitCode, boolean printcmd)
        throws Exception
	{
        // We need the process inputstream and errorstream
        ProcessStreamResult prout = null;
        ProcessStreamResult prerr = null;
            
        StringBuffer sb = new StringBuffer();
            
        for (int i = 0; i < args.length; i++)
        {
            sb.append(args[i] + " ");                    
        }
        if (printcmd)
            System.out.println(sb.toString());
        else
            System.out.println("org.apache.derby.drda.NetworkServerControl ping...");            
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
        
        // wait until the process exits
        pr.waitFor();
        
        // DERBY-214
        if ( pr.exitValue() != expectedExitCode )
        {
            System.out.println("FAIL: expected exit code of " +
                expectedExitCode + ", got exit code of " + pr.exitValue());
        }

	}


	public static void main (String args[]) throws Exception
	{
		hostName = TestUtil.getHostName();
		TestConnectionCmd2[3] = hostName;
		TestConnectionCmd4[3] = hostName;
		TestConnectionCmd5[3] = hostName;
		TestConnectionCmd6b[3] = hostName;
		TestConnectionCmd7b[3] = hostName;
        
 
		
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
			jvm = jvm.getJvm("j9_13");
		else
			jvm = jvm.getJvm("currentjvm");		// ensure compatibility
		vCmd = jvm.getCommandLine();
		try
		{
			Connection conn1 = ij.startJBMS();
        	bos = new BufferedOutputStream(System.out, 1024);

			/************************************************************
			 *  Test testconnection
			 ************************************************************/
			System.out.println("Testing testconnection");
			//test connection - no parameters
            if (!hostName.equals("localhost")) // except with remote server, add the hostName 
            {
                execCmdDumpResults(TestConnectionCmd4, 0, false);
            }
            else
		    	execCmdDumpResults(TestConnectionCmd1, false);	
			//test connection - specifying host and port
			execCmdDumpResults(TestConnectionCmd2);	
			//test connection - specifying non-existant host and port
			execCmdDumpResults(TestConnectionCmd3, 1);	
			//test connection - specifying non-existant host with '-' in the name
			execCmdDumpResults(TestConnectionCmd3a, 1);	
			//test connection - specifying host but no port
			execCmdDumpResults(TestConnectionCmd4);	
			//test connection - specifying host and invalid port
			execCmdDumpResults(TestConnectionCmd5, 1);	
			//test connection - specifying no host and valid port
            if (!hostName.equals("localhost")) // except with remote server, add the hostName
			    execCmdDumpResults(TestConnectionCmd6b, false);	
            else
			    execCmdDumpResults(TestConnectionCmd6, false);	
			//test connection - specifying no host and invalid port
            if (!hostName.equals("localhost")) // except with remote server, add the hostName
			    execCmdDumpResults(TestConnectionCmd7b, 1, false);	
            else
			    execCmdDumpResults(TestConnectionCmd7, 1, false);	

			System.out.println("End test");
			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}
