/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testProperties

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

import java.sql.*;
import java.util.Vector;
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.net.InetAddress;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.harness.ProcessStreamResult;

import org.apache.derby.drda.NetworkServerControl;

/**
	This test tests the cloudscape.properties, system properties and 
	command line parameters to make sure the pick up settings in
	the correct order. Search order is:
	   command line parameters
	   System properties
	   cloudscape.properties
	   default

	   The command line should take precedence
*/

public class testProperties
{

	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
    private static  BufferedOutputStream bos = null;


	/**
	 * Execute the given command and dump the results to standard out
	 *
	 * @param args	command and arguments
	 * @param wait  true =wait for completion
	 * @exception Exception
	 */

	private static void execCmdDumpResults (String[] args) throws Exception
	{
        // We need the process inputstream and errorstream
        ProcessStreamResult prout = null;
        ProcessStreamResult prerr = null;
            
		// Start a process to run the command
		Process pr = execCmd(args);
        prout = new ProcessStreamResult(pr.getInputStream(), bos, null);
        prerr = new ProcessStreamResult(pr.getErrorStream(), bos, null);

		// wait until all the results have been processed
		prout.Wait();
		prerr.Wait();

	}


	private static Process execCmd (String[] args) throws Exception
	{
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
		}
		int j = 0;
		for (; i < totalSize; i++)
		{
			serverCmd[i] = args[j++];
		}
 
		// Start a process to run the command
		Process pr = Runtime.getRuntime().exec(serverCmd);
		return pr;
	}


	/** 
	 * Issue derbyServer command if port is null, NetworkServerControl <cmd>
	 * else  NetworkServerControl <cmd> -p <portstring>
	 */
	private static void derbyServerCmd(String cmd, String  portString) throws Exception
	{
		String [] cmdArr = null;
		// For start we don't wait or capture results, just 
		// rely on test Connection to verify the start.
		boolean wait = (cmd.equals("start")) ? false : true;
		
		if (portString == null)
			cmdArr  = new String[] {"org.apache.derby.drda.NetworkServerControl", cmd};
		else if (portString.startsWith("-D"))
			cmdArr = new String[]
			 {portString,"org.apache.derby.drda.NetworkServerControl", cmd};
		else
			cmdArr = new String[] {"org.apache.derby.drda.NetworkServerControl", cmd,"-p", portString};
		if (!wait)
			execCmd(cmdArr);
		else 
			execCmdDumpResults(cmdArr);
	}	
	
	private static void waitForStart(String portString, int timeToWait) throws Exception
	{
		int waitTime = 0;
		int port = Integer.parseInt(portString);
		
		NetworkServerControl derbyServer = new NetworkServerControl( InetAddress.getByName("localhost"),
												  port);
		
		

        while (waitTime < timeToWait) {
            try {
                derbyServer.ping();
                return;
            } catch (Exception e) {
				Thread currentThread = Thread.currentThread();
				synchronized (currentThread) {
                    try {
                        currentThread.wait(1000);
						waitTime += 1000;
						if (waitTime >= timeToWait)
							throw e;
                    } catch (InterruptedException ie) {
                    }
				}
			}
        }
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
        	bos = new BufferedOutputStream(System.out, 1024);
			

			System.out.println("Start testProperties to test property priority");

			/************************************************************
			 *  Test port setting priorty
			 ************************************************************/
			// derby.drda.portNumber set in derby.properties to 1528
			System.out.println("Testing cloudscape.properties Port 1528 ");
			Properties derbyProperties = new Properties();
			derbyProperties.put("derby.drda.portNumber","1528");
			FileOutputStream propFile = new FileOutputStream("derby.properties");
			derbyProperties.store(propFile,"testing cloudscape.properties");
			propFile.close();
			//test start no parameters - Pickup 1528 from cloudscape.properties
			derbyServerCmd("start",null);	
			waitForStart("1528",15000);
			System.out.println("Successfully Connected");
			//shutdown - also picks up from cloudscape.properties
			derbyServerCmd("shutdown",null);
			System.out.println("Testing System properties  Port 1529 ");
			//test start with system property. Overrides cloudscape.properties
			derbyServerCmd("start","-Dderby.drda.portNumber=1529");

			waitForStart("1529",15000);	
			System.out.println("Successfully Connected");
			//shutdown - also picks up from System Properties
			derbyServerCmd("shutdown","1529");
			System.out.println("Testing command line option. Port 1530");
			derbyServerCmd("start","1530");
			waitForStart("1530",15000);		
			System.out.println("Successfully Connected");
			//shutdown - with command line option
			derbyServerCmd("shutdown","1530");
			System.out.println("End test");
			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			// If something went wrong,
			// make sure all these servers are shutdown
			try {derbyServerCmd("shutdown","1527");} catch (Exception se) {}
			try {derbyServerCmd("shutdown","1528");} catch (Exception se) {}
			try {derbyServerCmd("shutdown","1529");} catch (Exception se) {}
			try {derbyServerCmd("shutdown","1530");} catch (Exception se) {}
		}
		finally {
			try {
				File fileToDelete = new File("derby.properties");
				fileToDelete.delete();
			} catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
}





