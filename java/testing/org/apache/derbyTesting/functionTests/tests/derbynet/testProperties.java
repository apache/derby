/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.testProperties

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

import java.sql.*;
import java.util.Vector;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.net.InetAddress;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.harness.ProcessStreamResult;

import org.apache.derby.drda.NetworkServerControl;

/**
	This test tests the derby.properties, system properties and 
	command line parameters to make sure the pick up settings in
	the correct order. Search order is:
	   command line parameters
	   System properties
	   derby.properties
	   default

	   The command line should take precedence

	It also tests start server by specifying system properties without values.
	In this case the server will use default values.
*/

public class testProperties
{

	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
    private static  BufferedOutputStream bos = null;

    //Command to start server specifying system properties without values
    private static String[] startServerCmd =
					new String[] {  "-Dderby.drda.logConnections",
    								"-Dderby.drda.traceAll",
									"-Dderby.drda.traceDirectory",
									"-Dderby.drda.keepAlive",
									"-Dderby.drda.timeSlice",
									"-Dderby.drda.host",
									"-Dderby.drda.portNumber",
									"-Dderby.drda.minThreads",
									"-Dderby.drda.maxThreads",
									"-Dderby.drda.startNetworkServer",
									"-Dderby.drda.debug",
									"org.apache.derby.drda.NetworkServerControl",
									"start"};
    
    //No arguments
    private static String[] cmdWithoutArgs =
					new String[] {  "org.apache.derby.drda.NetworkServerControl"};
    
    //Unknown command
    private static String[] cmdUnknown =
					new String[] {  "org.apache.derby.drda.NetworkServerControl",
    								"unknowncmd"};
    
    //wrong no: of arguments
    private static String[] cmdWithWrongArgNum =
					new String[] {  "org.apache.derby.drda.NetworkServerControl",
    								"ping",
									"arg1"};
    
    //trace on
    private static String[] cmdTraceOn =
					new String[] {  "org.apache.derby.drda.NetworkServerControl",
    								"trace",
									"on",
									"-p",
									"1527"};
    
    //trace off
    private static String[] cmdTraceOff =
		new String[] {  "org.apache.derby.drda.NetworkServerControl",
						"trace",
						"off",
						"-p",
						"1527"};
    
    //logconnections on
    private static String[] cmdLogconnectionsOn =
					new String[] {  "org.apache.derby.drda.NetworkServerControl",
    								"logconnections",
									"on",
									"-p",
									"1527"};	
    /**
	 * Execute the given command and optionally wait and dump the results to standard out
	 *
	 * @param args	command and arguments
	 * @param wait  true =wait for completion and dump output, false don't wait and
     * ignore the output.
	 * @exception Exception
	 */

	private static void execCmdDumpResults (String[] args, boolean wait) throws Exception
	{
        // We need the process inputstream and errorstream
        ProcessStreamResult prout = null;
        ProcessStreamResult prerr = null;
            
        System.out.flush();
        bos.flush();
        
        BufferedOutputStream _bos = bos;
        if (!wait) {
            // not interested in the output, don't expect a huge amount.
            // information will just be written to the byte array in
            // memory and never used.
            _bos = new BufferedOutputStream(new ByteArrayOutputStream());
        }
		// Start a process to run the command
		Process pr = execCmd(args);
        prout = new ProcessStreamResult(pr.getInputStream(), _bos, null);
        prerr = new ProcessStreamResult(pr.getErrorStream(), _bos, null);
        
        if (!wait)
            return;

		// wait until all the results have been processed
		prout.Wait();
		prerr.Wait();
        _bos.flush();
        System.out.flush();

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
		
        execCmdDumpResults(cmdArr, wait);
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

	private static void listProperties(String portString) throws Exception{
		int port = Integer.parseInt(portString);
		NetworkServerControl derbyServer = new NetworkServerControl( InetAddress.getByName("localhost"),
													port);
		Properties p = derbyServer.getCurrentProperties();
		p.list(System.out);
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
			System.out.println("Testing derby.properties Port 1528 ");
			Properties derbyProperties = new Properties();
			derbyProperties.put("derby.drda.portNumber","1528");
			FileOutputStream propFile = new FileOutputStream("derby.properties");
			derbyProperties.store(propFile,"testing derby.properties");
			propFile.close();
			//test start no parameters - Pickup 1528 from derby.properties
			derbyServerCmd("start",null);	
			waitForStart("1528",15000);
			System.out.println("Successfully Connected");
			//shutdown - also picks up from derby.properties
			derbyServerCmd("shutdown",null);
			System.out.println("Testing System properties  Port 1529 ");
			//test start with system property. Overrides derby.properties
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

			/**********************************************************************
			 *  Test start server specifying system properties without values
			 *********************************************************************/
			System.out.println("Testing start server by specifying system properties without values");
			System.out.println("First shutdown server started on default port by the test harness");

			//Shutdown the server started by test
			derbyServerCmd("shutdown","1527");
            execCmdDumpResults(startServerCmd, false);
			waitForStart("1527",15000);
			//check that default properties are used
			listProperties("1527");
			
			//Test trace and logconnections commands
			execCmdDumpResults(cmdTraceOn, true);
			execCmdDumpResults(cmdLogconnectionsOn, true);
			listProperties("1527");
			execCmdDumpResults(cmdTraceOff, true);
			listProperties("1527");
			derbyServerCmd("shutdown","1527");
			
			//Test error conditions in command-line
			execCmdDumpResults(cmdWithoutArgs, true);
			execCmdDumpResults(cmdUnknown, true);
			execCmdDumpResults(cmdWithWrongArgNum, true);
			
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





