/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.timeslice

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
import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import org.apache.derbyTesting.functionTests.util.ExecProcUtil;
import org.apache.derby.tools.ij;

/**
	This tests the timeslice command
*/

public class timeslice
{

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
	private static String[] timesliceCmd6 = new String[] {"org.apache.derby.drda.NetworkServerControl",
			"timeslice", "a"};
    private static  BufferedOutputStream bos = null;
	private static  NetworkServerControl server;
	private static String host;
	private static int port = 1527;
    
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

	public static void main (String args[]) throws Exception
	{
		host = TestUtil.getHostName();
		timesliceCmd2[4] = host;
		if ((System.getProperty("java.vm.name") != null) && System.getProperty("java.vm.name").equals("J9"))
			jvm = jvm.getJvm("j9_13");
		else
			jvm = jvm.getJvm("currentjvm");		// ensure compatibility
		vCmd = jvm.getCommandLine();
		try
		{
			Connection conn1 = ij.startJBMS();
        	bos = new BufferedOutputStream(System.out, 1024);

			server = new NetworkServerControl();
			/************************************************************
			 *  Test timeslice
			 ************************************************************/
			System.out.println("Testing timeslice");
			//test timeslice 0
			ExecProcUtil.execCmdDumpResults(timesliceCmd1,vCmd,bos);	
			checkTimeSlice(0);
			//test timeslice -1 
			ExecProcUtil.execCmdDumpResults(timesliceCmd2,vCmd,bos);	
			checkTimeSlice(0);	//default is currently 0
			//test timeslice -12 - should error
			ExecProcUtil.execCmdDumpResults(timesliceCmd3,vCmd,bos);	
			checkTimeSlice(0);
			//test timeslice 2147483647 - should work
			ExecProcUtil.execCmdDumpResults(timesliceCmd4,vCmd,bos);	
			checkTimeSlice(2147483647);
			//test timeslice 9000 - should work
			ExecProcUtil.execCmdDumpResults(timesliceCmd5,vCmd,bos);	
			checkTimeSlice(9000);
			//test timeslice with invlaid value - NumberFormatException
			ExecProcUtil.execCmdDumpResults(timesliceCmd6,vCmd,bos);
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
