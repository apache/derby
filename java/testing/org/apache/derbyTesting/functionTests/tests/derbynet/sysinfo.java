/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.sysinfo

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
import java.net.InetAddress;

import org.apache.derbyTesting.functionTests.harness.jvm;
import org.apache.derbyTesting.functionTests.util.ExecProcUtil;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.tools.ij;

/**
	This tests the sysinfo command
*/

public class sysinfo
{

	private static Properties properties = new java.util.Properties();
	private static jvm jvm;
	private static Vector vCmd;
	private static BufferedOutputStream bos;
	private static String[] SysInfoCmd = new String[] {"org.apache.derby.drda.NetworkServerControl",
		"sysinfo"};
	private static String[] SysInfoLocaleCmd = new String[] {"-Duser.language=err",
		"-Duser.country=DE", "org.apache.derby.drda.NetworkServerControl", "sysinfo"};

    /*
     * Test calling server's sysinfo 
     * 
     */
	public static void test (String args[])
        throws Exception
	{
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
			 *  Test sysinfo
			 ************************************************************/
			System.out.println("Testing Sysinfo");
			ExecProcUtil.execCmdDumpResults(SysInfoCmd,vCmd,bos);	
			System.out.println("End test");
			
			/************************************************************
			 *  Test sysinfo by calling NetworkServerControl.getSysinfo()
			 ************************************************************/
			System.out.println("Testing Sysinfo (method)");
			NetworkServerControl derbyServer = 
				new NetworkServerControl( InetAddress.getByName("localhost"),
										NetworkServerControl.DEFAULT_PORTNUMBER);
			System.out.println(derbyServer.getSysinfo());
			System.out.println("End test (method)");
			
			/************************************************************
			 *  Test sysinfo w/ foreign (non-English) locale
			 ************************************************************/
			System.out.println("Testing Sysinfo (locale)");
			ExecProcUtil.execCmdDumpResults(SysInfoLocaleCmd,vCmd,bos);	
			System.out.println("End test (locale)");

			bos.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
    
    public static void main(String[] args)
        throws Exception
    {
       test(args);
    }

}
