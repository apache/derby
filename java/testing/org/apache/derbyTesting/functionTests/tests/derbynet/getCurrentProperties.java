/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.getCurrentProperties

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

import org.apache.derby.drda.NetworkServerControl;
import java.util.Properties;
import java.sql.*;
import java.util.Vector;
import java.util.Properties;
/**
	This tests getCurrentProperties
*/

public class getCurrentProperties
{
	private static String databaseURL = "jdbc:derby:net://localhost:1527/wombat;create=true";
	private static Properties properties = new java.util.Properties();
	private static Object joinsync = new Object();
	private static boolean start = false;
	public static void main (String args[]) throws Exception
	{
		try
		{
			NetworkServerControl server = new NetworkServerControl();
			Properties p = server.getCurrentProperties();
			p.list(System.out);
			// create a connection in a different thread
			startConnection();
			// wait for connection
			joinwait();
			//server.setLogWriter(System.out);
			// set tracing on for the waiting connection
			server.trace(3, true);
			// get properties
			System.out.println("Properties with tracing on");
			p = server.getCurrentProperties();
			p.list(System.out);
			// set tracing on for all connections
			server.trace(true);
			// get properties
			System.out.println("Properties with tracing on");
			p = server.getCurrentProperties();
			p.list(System.out);
			joinsignal();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private static void startConnection()
	{
		Runnable service = new Runnable() {
			public void run() {
				try {
					Class.forName("com.ibm.db2.jcc.DB2Driver");
					properties.put ("user", "admin");
					properties.put ("password", "admin");
					Connection conn = DriverManager.getConnection(databaseURL, properties); 

					// signal that connection has been established
					joinsignal();
					joinwait();
				}
				catch (Exception e) {
						throw new RuntimeException(e.getMessage());
					}
				}
			};
			new Thread(service).start();
	}
	private static void joinwait()
	{
		synchronized(joinsync) 
		{
			while(!start)
			{
				try
				{
					joinsync.wait();
				}
				catch(InterruptedException ie) 
				{
					ie.printStackTrace();
				}
			}
		start = false;
		}
	}
	private static void joinsignal() throws InterruptedException
	{
		synchronized(joinsync)
		{
			start = true;
			joinsync.notifyAll();
		}
		Thread.yield();
		Thread.sleep(10000);
	}
}


