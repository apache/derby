/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.GetPropertyInfoTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;


public class GetPropertyInfoTest
{
	static String protocol = "jdbc:derby:";
	static String url = "EncryptedDB;create=true;dataEncryption=true";
	static String driver = "org.apache.derby.jdbc.EmbeddedDriver";

	public static void main(String[] args) throws SQLException,
		InterruptedException, Exception 
    {
		boolean		passed = true;

		// adjust URL to compensate for other encryption providers
		String provider = System.getProperty("testEncryptionProvider");
		if (provider != null)
		{
		    url = "EncryptedDB;create=true;dataEncryption=true;encryptionProvider=" + provider;
		}

		System.out.println("Test GetPropertyInfoTest starting");
		try
		{
			Properties info = new Properties();
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            Class<?> clazz = Class.forName(driver);
			clazz.getConstructor().newInstance();
			Driver cDriver = DriverManager.getDriver(protocol);
			boolean canConnect = false;

			// Test getPropertyInfo by passing attributes in the
			// url.
			for(int i = 0; i < 2; i++)
			{
				// In order to check for inadequate properties, we omit the
				// bootPassword and call getPropertyInfo. (bootPassword is
				// required because dataEncryption is true)
				DriverPropertyInfo[] attributes = cDriver.getPropertyInfo(protocol+url,info);
				
				// zero length means a connection attempt can be made
				if (attributes.length == 0)
				{
					canConnect = true;
					break;
				}

				for (int j = 0; j < attributes.length; j++)
				{
					System.out.print(attributes[j].name + " - value: " + attributes[j].value);
					// Also check on the other PropertyInfo fields
//IC see: https://issues.apache.org/jira/browse/DERBY-1618
					String[] choices = attributes[j].choices;
					System.out.print(" - description: " 
						+ attributes[j].description +
						" - required " + attributes[j].required);
					if (choices != null)
					{
						for (int k = 0; k < choices.length; k++)
						{
							System.out.print("     - choices [" + k + "] : " + choices[k]);
						}
						System.out.print("\n");
					}
					else
						System.out.print(" - choices null \n");
				}

				// Now set bootPassword and call getPropertyInfo again.  
				// This time attribute length should be zero, sice we pass all
				// minimum required properties. 
				url = url + ";bootPassword=db2everyplace";
			}

			if(canConnect == false)
			{
				System.out.println("More attributes are required to connect to the database");
				passed = false;
			}
			else
			{			
				Connection conn = DriverManager.getConnection(protocol + url, info);
				conn.close();
			}
		
			canConnect = false;

			// Test getPropertyInfo by passing attributes in the
			// Properties array.
			info.put("create", "true");
			info.put("dataEncryption", "true");
			info.put("bootPassword", "db2everyplace");
			// Use alternate encryption provider if necessary.
			if (provider != null)
			{ 
			    info.put("encryptionProvider", provider);
			}

			for(int i = 0; i < 2; i++)
			{
				// In order to check for inadequate properties, we omit the
				// database name and call getPropertyInfo. 
				DriverPropertyInfo[] attributes = cDriver.getPropertyInfo(protocol,info);
			
				// zero length means a connection attempt can be made
				if (attributes.length == 0)
				{
					canConnect = true;
					break;
				}

				for (int j = 0; j < attributes.length; j++)
				{
					System.out.print(attributes[j].name + " - value: " + attributes[j].value);
					// Also check on the other PropertyInfo fields
//IC see: https://issues.apache.org/jira/browse/DERBY-1618
					String[] choices = attributes[j].choices;
					System.out.print(" - description: " 
						+ attributes[j].description +
						" - required " + attributes[j].required);
					if (choices != null)
					{
						for (int k = 0; k < choices.length; k++)
						{
							System.out.print("     - choices [" + k + "] : " + choices[k]);
						}
						System.out.print("\n");
					}
					else
						System.out.print(" - choices null \n");
				}

				// Now set database name and call getPropertyInfo again.  
				// This time attribute length should be zero, sice we pass all
				// minimum required properties. 
				info.put("databaseName", "EncryptedDB1");
			}

			if(canConnect == false)
			{
				System.out.println("More attributes are required to connect to the database");
				passed = false;
			}
			else
			{			
				Connection conn1 = DriverManager.getConnection(protocol, info);
				conn1.close();
			}

		}
		catch(SQLException sqle)
		{
			passed = false;
			do {
				System.out.println(sqle.getSQLState() + ":" + sqle.getMessage());
				sqle = sqle.getNextException();
			} while (sqle != null);
		}
		catch (Throwable e) 
		{
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace();
			passed = false;
		}

		if(passed)
			System.out.println("Test GetPropertyInfoTest finished");
		else
			System.out.println("Test GetPropertyInfoTest failed");
	}
}

	









