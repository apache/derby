/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.store.Beetle6038

   Copyright 2004, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.*;
import java.io.*;
import java.util.*;

/**
 *  Test that the two new encryption properties
 *	DATA_ENCRYPT_ALGORITHM_VERSION="data_encrypt_algorithm_version"
 *  LOG_ENCRYPT_ALGORITHM_VERSION="log_encrypt_algorithm_version"
 *	exist and verify the version. Note, these values start off with 1.
 */
public class Beetle6038  {

	public static void main(String[] args)
		throws Exception
	{
		String driver = "org.apache.derby.jdbc.EmbeddedDriver";
		Class.forName(driver).newInstance();
		String dburl = null;

		if(System.getProperty("java.vm.vendor") != null)
		{
			String vendor = System.getProperty("java.vm.vendor");
			if(vendor.toUpperCase().lastIndexOf("SUN") != -1)
				dburl="jdbc:derby:Beetle6038Db;create=true;dataEncryption=true;bootPassword=Thursday;encryptionAlgorithm=DES/CBC/NoPadding;encryptionProvider=com.sun.crypto.provider.SunJCE";
			else
			 dburl = "jdbc:derby:Beetle6038Db;create=true;dataEncryption=true;bootPassword=Thursday;encryptionAlgorithm=DES/CBC/NoPadding;encryptionProvider=com.ibm.crypto.provider.IBMJCE";
		}

		Connection conn = DriverManager.getConnection(dburl);
		conn.close();
		conn = DriverManager.getConnection(dburl);
		conn.close();

		// read in the properties in the service.properties file of the db
		Properties serviceProperties = new Properties();
		File f = new File("Beetle6038/Beetle6038Db/service.properties");
		serviceProperties.load(new FileInputStream(f.getAbsolutePath()));

		// check if the properties are set
		checkProperty("data_encrypt_algorithm_version",serviceProperties);
		checkProperty("log_encrypt_algorithm_version",serviceProperties);
	}

	public static void checkProperty(String name,Properties props)
	{
		String value = props.getProperty(name);

		if( value == null )
			System.out.println("Test failed!! - "+name + " not set in service.properties as expected");
		else
			System.out.println(name+"="+value);
	}


}
