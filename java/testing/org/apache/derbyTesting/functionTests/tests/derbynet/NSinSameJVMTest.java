/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.DRDAProtocolTest

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

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class NSinSameJVMTest extends BaseJDBCTestCase {
	public NSinSameJVMTest(String name) {
		super(name);
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}
	/** Test NetworkServer start and shutdown on a different port 
	 * other than 1527 in the same jvm
	 * @throws Exception
	 **/
	public void testShutdown() throws Exception {
		final int NETWORKSERVER_PORT = 20000; //port to start the server
		NetworkServerControl serverControl = null;
		try {
			serverControl = new NetworkServerControl(InetAddress
					.getByName("localhost"), NETWORKSERVER_PORT);//initialized for the shutdown.
			Connection connection = null;
			// Just connect, do something and close the connection
			connection = getConnection();
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("Select  tablename   from  sys.systables");	
			JDBC.assertDrainResults(rs);
			serverControl.shutdown();
			Thread.sleep(5000);
		} catch (Exception e) {
			System.out.print("FAIL: Unexpected exception" + e.getMessage());
			e.printStackTrace();
		}
	}
	 /**
     * Construct the name of the server policy file.
     */
	public static Test suite() {
        Test test;
        test = TestConfiguration.clientServerSuite(NSinSameJVMTest.class,20000);
        return test;
    }
}
