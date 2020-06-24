/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationSuite

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.replicationTests
 *
 */
public class ReplicationSuite extends BaseTestCase  
{
	/**
	 * Use suite method instead.
	 */
	private ReplicationSuite(String name) {
		super(name);
	}

	public static Test suite() throws SQLException {

        BaseTestSuite suite = new BaseTestSuite("ReplicationSuite");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // DERBY-5998: The replication code uses javax.net.ServerSocketFactory
        // to set up communication channels between the master and the slave.
        // That class is only available in an optional part of the Foundation
        // Profile API. Skip the replication tests if it is not available.
        try {
            Class.forName("javax.net.ServerSocketFactory");
        } catch (ClassNotFoundException cnfe) {
            println("Skipping replication tests since "
                    + "javax.net.ServerSocketFactory is not available");
            return suite;
        }

        // The tests require both DriverManager and ClientDataSource. None
        // of those classes are available in JSR-169, so only run the test
        // on platforms that support JDBC3 and higher. The tests also require
        // a network server.
        if (JDBC.vmSupportsJDBC3() && Derby.hasServer()) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5677

//IC see: https://issues.apache.org/jira/browse/DERBY-4638
            suite.addTest(ReplicationRun_Local.suite());
            suite.addTest(ReplicationRun_Local.localAuthenticationSuite());

            suite.addTest(ReplicationRun_Local_1.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3162

            suite.addTest(ReplicationRun_Local_1Indexing.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3921

            suite.addTest(ReplicationRun_Local_StateTest_part1.suite());
            suite.addTest(ReplicationRun_Local_StateTest_part1_1.suite());
            suite.addTest(ReplicationRun_Local_StateTest_part1_2.suite());
            suite.addTest(ReplicationRun_Local_StateTest_part1_3.suite());

            suite.addTest(ReplicationRun_Local_StateTest_part2.suite());

            // Run this separatly as it produces extra output:
            // suite.addTest(ReplicationRun_Local_showStateChange.suite());

//IC see: https://issues.apache.org/jira/browse/DERBY-3738
            suite.addTest(ReplicationRun_Local_3_p1.suite());
            suite.addTest(ReplicationRun_Local_3_p2.suite());
            suite.addTest(ReplicationRun_Local_3_p3.suite());
            suite.addTest(ReplicationRun_Local_3_p4.suite());

            // Test for DERBY-3878
            suite.addTest(ReplicationRun_Local_3_p5.suite());

            suite.addTest(ReplicationRun_Local_Encrypted_1.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3921

            suite.addTest(ReplicationRun_Local_3_p6.suite());

            suite.addTest(ReplicationRun_Local_Derby4910.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4910
//IC see: https://issues.apache.org/jira/browse/DERBY-4812

        }

        suite.addTest(Derby5937SlaveShutdownTest.suite());

		return suite;
	}
}
