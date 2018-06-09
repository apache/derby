/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ShutdownMasterServer
 
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

package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.io.IOException;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test (slave) behaviour after killing the master server.
 */
public class ShutdownMasterServerByOsKill extends ShutdownMaster
{
    
    public ShutdownMasterServerByOsKill(String testcaseName)
    {
        super(testcaseName);
    }
        
    public static Test suite()
    throws Exception
    {
        System.out.println("**** ShutdownMasterServerByOsKill.suite()");
        System.out.println("'ShutdownMasterServerByOsKill' can not be run outside the 'ReplicationRun' framework.");
        
        setEnv();
        
        BaseTestSuite suite =
            new BaseTestSuite("ShutdownMasterServerByOsKillCtrl");

        suite.addTest(ShutdownMasterServerByOsKill.suite(masterServerHost,masterServerPort)); // (masterHostName, masterPortNo));
        return (Test)suite;
    }
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    throws IOException
    {
        System.out.println("*** ShutdownMasterServerByOsKill.suite("+serverHost+","+serverPort+")");
        
        Test t = TestConfiguration.existingServerSuite(ShutdownMasterServerByOsKill.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(ShutdownMasterServerByOsKill.class,false,"
                +serverHost+":"+serverPort+")");
        return t;
    }
        
    /**
     * Test shut down master server during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testShutdownMasterServer()
    throws SQLException, IOException, InterruptedException
    {        
        shutdown(null, false, true); // (null: use NetworkServerControl, false : shutdown server,) true: us OS kill on server!
         
        // Check slave state:...
   }
}
