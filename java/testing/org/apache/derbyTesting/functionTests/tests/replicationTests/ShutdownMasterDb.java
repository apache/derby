/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ShutdownMasterDb
 
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
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ShutdownMasterDb extends ShutdownMaster
{
    
    public ShutdownMasterDb(String testcaseName)
    {
        super(testcaseName);
    }
    
    
    public static Test suite()
    throws Exception
    {
        System.out.println("**** ShutdownMasterDb.suite()");
        System.out.println("'ShutdownMasterDb' can not be run outside the 'ReplicationRun' framework.");
        
        setEnv();
        
        TestSuite suite = new TestSuite("ShutdownMasterDb");
        suite.addTest(ShutdownMasterDb.suite(masterServerHost,masterServerPort)); // (masterHostName, masterPortNo));
        return (Test)suite;
    }
    /**
     * Adds this class to the *existing server* suite.
     */
    public static Test suite(String serverHost, int serverPort)
    throws IOException
    {
        System.out.println("*** ShutdownMasterDb.suite("+serverHost+","+serverPort+")");
        
        Test t = TestConfiguration.existingServerSuite(ShutdownMasterDb.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.existingServerSuite(ShutdownMasterDb.class,false,"
                +serverHost+":"+serverPort+")");
        return t;
    }
        
    /**
     * Test shut down master database during replication.
     *
     * @throws SQLException, IOException, InterruptedException
     */
    public void testShutdownMasterDb()
    throws SQLException, IOException, InterruptedException
    {
        String masterServerURL = "jdbc:derby:"
                +"//"+masterServerHost+":"+masterServerPort+"/";
        String masterDbURL = masterServerURL
                +ReplicationRun.masterDatabasePath
                +"/"+ReplicationRun.masterDbSubPath
                +"/"+ReplicationRun.replicatedDb;
        
        shutdown(masterDbURL, true, false); // true : shutdown database only!
    }
}
