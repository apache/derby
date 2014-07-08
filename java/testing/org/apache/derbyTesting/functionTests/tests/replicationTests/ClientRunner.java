/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ClientRunner
 
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

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

public class ClientRunner extends BaseJDBCTestCase // FIXME! Use ClientRunner in e.g. Shutdown.... etc.
{
    private static ReplicationRun repRun = new ReplicationRun("ClientRunner");
    
    public ClientRunner(String testcaseName)
    {
        super(testcaseName);
    }
    
    /* */
    // Client reads from the same property file as the 'ReplicationRun' test 
    // which controls the complete replication test!
    static String replicatedDb = null;
    static String jvmVersion = null;
    static String slaveJvmVersion = null;
    static String derbyVersion = null;
    static String derbySlaveVersion = null;
    static String masterServerHost = null;
    static int masterServerPort = -1;
    static String slaveServerHost = null;
    static int slaveServerPort = -1;
    static int slaveReplPort = -1;
    static String masterDatabasePath = null;
    static String slaveDatabasePath = null;
    static String testClientHost = null;
    /* */
    
    public static void initEnvironment()
    throws Exception
    {
        System.out.println("**** ClientRunner.init()");
        System.out.println("'ClientRunner' can not be run outside the 'ReplicationRun' framework.");
        
        repRun.initEnvironment();
        testClientHost=      repRun.testClientHost;
        derbyVersion =       repRun.derbyVersion;
        jvmVersion =         repRun.jvmVersion;
        replicatedDb =       repRun.replicatedDb;
        masterServerHost =   repRun.masterServerHost;
        masterServerPort =   repRun.masterServerPort;
        masterDatabasePath = repRun.masterDatabasePath;
        slaveServerHost =    repRun.slaveServerHost;
        slaveServerPort =    repRun.slaveServerPort;
        slaveReplPort =      repRun.slaveReplPort;
        slaveDatabasePath =  repRun.slaveDatabasePath;
        slaveJvmVersion =    repRun.slaveJvmVersion;
        derbySlaveVersion =  repRun.derbySlaveVersion;
        
    }
    
}
