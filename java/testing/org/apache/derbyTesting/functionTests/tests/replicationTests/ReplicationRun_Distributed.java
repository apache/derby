/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun
 
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.SecurityManagerSetup;



/**
 * Run a replication test in a distributed
 * environment, where master and slave hosts, and
 * master and slave ports are specified in a property file.
 * Which test to run is also specified in the property file.
 * 
 */

public class ReplicationRun_Distributed extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Distributed
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Distributed(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("ReplicationRun_Distributed Suite");
        
        suite.addTestSuite( ReplicationRun_Distributed.class );
        
        return SecurityManagerSetup.noSecurityManager(suite);
    }
    
    //////////////////////////////////////////////////////////////
    ////
    //// The replication test framework (testReplication()):
    //// a) "clean" replication run starting master and slave servers,
    ////     preparing master and slave databases,
    ////     starting and stopping replication and doing
    ////     failover for a "normal"/"failure free" replication
    ////     test run.
    ////
    //////////////////////////////////////////////////////////////
    
    public void testReplication()
    throws Exception
    {
        cleanAllTestHosts();
        
        initEnvironment();
        
        initMaster(masterServerHost,
                replicatedDb);
        
        masterServer = startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
                masterDbSubPath); // Distinguishing master/slave
        
        slaveServer = startServer(slaveJvmVersion, derbySlaveVersion,
                slaveServerHost,
                ALL_INTERFACES, // slaveServerHost, // "0.0.0.0", // All. or use slaveServerHost for interfacesToListenOn,
                slaveServerPort,
                slaveDbSubPath); // Distinguishing master/slave
        
        startServerMonitor(slaveServerHost);
        
        bootMasterDatabase(jvmVersion,
                masterDatabasePath +FS+ masterDbSubPath,
                replicatedDb,
                masterServerHost, // Where the startreplication command must be given
                masterServerPort, // master server interface accepting client requests
                null // bootLoad, // The "test" to start when booting db.
                );
        
        initSlave(slaveServerHost,
                jvmVersion,
                replicatedDb); // Trunk and Prototype V2: copy master db to db_slave.
        
        startSlave(jvmVersion, replicatedDb,
                slaveServerHost, // slaveClientInterface // where the slave db runs
                slaveServerPort,
                slaveServerHost, // for slaveReplInterface
                slaveReplPort,
                testClientHost);
        
        startMaster(jvmVersion, replicatedDb,
                masterServerHost, // Where the startMaster command must be given
                masterServerPort, // master server interface accepting client requests
                masterServerHost, // An interface on the master: masterClientInterface (==masterServerHost),
                slaveServerPort, // Not used since slave don't allow clients.
                slaveServerHost, // for slaveReplInterface
                slaveReplPort);
        
        
        // Used to run positive tests.
        // Handle negative testing in State.testPostStartedMasterAndSlave().
        // Observe that it will not be meaningful to do runTest if State.XXXX()
        // has led to incorrect replication state wrt. replicationTest.
        runTest(replicationTest, // Returns immediatly if replicationTest is null.
                jvmVersion,
                testClientHost,
                masterServerHost, masterServerPort,
                replicatedDb);
        
        failOver(jvmVersion,
                masterDatabasePath, masterDbSubPath, replicatedDb,
                masterServerHost,  // Where the master db is run.
                masterServerPort,
                testClientHost);
        
        connectPing(slaveDatabasePath+FS+slaveDbSubPath+FS+replicatedDb,
                slaveServerHost,slaveServerPort,
                testClientHost);
        
        verifySlave();
        
        // We should verify the master as well, at least to see that we still can connect.
        verifyMaster();
        
        stopServer(jvmVersion, derbyVersion,
                slaveServerHost, slaveServerPort);
        
        stopServer(jvmVersion, derbyVersion,
                masterServerHost, masterServerPort);
        // As of 2008-02-06 master does not accept shutdown after replication, so:
        // do a 'kill pid' after ending the test run
        
    }
    
    /**
     * When running in a distributed context,
     * the environment is defined via the REPLICATIONTEST_PROPFILE.
     */
    void initEnvironment()
    throws IOException
    {
        
        System.out.println("*** Properties -----------------------------------------");
        userDir = System.getProperty("user.dir");
        System.out.println("user.dir:          " + userDir);
        
        System.out.println("derby.system.home: " + System.getProperty("derby.system.home"));
        
        String realPropertyFile = REPLICATIONTEST_PROPFILE; // Is just the plain file name in ${user.dir}
        System.out.println("realPropertyFile: " + realPropertyFile);
        
        InputStream isCp =  new FileInputStream(userDir + FS + realPropertyFile);
        Properties cp = new Properties();
        // testRunProperties = cp; // Make available for e.g. new Load(loadId)
        cp.load(isCp);
        // testRunProperties = cp; // Make available for e.g. new Load(loadId)
        // Now we can get the derby jar path, jvm path etc.
        
        util.printDebug = cp.getProperty("test.printDebug","false").equalsIgnoreCase("true");
        System.out.println("printDebug: " + util.printDebug);
        
        showSysinfo = cp.getProperty("test.showSysinfo","false").equalsIgnoreCase("true");
        System.out.println("showSysinfo: " + showSysinfo);
        
        testUser = cp.getProperty("test.testUser","false");
        System.out.println("testUser: " + testUser);
        
        masterServerHost = cp.getProperty("test.masterServerHost",masterServerHost);
        System.out.println("masterServerHost: " + masterServerHost);
        
        masterServerPort = Integer.parseInt(cp.getProperty("test.masterServerPort",""+masterServerPort));
        System.out.println("masterServerPort: " + masterServerPort);
        
        slaveServerHost = cp.getProperty("test.slaveServerHost",slaveServerHost);
        System.out.println("slaveServerHost: " + slaveServerHost);
        
        slaveServerPort = Integer.parseInt(cp.getProperty("test.slaveServerPort",""+slaveServerPort));
        System.out.println("slaveServerPort: " + slaveServerPort);
        
        slaveReplPort = Integer.parseInt(cp.getProperty("test.slaveReplPort",""+slaveReplPort));
        System.out.println("slaveReplPort: " + slaveReplPort);
        
        testClientHost = cp.getProperty("test.testClientHost",testClientHost);
        System.out.println("testClientHost: " + testClientHost);
        
        masterDatabasePath = cp.getProperty("test.master.databasepath");
        System.out.println("masterDatabasePath: " + masterDatabasePath);
        
        slaveDatabasePath = cp.getProperty("test.slave.databasepath");
        System.out.println("slaveDatabasePath: " + slaveDatabasePath);
        
        replicatedDb = cp.getProperty("test.databaseName","test");
        System.out.println("replicatedDb: " + replicatedDb);
        
        bootLoad = cp.getProperty("test.bootLoad");
        System.out.println("bootLoad: " + bootLoad);
        
        freezeDB = cp.getProperty("test.freezeDB");
        System.out.println("freezeDB: " + freezeDB);
        
        unFreezeDB = cp.getProperty("test.unFreezeDB");
        System.out.println("unFreezeDB: " + unFreezeDB);
        
        replicationTest = cp.getProperty("test.replicationTest");
        System.out.println("replicationTest: " + replicationTest);
        replicationVerify = cp.getProperty("test.replicationVerify");
        System.out.println("replicationVerify: " + replicationVerify);
        
        sqlLoadInit = cp.getProperty("test.sqlLoadInit");
        System.out.println("sqlLoadInit: " + sqlLoadInit);

        
        specialTestingJar = cp.getProperty("test.derbyTestingJar", null);
        System.out.println("specialTestingJar: " + specialTestingJar);
        
        jvmVersion = cp.getProperty("jvm.version");
        System.out.println("jvmVersion: " + jvmVersion);
        
        masterJvmVersion = cp.getProperty("jvm.masterversion");
        if ( masterJvmVersion == null )
        {masterJvmVersion = jvmVersion;}
        System.out.println("masterJvmVersion: " + masterJvmVersion);
        
        slaveJvmVersion = cp.getProperty("jvm.slaveversion");
        if ( slaveJvmVersion == null )
        {slaveJvmVersion = jvmVersion;}
        System.out.println("slaveJvmVersion: " + slaveJvmVersion);
        
        derbyVersion = cp.getProperty("derby.version");
        System.out.println("derbyVersion: " + derbyVersion);
        
        derbyMasterVersion = cp.getProperty("derby.masterversion");
        if ( derbyMasterVersion == null )
        {derbyMasterVersion = derbyVersion;}
        System.out.println("derbyMasterVersion: " + derbyMasterVersion);
        
        derbySlaveVersion = cp.getProperty("derby.slaveversion");
        if ( derbySlaveVersion == null )
        {derbySlaveVersion = derbyVersion;}
        System.out.println("derbySlaveVersion: " + derbySlaveVersion);
        
        String derbyTestingJar = derbyVersion + FS+"derbyTesting.jar";
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
        System.out.println("derbyTestingJar: " + derbyTestingJar);
        
        junit_jar = cp.getProperty("junit_jar");
        System.out.println("junit_jar: " + junit_jar);
        
        test_jars = derbyTestingJar
                + ":" + junit_jar
                ;
        System.out.println("test_jars: " + test_jars);
        
        sleepTime = Integer.parseInt(cp.getProperty("test.sleepTime","15000"));
        System.out.println("sleepTime: " + sleepTime);
        
        runUnReplicated = cp.getProperty("test.runUnReplicated","false").equalsIgnoreCase("true");
        System.out.println("runUnReplicated: " + runUnReplicated);
        
        localEnv = cp.getProperty("test.localEnvironment","false").equalsIgnoreCase("true");
        System.out.println("localEnv: " + localEnv);
        
        derbyProperties = 
                 "derby.infolog.append=true"+LF
                +"derby.drda.logConnections=true"+LF
                +"derby.drda.traceAll=true"+LF;

        
        System.out.println("--------------------------------------------------------");
        
        masterPreRepl = new Load("masterPreRepl", cp);
        masterPostRepl = new Load("masterPostRepl", cp);
        slavePreSlave = new Load("slavePreSlave", cp);
        masterPostSlave = new Load("masterPostSlave", cp);
        slavePostSlave = new Load("slavePostSlave", cp);
        
        System.out.println("--------------------------------------------------------");
        // for SimplePerfTest
        tuplesToInsertPerf = Integer.parseInt(cp.getProperty("test.inserts","10000"));
        commitFreq = Integer.parseInt(cp.getProperty("test.commitFreq","0")); // "0" is autocommit
        
        System.out.println("--------------------------------------------------------");
        
        state.initEnvironment(cp);
       
        System.out.println("--------------------------------------------------------");
       
    }
}
