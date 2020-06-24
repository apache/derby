/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Distributed
 
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
import org.apache.derbyTesting.junit.BaseTestSuite;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite =
            new BaseTestSuite("ReplicationRun_Distributed Suite");
        
        suite.addTestSuite( ReplicationRun_Distributed.class );
        
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-5729
        startServer(masterJvmVersion, derbyMasterVersion,
                masterServerHost,
                ALL_INTERFACES, // masterServerHost, // "0.0.0.0", // All. or use masterServerHost for interfacesToListenOn,
                masterServerPort,
//IC see: https://issues.apache.org/jira/browse/DERBY-3162
                masterDbSubPath); // Distinguishing master/slave
        
        startServer(slaveJvmVersion, derbySlaveVersion,
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
        
        // Allow the slave to reach the required state before attempting to start master:
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.sleep(sleepTime, "Before startMaster");  // startMaster_ij should retry connection? 
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
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.DEBUG("*** Properties -----------------------------------------");
        userDir = System.getProperty("user.dir");
        util.DEBUG("user.dir:          " + userDir);
        
        userHome = System.getProperty("user.home");
        util.DEBUG("user.home:          " + userHome);
        util.writeToFile("exit;", userHome+FS+"ij_dummy_script.sql");
        
        util.DEBUG("derby.system.home: " + System.getProperty("derby.system.home"));
        
        String realPropertyFile = REPLICATIONTEST_PROPFILE; // Is just the plain file name in ${user.dir}
        util.DEBUG("realPropertyFile: " + realPropertyFile);
        
        InputStream isCp =  new FileInputStream(userDir + FS + realPropertyFile);
        Properties cp = new Properties();
        // testRunProperties = cp; // Make available for e.g. new Load(loadId)
        cp.load(isCp);
        // testRunProperties = cp; // Make available for e.g. new Load(loadId)
        // Now we can get the derby jar path, jvm path etc.
        
        util.printDebug = cp.getProperty("test.printDebug","false").equalsIgnoreCase("true");
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.DEBUG("printDebug: " + util.printDebug);
        
        showSysinfo = cp.getProperty("test.showSysinfo","false").equalsIgnoreCase("true");
        util.DEBUG("showSysinfo: " + showSysinfo);
        
        testUser = cp.getProperty("test.testUser","UNKNOWN");
        util.DEBUG("testUser: " + testUser);
        
        masterServerHost = cp.getProperty("test.masterServerHost",masterServerHost);
        util.DEBUG("masterServerHost: " + masterServerHost);
        cp.setProperty("test.serverHost", masterServerHost); // Set for initially running tests against master.
        
        masterServerPort = Integer.parseInt(cp.getProperty("test.masterServerPort",""+masterServerPort));
        util.DEBUG("masterServerPort: " + masterServerPort);
        cp.setProperty("test.serverPort", ""+masterServerPort); // Set for initially running tests against master.
        
        slaveServerHost = cp.getProperty("test.slaveServerHost",slaveServerHost);
        util.DEBUG("slaveServerHost: " + slaveServerHost);
        
        slaveServerPort = Integer.parseInt(cp.getProperty("test.slaveServerPort",""+slaveServerPort));
        util.DEBUG("slaveServerPort: " + slaveServerPort);
        
        slaveReplPort = Integer.parseInt(cp.getProperty("test.slaveReplPort",""+slaveReplPort));
        util.DEBUG("slaveReplPort: " + slaveReplPort);
        
        testClientHost = cp.getProperty("test.testClientHost",testClientHost);
        util.DEBUG("testClientHost: " + testClientHost);
        
        masterDatabasePath = cp.getProperty("test.master.databasepath");
        util.DEBUG("masterDatabasePath: " + masterDatabasePath);
        
        slaveDatabasePath = cp.getProperty("test.slave.databasepath");
        util.DEBUG("slaveDatabasePath: " + slaveDatabasePath);
        
        replicatedDb = cp.getProperty("test.databaseName","test");
        util.DEBUG("replicatedDb: " + replicatedDb);
        
        bootLoad = cp.getProperty("test.bootLoad");
        util.DEBUG("bootLoad: " + bootLoad);
        
        freezeDB = cp.getProperty("test.freezeDB");
        util.DEBUG("freezeDB: " + freezeDB);
        
        unFreezeDB = cp.getProperty("test.unFreezeDB");
        util.DEBUG("unFreezeDB: " + unFreezeDB);
        
        simpleLoad = System.getProperty("derby.tests.replSimpleLoad", "true")
                                                     .equalsIgnoreCase("true");
        util.DEBUG("simpleLoad: " + simpleLoad);
        simpleLoadTuples = Integer.parseInt(cp.getProperty("test.simpleloadtuples","10000"));
        util.DEBUG("simpleLoadTuples: " + simpleLoadTuples);
        
        replicationTest = cp.getProperty("test.replicationTest");
        util.DEBUG("replicationTest: " + replicationTest);
        replicationVerify = cp.getProperty("test.replicationVerify");
        util.DEBUG("replicationVerify: " + replicationVerify);
        junitTest = cp.getProperty("test.junitTest", "true")
                                                     .equalsIgnoreCase("true");
        util.DEBUG("junitTest: " + junitTest);
        
        THREADS = Integer.parseInt(cp.getProperty("test.stressMultiThreads","0"));
        util.DEBUG("THREADS: " + THREADS);
        MINUTES = Integer.parseInt(cp.getProperty("test.stressMultiMinutes","0"));
        util.DEBUG("MINUTES: " + MINUTES);
                
        sqlLoadInit = cp.getProperty("test.sqlLoadInit");
        util.DEBUG("sqlLoadInit: " + sqlLoadInit);

        
        specialTestingJar = cp.getProperty("test.derbyTestingJar", null);
        util.DEBUG("specialTestingJar: " + specialTestingJar);
        
        jvmVersion = cp.getProperty("jvm.version");
        util.DEBUG("jvmVersion: " + jvmVersion);
        
        masterJvmVersion = cp.getProperty("jvm.masterversion");
        if ( masterJvmVersion == null )
        {masterJvmVersion = jvmVersion;}
        util.DEBUG("masterJvmVersion: " + masterJvmVersion);
        
        slaveJvmVersion = cp.getProperty("jvm.slaveversion");
        if ( slaveJvmVersion == null )
        {slaveJvmVersion = jvmVersion;}
        util.DEBUG("slaveJvmVersion: " + slaveJvmVersion);
        
        derbyVersion = cp.getProperty("derby.version");
        util.DEBUG("derbyVersion: " + derbyVersion);
        
        derbyMasterVersion = cp.getProperty("derby.masterversion");
        if ( derbyMasterVersion == null )
        {derbyMasterVersion = derbyVersion;}
        util.DEBUG("derbyMasterVersion: " + derbyMasterVersion);
        
        derbySlaveVersion = cp.getProperty("derby.slaveversion");
        if ( derbySlaveVersion == null )
        {derbySlaveVersion = derbyVersion;}
        util.DEBUG("derbySlaveVersion: " + derbySlaveVersion);
        
        String derbyTestingJar = derbyVersion + FS+"derbyTesting.jar";
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
        util.DEBUG("derbyTestingJar: " + derbyTestingJar);
        
        junit_jar = cp.getProperty("junit_jar");
        util.DEBUG("junit_jar: " + junit_jar);
        
        test_jars = derbyTestingJar
                + ":" + junit_jar
                ;
        util.DEBUG("test_jars: " + test_jars);
        
        sleepTime = Integer.parseInt(cp.getProperty("test.sleepTime","15000"));
        util.DEBUG("sleepTime: " + sleepTime);
        
        runUnReplicated = cp.getProperty("test.runUnReplicated","false").equalsIgnoreCase("true");
        util.DEBUG("runUnReplicated: " + runUnReplicated);
        
        localEnv = cp.getProperty("test.localEnvironment","false").equalsIgnoreCase("true");
        util.DEBUG("localEnv: " + localEnv);
        
        derbyProperties = 
                 "derby.infolog.append=true"+LF
                +"derby.drda.logConnections=true"+LF
                +"derby.drda.traceAll=true"+LF;

        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.DEBUG("--------------------------------------------------------");
        
        masterPreRepl = new Load("masterPreRepl", cp);
        masterPostRepl = new Load("masterPostRepl", cp);
        slavePreSlave = new Load("slavePreSlave", cp);
        masterPostSlave = new Load("masterPostSlave", cp);
        slavePostSlave = new Load("slavePostSlave", cp);
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        util.DEBUG("--------------------------------------------------------");
        // for SimplePerfTest
//IC see: https://issues.apache.org/jira/browse/DERBY-3738
        tuplesToInsertPerf = Integer.parseInt(cp.getProperty("test.inserts","10000"));
        util.DEBUG("tuplesToInsertPerf: " + tuplesToInsertPerf);
        commitFreq = Integer.parseInt(cp.getProperty("test.commitFreq","0")); // "0" is autocommit
        util.DEBUG("commitFreq: " + commitFreq);
        
        util.DEBUG("--------------------------------------------------------");
        
        state.initEnvironment(cp);
       
        util.DEBUG("--------------------------------------------------------");
       
    }
}
