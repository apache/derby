/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilityCombinations
 
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

package org.apache.derbyTesting.functionTests.tests.junitTests.compatibility;

import org.apache.derbyTesting.junit.BaseTestCase;

import org.apache.derby.drda.NetworkServerControl;

import java.sql.*;
import java.io.*;
import java.util.*;

import junit.framework.Test;
import junit.framework.TestSuite;



/**
 * Run a combination of
 * <ol>
 *    <li> server Derby versions on
 *    <li>  server JVM versions
 *    <br>against
 *    <li>  client Derby versions on
 *    <li>  client JVM versions.
 * </ol>
 * Which <B>Derby versions</B> and <B>JVM versions</B> should be used are 
 * specified in the file <CODE>compatibilitytest.properties</CODE>
 * which must be available in <CODE>${user.dir}</CODE>.
 * <p>
 * The compatibility tests are (currently) run without a SecurityManager.
 * <!-- <code>test.securityOption</code> is the option string used to specify 
 * how to <b>not</b> use a SecurityManager. --> <br><br>
 * Run as<br>
 *     <code>java -Djava.security.policy="&lt;NONE&gt;" junit.textui.TestRunner org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilityCombinations</code>
 * </p>
 * <h3>compatibilitytest.properties</h3>
 * <p>
 *     The following is an example and explanation of the 
 *     <CODE>compatibilitytest.properties</CODE> file:
 *     <font size="-1"><PRE>
 * #############################
 * # The test suite to be run for compatibility testing:
 * test.testSuite=org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite
 * # Optional, default false
 * # test.printDebug=false
 * 
 * # Optional, default false
 * # test.showSysinfo=true
 * 
 * # Optional, default false
 * # test.includeUpgrade=true
 * # Simply means the database files are NOT removed between server starts.
 * # Observe that trunk should not be included when test.includeUpgrade=true.
 * 
 * #############################
 * # Server port to use: optional, default 1527
 * test.serverPort=1527
 * # Since  CompatibilitySuite and JDBCDriverTest only handles default.. 
 * # Most tests are not yet ready for a non-default value.
 * 
 * #############################
 * # Jvms to be used for server and client side:
 * # 'jvm.versions' tells how many. 'jvm.N=<descriptive_name>' defines 
 * # names of properties giving the full path to the actual jvms. 
 * jvm.versions=3
 * # jvm.0=j13lib
 * jvm.0=j14lib
 * jvm.1=j15lib
 * jvm.2=j16lib
 * 
 * # j13lib=/usr/local/java/jdk1.3/jre/lib
 * j14lib=/usr/local/java/jdk1.4/jre/lib
 * j15lib=/usr/local/java/jdk1.5/jre/lib
 * j16lib=/usr/local/java/jdk1.6/jre/lib
 * 
 * ##############################
 * # Derby versions to be used for server and client side:
 * #-----------------------------
 * # 'derby.versions' how many. 'derby.versionN=<descriptive_name>' defines 
 * # names of properties giving the full path to the actual Derby libraries. 
 * derby.versions=6
 * derby.version0=10.0.2.1
 * derby.version1=10.1.1.0
 * derby.version2=10.1.2.1
 * derby.version3=10.1.3.1
 * derby.version4=10.2.2.0
 * derby.version5=Trunk
 * 
 * 10.0.2.1=/usr/local/share/java/javadb/JavaDB-10.0.2.1/lib
 * 10.1.1.0=/usr/local/share/java/javadb/JavaDB-10.1.1.0/lib
 * 10.1.2.1=/usr/local/share/java/javadb/JavaDB-10.1.2.1/lib
 * 10.1.3.1=/usr/local/share/java/javadb/JavaDB-10.1.3.1/lib
 * 10.2.2.0=/usr/local/share/java/javadb/JavaDB-10.2.2.0/lib
 * Trunk=/usr/local/share/java/javadb/JavaDB-trunk/lib
 * # Trunk=/home/os136789/Apache/myDerbySandbox/trunk/jars/insane
 * 
 * #-----------------------------
 * # Which Derby versions are security enabled:
 * 10.0.2.1_SA=false
 * 10.1.1.0_SA=false
 * 10.1.2.1_SA=false
 * 10.1.3.1_SA=false
 * 10.2.2.0_SA=false
 * Trunk_SA=true
 * 
 * #-----------------------------
 * # Specify security option string for security enabled version(s)
 * test.securityOption=noSecurityManager
 * 
 * #-----------------------------
 * # Driver to use for 10.0.2.1(incubator version - no Derby provided driver):
 * db2jcc_lib=/usr/local/share/java/db2jcc/lib
 * 
 * #-----------------------------
 * # Use a special testing jar? Optional
 * # E.g. your own experimental:
 * # test.derbyTestingJar=/home/testuser/Derby/testSandbox/trunk/jars/insane/derbyTesting.jar
 * 
 * #-----------------------------
 * # Use one single derby version server, optional:
 * # test.singleServer=5
 * # 5 for derby.version5, which in this example maps to Trunk
 * #-----------------------------
 * # Use one single jvm version server, optional:
 * # test.singleServerVM=2
 * # 2 for jvm.2, which in this example maps to j16lib
 * 
 * #-----------------------------
 * # Use one single derby version client, optional:
 * # test.singleClient=5
 * # for derby.version5, which in this example maps to Trunk
 #-----------------------------
 * # Use one single jvm version server, optional:
 * # test.singleClientVM=2
 * # 2 for jvm.2, which in this example maps to j16lib
 * 
 * ##############################
 * # Utilities...
 * junit_jar=/usr/local/share/java/junit3.8.2/junit.jar
 * jce_jar=/usr/local/share/java/jce1.2.2/lib/jce1_2_2.jar
 * jdbc2_0_stdext_jar=/usr/local/share/java/jdbc2.0-stdext/jdbc2_0-stdext.jar
 * 
 * ##############################
 *        
 *     </PRE></font>
 </p>
 */
public class CompatibilityCombinations extends BaseTestCase
{
    
    /**
     * Name of properties file defining the test environment 
     * and compatibility test combinations to be run.
     * Located in <CODE>${user.dir}</CODE>
     */
    private final static String COMPATIBILITYTEST_PROPFILE = "compatibilitytest.properties";
    
    /**
     * The option string used to turn off running Derby server with a SecurityManager.
   * Read as e.g <code>test.securityOption=noSecurityManager</code> from the file given by
     * <code>COMPATIBILITYTEST_PROPFILE</code>.
     */
    private       static String securityProperty = ""; // Read from COMPATIBILITYTEST_PROPFILE. 
    
    private       static String[] derbyVersionNames = null; // Short names for 
    private       static String[] derbyVerLibs = null;      // full paths to jar files for derby versions.
    private       static boolean[] derbySecurityEnabled = null;
    
    private       static String[] vmNames = null; // Short names for 
    private       static String[] VM_Ids = null;  // full paths to jvm lib directories.
    
    private final static int DERBY_JAR = 0; // Index of derby.jar in derbyLib[DerbyVersion][]
    private final static int DERBYCLIENT_JAR = DERBY_JAR +1; // ditto
    private final static int DERBYNET_JAR = DERBYCLIENT_JAR +1; // ditto
    private final static int DERBYTESTING_JAR = DERBYNET_JAR +1; // ditto
    private final static int DERBYMAX_JAR = DERBYTESTING_JAR;
    
    private       static String[][] derbyLib = null; // Size: [derbyVerLibs.length()][DERBYMAX_JAR]
    private       static String db2jcc_lib = null; // Path for db2jcc library.
    private       static String CLIENT_DB2JCC = null; // Path for db2jcc.jar and db2jcc_license_c.jar (in db2jcc_lib)
    private       static String junit_jar = null; // Path for JUnit jar
    private       static String jce_jar = null; // Path for jce_jar
    private       static String jdbc2_0_stdext_jar = null; // Path for jdbc2_0-stdext.jar
    
    private       static String test_jars = null; // Path for derbyTesting.jar:junit_jar:jce_jar
    
    private       static String serverHost = "localhost"; // Currently only handles localhost!
    private       static int serverPort = 1527; // Since  CompatibilitySuite and JDBCDriverTest only handles default..
    
    /////////////
    ////
    //// Which suite to run for compatibility testing is read from 'compatibilitytest.properties'.'
    ////
    /////////////
    private static String compatibilityTestSuite = null;
    //                             // The suite of tests run for each compatibility combination.
    private final static String embeddedDriver = "org.apache.derby.jdbc.EmbeddedDriver";
    private final static String networkServerControl = "org.apache.derby.drda.NetworkServerControl";
    private       static String specialTestingJar = null;
                                // None null if using e.g. your own modified tests.
    private       static String singleClient = null;
                                // Integer string property specifying which Derby version to use for client.
    private       static String singleClientVM = null;
                                // Integer string property specifying which Jvm version to use for client.
    private       static String singleServer = null;
                              // Integer string property specifying which Derby version to use for server.
    private       static String singleServerVM = null;
                                // Integer string property specifying which Jvm version to use for server.
    
    private final static String PS = File.separator;
    private final static String JVMloc = PS+".."+PS+"bin"+PS+"java"; // "/../bin/java"
    
    private static boolean runEmbedded = true;
    private static boolean runSrvrClnt = false;
    private static boolean printDebug = false;
    private static boolean showSysinfo = false;
    private static boolean includeUpgrade = false;
    
    private static long SLEEP_TIME_MILLIS = 5000L;
    
    /**
     * Creates a new instance of CompatibilityCombinations
     * @param testcaseName Identifying the test.
     */
    public CompatibilityCombinations(String testcaseName)
    {
        super(testcaseName);
        
    }
    
    /**
     * Parent super()
     * @throws java.lang.Exception .
     */
    protected void setUp() throws Exception
    {
        super.setUp();
    }
    
    /**
     * Parent super()
     * @throws java.lang.Exception .
     */
    protected void tearDown() throws Exception
    {
        super.tearDown();
    }
    
    
  //////////////////////////////////////////////////////////////
    ////
    //// Infrastructure for RUNNING as JUnit: e.g. adding tests to suite.
    ////
    //////////////////////////////////////////////////////////////
    /**
     * Create the suite of tests
     * @return Test created.
     */

    public static Test suite()
    {
        TestSuite	testSuite = new TestSuite();
        
        testSuite.addTestSuite( CompatibilityCombinations.class );
        
        return testSuite;
    }
    
    //////////////////////////////////////////////////////////////
    ////
    //// The tests. 
    ////
    //////////////////////////////////////////////////////////////
    
    /**
     * Run the combinations of client Derby and JVM versions
     * against the embedded highest(usually trunk) version on JVM versions.
     * <p>
     * Observe that <code>testEmbeddedTrunk</code> is really just 
     * code to select combination of Derby and Jvm versions
     * to be run.
     * <br>
     * The real compatibility tests are given by 
     * <code>jdbcSuite = "org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite"</code>
     * The number of combinations can be restricted by specifying the
     * properties
     * <ul>
     *    <li> test.singleServerVM
     *    <li> test.singleClient
     *    <li> test.singleClientVM
     * </ul>
     * @throws java.lang.Exception .
     */
    public void testEmbeddedTrunk() throws Exception
    {
        DEBUG("");
        DEBUG("+++ testEmbeddedTrunk");
        
        initEnvironment();
        
        boolean debugVal = true;
        String testName = compatibilityTestSuite;
        String databaseName = "wombat"; // Is hardwired in several tests....
        
        if ( !runEmbedded ) {DEBUG("--- testEmbeddedTrunk ignored"); return;}
        String workingDirName = System.getProperty("user.dir");
        PrintWriter summaryFile = new PrintWriter(new FileWriter(workingDirName+PS
                +"Embedded_"+databaseName+"_summary.log"));
        PrintWriter failFile = new PrintWriter(new FileWriter(workingDirName+PS
                +"Embedded_"+databaseName+"_failed"));
        
        int currentTestVersion = derbyVerLibs.length -1; // Always use test from newest/highest version.
        
        int noOfCombinations = 0; // Used for
        int successFullTests = 0; // reporting.
        
        String derbyTestingJar = derbyLib[currentTestVersion][DERBYTESTING_JAR];
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
        
        long startTime = System.currentTimeMillis();
      
        int trunkVersion = derbyLib.length-1; // Assuming trunk is last....
        
        int serverVmLow = 0;
        int serverVmHigh = VM_Ids.length-1;
        if ( singleServerVM != null )
        {
            serverVmLow = Integer.parseInt(singleServerVM);
            serverVmHigh = serverVmLow;
        }
        for (int serverVM=serverVmLow;serverVM<=serverVmHigh;serverVM++)
        {
            
            String creatorJvm = VM_Ids[serverVM]+JVMloc;   // Create db using server VM
              // Original ant testScript uses CLIENT_DB2JCC:
            String derbyCreatorJar = derbyLib[trunkVersion][DERBY_JAR] // and trunk Derby version.
                    +":"+derbyLib[trunkVersion][DERBYNET_JAR];

            DEBUG("derbyCreatorJar: "+derbyCreatorJar);
            String creatorClassPath = derbyCreatorJar
                    +":"+derbyTestingJar
                    +":"+junit_jar
                    +":"+jce_jar
                    ;
            recreateDB(trunkVersion
                    , creatorJvm
                    , creatorClassPath
                  , true
                    , databaseName
                    , true // Always remove database files. Otherwise attempts to do upgrade
                           // from originally created db (e.g. 10.0)
                           // Can NOT upgrade to a beta, i.e. normal trunk version!
                    );
            
            int clientVmLow = 0;
            int clientVmHigh = VM_Ids.length-1;
            if ( singleClientVM != null )
            {
                clientVmLow = Integer.parseInt(singleClientVM);
                clientVmHigh = clientVmLow;
            }
            for (int clientVM=clientVmLow;clientVM<=clientVmHigh;clientVM++)
            {
                for (int clientVersion=derbyLib.length-1;clientVersion<derbyLib.length;clientVersion++ )
                {
                    noOfCombinations++;
                    
                    String clientName = derbyVersionNames[clientVersion];
                    String derbyClientJar = derbyCreatorJar;
                    if ( derbyVersionNames[clientVersion].equalsIgnoreCase("10.0.2.1") ) // Has no own client
                    {
                        derbyClientJar = CLIENT_DB2JCC;
                        clientName = "10.0.DB2JCC"; // Pre-pend 10.0. to get a "natural" sorting of log files...
                    }
                    DEBUG("derbyClientJar: "+derbyClientJar);
                    String clientJvm = VM_Ids[clientVM]+JVMloc;
                    
                    String clientClassPath = derbyClientJar
                            +":"+derbyTestingJar
                            +":"+junit_jar
                            +":"+jce_jar
                            +":"+jdbc2_0_stdext_jar
                            ;
                    String combinationName =
                            "Embedded_"+derbyVersionNames[trunkVersion]+"VM"+vmNames[serverVM]
                            +"_vs_"
                            +"ClientVM-"+vmNames[clientVM]+"_client"+clientName
                            ;
                    
                    if ( showSysinfo )
                        sysinfoEmbedded(clientVM, clientVersion,combinationName);
                    
                    DEBUG("**************** oneTest("+combinationName+")");
                    try
                    {
                        boolean OK =
                                oneTest(clientJvm
                                , clientClassPath
                                , debugVal
                                , testName
                                , embeddedDriver
                                , databaseName
                                , combinationName
                              , summaryFile
                                );
                        if ( OK )
                        {
                            successFullTests++;
                        }
                        else
                        {
                            System.out.println("************ " + combinationName + " failed!");
                            failFile.println(combinationName);
                            failFile.flush();
                        }
                    }
                    catch (Exception e)
                   {
                        e.printStackTrace();
                    }
                                        
                } // clientVersion
            } // clientVM
        } // serverVM
        
        long endTime = System.currentTimeMillis();
        float timeUsed = (float)((endTime - startTime) / 1000 );
        // DEBUG(combinations);
        String summary = "Attempted 'embedded vs. network client' tests: " + noOfCombinations
                + ", OK: " + successFullTests
                + ", Failed: " + (noOfCombinations-successFullTests)
              + ", Time: " + timeUsed + " seconds"
                ;
        summaryFile.println();
        summaryFile.println(summary);
        summaryFile.close();
        failFile.close();
        System.out.println(summary);
        DEBUG("--- testEmbeddedTrunk");
        DEBUG("");
        assertTrue( summary, (noOfCombinations-successFullTests) == 0 );
    } // testEmbeddedTrunk
    
    /**
     * Run the combinations of Derby versions and JVM versions on the client side
     * against the Derby versions and JVM versions on the server side. 
     * <p>
     * Observe that <code>testLoopThruAllCombinations</code> is really just 
     * code to select combination of Derby and Jvm versions
     * to be run.
     * <br>
     * The real compatibility tests are given by 
     * <code>jdbcSuite = "org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite"</code>
     * The number of combinations can be restricted by specifying the
     * properties
     * <ul>
     *    <li> test.singleServer
     *    <li> test.singleServerVM
     *    <li> test.singleClient
     *    <li> test.singleClientVM
     * </ul>
     * @throws java.lang.Exception .
     */
    public void testLoopThruAllCombinations() throws Exception
    {
        DEBUG("");
        DEBUG("+++ testLoopThruAllCombinations");
        
        initEnvironment();
        
        boolean debugVal = true;
        String testName = compatibilityTestSuite;
        String databaseName = "wombat"; // Is hardwired in several tests....
      
        if ( !runSrvrClnt ) {DEBUG("--- testLoopThruAllCombinations ignored"); return;}
        String workingDirName = System.getProperty("user.dir");
        PrintWriter summaryFile = new PrintWriter(new FileWriter(workingDirName+PS
                +"ServerClient_"+databaseName+"_summary.log"));
        PrintWriter failFile = new PrintWriter(new FileWriter(workingDirName+PS
                +"ServerClient_"+databaseName+"_failed"));
        
        int currentTestVersion = derbyVerLibs.length -1; // Always use test from newest/highest version.
        
        int noOfCombinations = 0;
        int successFullTests = 0;
        
        String derbyTestingJar = derbyLib[currentTestVersion][DERBYTESTING_JAR];
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
      
        int serverVmLow = 0;
        int serverVmHigh = VM_Ids.length-1;
        if ( singleServerVM != null )
        {
            serverVmLow = Integer.parseInt(singleServerVM);
            serverVmHigh = serverVmLow;
        }
        long startTime = System.currentTimeMillis();
        for (int serverVM=serverVmLow;serverVM<=serverVmHigh;serverVM++)
        {
            int serverVersionLow = 0;
            int serverVersionHigh = derbyLib.length-1;
            if ( singleServer != null )
          {
                serverVersionLow = Integer.parseInt(singleServer);
                serverVersionHigh = serverVersionLow;
            }
            for (int serverVersion=serverVersionLow;serverVersion<=serverVersionHigh;serverVersion++ )
            {
                
                startServer(serverVM, serverVersion);
                
                String creatorJvm = VM_Ids[serverVM]+JVMloc;   // Create db using server VM
                // Original ant testScript uses CLIENT_DB2JCC:
                String derbyCreatorJar = derbyLib[0][DERBYCLIENT_JAR]; // and first(lowest) Derby version.
                if ( derbyVersionNames[0].equalsIgnoreCase("10.0.2.1") ) // Has no own client
                {
                  derbyCreatorJar = CLIENT_DB2JCC;
                }
                DEBUG("derbyCreatorJar: "+derbyCreatorJar);
                String creatorClassPath = derbyCreatorJar
                        +":"+derbyTestingJar
                        +":"+junit_jar
                        +":"+jce_jar
                        ;
                boolean deleteDatabaseFiles = !includeUpgrade;
                if ( serverVersion == 0 ) deleteDatabaseFiles = true; // Always remove when starting from the initial Derby version.
                recreateDB(serverVersion
                        , creatorJvm
                        , creatorClassPath
                        , true
                        , databaseName
                        , deleteDatabaseFiles
                        // , true // Always remove database files. Otherwise attempts to do upgrade
                        // from originally created db (e.g. 10.0)
                        // Can NOT upgrade to a beta, i.e. normal trunk version!
                        );
                
                int clientVmLow = 0;
                int clientVmHigh = VM_Ids.length-1;
                if ( singleClientVM != null )
                {
                    clientVmLow = Integer.parseInt(singleClientVM);
                    clientVmHigh = clientVmLow;
                }
                for (int clientVM=clientVmLow;clientVM<=clientVmHigh;clientVM++)
                {
                    int clientVersionLow = 0;
                    int clientVersionHigh = derbyLib.length-1;
                    if ( singleClient != null )
                    {
                        clientVersionLow = Integer.parseInt(singleClient);
                        clientVersionHigh = clientVersionLow;
                    }
                    for (int clientVersion=clientVersionLow;clientVersion<=clientVersionHigh;clientVersion++ )
                    {
                        noOfCombinations++;
                        
                      String clientName = derbyVersionNames[clientVersion];
                        String derbyClientJar = derbyLib[clientVersion][DERBYCLIENT_JAR];
                        if ( derbyVersionNames[clientVersion].equalsIgnoreCase("10.0.2.1") ) // Has no own client
                        {
                            derbyClientJar = CLIENT_DB2JCC; // or derbynet
                            clientName = "10.0.DB2JCC"; // Pre-pend 10.0. to get a "natural" sorting of log files...
                        }
                        DEBUG("derbyClientJar: "+derbyClientJar);
                        String clientJvm = VM_Ids[clientVM]+JVMloc;
                        
                        String clientClassPath = derbyClientJar
                                +":"+derbyTestingJar
                                +":"+junit_jar
                                +":"+jce_jar
                                +":"+jdbc2_0_stdext_jar
                                ;
                        String combinationName =
                                "ServerVM-"+vmNames[serverVM]+"_server"+derbyVersionNames[serverVersion]
                                +"_vs_"
                                +"ClientVM-"+vmNames[clientVM]+"_client"+clientName
                                ;
                        
                        if ( showSysinfo )
                            sysinfoServerFromClient(clientVM, clientVersion,combinationName);
                        
                        DEBUG("**************** oneTest("+combinationName+")");
                        try
                        {
                          boolean OK =
                                    oneTest(clientJvm
                                    , clientClassPath
                                    , debugVal
                                    , testName
                                    , null // driver - null means find default
                                    , databaseName
                                    , combinationName
                                    , summaryFile
                                    );
                            if ( OK )
                            {
                                successFullTests++;
                            }
                            else
                            {
                                System.out.println("************ " + combinationName + " failed!");
                                failFile.println(combinationName);
                                failFile.flush();
                            }
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                        
                    } // clientVersion
                } // clientVM
              stopServer(serverVM, serverVersion);
            } // serverVersion
        } // serverVM
        
        long endTime = System.currentTimeMillis();
        float timeUsed = (float)((endTime - startTime) / 1000 );
        // DEBUG(combinations);
        String summary = "Attempted 'server - network client' tests: " + noOfCombinations
                + ", OK: " + successFullTests
                + ", Failed: " + (noOfCombinations-successFullTests)
                + ", Time: " + timeUsed + " seconds"
                ;
        summaryFile.println();
        summaryFile.println(summary);
        summaryFile.close();
        failFile.close();
        System.out.println(summary);
        DEBUG("--- testLoopThruAllCombinations");
        DEBUG("");
        assertTrue( summary, (noOfCombinations-successFullTests) == 0 );
    } // testLoopThruAllCombinations
    
    /**
     * Run the compatibility tests for one given combination of
     * Derby client version, client jvm version, server jvm version and Derby server version.
     * @param clientJvm Path for client Jvm to be used.
     * @param clientClassPath Class path for Derby client.
     * @param debug Print debug.
     * @param testName The test suite to be used for the compatibility test. 
     * Currently only using <code>org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite</code>.
     * @param driverName Driver to be used: None null for embedded only. Null means find default in jars.
     * @param databaseName Name of database to connect to.
     * @param combinationName Name describing the combination. Used as part of report file(s) for this test combination.
     * @param summaryFile Name of file summarizing results for all combinations of tests.
     * @return Success or failure for this test combination.
     * @throws java.lang.Exception .
     */
    private boolean oneTest(String clientJvm
            , String clientClassPath
            , boolean debug
            , String testName
            , String driverName
            , String databaseName
            , String combinationName
            , PrintWriter summaryFile
            )
            throws Exception
    {
        DEBUG("");
        DEBUG("+++ oneTest: "+combinationName+" ++++++++++++++++++++++++++++++++++++++");
        DEBUG("clientJvm:       " + clientJvm);
        DEBUG("clientClassPath: " + clientClassPath);
        DEBUG("debug:           " + debug);
        DEBUG("testName:        " + testName);
        DEBUG("driverName:      " + driverName);
        DEBUG("databaseName:    " + databaseName);
        DEBUG("combinationName: " + combinationName);
        
        boolean testOK = false;
        
        int port = serverPort;

        if ( driverName == null ) driverName = ""; // Not null is used for embedded only!
        final String[] commandElements = {clientJvm
                , " -Ddrb.tests.debug=true" // Used by JDBCDriverTest.
                // , " -Dderby.tests.debug=true" // Used by DerbyJUnitTest
                , " -Dderby.tests.trace=true" // Used by DerbyJUnitTest
                , " -cp ", clientClassPath
                , " " + testName
                , " " + databaseName
                , " " + driverName // Specified for embedded only! Otherwise find default.
            };
        final String[] envElements = {"CLASS_PATH="+clientClassPath
            };
        
        String workingDirName = System.getProperty("user.dir");
        DEBUG("user.dir: " + workingDirName);
        String tmp ="";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        DEBUG("commandElements: " + tmp);
        
        final String fullCmd = tmp;
        tmp ="";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp);
        final File workingDir = new File(workingDirName);
        
        DEBUG(
                "proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);"
                );
        
        try
        {
            Process proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
            PrintWriter out = new PrintWriter(new FileWriter(workingDirName+PS+combinationName));
            String result = testOutput(proc, out); // Scans test report for OK and Time...
            if ( result.indexOf(" OK ") != -1 ) testOK = true;
            result= combinationName+":" + result;
            summaryFile.println(result);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        
        summaryFile.flush();
        DEBUG("--- oneTest: "+combinationName+" --------------------------------------");
        DEBUG("");
        
        // throw new UnsupportedOperationException("Not yet implemented");
        
        return testOK;
  }
    
    /**
     * Get .out and .err from the process running oneTest. Print to PrintWriter out.
     * @return Single line containing "OK" and "Time" results from the oneTest combination.
     */
    private static String testOutput(Process proc, PrintWriter out)
        throws IOException
    {
        InputStream serveInputStream = proc.getInputStream();
        InputStream serveErrorStream = proc.getErrorStream();
        InputStreamReader isr = new InputStreamReader(serveInputStream);
        InputStreamReader esr = new InputStreamReader(serveErrorStream);
        BufferedReader bir = new BufferedReader(isr);
        BufferedReader ber = new BufferedReader(isr);
        String line=null;
        
        String result = "";
        boolean foundTime = false;
        boolean foundOK = false;
        boolean foundFail = false;
        while ( (line = bir.readLine()) != null)
        {
            out.println(line);
            if ( (!foundTime) && (line.indexOf("Time:",0) != -1) )
            {
                foundTime = true;
                result = result +  " " + line;
            }
            else if ( (!foundOK) && (!foundFail) && (line.indexOf("OK ",0) != -1) )
            {
                foundOK = true;
                result = result +  " " + line;
            }
            else if ( (!foundFail) && (!foundFail) && (line.indexOf("Failures:",0) != -1) )
            {
                foundFail = true;
                result = result +  " " + line;
            }

        }
        out.close();
        return result;
    }
    
    /**
     * Set up the total environment for running the compatibility test
     * combinations as given by the <CODE>compatibilitytest.properties</CODE> file.
     * 
     * The following properties are recognized:
     * <ul>
     *     <li>test.printDebug 
     *     <li>test.showSysinfo 
     *     <li>test.serverPort 
     *     <li>test.includeUpgrade 
     *     <li>test.singleClient 
     *     <li>test.singleClientVM 
     *     <li>test.singleServer 
     *     <li>test.singleServerVM 
     *     <li>test.derbyTestingJar
     *     <li>test.securityOption 
     *     <li>jvm.versions - number of jvm versions each with:
     *         <ul>
     *         <li>"jvm."+vm 
     *         <li>vmNames[vm] 
     *         </ul>
     *     <li>derby.versions - number of derby versions each with:
     *         <ul>
     *         <li>"derby.version"+v 
     *         <li>derbyVersionNames[v] 
     *         <li>derbyVersionNames[v]+"_SA"
     *         </ul>
     *     <li>db2jcc_lib 
     *     <li>junit_jar 
     *     <li>jce_jar 
     *     <li>jdbc2_0_stdext_jar
     * </ul>
   * @throws java.io.IOException .
     */
    private void initEnvironment()
    throws IOException
    {
        
        System.out.println("*** Properties -----------------------------------------");
        String userDir = System.getProperty("user.dir");
        System.out.println("user.dir:          " + userDir);
        
        System.out.println("derby.system.home: " + System.getProperty("derby.system.home"));

        String realPropertyFile = COMPATIBILITYTEST_PROPFILE; // Is just the plain file name in ${user.dir}
        System.out.println("realPropertyFile: " + realPropertyFile);
        
        InputStream isCp =  new FileInputStream(userDir + PS + realPropertyFile);
        Properties cp = new Properties();
        cp.load(isCp);
        // Now we can get the derby versions, jvm versions paths etc.
        
        printDebug = cp.getProperty("test.printDebug","false").equalsIgnoreCase("true");
        System.out.println("printDebug: " + printDebug);
        
        showSysinfo = cp.getProperty("test.showSysinfo","false").equalsIgnoreCase("true");
        System.out.println("showSysinfo: " + showSysinfo);
        
        serverPort = Integer.parseInt(cp.getProperty("test.serverPort","1527"));
        System.out.println("serverPort: " + serverPort);
        
        compatibilityTestSuite = cp.getProperty("test.testSuite",
                "org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.CompatibilitySuite");
        System.out.println("testSuite: " + compatibilityTestSuite);

        runEmbedded = cp.getProperty("test.runEmbedded","true").equalsIgnoreCase("true");
        System.out.println("runEmbedded: " + runEmbedded);

        runSrvrClnt = cp.getProperty("test.runServerClient","true").equalsIgnoreCase("true");
        System.out.println("runSrvrClnt: " + runSrvrClnt);

        includeUpgrade = cp.getProperty("test.includeUpgrade","false").equalsIgnoreCase("true");
        System.out.println("includeUpgrade: " + includeUpgrade);
      
        singleClient = cp.getProperty("test.singleClient",null); // E.g. 5 for derby.version5, see property file
        System.out.println("singleClient: " + singleClient);
        
        singleClientVM = cp.getProperty("test.singleClientVM",null); // E.g. 2 for jvm.2, see property file
        System.out.println("singleClientVM: " + singleClientVM);
        
        singleServer = cp.getProperty("test.singleServer",null); // E.g. 5 for derby.version5, see property file
        System.out.println("singleServer: " + singleServer);
        
        singleServerVM = cp.getProperty("test.singleServerVM",null); // E.g. 2 for jvm.2, see property file
        System.out.println("singleServerVM: " + singleServerVM);
        
        specialTestingJar = cp.getProperty("test.derbyTestingJar", null);
        System.out.println("specialTestingJar: " + specialTestingJar);
        
        securityProperty = cp.getProperty("test.securityOption");
        System.out.println("securityProperty: " + securityProperty);
        
        int jvmVersions = Integer.parseInt(cp.getProperty("jvm.versions"));
        vmNames = new String[jvmVersions];
        VM_Ids = new String[jvmVersions];
        for (int vm=0;vm<jvmVersions;vm++)
        {
            vmNames[vm] = cp.getProperty("jvm."+vm);
            // E.g. jvm.0 = j13lib, ..., jvm.3 = j16lib
            System.out.println(vm + ": " + vmNames[vm]);
        }
        for (int vm=0;vm<jvmVersions;vm++)
        {
            VM_Ids[vm] = cp.getProperty(vmNames[vm]);
            // E.g. j13lib = /usr/local/java/jdk1.3/jre/lib
            System.out.println(vmNames[vm] + ": " + VM_Ids[vm]);
        }        
        
        
        int derbyVersions = Integer.parseInt(cp.getProperty("derby.versions"));
        // Read the names given for these versions
        derbyVersionNames = new String[derbyVersions];
        for (int v=0;v<derbyVersions;v++)
        {
            derbyVersionNames[v] = cp.getProperty("derby.version"+v);
          // Using the following name pattern:
            // derby.version0=10.0.2.1
            // derby.version1=10.1.1.0
            // ...
            // derby.version4=10.2.2.0
            // derby.version5=Trunk
        }
        // Properties with these names then give the path to the appropriate libs:
        derbyVerLibs = new String[derbyVersions];
        derbySecurityEnabled = new boolean[derbyVersions];
        for (int v=0;v<derbyVersions;v++)
        {
            derbyVerLibs[v] = cp.getProperty(derbyVersionNames[v]);
            derbySecurityEnabled[v] = cp.getProperty(derbyVersionNames[v]+"_SA").equalsIgnoreCase("true");
          // Using the following name pattern:
            // 10.2.2.0=/usr/local/share/java/javadb/JavaDB-10.2.2.0/lib
            // Trunk=/usr/local/share/java/javadb/JavaDB-trunk/lib
            System.out.println(derbyVersionNames[v] + ": " + derbyVerLibs[v]
                    + " " + derbySecurityEnabled[v]);
        }
        
        derbyLib = new String[derbyVersions][DERBYMAX_JAR+1];
        for (int drbV=0;drbV<derbyVersions;drbV++)
        {
            // DEBUG(drbV + " / " + derbyVersions);
            derbyLib[drbV][DERBY_JAR] =        derbyVerLibs[drbV] + PS+"derby.jar";
            derbyLib[drbV][DERBYCLIENT_JAR] =  derbyVerLibs[drbV] + PS+"derbyclient.jar";
            derbyLib[drbV][DERBYTESTING_JAR] = derbyVerLibs[drbV] + PS+"derbyTesting.jar";
            derbyLib[drbV][DERBYNET_JAR] =     derbyVerLibs[drbV] + PS+"derbynet.jar";
        }
        
        db2jcc_lib = cp.getProperty("db2jcc_lib");
        System.out.println("db2jcc_lib: " + db2jcc_lib);
        CLIENT_DB2JCC=db2jcc_lib+PS+"db2jcc.jar"
                +":"+db2jcc_lib+PS+"db2jcc_license_c.jar";
        
        junit_jar = cp.getProperty("junit_jar");
        System.out.println("junit_jar: " + junit_jar);
        jce_jar = cp.getProperty("jce_jar");
        System.out.println("jce_jar: " + jce_jar);
        jdbc2_0_stdext_jar = cp.getProperty("jdbc2_0_stdext_jar");
        System.out.println("jdbc2_0_stdext_jar: " + jdbc2_0_stdext_jar);
      
        int currentTestVersion = derbyVerLibs.length -1; // Always use test from newest/highest version.
        
        String derbyTestingJar = derbyVerLibs[currentTestVersion] + PS+"derbyTesting.jar"; // Current/highest
        if ( specialTestingJar != null )  derbyTestingJar = specialTestingJar;
        
        test_jars = derbyTestingJar
                + ":" + junit_jar
                + ":" + jce_jar
                ;
        System.out.println("test_jars: " + test_jars);
        System.out.println("--------------------------------------------------------");
        
    }
  
    
    private void startServer(int serverVM, int serverVersion)
    throws Exception
    { // See NetworkServerTestSetup.startSeparateProcess() for the default JUnit test setup...
        DEBUG("");
        DEBUG("+++ StartServer");
        DEBUG("startServer: " + serverVersion + " / " + serverVM);
        DEBUG("startServer: " + derbyVersionNames[serverVersion] + " on " + VM_Ids[serverVM] );
        
        String serverJvm = VM_Ids[serverVM]+JVMloc;
        String serverClassPath = derbyVerLibs[serverVersion] + PS+"derby.jar"
                + ":" + derbyVerLibs[serverVersion] + PS+"derbynet.jar"
                + ":" + test_jars; // Do we need test_jars here for the server?
      
        String command = "start";
        String allowedClients = "0.0.0.0"; // I.e. any
        String securityOption = "";

        // Is this server version security enabled? If so turn off running with SecurityManger!
        if ( (securityProperty.length() != 0) && derbySecurityEnabled[serverVersion] )
        {
            securityOption = "-"+securityProperty;
        }
        
        final String[] commandElements = {serverJvm
                , " -Dderby.infolog.append=true"
                , " -cp ", serverClassPath
                , " " + networkServerControl
                , " " + command
                , " -h ", allowedClients
                , " -p ", serverPort+""
                , " " + securityOption
                };
        final String[] envElements = {"CLASS_PATH="+serverClassPath
                , "PATH="+VM_Ids[serverVM]+PS+".."+PS+"bin" // "/../bin"
                };
        
        String workingDirName = System.getProperty("user.dir");
        DEBUG("user.dir: " + workingDirName);
        String tmp ="";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        DEBUG("commandElements: " + tmp);
        final String fullCmd = tmp;
        tmp ="";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp);
        final File workingDir = new File(workingDirName);
        
        
        if ( serverHost.equalsIgnoreCase("localhost") )
        {
            DEBUG(
                    "proc = Runtime.getRuntime().exec(commandElements,envElements,workingDir);"
                 );

            Thread serverThread = new Thread(
                    new Runnable()
            {
                public void run()
                {
                    Process proc = null;
                    try
                    {
                        DEBUG("************** In run().");
                        proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
                        // proc = Runtime.getRuntime().exec(commandElements,envElements,workingDir);
                        DEBUG("************** Done exec().");
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                    }
                    
                }
            }
            );
            DEBUG("************** Do .start().");
            serverThread.start();
            pingServer(1, envElements); // Wait for the server to come up in a reasonable time....
            serverThread.join();
            DEBUG("************** Done .join().");
        }
        else
        {
            throw new UnsupportedOperationException
                    ("Starting server on non-local host Not yet implemented");
        }
        
        DEBUG("--- StartServer");
        DEBUG("");
    }
    
    /**
     * Recreate - create database from scratch or keep database but re-initialize user defined tables.
     * <p>
     *     With <code>removeDBfiles</code> existing database files are deleted.<br>
     *     This is normally done when starting a new Derby server version.<br>
     *     By setting <code>test.includeUpgrade=true</code> in 
     *     <code>COMPATIBILITYTEST_PROPFILE</code> database files will <b>not</b>
     *     be deleted, thus forcing upgrade to be performed.
     * </p>
     * @param serverVersion 
     * @param clientJvm 
     * @param clientClassPath 
     * @param debug 
     * @param databaseName 
     * @param removeDBfiles Remove data base files when (re-)creating the database.<br>
     *    If we do not remove database files: Derby attempts to do upgrade 
     *    from originally created db (e.g. 10.0) when restarted on a 
     *    different server version. Can NOT upgrade to 
     *    a alpha/beta, i.e. normal trunk version!
     */
    private void recreateDB(int serverVersion
            , String clientJvm
            , String clientClassPath
            , boolean debug
            , String databaseName
            , boolean removeDBfiles
            )
    {
        DEBUG("");
        DEBUG("+++ recreateDB");
        
        String creator = compatibilityTestSuite + "$Creator";
        
        String securityOption = "";
        if ( (securityProperty.length() != 0) && derbySecurityEnabled[serverVersion] )
        {
            securityOption = "-"+securityProperty;
        }
        final String[] commandElements = {clientJvm
                , " -Ddrb.tests.debug=true" // Used by JDBCDriverTest.
                // , " -Dderby.tests.debug=true" // Used by DerbyJUnitTest
                , " -Dderby.tests.trace=true" // Used by DerbyJUnitTest
                , " -cp ", clientClassPath
                , " " + creator
                , " " + databaseName
                , " " + securityOption
                };
        final String[] envElements = {"CLASS_PATH="+clientClassPath
                // , "PATH="+VM_Ids[clientVM]+"/../bin"
                };
        
        String workingDirName = System.getProperty("user.dir");
        DEBUG("user.dir: " + workingDirName);
        
        String fullPath = workingDirName+PS+databaseName;
        
        // If we do not remove database files: attempts to do upgrade
        // from originally created db (e.g. 10.0)
        // Can NOT upgrade to a alpha/beta, i.e. normal trunk version!
        
        if ( removeDBfiles )
        {
            DEBUG("Deleting database dir '" + fullPath + "'");
            BaseTestCase.removeDirectory(fullPath);
        }
        else
        {
            DEBUG("Keeping database dir '" + fullPath +"'");
        }
        
        String tmp = "";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        final String fullCmd = tmp;
        DEBUG("commandElements: " + fullCmd);
        
        tmp = "";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp);
        final File workingDir = new File(workingDirName);
        
        DEBUG(
                "proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);"
              );
        
        try
        {
            Process proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
            proc.waitFor();
            processDEBUGOutput(proc);
                        
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        
        
        DEBUG("--- recreateDB");
        DEBUG("");
    }
    
    
    /**
     * <p>
     * Checks to see that the server is up. If the server doesn't
     * come up in a reasonable amount of time, (re-)throw the
     * final exception.
     * </p>
     * @param iterations How many times to try pinging the server to see if it is running. Sleeps <CODE>SLEEP_TIME_MILLIS</CODE> between tries.
     * @param serverEnvironment Paths used in bringing up server
     * @throws java.lang.Exception .
     */
    // Copied from org.apache.derbyTesting.functionTests.tests.junitTests.compatibility.Pinger
    private	void	pingServer( int iterations, String[] serverEnvironment )
    throws Exception
    {
        DEBUG("+++ pingServer");
        ping( new NetworkServerControl(), iterations, serverEnvironment );
        DEBUG("--- pingServer");
    }
    
    /**
     * It used to be possible to ping down-rev servers using an up-rev NetworkServerControl.
     * This is no longer possible. So we will just take it on faith that the server comes up after
     * a decent interval.
     */
    private	void	ping( NetworkServerControl controller, int iterations, String[] serverEnvironment )
    throws Exception
    {
        Exception	finalException = null;
        
        for ( int i = 0; i < iterations; i++ )
        {
            /*
            try
            {
                controller.ping();
                // DEBUG("Server came up in less than " + i*(SLEEP_TIME_MILLIS/1000) + " secs.");
                return;
            }
            catch (Exception e)
          { finalException = e; }
            */
            
            Thread.sleep( SLEEP_TIME_MILLIS );
        }

        /*
        StringBuffer buffer = new StringBuffer();
        buffer.append( "Server did not come up: " + finalException.getMessage() );
        int pathCount = serverEnvironment.length;
        for ( int i = 0; i < pathCount; i++ )
        {
            buffer.append( "\n\t" + serverEnvironment[ i ] );
        }
        System.out.println( buffer.toString() );
        finalException.printStackTrace();
        */
        
    }
    
    
    private void stopServer(int serverVM, int serverVersion)
    {
        DEBUG("");
        DEBUG("+++ stopServer");
        DEBUG("stopServer: " + serverVersion + " / " + serverVM);
        DEBUG("stopServer: " + derbyVersionNames[serverVersion] + " on " + VM_Ids[serverVM] );
        
        String serverJvm = VM_Ids[serverVM]+JVMloc;
        String serverClassPath = derbyVerLibs[serverVersion] + PS+"derby.jar"
                + ":" + derbyVerLibs[serverVersion] + PS+"derbynet.jar"
                + ":" + test_jars // Do we need test_jars here for the server?
                ;
        
        String command = "shutdown";
        int port = serverPort;
        
        final String[] commandElements = {serverJvm
                , " -Dderby.infolog.append=true"
                , " -cp ", serverClassPath
                , " " + networkServerControl
                , " " + command
                , " -p ", serverPort+""
                // , " " + securityOption
                };
        final String[] envElements = {"CLASS_PATH="+serverClassPath
                , "PATH="+VM_Ids[serverVM]+PS+".."+PS+"bin" // "/../bin"
                };
        
        String workingDirName = System.getProperty("user.dir");
        DEBUG("user.dir: " + workingDirName);
        String tmp ="";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        DEBUG("commandElements: " + tmp);
        final String fullCmd = tmp;
        tmp ="";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp);
        final File workingDir = new File(workingDirName);
        
        if ( serverHost.equalsIgnoreCase("localhost") )
        {
            DEBUG(
                    "proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);"
                );
            try
            {
                Process proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
                processDEBUGOutput(proc);
                
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
            }
        }
        else
        {
          throw new UnsupportedOperationException
                    ("Starting server on non-local host Not yet implemented");
        }
        
        DEBUG("--- stopServer");
        DEBUG("");
        
    }
    
    private void sysinfoServerFromClient(int clientVM
            , int clientVersion
            , String combinationName)
            throws Exception
    {
        DEBUG("");
        DEBUG("+++ sysinfoServerFromClient ++++++++++++++++++++++++++++++++++++++");
        DEBUG("    sysinfoServerFromClient: " + clientVersion + " / " + clientVM);
        DEBUG("    sysinfoServerFromClient: " + derbyVersionNames[clientVersion] + " on " + VM_Ids[clientVM]);
        
        String clientJvm = VM_Ids[clientVM]+JVMloc;
        String clientClassPath = derbyVerLibs[clientVersion] + PS+"derby.jar"
                + ":" + derbyVerLibs[clientVersion] + PS+"derbynet.jar"
                + ":" + test_jars; // Do we need test_jars here for the server?
        
        String command = "sysinfo";
        int port = serverPort;
        
        final String[] commandElements = {clientJvm
                , " -Dderby.infolog.append=true"
                , " -cp ", clientClassPath
                , " " + networkServerControl
                , " " + command
                , " -h ", serverHost
                , " -p ", serverPort+""
                // , " " + securityOption
                };
        final String[] envElements = {"CLASS_PATH="+clientClassPath
                , "PATH="+VM_Ids[clientVM]+PS+".."+PS+"bin" // "/../bin""
                };
        
        String workingDirName = System.getProperty("user.dir");
        PrintWriter out = new PrintWriter(new FileWriter(workingDirName+PS+combinationName+".sys"));
        DEBUG(combinationName+" sys:", out);
        DEBUG("user.dir: " + workingDirName, out);
        String tmp ="";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        DEBUG("commandElements: " + tmp, out);
        final String fullCmd = tmp;
        tmp ="";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp, out);
        final File workingDir = new File(workingDirName);
        
        DEBUG(
              "proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);"
             );
        try
        {
            Process proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
            processOutput(proc, out);
            
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        out.close();
        
        DEBUG("--- sysinfoServerFromClient --------------------------------------");
        DEBUG("");
        
        
    }
    
    private void sysinfoEmbedded(int clientVM
            , int clientVersion
            , String combinationName)
            throws Exception
    {
        DEBUG("");
        DEBUG("+++ sysinfoEmbedded ++++++++++++++++++++++++++++++++++++++");
        DEBUG("    sysinfoEmbedded: " + clientVersion + " / " + clientVM);
        DEBUG("    sysinfoEmbedded: " + derbyVersionNames[clientVersion] + " on " + VM_Ids[clientVM]);
        
        String clientJvm = VM_Ids[clientVM]+JVMloc;
        String clientClassPath = derbyVerLibs[clientVersion] + PS+"derby.jar"
                + ":" + derbyVerLibs[clientVersion] + PS+"derbynet.jar"
                ;
        
        final String[] commandElements = {clientJvm
                , " -Dderby.infolog.append=true"
                , " -cp ", clientClassPath
                , " " + "org.apache.derby.tools.sysinfo"
                };
        final String[] envElements = {"CLASS_PATH="+clientClassPath
                , "PATH="+VM_Ids[clientVM]+PS+".."+PS+"bin" // "/../bin""
                };
        
        String workingDirName = System.getProperty("user.dir");
        PrintWriter out = new PrintWriter(new FileWriter(workingDirName+PS+combinationName+".sys"));
        DEBUG(combinationName+" sys:", out);
        DEBUG("user.dir: " + workingDirName, out);
        String tmp ="";
        for ( int i=0;i<commandElements.length;i++)
        {tmp = tmp + commandElements[i];}
        DEBUG("commandElements: " + tmp, out);
        final String fullCmd = tmp;
        tmp ="";
        for ( int i=0;i<envElements.length;i++)
        {tmp = tmp + envElements[i] + " ";}
        DEBUG("envElements: " + tmp, out);
        final File workingDir = new File(workingDirName);
        
        DEBUG(
                "proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);"
             );
        try
        {
            Process proc = Runtime.getRuntime().exec(fullCmd,envElements,workingDir);
            processOutput(proc, out);
            
        }
        catch (Exception ex)
        {
          ex.printStackTrace();
        }
        out.close();
        DEBUG("+++ sysinfoEmbedded ++++++++++++++++++++++++++++++++++++++");
        DEBUG("");
    }
    
    /////////////////////
    
  private static void processOutput(Process proc, PrintWriter out)
    throws Exception
    {
        InputStream serveInputStream = proc.getInputStream();
        InputStream serveErrorStream = proc.getErrorStream();
        
        InputStreamReader isr = new InputStreamReader(serveInputStream);
        InputStreamReader esr = new InputStreamReader(serveErrorStream);
        BufferedReader bir = new BufferedReader(isr);
        BufferedReader ber = new BufferedReader(esr);
        String line=null;
        DEBUG("---- out:", out);
        while ( (line = bir.readLine()) != null)
        {
          out.println(line);
        }
        DEBUG("---- err:",out);
        while ( (line = ber.readLine()) != null)
        {
            out.println(line);
        }
        
    }
    private static void processDEBUGOutput(Process proc)
    throws Exception
    {
        InputStream serveInputStream = proc.getInputStream();
        InputStream serveErrorStream = proc.getErrorStream();
      
        InputStreamReader isr = new InputStreamReader(serveInputStream);
        InputStreamReader esr = new InputStreamReader(serveErrorStream);
        BufferedReader bir = new BufferedReader(isr);
        BufferedReader ber = new BufferedReader(esr);
        String line=null;
        DEBUG("---- out:");
        while ( (line = bir.readLine()) != null)
        {
            DEBUG(line);
        }
        DEBUG("---- err:");
        while ( (line = ber.readLine()) != null)
        {
          DEBUG(line);
        }
        
    }
    
    private static void DEBUG(String s)
    {
        if ( printDebug )
            System.out.println(s);
    }
    private static void DEBUG(String s, PrintWriter out)
    {
        if ( printDebug )
            out.println(s);
    }
    
}
