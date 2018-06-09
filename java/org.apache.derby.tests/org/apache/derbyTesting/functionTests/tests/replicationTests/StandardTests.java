/*

Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.StandardTests

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

import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.suites.AllPackages;
import org.apache.derbyTesting.functionTests.suites.EncryptionSuite;
import org.apache.derbyTesting.functionTests.tests.derbynet.PrepareStatementTest;
import org.apache.derbyTesting.functionTests.tests.lang.AnsiTrimTest;
import org.apache.derbyTesting.functionTests.tests.lang.CreateTableFromQueryTest;
import org.apache.derbyTesting.functionTests.tests.lang.DatabaseClassLoadingTest;
import org.apache.derbyTesting.functionTests.tests.lang.LangScripts;
import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Wrap some JUnit tests in TestConfiguration.existingServerSuite() to
 * run these tests against an already started server on a given host
 * using a given port.
 *
 * Initially used in testing replication functionality - DERBY-2872.
 */

public class StandardTests extends BaseJDBCTestCase
{
    
    /* Creates a new instance of StandardTests */
    public StandardTests(String testcaseName)
    {
        super(testcaseName);
    }
    
    /*
     * Template  which adds this class to the *existing server* suite.
     */
    /* BEGIN Template
    public static Test simpleTest(String serverHost, int serverPort)
    {
        Test t = TestConfiguration.existingServerSuite(SimpleTest.class,false,serverHost,serverPort);
        System.out.println("*** Done TestConfiguration.defaultExistingServerSuite(SimpleTest.class,false)");
        return t;
        
        // /* Common pattern as below, but since we do not need to decorate her, just skip. 
        CleanDatabaseTestSetup cdts = 
                new CleanDatabaseTestSetup(t, 
                        true,// Use networkclient when running setUp/decorateSQL
                        serverHost,
                        serverPort
                    ) {
            public void decorateSQL(Statement s)
                    throws SQLException {
                s.executeUpdate(".....");
                .  
                .  
                }            
        };
        return cdts;
         //
    }
    END Template*/


     /*
      * Adds this class to the *existing server* suite.
      */
    public static Test simpleTest(String serverHost, int serverPort)
    {
        return TestConfiguration.existingServerSuite(SimpleTest.class,
                false,serverHost,serverPort);
        // return DatabasePropertyTestSetup.setLockTimeouts(suite,3,3); ??
    }
    
    public static Test prepareStatementTest(String hostName, int portNo)
    {
        return TestConfiguration.existingServerSuite(PrepareStatementTest.class,
                false,hostName, portNo);
    }

    public static Test ansiTrimTest(String serverHost, int serverPort)
    {
        Test t = TestConfiguration.existingServerSuite(AnsiTrimTest.class, false, // false: because adds clean/decorate below
                serverHost,serverPort); 
        CleanDatabaseTestSetup cdts = 
                new CleanDatabaseTestSetup(t, 
                        true,// Use networkclient when running setUp/decorateSQL
                        serverHost,
                        serverPort
                    ) 
        {
            public void decorateSQL(Statement s)
                    throws SQLException 
            {
                AnsiTrimTest.decorate(s);
            }            
        };
        return cdts;
    }
    
    public static Test createTableFromQueryTest(String serverHost, int serverPort)
    {
        Test t = TestConfiguration.existingServerSuite(CreateTableFromQueryTest.class, false, // false: because adds clean/decorate below
                serverHost,serverPort);
        CleanDatabaseTestSetup cdts = 
                new CleanDatabaseTestSetup(t, 
                        true,// Use networkclient when running setUp/decorateSQL
                        serverHost,
                        serverPort
                    ) 
        {
            protected void decorateSQL(Statement stmt) 
                throws SQLException
            {
                CreateTableFromQueryTest.decorate(stmt);
            }
        };
        return cdts;
    }
    
    public static Test databaseClassLoadingTest(String hostName, int portNo)
    {
        return TestConfiguration.existingServerSuite(DatabaseClassLoadingTest.class,
                false,hostName, portNo);
    }
    
    public static Test dynamicLikeOptimizationTest(String hostName, int portNo)
    {
        System.out.println("********* FIXME!");return null;
        /* factor out DynamicLikeOptimizationTest.decorate()
        return TestConfiguration.existingServerSuite(DynamicLikeOptimizationTest.class,
                false,hostName, portNo);
         */
    }
    public static Test grantRevokeTest(String hostName, int portNo)
    {
        System.out.println("********* FIXME!");return null;
        /* factor out GrantRevokeTest.decorate()
        return TestConfiguration.existingServerSuite(GrantRevokeTest.class,
                false,hostName, portNo);
        */
    }
    
    public static Test groupByExpressionTest(String hostName, int portNo)
    {
        System.out.println("********* FIXME!");return null;
        /* factor out GrantRevokeTest.decorate() 
        return TestConfiguration.existingServerSuite(GroupByExpressionTest.class,
                false,hostName, portNo);
        */
    }

    public static Test langScripts(String hostName, int portNo)
    {
        // System.out.println("********* FIXME!");return null;
        /* factor out GrantRevokeTest.decorate() */
        return TestConfiguration.existingServerSuite(LangScripts.class,
                false,hostName, portNo);
    }

    
    /* All the above are pure Tests. To handle suites
     * we will have to duplicate the .suite() structure starting at .suites.all!
     *
     * The following is WORK IN PROGRESS: NOT READY FOR USE! FIXME!
     */
    public static Test all(String serverHost, int serverPort) 
        throws Exception 
    {

        BaseTestSuite suite =
            new BaseTestSuite("All_"+serverHost+":"+serverPort);

        // All package tests 
        // This won't work as there are no 'testXXXX' methods 
        // in AllPackages. Must create a suite() following the pattern of suites.All suite().
        // This is probably correct anyway as we presumably won't use all
        // tests in the replication testing?
        // Problem is we get a parallell structure which needs maintenance!
        
        suite.addTest(TestConfiguration.existingServerSuite(AllPackages.class, false, // false: because adds clean/decorate below
                serverHost,serverPort));
        // Instead:
        suite.addTest(TestConfiguration.existingServerSuite(allPackagesSuite(), false, // false: because adds clean/decorate below
                serverHost,serverPort));
        
        // Encrypted tests
        suite.addTest(TestConfiguration.existingServerSuite(EncryptionSuite.class, false, // false: because adds clean/decorate below
                serverHost,serverPort));
        CleanDatabaseTestSetup cdts = 
                new CleanDatabaseTestSetup(suite, 
                        true,// Use networkclient when running setUp/decorateSQL
                        serverHost,
                        serverPort
                    ) 
        {
            public void decorateSQL(Statement s)
                    throws SQLException {
                // decorate(s);
            }            
        };
        return cdts;
    }

    private static Class allPackagesSuite()
    {
        return null;
    }
}
