/*

Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationTestRun

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

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;

public class ReplicationTestRun extends BaseJDBCTestCase
{
    
    /** Creates a new instance of ReplicationTestRun */
    public ReplicationTestRun(String testcaseName)
    {
        super(testcaseName);
    }
    
    public static Test suite()
        throws Exception
    {
        System.out.println("*** ReplicationTestRun.suite()");
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("ReplicationTestRun");
        System.out.println("*** Done new BaseTestSuite()");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        suite.addTest(StandardTests.simpleTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.simpleTest())");
        
        suite.addTest(StandardTests.prepareStatementTest(masterHostName, masterPortNo)); 
        System.out.println("*** Done suite.addTest(StandardTests.prepareStatementTest())");
        
        suite.addTest(StandardTests.ansiTrimTest(masterHostName, masterPortNo)); // Something wrong with the instants!
        System.out.println("*** Done suite.addTest(StandardTests.ansiTrimTest())");
        
        suite.addTest(StandardTests.createTableFromQueryTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.createTableFromQueryTest())");
        
        /* Need decoration?
//IC see: https://issues.apache.org/jira/browse/DERBY-3126
        suite.addTest(StandardTests.databaseClassLoadingTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.databaseClassLoadingTest())"); */
        
        /* Need decoration!
        suite.addTest(StandardTests.dynamicLikeOptimizationTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.dynamicLikeOptimizationTest())"); */

        // suite.addTest(ExistsWithSetOpsTest.suite()); GONE!
        
        /* Need decoration!
        suite.addTest(StandardTests.grantRevokeTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.grantRevokeTest())"); */
        
        /* Need decoration!
        suite.addTest(StandardTests.groupByExpressionTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.groupByExpressionTest())"); */
      
        /* Need decoration?
        suite.addTest(StandardTests.langScripts(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.langScripts())"); */
        
        /*
        suite.addTest(MathTrigFunctionsTest.suite());
        suite.addTest(PrepareExecuteDDL.suite());
        suite.addTest(RoutineSecurityTest.suite());
        suite.addTest(RoutineTest.suite());
        suite.addTest(SQLAuthorizationPropTest.suite());
        suite.addTest(StatementPlanCacheTest.suite());
        suite.addTest(StreamsTest.suite());
        suite.addTest(TimeHandlingTest.suite());
        suite.addTest(TriggerTest.suite());
        suite.addTest(VTITest.suite());
         */
        /* 
        suite.addTest(org.apache.derbyTesting.functionTests.suites.All.replSuite());
         */
        
        return (Test)suite;
    }
    
}
