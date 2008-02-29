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
import junit.framework.TestSuite;
import org.apache.derbyTesting.functionTests.tests.derbynet.PrepareStatementTest;
import org.apache.derbyTesting.functionTests.tests.lang.AnsiTrimTest;
import org.apache.derbyTesting.functionTests.tests.lang.CreateTableFromQueryTest;
import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBCClient;
import org.apache.derbyTesting.junit.TestConfiguration;

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
        
        TestSuite suite = new TestSuite("ReplicationTestRun");
        System.out.println("*** Done new TestSuite()");
        
        String masterHostName = System.getProperty("test.serverHost", "localhost");
        int masterPortNo = Integer.parseInt(System.getProperty("test.serverPort", "1527"));
        
        suite.addTest(StandardTests.simpleTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.simpleTest())");
        
        /* PoC: Gives 'Something wrong with the instants!' Seems to be volume related? * /
        suite.addTest(StandardTests.prepareStatementTest(masterHostName, masterPortNo)); 
        System.out.println("*** Done suite.addTest(StandardTests.prepareStatementTest())");
        / * */
        
        suite.addTest(StandardTests.ansiTrimTest(masterHostName, masterPortNo)); // Something wrong with the instants!
        System.out.println("*** Done suite.addTest(StandardTests.ansiTrimTest())");
        
        suite.addTest(StandardTests.createTableFromQueryTest(masterHostName, masterPortNo));
        System.out.println("*** Done suite.addTest(StandardTests.createTableFromQueryTest())");
        
        /*
        suite.addTest(DatabaseClassLoadingTest.suite());
        suite.addTest(DynamicLikeOptimizationTest.suite());
        suite.addTest(ExistsWithSetOpsTest.suite());
        suite.addTest(GrantRevokeTest.suite());
        suite.addTest(GroupByExpressionTest.suite());
		suite.addTest(LangScripts.suite());
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
