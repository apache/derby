/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang._Suite

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
package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derbyTesting.functionTests.suites.XMLSuite;
import org.apache.derbyTesting.functionTests.tests.nist.NistScripts;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test; 
import junit.framework.TestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.lang
 * <P>
 * All tests are run "as-is", just as if they were run
 * individually. Thus this test is just a collection
 * of all the JUNit tests in this package (excluding itself).
 * While the old test harness is in use, some use of decorators
 * may be required.
 *
 */
public class _Suite extends BaseTestCase  {

	/**
	 * Use suite method instead.
	 */
	private _Suite(String name) {
		super(name);
	}

	public static Test suite() {

		TestSuite suite = new TestSuite("lang");
        
        // DERBY-1315 and DERBY-1735 need to be addressed
        // before re-enabling this test as it's memory use is
        // different on different vms leading to failures in
        // the nightly runs.
        // suite.addTest(largeCodeGen.suite());

        suite.addTest(AnsiTrimTest.suite());
        suite.addTest(CreateTableFromQueryTest.suite());
        suite.addTest(CompressTableTest.suite());
        suite.addTest(DatabaseClassLoadingTest.suite());
        suite.addTest(DynamicLikeOptimizationTest.suite());
        suite.addTest(ExistsWithSubqueriesTest.suite());
        suite.addTest(GrantRevokeTest.suite());
        suite.addTest(GroupByExpressionTest.suite());
		suite.addTest(LangScripts.suite());
        suite.addTest(MathTrigFunctionsTest.suite());
        suite.addTest(PrepareExecuteDDL.suite());
        suite.addTest(RolesTest.suite());
        suite.addTest(RolesConferredPrivilegesTest.suite());
        suite.addTest(SQLSessionContextTest.suite());
        suite.addTest(RoutineSecurityTest.suite());
        suite.addTest(RoutineTest.suite());
        suite.addTest(SQLAuthorizationPropTest.suite());
        suite.addTest(StatementPlanCacheTest.suite());
        suite.addTest(StreamsTest.suite());
        suite.addTest(TimeHandlingTest.suite());
        suite.addTest(TriggerTest.suite());
        suite.addTest(TruncateTableTest.suite());
        suite.addTest(VTITest.suite());
        suite.addTest(SysDiagVTIMappingTest.suite());
        suite.addTest(UpdatableResultSetTest.suite());
        suite.addTest(CurrentOfTest.suite());
	    suite.addTest(CursorTest.suite());
        suite.addTest(CastingTest.suite());
        suite.addTest(ScrollCursors2Test.suite());
        suite.addTest(NullIfTest.suite());
        suite.addTest(InListMultiProbeTest.suite());
        suite.addTest(SecurityPolicyReloadingTest.suite());
        suite.addTest(CurrentOfTest.suite());
        suite.addTest(UnaryArithmeticParameterTest.suite());
        suite.addTest(HoldCursorTest.suite());
        suite.addTest(ShutdownDatabaseTest.suite());
        suite.addTest(StalePlansTest.suite());
        suite.addTest(SystemCatalogTest.suite());
        suite.addTest(ForBitDataTest.suite());
        suite.addTest(DistinctTest.suite());
        suite.addTest(GroupByTest.suite());
        suite.addTest(UpdateCursorTest.suite());
        suite.addTest(CoalesceTest.suite());
        suite.addTest(ProcedureInTriggerTest.suite());
        suite.addTest(ForUpdateTest.suite());
        suite.addTest(CollationTest.suite());
        suite.addTest(CollationTest2.suite());
        suite.addTest(ScrollCursors1Test.suite());
        suite.addTest(SimpleTest.suite());
        suite.addTest(GrantRevokeDDLTest.suite());
        suite.addTest(ReleaseCompileLocksTest.suite());
        suite.addTest(LazyDefaultSchemaCreationTest.suite());
        suite.addTest(ErrorCodeTest.suite());
        suite.addTest(TimestampArithTest.suite());
        suite.addTest(SpillHashTest.suite());
        suite.addTest(CaseExpressionTest.suite());
        suite.addTest(CharUTF8Test.suite());
        suite.addTest(AggregateClassLoadingTest.suite());
        suite.addTest(TableFunctionTest.suite());
        suite.addTest(DeclareGlobalTempTableJavaTest.suite());
        suite.addTest(PrimaryKeyTest.suite());
        suite.addTest(RenameTableTest.suite());
        suite.addTest(RenameIndexTest.suite());
        suite.addTest(Bug5052rtsTest.suite());
        suite.addTest(Bug5054Test.suite());
        suite.addTest(Bug4356Test.suite());
        suite.addTest(SynonymTest.suite());
        suite.addTest(CommentTest.suite());
        suite.addTest(NestedWhereSubqueryTest.suite());
        suite.addTest(ConglomerateSharingTest.suite());
        suite.addTest(NullableUniqueConstraintTest.suite());
        suite.addTest(OLAPTest.suite());
        suite.addTest(UniqueConstraintSetNullTest.suite());
        suite.addTest(ViewsTest.suite());
        suite.addTest(DeadlockModeTest.suite());
        suite.addTest(AnsiSignaturesTest.suite());
        suite.addTest(PredicatePushdownTest.suite());
        suite.addTest(UngroupedAggregatesNegativeTest.suite());
        // Add the XML tests, which exist as a separate suite
        // so that users can "run all XML tests" easily.
        suite.addTest(XMLSuite.suite());
         
        // Add the NIST suite in from the nist package since
        // it is a SQL language related test.
        suite.addTest(NistScripts.suite());
        
        // Add the java tests that run using a master
        // file (ie. partially converted).
        suite.addTest(LangHarnessJavaTest.suite());
        		
        suite.addTest(ResultSetsFromPreparedStatementTest.suite());

        // tests that do not run with JSR169
        if (JDBC.vmSupportsJDBC3())  
        {
            // test uses triggers interwoven with other tasks
            // triggers may cause a generated class which calls 
            // java.sql.DriverManager, which will fail with JSR169.
            // also, test calls procedures which use DriverManager
            // to get the default connection.
            suite.addTest(GrantRevokeDDLTest.suite());

            // test uses regex classes that are not available in Foundation 1.1
            suite.addTest(ErrorMessageTest.suite());
            // Test uses DriverManager to connect to database in jar.
            suite.addTest(DBInJarTest.suite());
            suite.addTest(ConnectTest.suite());
        }
        
        suite.addTest(BigDataTest.suite());
        suite.addTest(MixedCaseExpressionTest.suite());
        suite.addTest(UpdateStatisticsTest.suite());
        suite.addTest(MiscErrorsTest.suite());
        suite.addTest(NullsTest.suite());
        suite.addTest(ArithmeticTest.suite());
        suite.addTest(ConstantExpressionTest.suite());
        suite.addTest(OptimizerOverridesTest.suite());
        suite.addTest(PrecedenceTest.suite());
        suite.addTest(GeneratedColumnsTest.suite());
        suite.addTest(GeneratedColumnsPermsTest.suite());
        suite.addTest(AlterColumnTest.suite());
        suite.addTest(UserLobTest.suite());
        suite.addTest(OffsetFetchNextTest.suite());

        return suite;
	}
}
