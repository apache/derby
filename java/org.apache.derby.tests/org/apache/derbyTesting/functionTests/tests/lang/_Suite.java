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

import junit.framework.Test;
import org.apache.derbyTesting.functionTests.suites.XMLSuite;
import org.apache.derbyTesting.functionTests.tests.nist.NistScripts;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

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

    public static Test suite() throws Exception {

        BaseTestSuite suite = new BaseTestSuite("lang");
        
        // DERBY-1315 and DERBY-1735 need to be addressed
        // before re-enabling this test as it's memory use is
        // different on different vms leading to failures in
        // the nightly runs.
        // suite.addTest(largeCodeGen.suite());
	  
		suite.addTest(org.apache.derbyTesting.functionTests.tests.memory.TriggerTests.suite());
	  suite.addTest(CheckConstraintTest.suite());
        suite.addTest(AnsiTrimTest.suite());
        suite.addTest(AlterTableTest.suite());
        suite.addTest(CreateTableFromQueryTest.suite());     
        suite.addTest(ColumnDefaultsTest.suite());
        suite.addTest(CompressTableTest.suite());
        suite.addTest(DatabaseClassLoadingTest.suite());
        suite.addTest(DropTableTest.suite());
	  suite.addTest(DynamicLikeOptimizationTest.suite());
        suite.addTest(ExistsWithSubqueriesTest.suite());
        suite.addTest(FloatTypesTest.suite());
        suite.addTest(GrantRevokeTest.suite());
        suite.addTest(GroupByExpressionTest.suite());
        suite.addTest(InbetweenTest.suite());
        suite.addTest(InsertTest.suite());
        suite.addTest(JoinTest.suite());
        suite.addTest(LangProcedureTest.suite());
        suite.addTest(LangScripts.suite());
        suite.addTest(LikeTest.suite());
        suite.addTest(LojReorderTest.suite());
        suite.addTest(MathTrigFunctionsTest.suite());
        suite.addTest(OuterJoinTest.suite());
        suite.addTest(PredicateTest.suite());
        suite.addTest(PrepareExecuteDDL.suite());
        suite.addTest(ReferentialActionsTest.suite());
        suite.addTest(RolesTest.suite());
        suite.addTest(RolesConferredPrivilegesTest.suite());
        suite.addTest(SQLSessionContextTest.suite());
        suite.addTest(RoutineSecurityTest.suite());
        suite.addTest(RoutineTest.suite());
        suite.addTest(RoutinesDefinersRightsTest.suite());
        suite.addTest(SQLAuthorizationPropTest.suite());
        suite.addTest(StatementPlanCacheTest.suite());
        suite.addTest(StreamsTest.suite());
        suite.addTest(SubqueryFlatteningTest.suite());
        suite.addTest(TimeHandlingTest.suite());
        suite.addTest(TriggerTest.suite());
        suite.addTest(TriggerWhenClauseTest.suite());
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
        suite.addTest(InPredicateTest.suite());
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
        suite.addTest(ReleaseCompileLocksTest.suite());
        suite.addTest(LazyDefaultSchemaCreationTest.suite());
        suite.addTest(ErrorCodeTest.suite());
        suite.addTest(TimestampArithTest.suite());
        suite.addTest(SpillHashTest.suite());
        suite.addTest(CaseExpressionTest.suite());
        suite.addTest(CharUTF8Test.suite());
        suite.addTest(AggregateClassLoadingTest.suite());
        suite.addTest(LockTableTest.suite());
        suite.addTest(TableFunctionTest.suite());
        suite.addTest(VarargsTest.suite());
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
        suite.addTest(UniqueConstraintSetNullTest.suite());
        suite.addTest(UniqueConstraintMultiThreadedTest.suite());
        suite.addTest(ViewsTest.suite());
        suite.addTest(DeadlockDetectionTest.suite());
        suite.addTest(DeadlockModeTest.suite());
        suite.addTest(AnsiSignaturesTest.suite());
        suite.addTest(PredicatePushdownTest.suite());
        suite.addTest(UngroupedAggregatesNegativeTest.suite());
        suite.addTest(XplainStatisticsTest.suite());
        suite.addTest(SelectivityTest.suite());
        suite.addTest(Derby6587Test.suite());
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5412
        if (!isPhoneME())
        { // Disable temporarily until CVM/phoneME is fixed.. See DERBY-4290)
        suite.addTest(OrderByAndSortAvoidance.suite());
        }

        // tests that do not run with JSR169
//IC see: https://issues.apache.org/jira/browse/DERBY-2525
        if (JDBC.vmSupportsJDBC3())  
        {
            // test uses triggers interwoven with other tasks
            // triggers may cause a generated class which calls 
            // java.sql.DriverManager, which will fail with JSR169.
            // also, test calls procedures which use DriverManager
            // to get the default connection.
            suite.addTest(GrantRevokeDDLTest.suite());

            // test uses regex classes that are not available in Foundation 1.1
//IC see: https://issues.apache.org/jira/browse/DERBY-2829
//IC see: https://issues.apache.org/jira/browse/DERBY-2817
            suite.addTest(ErrorMessageTest.suite());
            // Test uses DriverManager to connect to database in jar.
            suite.addTest(DBInJarTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3754
            suite.addTest(ConnectTest.suite());
            
            // test uses PooledConnections and Savepoints
            suite.addTest(DeclareGlobalTempTableJavaJDBC30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2895

            // requires Java 5 (generics)
            suite.addTest(UserDefinedAggregatesTest.suite());
            suite.addTest(UDAPermsTest.suite());
        }
         // tests that require Java 6
        if (JDBC.vmSupportsJDBC4())  
        {
            suite.addTest(OptionalToolsTest.suite());
        }
       
//IC see: https://issues.apache.org/jira/browse/DERBY-3724
        suite.addTest(BigDataTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3751
        suite.addTest(MixedCaseExpressionTest.suite());
        suite.addTest(UpdateStatisticsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3760
        suite.addTest(MiscErrorsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3767
        suite.addTest(NullsTest.suite());
        suite.addTest(ArithmeticTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3750
        suite.addTest(ConstantExpressionTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3802
        suite.addTest(OptimizerOverridesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3758
        suite.addTest(PrecedenceTest.suite());
        suite.addTest(GeneratedColumnsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3932
        suite.addTest(GeneratedColumnsPermsTest.suite());
        suite.addTest(RestrictedVTITest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6117
        suite.addTest(AwareVTITest.suite());
        suite.addTest(UDTTest.suite());
        suite.addTest(UDTPermsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4663
        suite.addTest(BooleanValuesTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4013
//IC see: https://issues.apache.org/jira/browse/DERBY-4014
        suite.addTest(AlterColumnTest.suite());
        suite.addTest(UserLobTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4079
        suite.addTest(OffsetFetchNextTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6523
        suite.addTest(TriggerBeforeTrigTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6520
        suite.addTest(TriggerGeneralTest.suite());
        suite.addTest(SequenceTest.suite());
        suite.addTest(SequencePermsTest.suite());
        suite.addTest(SequenceGeneratorTest.suite());
        suite.addTest(DBOAccessTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3634
//IC see: https://issues.apache.org/jira/browse/DERBY-4069
        suite.addTest(OLAPTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4398
        suite.addTest(OrderByAndOffsetFetchInSubqueries.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5005
        suite.addTest(Derby5005Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4509
        suite.addTest(AutoIncrementTest.suite());
        suite.addTest(HalfCreatedDatabaseTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5601
//IC see: https://issues.apache.org/jira/browse/DERBY-5607
        suite.addTest(NativeAuthenticationServiceTest.suite());
        suite.addTest(Derby5652.suite());
        suite.addTest(TruncateTableAndOnlineBackupTest.suite()); 
        suite.addTest(QueryPlanTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6519
        suite.addTest(JoinDeadlockTest.suite());
        suite.addTest(Derby6131.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6514
//IC see: https://issues.apache.org/jira/browse/DERBY-5614
        suite.addTest(AggBuiltinTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6267
        suite.addTest(NewOptimizerOverridesTest.suite());
        suite.addTest(MergeStatementTest.suite());
        suite.addTest(IdentitySequenceTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6516
//IC see: https://issues.apache.org/jira/browse/DERBY-6515
        suite.addTest(NestedCommitTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6507
//IC see: https://issues.apache.org/jira/browse/DERBY-6707
        suite.addTest(ForeignKeysNonSpsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6508
        suite.addTest(LOBDB2compatibilityTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6509
        suite.addTest(CurrentSchemaTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6496
        suite.addTest(Test_6496.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        suite.addTest(ConstraintCharacteristicsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-532
        suite.addTest(ForeignKeysDeferrableTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6276
        suite.addTest(DB2IsolationLevelsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        suite.addTest(LuceneSuite.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6825
        suite.addTest(JsonSuite.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6517
//IC see: https://issues.apache.org/jira/browse/DERBY-5617
        suite.addTest(ConsistencyCheckerTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6136
        suite.addTest(RawDBReaderTest.suite());
        suite.addTest(Derby5866TriggerOrderTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        suite.addTest(NoDBInternalsPermissionTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6654
        suite.addTest(ClassLoadingTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
        if (TestConfiguration.loadingFromJars()) { suite.addTest(VetJigsawTest.suite()); }
        return suite;
	}
}
