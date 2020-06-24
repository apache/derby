/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi._Suite

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.jdbcapi
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

        BaseTestSuite suite = new BaseTestSuite("jdbcapi");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest(BlobSetBytesBoundaryTest.suite());
		suite.addTest(ConcurrencyTest.suite());
        suite.addTest(DaylightSavingTest.suite());
		suite.addTest(HoldabilityTest.suite());
        suite.addTest(Derby5158Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1893
//IC see: https://issues.apache.org/jira/browse/DERBY-1982
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
		suite.addTest(LobLengthTest.suite()); 
		suite.addTest(ProcedureTest.suite());
		suite.addTest(SURQueryMixTest.suite());
		suite.addTest(SURTest.suite());
		suite.addTest(UpdatableResultSetTest.suite());
		suite.addTest(UpdateXXXTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
//IC see: https://issues.apache.org/jira/browse/DERBY-1971
		suite.addTest(URCoveringIndexTest.suite());
		suite.addTest(ResultSetCloseTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1895
		suite.addTest(BlobClob4BlobTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1979
		suite.addTest(CharacterStreamsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2293
		suite.addTest(BatchUpdateTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2005
		suite.addTest(StreamTest.suite());
		suite.addTest(DboPowersTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2257
		suite.addTest(BlobStoredProcedureTest.suite());
		suite.addTest(ClobStoredProcedureTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2304
//IC see: https://issues.apache.org/jira/browse/DERBY-1982
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
		suite.addTest(CallableTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2429
		suite.addTest(ResultSetMiscTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2452
		suite.addTest(PrepStmtMetaDataTest.suite());
		suite.addTest(ScrollResultSetTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1889
		suite.addTest(LobStreamsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1962
		suite.addTest(ResultSetJDBC30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2242
		suite.addTest(DatabaseMetaDataTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2324
		suite.addTest(ClosedObjectTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2463
		suite.addTest(SetTransactionIsolationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
		suite.addTest(AuthenticationTest.suite());
		if (!JDBC.vmSupportsJSR169()) {
		    // DERBY-5069 Suites.All fails with InvocationTargetException
//IC see: https://issues.apache.org/jira/browse/DERBY-2492
		    suite.addTest(DriverTest.suite());
		}
//IC see: https://issues.apache.org/jira/browse/DERBY-3828
		suite.addTest(SURijTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2841
		suite.addTest(NullSQLTextTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2981
		suite.addTest(PrepStmtNullTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2942
		suite.addTest(StatementJdbc30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3203
		suite.addTest(StatementJdbc20Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2646
        suite.addTest(ClobTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2830
        suite.addTest(BlobUpdatableStreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2726
        suite.addTest(AIjdbcTest.suite());
        suite.addTest(LargeDataLocksTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3000
//IC see: https://issues.apache.org/jira/browse/DERBY-1790
        suite.addTest(DMDBugsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3410
        suite.addTest(DataSourceTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3568
        suite.addTest(SavepointJdbc30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3587
        suite.addTest(RelativeTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3797
        suite.addTest(metadataMultiConnTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3990
        suite.addTest(ResultSetStreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
        suite.addTest(InternationalConnectSimpleDSTest.suite());
        suite.addTest(Derby2017LayerATest.suite());
        suite.addTest(LobRsGetterTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5489

        // Old harness .java tests that run using the HarnessJavaTest
        // adapter and continue to use a single master file.
        suite.addTest(JDBCHarnessJavaTest.suite());
        
//IC see: https://issues.apache.org/jira/browse/DERBY-1897
        if (JDBC.vmSupportsJDBC3())
        {
            // Tests that do not run under JSR169
            // DERBY-2403 blocks ParameterMappingTest from running
            // under JSR169
            suite.addTest(ParameterMappingTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2411

            // Class requires javax.sql.PooledConnection
            // even to load, even though the suite method
            // is correctly implemented.
            suite.addTest(DataSourcePropertiesTest.suite());

            // Tests JDBC 3.0 ability to establish a result set of 
            // auto-generated keys.
            suite.addTest(AutoGenJDBC30Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2398

            // Test uses DriverManager
//IC see: https://issues.apache.org/jira/browse/DERBY-1496
            suite.addTest(DriverMgrAuthenticationTest.suite());
            // Tests uses JDBC 3.0 datasources
            suite.addTest(PoolDSAuthenticationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2576
            suite.addTest(PoolXADSCreateShutdownDBTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-5165
            suite.addTest(Derby5165Test.suite());
            suite.addTest(XADSAuthenticationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2551
            suite.addTest(XATransactionTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
            suite.addTest(XATest.suite());
            
            // Test uses JDBC 3.0 datasources, and javax.naming.Reference etc.
//IC see: https://issues.apache.org/jira/browse/DERBY-2296
//IC see: https://issues.apache.org/jira/browse/DERBY-2296
            suite.addTest(DataSourceReferenceTest.suite());
            suite.addTest(DataSourceSerializationTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1318

            // Test uses DriverManager, Pooled and XADataSources, and
            // an inner class implements ConnectionEventListener.
//IC see: https://issues.apache.org/jira/browse/DERBY-3410
            suite.addTest(J2EEDataSourceTest.suite());
            // Test requires ClientConnectionPoolDataSource.
//IC see: https://issues.apache.org/jira/browse/DERBY-3325
//IC see: https://issues.apache.org/jira/browse/DERBY-3306
            suite.addTest(ClientConnectionPoolDataSourceTest.suite());
            // Test requires ClientConnectionPoolDataSource.
            suite.addTest(StatementPoolingTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3329

            //suite to test updatable reader for clob in embedded driver
//IC see: https://issues.apache.org/jira/browse/DERBY-2823
            suite.addTest (ClobUpdatableReaderTest.suite());
            
            //truncate test for clob
            suite.addTest (ClobTruncateTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2800

            //JSR169 does not support ParameterMetaData
//IC see: https://issues.apache.org/jira/browse/DERBY-2862
            suite.addTest(ParameterMetaDataJdbc30Test.suite());
            suite.addTest(CacheSessionDataTest.suite());

            // LDAPAuthentication and InvalidLDAPSrvAuth cannot run with JSR169
            // implementation because of missing support for authentication 
            // functionality. 
            // Also, LDAPAuthentication needs properties passed in or is 
            // pointless (unless we can find a test LDAP Server)
//IC see: https://issues.apache.org/jira/browse/DERBY-3659
            String ldapServer=getSystemProperty("derbyTesting.ldapServer");
            if (ldapServer == null || ldapServer.length() < 1)
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
                suite.addTest(new BaseTestSuite(
                    "LDAPAuthenticationTest and XAJNDITest require " +
                    "derbyTesting.ldap* properties."));
            else
            {
                suite.addTest(LDAPAuthenticationTest.suite());
                suite.addTest(XAJNDITest.suite());
            }
            suite.addTest(InvalidLDAPServerAuthenticationTest.suite());
            
            // XA and ConnectionPool Datasource are not available with
            // JSR169 so can't run InternationalConnectTest. 
            suite.addTest(InternationalConnectTest.suite());

            // Test requires java.sql.DriverManager
//IC see: https://issues.apache.org/jira/browse/DERBY-5664
            suite.addTest(AutoloadTest.fullAutoloadSuite());
        }

        return suite;
	}
}
