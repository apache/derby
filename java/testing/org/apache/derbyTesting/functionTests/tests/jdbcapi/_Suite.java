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

import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test; 
import junit.framework.TestSuite;

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

		TestSuite suite = new TestSuite("jdbcapi");

        suite.addTest(BlobSetBytesBoundaryTest.suite());
		suite.addTest(ConcurrencyTest.suite());
        suite.addTest(DaylightSavingTest.suite());
		suite.addTest(HoldabilityTest.suite());
		suite.addTest(LobLengthTest.suite()); 
		suite.addTest(ProcedureTest.suite());
		suite.addTest(SURQueryMixTest.suite());
		suite.addTest(SURTest.suite());
		suite.addTest(UpdatableResultSetTest.suite());
		suite.addTest(UpdateXXXTest.suite());
		suite.addTest(URCoveringIndexTest.suite());
		suite.addTest(ResultSetCloseTest.suite());
		suite.addTest(BlobClob4BlobTest.suite());
		suite.addTest(CharacterStreamsTest.suite());
		suite.addTest(BatchUpdateTest.suite());
		suite.addTest(StreamTest.suite());
		suite.addTest(DboPowersTest.suite());
		suite.addTest(BlobStoredProcedureTest.suite());
		suite.addTest(ClobStoredProcedureTest.suite());
		suite.addTest(CallableTest.suite());
		suite.addTest(ResultSetMiscTest.suite());
		suite.addTest(PrepStmtMetaDataTest.suite());
		suite.addTest(ScrollResultSetTest.suite());
		suite.addTest(LobStreamsTest.suite());
		suite.addTest(ResultSetJDBC30Test.suite());
		suite.addTest(DatabaseMetaDataTest.suite());
		suite.addTest(ClosedObjectTest.suite());
		suite.addTest(SetTransactionIsolationTest.suite());
		suite.addTest(AuthenticationTest.suite());
		suite.addTest(DriverTest.suite());
		suite.addTest(SURijTest.suite());
		suite.addTest(NullSQLTextTest.suite());
		suite.addTest(PrepStmtNullTest.suite());
		suite.addTest(StatementJdbc30Test.suite());
		suite.addTest(StatementJdbc20Test.suite());
        suite.addTest(ClobTest.suite());
        suite.addTest(BlobUpdatableStreamTest.suite());
        suite.addTest(AIjdbcTest.suite());
        suite.addTest(LargeDataLocksTest.suite());
        suite.addTest(DMDBugsTest.suite());
        suite.addTest(DataSourceTest.suite());
        suite.addTest(SavepointJdbc30Test.suite());
        suite.addTest(RelativeTest.suite());
        suite.addTest(metadataMultiConnTest.suite());
        suite.addTest(ResultSetStreamTest.suite());
        suite.addTest(InternationalConnectSimpleDSTest.suite());
        suite.addTest(Derby2017LayerATest.suite());
        
        // Old harness .java tests that run using the HarnessJavaTest
        // adapter and continue to use a single master file.
        suite.addTest(JDBCHarnessJavaTest.suite());
        
        if (JDBC.vmSupportsJDBC3())
        {
            // Tests that do not run under JSR169
            // DERBY-2403 blocks ParameterMappingTest from running
            // under JSR169
            suite.addTest(ParameterMappingTest.suite());

            // Class requires javax.sql.PooledConnection
            // even to load, even though the suite method
            // is correctly implemented.
            suite.addTest(DataSourcePropertiesTest.suite());

            // Tests JDBC 3.0 ability to establish a result set of 
            // auto-generated keys.
            suite.addTest(AutoGenJDBC30Test.suite());

            // Test uses DriverManager
            suite.addTest(DriverMgrAuthenticationTest.suite());
            // Tests uses JDBC 3.0 datasources
            suite.addTest(PoolDSAuthenticationTest.suite());
            suite.addTest(PoolXADSCreateShutdownDBTest.suite());
            suite.addTest(XADSAuthenticationTest.suite());
            suite.addTest(XATransactionTest.suite());
            suite.addTest(XATest.suite());
            
            // Test uses JDBC 3.0 datasources, and javax.naming.Reference etc.
            suite.addTest(DataSourceReferenceTest.suite());
            suite.addTest(DataSourceSerializationTest.suite());

            // Test uses DriverManager, Pooled and XADataSources, and
            // an inner class implements ConnectionEventListener.
            suite.addTest(J2EEDataSourceTest.suite());
            // Test requires ClientConnectionPoolDataSource.
            suite.addTest(ClientConnectionPoolDataSourceTest.suite());
            // Test requires ClientConnectionPoolDataSource.
            suite.addTest(StatementPoolingTest.suite());

            //suite to test updatable reader for clob in embedded driver
            suite.addTest (ClobUpdatableReaderTest.suite());
            
            //truncate test for clob
            suite.addTest (ClobTruncateTest.suite());

            //JSR169 does not support ParameterMetaData
            suite.addTest(ParameterMetaDataJdbc30Test.suite());
            suite.addTest(CacheSessionDataTest.suite());

            // LDAPAuthentication and InvalidLDAPSrvAuth cannot run with JSR169
            // implementation because of missing support for authentication 
            // functionality. 
            // Also, LDAPAuthentication needs properties passed in or is 
            // pointless (unless we can find a test LDAP Server)
            String ldapServer=getSystemProperty("derbyTesting.ldapServer");
            if (ldapServer == null || ldapServer.length() < 1)
                suite.addTest(new TestSuite(
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
        }

        return suite;
	}
}
