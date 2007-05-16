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
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

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

		suite.addTest(ConcurrencyTest.suite());
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
		suite.addTest(XATransactionTest.suite());
		suite.addTest(DriverTest.suite());
        
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
            suite.addTest(XADSAuthenticationTest.suite());

            // Test uses JDBC 3.0 datasources, and javax.naming.Reference etc.
            suite.addTest(DataSourceReferenceTest.suite());

            // Test uses DriverManager, Pooled and XADataSources, and
            // an inner class implements ConnectionEventListener.
            suite.addTest(DataSourceTest.suite());

            //suite to test updateable reader for clob in embedded driver
            suite.addTest (ClobUpdateableReaderTest.suite());
        }
		
        return suite;
	}
}
