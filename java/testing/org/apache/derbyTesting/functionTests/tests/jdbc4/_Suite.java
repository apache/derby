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
package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.SQLException;

import org.apache.derbyTesting.junit.BaseTestCase;

import junit.framework.Test; 
import junit.framework.TestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.jdbc4
 *
 */
public class _Suite extends BaseTestCase  {

	/**
	 * Use suite method instead.
	 */
	private _Suite(String name) {
		super(name);
	}

	public static Test suite() throws SQLException {

		TestSuite suite = new TestSuite();

        // These really need to run standalone.
		//suite.addTestSuite(AutoloadBooting.class);
		//suite.addTestSuite(AutoloadTest.class);
        
		suite.addTest(BlobTest.suite());
		suite.addTest(CallableStatementTest.suite());
		suite.addTest(ClobTest.suite());
		suite.addTest(ClosedObjectTest.suite());
		suite.addTest(ConnectionTest.suite());
		suite.addTest(DataSourceTest.suite());
		suite.addTestSuite(JDBC40TranslationTest.class);	
		suite.addTest(ParameterMetaDataWrapperTest.suite());
		suite.addTest(PreparedStatementTest.suite());
		suite.addTest(ResultSetMetaDataTest.suite());
		suite.addTest(ResultSetTest.suite());
		suite.addTest(RowIdNotImplementedTest.suite());
		suite.addTest(StatementEventsTest.suite());
		suite.addTest(StatementTest.suite());
		suite.addTestSuite(UnsupportedVetter.class);		
		suite.addTest(XA40Test.suite());
        
        // This test is a little strange in its suite
        // method in that it accesses 
        // the database to determine the set of tests to run.
        // suite.addTest(VerifySignatures.suite());
		
		return suite;
	}
}
