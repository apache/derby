/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4._Suite

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

import java.lang.reflect.Method;
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

		TestSuite suite = new TestSuite("jdbc4");

        // These really need to run standalone.
		//suite.addTestSuite(AutoloadBooting.class);

		suite.addTest(BlobTest.suite());
		suite.addTest(CallableStatementTest.suite());
		suite.addTest(ClobTest.suite());
		suite.addTest(ConnectionTest.suite());
		suite.addTest(DataSourceTest.suite());
		suite.addTest(ParameterMetaDataWrapperTest.suite());
		suite.addTest(PreparedStatementTest.suite());
		suite.addTest(ResultSetMetaDataTest.suite());
		suite.addTest(ResultSetTest.suite());
		suite.addTest(RowIdNotImplementedTest.suite());
		suite.addTest(SetObjectUnsupportedTest.suite());
		suite.addTest(StatementEventsTest.suite());
		suite.addTest(StatementTest.suite());
		suite.addTest(TestDbMetaData.suite());
		suite.addTest(TestJDBC40Exception.suite());
		suite.addTest(UnsupportedVetter.suite());
		suite.addTest(XA40Test.suite());
		suite.addTest(ConnectionMethodsTest.suite());
        suite.addTest(VerifySignatures.suite());
        suite.addTest (LobStreamTest.suite());
        suite.addTest(LobSortTest.suite());
        suite.addTest (BlobSetMethodsTest.suite());
        suite.addTest (JDBC4FromJDBC3DataSourceTest.suite());
        suite.addTest(Derby3650Test.suite());
        suite.addTest(Derby2017LayerBTest.suite());
        suite.addTest(AbortTest.suite());
        suite.addTest(Driver40Test.suite());
        suite.addTest(Driver40UnbootedTest.suite());
        suite.addTest(LoginTimeoutTest.suite());

        if ( isJava8() )
        {
            suite.addTest( getSuite( "org.apache.derbyTesting.functionTests.tests.jdbc4.PreparedStatementTest42" ) );
        }
		
		return suite;
	}

    /**
     * <p>
     * Get the test suite from a class name.
     * </p>
     */
    private static  Test    getSuite( String className )
        throws SQLException
    {
        try {
            Class<?>   klass = Class.forName( className );
            Method  suiteMethod = klass.getMethod( "suite", new Class<?>[] {} );

            return (Test) suiteMethod.invoke( null, new Object[] {} );
        }
        catch (Exception e) { throw new SQLException( e.getMessage(), e ); }
    }
    
}
