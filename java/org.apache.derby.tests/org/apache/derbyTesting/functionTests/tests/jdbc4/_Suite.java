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
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

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

        BaseTestSuite suite = new BaseTestSuite("jdbc4");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1989
		suite.addTest(TestDbMetaData.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1988
		suite.addTest(TestJDBC40Exception.suite());
		suite.addTest(UnsupportedVetter.suite());
		suite.addTest(XA40Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2855
		suite.addTest(ConnectionMethodsTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-1952
        suite.addTest(VerifySignatures.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2247
        suite.addTest (LobStreamTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4245
        suite.addTest(LobSortTest.suite());
        suite.addTest (BlobSetMethodsTest.suite());
        suite.addTest (JDBC4FromJDBC3DataSourceTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-3650
        suite.addTest(Derby3650Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-2017
        suite.addTest(Derby2017LayerBTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        suite.addTest(AbortTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        suite.addTest(Driver40Test.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        suite.addTest(Driver40UnbootedTest.suite());
        suite.addTest(LoginTimeoutTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6094

//IC see: https://issues.apache.org/jira/browse/DERBY-6614
        if (JDBC.vmSupportsJDBC42())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6000
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
