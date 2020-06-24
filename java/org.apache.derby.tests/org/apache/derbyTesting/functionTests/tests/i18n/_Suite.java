/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.18n._Suite

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
package org.apache.derbyTesting.functionTests.tests.i18n;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBC;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.i18n
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

        BaseTestSuite suite = new BaseTestSuite("i18n");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Also, none of these tests will run with JSR169.
        if (JDBC.vmSupportsJSR169())
            return suite;
        suite.addTest(LocalizedAttributeScriptTest.suite());
        suite.addTest(LocalizedDisplayScriptTest.suite());
        suite.addTest(JapanCodeConversionTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6244
        suite.addTest(CaseI_tr_TRTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-6246
        suite.addTest(UrlLocaleTest.suite());
//IC see: https://issues.apache.org/jira/browse/DERBY-4035
        suite.addTest(I18NImportExport.suite());
        suite.addTest(ImportExportProcedureESTest.suite());
        return suite;
	}
}
