/*
 * Derby - Class 
 * org.apache.derbyTesting.functionTests.tests.tools.ImportExportIJTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.tools;

import java.util.Locale;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 *	Test that runs the importExportThruIJ.sql script and compares the output 
 *	to importExportThruIJ.out.
 */
public final class ImportExportIJTest extends ScriptTestCase {
	
	/**
	 * Constructor that runs a single script.
	 * 
	 * @param script - the name of the script
	 */
	private ImportExportIJTest(String script) {
//IC see: https://issues.apache.org/jira/browse/DERBY-5217
		super(script, true);
	}

	
	/**
	 * Return the suite that runs the script.
	 */
	public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("importExportIJ");
		
        // only run with embedded
        // network server makes slightly different output
        // ('statement executed' instead of '# rows inserted/deteled', etc.)
        // and this test would never work if the server were on 
        // a remote system because the export file would be on the
        // server side, and import would be looking on the client.
        // Also, running client & embedded would require some cleanup magic to
        // remove the exported files (see e.g. ImportExportTest).
//IC see: https://issues.apache.org/jira/browse/DERBY-5368
		Test test = new ImportExportIJTest("importExportIJ");
		
        // This test should run in English locale since it compares error
        // messages against a canon based on the English message text. Also,
        // run the test in a fresh database, since the language of the message
        // text is determined when the database is created.        
        test = new LocaleTestSetup(test, Locale.ENGLISH);	
        test = TestConfiguration.singleUseDatabaseDecorator(test);
		
		suite.addTest(new CleanDatabaseTestSetup(test));

        return new SupportFilesSetup(suite, new String[] {
            "functionTests/testData/ImportExport/TwoLineBadEOF.dat",
            "functionTests/testData/ImportExport/NoEOR.dat",
            "functionTests/testData/ImportExport/Access1.txt",
            "functionTests/testData/ImportExport/AccountData_defaultformat.dat",
            "functionTests/testData/ImportExport/AccountData_format1.dat",
            "functionTests/testData/ImportExport/AccountData_format2.dat",
            "functionTests/testData/ImportExport/AccountData_format2oops.dat",
            "functionTests/testData/ImportExport/AccountData_NullFields.dat",
            "functionTests/testData/ImportExport/UnsupportedFormat1.dat",
            "functionTests/testData/ImportExport/UnsupportedFormat2.dat",
            "functionTests/testData/ImportExport/derby-2193.txt",
            "functionTests/testData/ImportExport/derby-2193-linenumber.txt"
            }
        );
	}
}
