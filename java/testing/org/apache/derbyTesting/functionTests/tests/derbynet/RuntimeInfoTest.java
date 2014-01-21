/*

   Derby - Class 
   org.apache.derbyTesting.functionTests.tests.derbynet.RuntimeInfoTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Locale;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 This tests the runtimeinfo command
 */

public class RuntimeInfoTest extends BaseJDBCTestCase {

	private static String[] RuntimeinfoCmd = new String[] {
            "-Demma.verbosity.level=silent",
			"org.apache.derby.drda.NetworkServerControl", "runtimeinfo",
			"-p", String.valueOf(TestConfiguration.getCurrent().getPort()) };
	private static String[] RuntimeinfoLocaleCmd = new String[] {
            "-Demma.verbosity.level=silent",
			"-Duser.language=err", "-Duser.country=DE",
			"org.apache.derby.drda.NetworkServerControl", "runtimeinfo",
			"-p", String.valueOf(TestConfiguration.getCurrent().getPort()) };
	
    private static final String POLICY_FILE_NAME =
        "org/apache/derbyTesting/functionTests/tests/derbynet/RuntimeInfoTest.policy";

    private static final Locale englishLocale = new Locale("en", "US");
    private static final Locale germanLocale = new Locale("de", "DE");
    private static final String stdout_err_tags = "<[^<>]*STD.*>";
	
	/**
	 * Constructor
	 * 
	 * @param name
	 */
	public RuntimeInfoTest(String name) {
		super(name);
	}

	/**
	 * Creates a suite with two testcases, with and without some extra 
	 * system properties.
	 * 
	 * @return The test suite with both English and German locales.
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("RuntimeInfoTest");

        // Run testRunTests in both English and German locale
        suite.addTest(decorateTest(englishLocale, "testRunTests"));
        suite.addTest(decorateTest(germanLocale, "testRunTests"));

        // Other test cases, only tested in a single locale.
        suite.addTest(
                decorateTest(englishLocale, "testRuntimeInfoWithLongValues"));

		return suite;
	}	
	
	/**
	 * This is the wrapper that calls the x_tests in order.
	 * These fixtures rely on the order of the commands being issued.
	 */
	public void testRunTests() throws Exception {
		x_testRuntimeInfoWithActiveConn();
		x_testRuntimeInfoLocale();
		x_testRuntimeInfoAfterConnClose();
	}
	
	/**
	 * Test runtimeinfo
	 * 
	 * @throws Exception
	 */
	public void x_testRuntimeInfoWithActiveConn() throws Exception {
		Process p = execJavaCmd(RuntimeinfoCmd);
		String output = sed(readProcessOutput(p));
		
		printIfVerbose("testRuntimeInfo", output);
		
		String expectedOutput = ((HashMap)outputs.get(Locale.getDefault())).get("RuntimeInfoWithActiveConn").toString();
		assertEquals("Output doesn't match", expectedOutput, output);
	}
	
	/**
	 * Test runtimeinfo w/ foreign (non-English) locale
	 */
	public void x_testRuntimeInfoLocale() throws Exception {      
		Connection conn1 = getConnection();
		// Now get a couple of connections with some prepared statements
		Connection conn2 = openDefaultConnection();
		PreparedStatement ps = prepareAndExecuteQuery(conn1,
				"SELECT count(*) from sys.systables");
		PreparedStatement ps2 = prepareAndExecuteQuery(conn1, "VALUES(1)");

		Connection conn3 = openDefaultConnection();
		PreparedStatement ps3 = prepareAndExecuteQuery(conn2,
				"SELECT count(*) from sys.systables");
		PreparedStatement ps4 = prepareAndExecuteQuery(conn2, "VALUES(2)");

		Process p = execJavaCmd(RuntimeinfoLocaleCmd);
		String output = sed(readProcessOutput(p));
		
		printIfVerbose("testRuntimeInfoLocale", output);
		
		int matched = 0;
		String matchString = "\tSYSLH0001\tSELECT count(*) from sys.systables\n	SYSLH0002\tVALUES(1)\n";
		String invertedMatchString = "\tSYSLH0002\tVALUES(1)\n\tSYSLH0001\tSELECT count(*) from sys.systables\n";
		
		/* The IF clause accomodates for the different order that the result may have */ 
		matched = output.indexOf(matchString);
		if (matched == -1) { /* The order was incorrect, try the other one */
			matched = output.indexOf(invertedMatchString);
			assertTrue(matched != -1);
		}
		
		matched = 0;
		matchString = "\tSYSLH0001\tSELECT count(*) from sys.systables\n	SYSLH0002\tVALUES(2)\n";
		invertedMatchString = "\tSYSLH0002\tVALUES(2)\n\tSYSLH0001\tSELECT count(*) from sys.systables\n";
		
		/* Same as above, but with VALUES(2) */ 
		matched = output.indexOf(matchString);
		if (matched == -1) { /* The order was incorrect, try the other one */
			matched = output.indexOf(invertedMatchString);
			assertTrue(matched != -1);
		}
		
		/* Match the empty session */
		matched = 0;
		matchString = ((HashMap)outputs.get(Locale.getDefault())).get("RuntimeInfoLocaleString").toString();
		
		assertTrue(output.indexOf(matchString) != -1);

		ps.close();
		ps2.close();
		ps3.close();
		ps4.close();
		conn1.close();
		conn2.close();
		conn3.close();

	}

	/**
	 * once more after closing the connections 
	 * - by calling NetworkServerControl.getRuntimeInfo 
	 * @throws Exception
	 */
	public void x_testRuntimeInfoAfterConnClose() throws Exception {
		// give the network server a second to clean up (DERBY-1455)
		Thread.sleep(1000);

		String s = NetworkServerTestSetup
					.getNetworkServerControl(TestConfiguration.getCurrent().getPort())
					.getRuntimeInfo();
		s = sed(s);
		NetworkServerTestSetup.getNetworkServerControl().shutdown();
		
		printIfVerbose("testRuntimeInfoMethod", s);
		
		String expectedOutput = ((HashMap)outputs.get(Locale.getDefault())).get("RuntimeInfoAfterConnClose").toString();
		assertEquals("Output doesn't match", expectedOutput, s);
	}

    /**
     * Regression test case for DERBY-6456, which caused an infinite loop if
     * the runtimeinfo output was more than 32KB.
     */
    public void testRuntimeInfoWithLongValues() throws Exception {
        // First open many connections on the server, so that the reply from
        // getRuntimeInfo() will be long.
        for (int i = 0; i < 200; i++) {
            prepareAndExecuteQuery(openDefaultConnection(),
                "VALUES 'Hello, World! How are you today?',\n"
              + "'Not that bad today, actually. Thanks for asking.'\n"
              + "-- Let's add some more text to increase the output length.\n"
              + "-- And even more here... The statement text, including this\n"
              + "-- comment, will be included in the runtimeinfo output.\n");
        }

        // This call used to hang.
        String runtimeinfo =
            NetworkServerTestSetup.getNetworkServerControl().getRuntimeInfo();

        // For debugging:
        println(runtimeinfo);

        // Output gets truncated to 65535 bytes (DERBY-5220).
        assertEquals(65535, runtimeinfo.length());
    }

	public static PreparedStatement prepareAndExecuteQuery(Connection conn,
			String sql) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		ResultSet rs = ps.executeQuery();
		rs.next();
		return ps;
	}
	
	/**
     * Replace memory values in the output string
     * Removes output tags for STDOUT and STDERR from readProcessOutput
     * Also trims the string to make it easier to compare.
     * 
     * @param s the string to remove lines from
     * @return the string with the lines removed
     */
    private String sed(String s) {
    	String searchString = ((HashMap)outputs.get(Locale.getDefault())).get("sedMemorySearch").toString();
    	String replaceString = ((HashMap)outputs.get(Locale.getDefault())).get("sedMemoryReplace").toString();
		s = s.replaceAll(searchString, replaceString);
		s = s.replaceAll(stdout_err_tags, "");
		s = s.trim();
		return s;
    }
    
    /**
     * Prints strings to System.out to make it easier to update the tests
     * when the output changes. BaseTestCase.println() only prints when on VERBOSE
     * 
     * @param name just a label to identify the string
     * @param s the string to be printed
     */
    private void printIfVerbose(String name,String s) {
		println("\n\n>>>" + name + ">>>");
		println(s);
		println("<<<" + name + "<<<\n\n");
    }

	/**
	 * Decorate a test with SecurityManagerSetup, clientServersuite, and
	 * SupportFilesSetup.
	 * 
	 * @return the decorated test
	 */
    private static Test decorateTest(Locale serverLocale, String testName) {
        Test test = new RuntimeInfoTest(testName);

        test = TestConfiguration.clientServerDecorator(test);
        
        /* A single use database must be used to ensure the consistent output.
         * The output would change whether the test was being ran for the first
         * or subsequent times. */
        test = TestConfiguration.singleUseDatabaseDecorator(test);
        test = new LocaleTestSetup(test, serverLocale);
        // Install a security manager using the initial policy file.
        return new SecurityManagerSetup(test, POLICY_FILE_NAME);
    }
	
	private static final HashMap<Locale, HashMap<String, String>> outputs;
	static {
		HashMap<String, String> englishOutputs = new HashMap<String, String>();
		englishOutputs.put("RuntimeInfoWithActiveConn",
				"--- Derby Network Server Runtime Information ---\n" + 
				"---------- Session Information ---------------\n" + 
				"Session # :2\n" + 
				"\n" + 
				"\n" + 
				"-------------------------------------------------------------\n" + 
				"# Connection Threads : 1\n" + 
				"# Active Sessions : 1\n" + 
				"# Waiting  Sessions : 0\n" + 
				"\n" + 
				"Total Memory : #####	Free Memory : #####");
		englishOutputs.put("RuntimeInfoAfterConnClose", 
				"--- Derby Network Server Runtime Information ---\n" + 
				"---------- Session Information ---------------\n" + 
				"Session # :8\n" + 
				"\n" + 
				"\n" + 
				"-------------------------------------------------------------\n" + 
				"# Connection Threads : 4\n" + 
				"# Active Sessions : 1\n" + 
				"# Waiting  Sessions : 0\n" + 
				"\n" + 
				"Total Memory : #####	Free Memory : #####");
		englishOutputs.put("sedMemorySearch", "(?m)Memory : [0-9]*");
		englishOutputs.put("sedMemoryReplace", "Memory : #####");
		englishOutputs.put("RuntimeInfoLocaleString", "\tStmt ID\t\tSQLText\n\t-------------\t-----------\n\n\n\nSession");
		
		HashMap<String, String> germanOutputs = new HashMap<String, String>();
		germanOutputs.put("RuntimeInfoWithActiveConn",
				"--- Laufzeitinformationen zu Derby Network Server ---\n" + 
				"---------- Sessioninformationen ---------------\n" + 
				"Sessionnummer:2\n" + 
				"\n" + 
				"\n" + 
				"-------------------------------------------------------------\n" + 
				"Anzahl Verbindungsthreads: 1\n" + 
				"Anzahl aktive Sessions: 1\n" + 
				"Anzahl wartende Sessions: 0\n" + 
				"\n" + 
				"Speicher gesamt: #####	Freier Speicher: #####");
		germanOutputs.put("RuntimeInfoAfterConnClose", 
				"--- Laufzeitinformationen zu Derby Network Server ---\n" + 
				"---------- Sessioninformationen ---------------\n" + 
				"Sessionnummer:8\n" + 
				"\n" + 
				"\n" + 
				"-------------------------------------------------------------\n" + 
				"Anzahl Verbindungsthreads: 4\n" + 
				"Anzahl aktive Sessions: 1\n" + 
				"Anzahl wartende Sessions: 0\n" + 
				"\n" + 
				"Speicher gesamt: #####	Freier Speicher: #####");
		germanOutputs.put("sedMemorySearch", "Speicher gesamt: [0-9]*	Freier Speicher: [0-9]*");
		germanOutputs.put("sedMemoryReplace", "Speicher gesamt: #####	Freier Speicher: #####");
		germanOutputs.put("RuntimeInfoLocaleString", "\tAnwsg-ID\t\tSQL-Text\n\t-------------\t-----------\n\n\n\nSessionnummer");
		
		outputs = new HashMap<Locale, HashMap<String, String>>();
		outputs.put(englishLocale, englishOutputs);
		outputs.put(germanLocale, germanOutputs);
	}
}
