/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.nist.NistScripts
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
package org.apache.derbyTesting.functionTests.tests.nist;

import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Run the NIST scripts as a single suite.
 *
 */
public final class NistScripts extends ScriptTestCase {

    /**
     * The complete set of NIST scripts for Derby.
     * First element of array is the script name,
     * second element is the user name for the connection.
     */  
    private static final String[][] TESTS = {
    {"schema1", "HU"},
    { "basetab", "HU"},
    { "schema5", "FLATER"},
    { "schema8", "SUN"},
    { "temp_schem10", "HU"},
    { "temp_cts5sch2", "HU"},
    { "temp_cts5tab", "CTS1"},
    { "flattab", "FLATER"},
    { "dml012", "HU"},
    { "dml013", "HU"},
    { "dml018", "HU"},
    { "dml022", "HU"},
    {"dml025", "HU"},
    {"dml044", "HU"},
    {"dml045", "HU"},
    {"dml051", "HU"},
    {"dml059", "HU"},
    {"dml061", "HU"},
    {"dml073", "HU"},
    {"dml087", "FLATER"},
    {"dml090", "HU"},
    {"dml106", "FLATER"},
    {"dml108", "FLATER"},
    {"dml114", "FLATER"},
    {"dml141", "FLATER"},
    {"dml144", "FLATER"},
    {"dml162", "FLATER"},
    {"dml177", "FLATER"},
    {"dml010", "HU"},
    {"dml015", "HU"},
    {"dml020", "HU"},
    {"dml037", "HU"},
    {"dml038", "HU"},
    {"dml042", "HU"},
    {"dml043", "HU"},
    {"dml047", "HU"},
    {"dml056", "HU"},
    {"dml065", "HU"},
    {"dml076", "HU"},
    {"sdl012", "HU"},
    {"dml029", "HU"},
    {"yts796", "CTS1"}, 
    {"dml075", "HU"},
    {"dml024", "HU"},
    {"dml070", "HU"},
    {"dml147", "FLATER"},
    {"dml009", "HU"},
    {"dml008", "HU"},
    {"dml014", "HU"},
    {"dml016", "SULLIVAN1"},
    {"dml021", "HU"},
    {"dml034", "HU"},
    {"dml023", "HU"},
    {"dml026", "HU"},
    {"dml033", "HU"},
    {"dml039", "HU"},
    {"dml050", "HU"},
    {"dml052", "HU"},
    {"dml053", "HU"},
    {"dml055", "HU"},
    {"dml057", "HU"},
    {"dml058", "HU"},
    {"dml155", "FLATER"},
    {"xts729", "CTS1"},
    {"xts730", "CTS1"},
    {"yts797", "CTS1"},
    {"yts798", "CTS1"},
    {"dml069", "HU"},
    {"dml080", "SCHANZLE"},
    {"dml081", "SCHANZLE"},
    {"dml083", "SCHANZLE"},
    {"dml085", "SCHANZLE"},
    {"dml132", "FLATER"},
    {"dml099", "FLATER"},
    {"dml049", "HU"},
    {"dml173", "FLATER"},
    {"dml174", "FLATER"},
    {"dml179", "FLATER"},
    {"yts812", "CTS1"},
    {"dml001", "HU"},
    {"dml004", "HU"},
    {"dml035", "HU"},
    {"dml046", "HU"},
    {"dml060", "HU"},
    {"dml068", "HU"},
    {"yts799", "CTS1"},
    {"dml001", "HU"},
    {"dml079", "HU"},
    {"dml165", "FLATER"},
    {"dml104", "FLATER"},
    {"dml112", "FLATER"},
    {"dml148", "FLATER"},
    {"dml019", "HU"},
    {"dml149", "FLATER"},
    {"dml168", "FLATER"},
    {"dml170", "FLATER"},
    {"xts752", "CTS1"},
    {"xts753", "CTS1"},
    {"cdr002", "SUN"},
    {"cdr003", "SUN"},
    {"cdr004", "SUN"},
    {"cdr005", "SUN"},
    {"cdr006", "SUN"},
    {"cdr007", "SUN"},
    {"cdr027", "SUN"},
    {"cdr030", "SUN"},
    {"dml134", "FLATER"},
    {"dml005", "HU"},
    {"dml011", "HU"},
    {"dml027", "HU"},
    {"dml082", "SCHANZLE"},
    {"dml091", "SCHANZLE"},
    {"dml119", "FLATER"},
    {"dml130", "FLATER"},
    {"dml158", "HU"},
    {"dml178", "FLATER"},
    {"dml181", "FLATER"},
    {"dml182", "FLATER"},
    {"xts701", "CTS1"},
    {"xts731", "CTS1"},
    {"xts740", "CTS1"},
    {"xts742", "CTS1"},
    {"xts760", "CTS1"},
    {"yts811", "CTS1"},
    {"dml160", "FLATER"},
    {"schema4", "SULLIVAN1"} 
    };

	/**
	 * Return the suite that runs the NIST SQL scripts.
	 */
	public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("NIST");
        
        String suiteUser = null;
        BaseTestSuite userSuite = null;
        for (int i = 0; i < TESTS.length; i++) {
            
            String testScript = TESTS[i][0];
            String testUser = TESTS[i][1];
            
            Test test = new NistScripts(testScript);
            
            if (testUser.equals(suiteUser))
            {
                userSuite.addTest(test);
                continue;
            }
            
            // Add the new user suite with the change user decorator to
            // the main suite but continue to add tests to the user suite.
            userSuite = new BaseTestSuite("NIST user="+testUser);
            String password = testUser.concat("ni8s4T");
            suite.addTest(
                    TestConfiguration.changeUserDecorator(userSuite, testUser, password));
            suiteUser = testUser;
            
            userSuite.addTest(test);
        }
        
        Test test = getIJConfig(suite);
        
        // Setup user authentication
        test = DatabasePropertyTestSetup.builtinAuthentication(test,
                new String[] {"APP", "HU","FLATER","SUN","CTS1","SULLIVAN1","SCHANZLE"},
                "ni8s4T");
        
        // Lock timeout settings that were set for the old harness when
        // running nist.
        test = DatabasePropertyTestSetup.setLockTimeouts(test, 2, 4);
        
        return new CleanDatabaseTestSetup(test);
    }
    
	/*
	 * A single JUnit test that runs a single Nist SQL script.
	 */
	private NistScripts(String nistScript){
		super(nistScript);
	}
}
