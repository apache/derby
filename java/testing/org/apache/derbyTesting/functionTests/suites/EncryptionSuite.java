/*

   Derby - Class org.apache.derbyTesting.functionTests.suites.EncryptionSuite

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.suites;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.derbyTesting.functionTests.tests.store.AccessTest;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * A suite that runs a set of tests using encrypted
 * databases with a number of algorithms.
 * This is a general encryption test to see if
 * tests run without any problems when encryption
 * is enabled.
 * <BR>
 * It is not for testing of encryption functionality,
 * e.g. testing that bootPassword must be a certain
 * length etc. That should be in a specific JUnit test
 * that probably needs to control database creation
 * more carefully than this.
 * <BR>
 * The same set of tests is run for each algorithm,
 * and each algorithm (obviously) uses a single
 * use database with the required encryption setup.
 * 
 * @see Decorator#encryptedDatabase(Test)
 * @see Decorator#encryptedDatabase(Test, String)
 *
 */
public final class EncryptionSuite extends BaseJDBCTestCase {
    

    public EncryptionSuite(String name) {
        super(name);
    }
    
    /**
     * Runs tests with a set of encryption algorithms.
     * The set comes from the set of algorithms used
     * for the same purpose in the old harness.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("Encrpytion Suite");
        
        // Encryption only supported for Derby in J2SE/J2EE environments.
        // J2ME (JSR169) does not support encryption.
        if (JDBC.vmSupportsJDBC3()) {
        
          suite.addTest(Decorator.encryptedDatabase(baseSuite("default")));
          suite.addTest(encryptedSuite("AES/CBC/NoPadding"));
          suite.addTest(encryptedSuite("DES/ECB/NoPadding"));
          suite.addTest(encryptedSuite("DESede/CFB/NoPadding"));
          suite.addTest(encryptedSuite("DES/CBC/NoPadding"));
          suite.addTest(encryptedSuite("Blowfish/CBC/NoPadding"));
          suite.addTest(encryptedSuite("AES/OFB/NoPadding"));
        }
        
        return suite;
    }
    
    private static Test encryptedSuite(String algorithm)
    {
        return Decorator.encryptedDatabase(baseSuite(algorithm), algorithm);
    }
    
    /**
     * Set of tests which are run for each encryption algorithm.
     */
    private static Test baseSuite(String algorithm)
    {
        TestSuite suite = new TestSuite("Encryption Algorithm: " + algorithm);
        
        // Very simple test to get the setup working while we have
        // no tests that were previously run under encryption converted.
        suite.addTestSuite(EncryptionSuite.class);
        
        Properties sysProps = new Properties();
        sysProps.put("derby.optimizer.optimizeJoinOrder", "false");
        sysProps.put("derby.optimizer.ruleBasedOptimization", "true");
        sysProps.put("derby.optimizer.noTimeout", "true");
        
        suite.addTestSuite(AccessTest.class);
        
        return suite;
    }
    
    protected void setUp() {
        
        try { 
                Connection conn = getConnection();
                Statement s = createStatement();

                s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                        + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME " +
                        "'org.apache.derbyTesting.functionTests.util.Formatters" +
                ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
                s.close();
                conn.close();

        } catch (SQLException se) {
            // ignore
        }
    }
    
    public void tearDown() throws Exception {
        Statement st = createStatement();
        super.tearDown();
        try {
            st.executeUpdate("DROP FUNCTION PADSTRING");
        } catch (SQLException e) {
            // never mind.
        }
    }
    
    /**
     * Very simple test that ensures we can get a connection to
     * the booted encrypted database.
     * @throws SQLException
     */
    public void testEncryptedDBConnection() throws SQLException
    {
        getConnection().close();
    }
    
    
}
