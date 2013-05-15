/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.SSLTest

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
import java.sql.SQLException;
import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Derby;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBCDataSource;


/**
 * Tests connects to an SSL server
 */

public class SSLTest extends BaseJDBCTestCase
{
    // Constructors

    public SSLTest(String testName)
    {
        super(testName);
    }
    
    // JUnit machinery
    
    /**
     * Tests to run.
     */
    public static Test suite()
    {
        //NetworkServerTestSetup.setWaitTime(10000L);
        
        TestSuite suite = new TestSuite("SSLTest");
        
        // Server booting requires that we run from the jar files
        if (!TestConfiguration.loadingFromJars()) { return suite; }
        
        // Need derbynet.jar in the classpath!
        if (!Derby.hasServer())
            return suite;
        
        suite.addTest(decorateTest("testSSLBasicDSConnect"));
        suite.addTest(decorateTest("testSSLBasicDSPlainConnect"));
        return suite;
    }   

    // Test decoration
    
    /**
     * <p>
     * Compose the required decorators to bring up the server in the correct
     * configuration.
     * </p>
     */
    private static Test decorateTest(String testName)
    {
        SSLTest sslTest = 
            new SSLTest(testName);
        
        String[] startupProperties = 
            getStartupProperties();

        String[] startupArgs = new String[]{};
        
        NetworkServerTestSetup networkServerTestSetup =
            new NetworkServerTestSetup(sslTest,
                                       startupProperties,
                                       startupArgs,
                                       true);
        
        Test testSetup =
            SecurityManagerSetup.noSecurityManager(networkServerTestSetup);
        
        testSetup = 
            new SupportFilesSetup(testSetup,
                                  null,
                                  new String[] 
                                  {"functionTests/tests/derbynet/SSLTestServerKey.key"},
                                  null,
                                  new String[] 
                                  {"SSLTestServerKey.key"}
                                  );
        Test test = TestConfiguration.defaultServerDecorator(testSetup);

        test = TestConfiguration.changeSSLDecorator(test, "basic");

        return test;
    }
    
    /**
     * <p>
     * Return a set of startup properties suitable for SSLTest.
     * </p>
     */
    private static  String[]  getStartupProperties()
    {
        return new String[] {
            "javax.net.ssl.keyStore=extinout/SSLTestServerKey.key",
            "javax.net.ssl.keyStorePassword=qwerty",
        };
    }
    
    // JUnit Tests
    
    /**
     * Test that a basic SSL connect succeeds.
     **/

    public void testSSLBasicDSConnect()
        throws Exception
    {   
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds,"createDatabase","create");
        JDBCDataSource.setBeanProperty(ds,"ssl","basic");
        Connection c1 = ds.getConnection();
        c1.close();
    }

    /**
     * Test that a plaintext connect will fail.
     **/

    public void testSSLBasicDSPlainConnect()
        throws Exception
    {   
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds,"createDatabase","create");
        
        try {
            Connection c2 = ds.getConnection();
            c2.close();
            fail();
        } catch (SQLException e) {
            assertSQLState("08006", e);
        }
    }
}

