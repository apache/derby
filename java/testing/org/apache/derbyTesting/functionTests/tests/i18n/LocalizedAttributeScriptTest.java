/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.i18n.LocalizedAttributeScriptTest
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

package org.apache.derbyTesting.functionTests.tests.i18n;

import java.io.File;
import java.security.AccessController;
import java.util.Properties;

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * LocalizedAttributeScriptTest runs the ij scripts (.sql file) 
 * LocalizedConnectionAttribute.sql and compares the output to a canon file in
 * the standard master package.
 * <BR>
 * Its suite() method returns the test as an instance of
 * this class for the specific script wrapped in a decorator that sets the
 * specific encoding properties, surrounded by a clean database decorator.
 * <BR>
 * It can also be used as a command line program
 *
 */
public final class LocalizedAttributeScriptTest extends ScriptTestCase {
    
    /**
     * Run LocalizedConnectionAttribute.sql 
     * <code>
     * example
     * java org.apache.derbyTesting.functionTests.tests.i18n.LocalizedAttributeScriptTest
     * </code>
     */
    public static void main()
    {
        junit.textui.TestRunner.run(getSuite());
    }

    /**
     * Return the suite that runs the Localized script.
     */
    public static Test suite() {

        TestSuite suite = new TestSuite("LocalizedScripts");

        // Note that it doesn't really make sense to run with network 
        // server/client, as LocalizedConnectionAttributes.sql has a hardcoded
        // connect with protocol, so we can test connect 'as' with a us-ascii
        // character. So only run with embedded. 
        // Similarly, the script cannot run with JSR169, because the connect
        // statement automatically invokes DriverManager.
        if (JDBC.vmSupportsJSR169())
            return suite;
        TestSuite localizedTests = new TestSuite("LocalizedScripts:embedded");
        localizedTests.addTest(getSuite());
        Test local = TestConfiguration.singleUseDatabaseDecoratorNoShutdown(
            localizedTests);

        // add those client tests into the top-level suite.
        suite.addTest(local);

        return suite;
    }

    /*
     * A single JUnit test that runs a single Localized script, specifying the
     * desired input encoding.
     * Needs ISO-8859-1 encoding, or we get a syntax error on connecting to
     * the database with the u-umlaut in the 'as' clause.
     */
    private LocalizedAttributeScriptTest(String localizedTest){
        super(localizedTest, "ISO-8859-1");
    }

    /**
     * Return a suite of localized tests based on the 
     * script name. The test is surrounded in a decorator
     * that sets localization properties wrapped in a decorator
     * that cleans the database.
     */
    private static Test getSuite() {
        TestSuite suite = new TestSuite("localized scripts");
        Properties uiProps = new Properties();

        uiProps.put("derby.ui.locale","de_DE");
        uiProps.put("derby.ui.codeset","ISO-8859-1");
        suite.addTest(new SystemPropertyTestSetup(
                new LocalizedAttributeScriptTest("LocalizedConnectionAttribute"), uiProps));
        
        return getIJConfig(suite);
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of the extra database.
        // this also will get done if there are failures, and the database will
        // not be saved in the 'fail' directory.
        // We can't rely on an additionalDatabaseDecorator because 'detest'
        // is not just a logical, but a physical db name.
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                    removeDatabase("detest");
                return null;
            }
            
            void removeDatabase(String dbName)
            {
                //TestConfiguration config = TestConfiguration.getCurrent();
                dbName = dbName.replace('/', File.separatorChar);
                String dsh = getSystemProperty("derby.system.home");
                if (dsh == null) {
                    fail("not implemented");
                } else {
                    dbName = dsh + File.separator + dbName;
                }
                removeDirectory(dbName);
            }

            void removeDirectory(String path)
            {
                final File dir = new File(path);
                removeDir(dir);
            }

            private void removeDir(File dir) {
                
                // Check if anything to do!
                // Database may not have been created.
                if (!dir.exists())
                    return;

                String[] list = dir.list();

                // Some JVMs return null for File.list() when the
                // directory is empty.
                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        File entry = new File(dir, list[i]);

                        if (entry.isDirectory()) {
                            removeDir(entry);
                        } else {
                            entry.delete();
                            //assertTrue(entry.getPath(), entry.delete());
                        }
                    }
                }
                dir.delete();
                //assertTrue(dir.getPath(), dir.delete());
            }
        });
        LocalizedResource.resetLocalizedResourceCache();
        super.tearDown();
    }    
    
    /**
     * Set up the test environment.
     */
    protected void setUp() {
        LocalizedResource.resetLocalizedResourceCache();
    }
}