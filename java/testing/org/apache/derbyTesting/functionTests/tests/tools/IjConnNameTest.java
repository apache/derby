/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.IjConnNameTest

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
package org.apache.derbyTesting.functionTests.tests.tools;

import java.io.File;

import java.security.AccessController;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;


import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;




/**
 * Test case for ijConnName.sql. 
 *
 */
public class IjConnNameTest extends ScriptTestCase {

    private static String test_script = "ijConnName";

    public IjConnNameTest(String name) {
        super(name, true);
    }    

    public static Test suite() {
        TestSuite suite = new TestSuite("IjConnNameTest");
        
        // Test does not run on J2ME
        if (JDBC.vmSupportsJSR169())
            return suite;
        
        Properties props = new Properties();
        
        props.setProperty("ij.connection.connOne", "jdbc:derby:wombat;create=true");
        props.setProperty("ij.connection.connFour", "jdbc:derby:nevercreated");     
        
        props.setProperty("ij.showNoConnectionsAtStart", "true");
        props.setProperty("ij.showNoCountForSelect", "true");
        
        Test test = new SystemPropertyTestSetup(new IjConnNameTest(test_script), props);
        //test = SecurityManagerSetup.noSecurityManager(test);
        test = new CleanDatabaseTestSetup(test);   
        
        return getIJConfig(test); 
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of the extra database.
        // this also will get done if there are failures, and the database will
        // not be saved in the 'fail' directory.
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                    removeDatabase("lemming" );
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
        super.tearDown();
    }   
}
