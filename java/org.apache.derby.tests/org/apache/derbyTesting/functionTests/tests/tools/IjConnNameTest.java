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

import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;




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
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = new BaseTestSuite("IjConnNameTest");
        
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        removeDirectory(
            TestConfiguration.getCurrent().getDatabasePath("lemming"));
        super.tearDown();
    }   
}
