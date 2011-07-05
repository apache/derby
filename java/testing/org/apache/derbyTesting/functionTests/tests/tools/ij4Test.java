/*

Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ij4Test

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
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

public class ij4Test extends ScriptTestCase {

    public ij4Test(String script) {
        super(script, true);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("ij4Test");
        
        // Test does not run on J2ME
        if (JDBC.vmSupportsJSR169())
            return suite;
        
        Properties props = createSystemProperties();       
        
        Test test = new SystemPropertyTestSetup(new ij4Test("ij4"), props);
        test = SecurityManagerSetup.noSecurityManager(test);
        test = new CleanDatabaseTestSetup(test);   
        
        return getIJConfig(test); 
    }

    private static Properties createSystemProperties() {
        Properties props = new Properties();
        
        props.setProperty("ij.showNoConnectionsAtStart", "true");        
        props.setProperty("ij.showNoCountForSelect", "true");
        
        return props;        
    }
}
