/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ij2Test

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.util.Locale;
import java.util.Properties;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.LocaleTestSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ij2Test extends ScriptTestCase {

    public ij2Test(String script) {
        super(script, true);
    }
    
    public static Test suite() {        
        Properties props = new Properties();

        props.setProperty("derby.infolog.append", "true");  
        props.setProperty("ij.protocol", "jdbc:derby:");
        props.setProperty("ij.database", "wombat;create=true");

        Test test = new SystemPropertyTestSetup(new ij2Test("ij2"), props);
        test = new LocaleTestSetup(test, Locale.ENGLISH);   
        test = TestConfiguration.singleUseDatabaseDecorator(test, "wombat1");
        test = new CleanDatabaseTestSetup(test);

        BaseTestSuite suite = new BaseTestSuite("ij2Scripts");
        suite.addTest(test);

        if (JDBC.vmSupportsJDBC3()) {
            props.setProperty("ij.protocol", "jdbc:derby:");
            props.setProperty("ij.showNoConnectionsAtStart", "true");
            
            Test testb = new SystemPropertyTestSetup(new ij2Test("ij2b"), props);
            testb = new LocaleTestSetup(testb, Locale.ENGLISH);   
            testb = TestConfiguration.singleUseDatabaseDecorator(testb, "wombat1");
            testb = new CleanDatabaseTestSetup(testb);
            suite.addTest(testb);
        }
        return getIJConfig(suite); 
    }   
}
