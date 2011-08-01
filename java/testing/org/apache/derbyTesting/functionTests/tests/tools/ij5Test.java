/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.ij5Test

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

import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;

public class ij5Test extends ScriptTestCase {

    public ij5Test(String script) {
        super(script, true);
    }   
    
    public static Test suite() {        
        Properties props = new Properties();
        
        props.setProperty("ij.showNoConnectionsAtStart", "false");
        props.setProperty("ij.showNoCountForSelect", "false");

        Test test = new SystemPropertyTestSetup(new ij5Test("ij5"), props);
        test = new CleanDatabaseTestSetup(test);   
        
        return getIJConfig(test); 
    }   
}
