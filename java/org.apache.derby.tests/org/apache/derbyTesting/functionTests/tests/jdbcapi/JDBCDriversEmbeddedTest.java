/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.JDBCDriversEmbeddedTest

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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;

/**
 * Test autoloading with the embedded driver in jdbc.drivers.
 * Note this test is intended to be run in its own JVM, see
 * the parent class for details.
 *
 */
public class JDBCDriversEmbeddedTest extends JDBCDriversPropertyTest {
    
    public static Test suite() throws Exception
    {
        // Also test that the network server comes up automatically
        // when this property is set and the embedded driver
        // is autoloaded by jdbc.drivers
        System.setProperty("derby.drda.startNetworkServer", "true");
        // if the property derby.tests.basePort has been set, we need to
        // ensure we specify the portNumber as well, because we're then
        // not using 1527 for checks and shutting down.
        String portNumberString = System.getProperty("derby.tests.basePort");
        if (portNumberString != null )
        {
            System.setProperty("derby.drda.portNumber", portNumberString);
        } 
        
        return getSuite("org.apache.derby.jdbc.EmbeddedDriver");
    }
}
