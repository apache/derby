/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.engine.ShutdownWithoutDeregisterPermissionTest

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

package org.apache.derbyTesting.functionTests.tests.engine;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that shutdown works even if derby.jar does not have permission to
 * deregister the JDBC driver. Regression test case for DERBY-6224.
 */
public class ShutdownWithoutDeregisterPermissionTest extends BaseJDBCTestCase {
    public ShutdownWithoutDeregisterPermissionTest(String name) {
        super(name);
    }

    public static Test suite() {
        return new SecurityManagerSetup(
                TestConfiguration.embeddedSuite(
                        ShutdownWithoutDeregisterPermissionTest.class),
                "org/apache/derbyTesting/functionTests/tests/engine/" +
                "noDeregisterPermission.policy");
    }

    public void testShutdownWithoutPermission() throws SQLException {
        // First get a connection to make sure the engine is booted.
        getConnection().close();

        // Shut down the engine. This used to fail with an
        // AccessControlException on Java 8 before DERBY-6224.
        TestConfiguration config = TestConfiguration.getCurrent();
        config.shutdownEngine();

        // Test whether shutdown deregistered the driver. On versions prior
        // to Java 8/JDBC 4.2, we expect the driver to be deregistered even
        // though the permission is missing, and the call to getDrivers()
        // should not return any instance of AutoloadedDriver.
        // On Java 8/JDBC 4.2 and higher, we expect AutoloadedDriver to
        // be in the list of registered drivers.

        Enumeration<Driver> drivers = DriverManager.getDrivers();
        Driver found = null;
        while (found == null && drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver.getClass().getName().startsWith(
                    "org.apache.derby.jdbc.AutoloadedDriver")) {
                found = driver;
            }
        }

        if (JDBC.vmSupportsJDBC42()) {
            assertNotNull("Expected driver to be registered", found);
        } else {
            assertNull("Expected driver to be deregistered", found);
        }
    }
}
