/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.DriverUnloader

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

package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;

/**
 * Helper class used by the upgrade tests to unload JDBC drivers loaded in
 * separate class loaders. This class must live in the same class loader as
 * the drivers it attempts to unload.
 */
public class DriverUnloader {
    /**
     * Deregister all Derby drivers accessible from the class loader in which
     * this class lives.
     *
     * @return {@code true} if a driver was unloaded, {@code false} otherwise
     */
    public static boolean unload() throws SQLException {
        boolean ret = false;
        Enumeration e = DriverManager.getDrivers();
        while (e.hasMoreElements()) {
            Driver driver = (Driver) e.nextElement();
            if (driver.getClass().getName().startsWith("org.apache.derby.")) {
                DriverManager.deregisterDriver(driver);
                ret = true;
            }
        }
        return ret;
    }
}
