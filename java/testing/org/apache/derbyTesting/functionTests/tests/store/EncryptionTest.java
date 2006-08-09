/*
 
 Derby - Class org.apache.derbyTesting.functionTests.tests.store.EncryptionTest
 
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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.DriverManager;
import java.util.Properties;
import java.io.*;

/**
 * check if bootpassword is not written out in plain text into service.properties
 * for an encrypted database run within the test harness.
 * In future encryption related testcases can be added to this test
 */
public class EncryptionTest {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            // use the ij utility to read the property file and
            // make the initial connection.
            org.apache.derby.tools.ij.getPropertyArg(args);
            conn = org.apache.derby.tools.ij.startJBMS();

            // Test 1
            // Derby 236 - boot password should not be written out
            // into service.properties
            String derbyHome = System.getProperty("derby.system.home");

            // read in the properties in the service.properties file of the db
            Properties serviceProperties = new Properties();
            File f = new File(derbyHome + "/wombat/service.properties");
            serviceProperties.load(new FileInputStream(f.getAbsolutePath()));
            if (serviceProperties.getProperty("bootPassword") == null)
                report("TEST PASSED");
            else
                report("FAIL -- bootPassword should not be written out into service.properties");
            
            conn.close();
        } catch (Throwable e) {
            report("FAIL -- unexpected exception: " + e);
            e.printStackTrace();
        }

    }

    /**
     * print message
     * @param msg to print out 
     */
    public static void report(String msg) {
        System.out.println(msg);
    }

}
