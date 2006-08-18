/*
 
   Derby - Class org.apache.derbyTesting.functionTests.test.perf.StartupTest
 
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
   under the License.
 */

package org.apache.derbyTesting.functionTests.tests.perf;

import org.apache.derbyTesting.functionTests.util.JarUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;

import java.sql.*;

/**
 * This test test the timing of starting up Derby.  It tries to divide the
 * total time up into reasonable chunks.  It's written as a JUnit test but
 * really can't be automated because the timings are so dependent upon
 * the exact hardware, operating system and software environment the test
 * is running in.  I just use JUnit because of the convenient framework
 * it gives me...
 */
public class StartupExistingDBTest extends BaseJDBCTestCase {
    public StartupExistingDBTest(String name) {
        super(name);
    }
    
    
    
    public void testExistingDB() throws Exception {
        JarUtil.unjar("existingDb.jar", null);
        
        long startTime = System.currentTimeMillis();
        System.out.println("Testing startup with an EXISTING database... " +
            "All measurements are in milliseconds.");
        
        // Load the driver
        Class driver = Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        long currentTime = System.currentTimeMillis();
        System.out.println("Loading driver:  " + (currentTime - startTime));

        // Use an existing DB.  This is copied over by the harness
        startTime = System.currentTimeMillis();
        Connection conn = 
            DriverManager.getConnection("jdbc:derby:../existingDb");
        currentTime = System.currentTimeMillis();
        System.out.println("Open connection with existing database:  " 
            + (currentTime - startTime));

        // Create a table
        startTime = System.currentTimeMillis();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE test_table(id integer primary key, " +
            "last_name varchar(80), first_name varchar(80), " +
            "mi char(1), address varchar(100), city varchar(80))");
        currentTime = System.currentTimeMillis();
        System.out.println("Creating a table:  " 
            + (currentTime - startTime));
    }
}
