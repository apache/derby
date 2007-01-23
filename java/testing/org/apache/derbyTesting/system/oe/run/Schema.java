/*
 * 
 * Derby - Class org.apache.derbyTesting.system.oe.run.Schema
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.derbyTesting.system.oe.run;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.PrivilegedActionException;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Load the OE schema
 */
public class Schema extends JDBCPerfTestCase {


    /**
     * Create a test case with the given name.
     * 
     * @param name
     *            of the test case.
     */
    public Schema(String name) {
        super(name);
    }

    /**
     * junit tests to create schema
     * 
     * @return the tests to run
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("Order Entry- Schema");

        // Create Schema
        suite.addTest(new Schema("testSchema"));
        addConstraints(suite);
        return suite;
    }

    /**
     * Add constraint tests to suite.
     * 
     * @param suite
     */
    static void addConstraints(TestSuite suite) {
        suite.addTest(new Schema("testPrimaryKey"));
        suite.addTest(new Schema("testForeignKey"));
        suite.addTest(new Schema("testIndex"));

    }

    /**
     * Test setting up the base tables.
     */
    public void testSchema() throws UnsupportedEncodingException, SQLException,
    PrivilegedActionException, IOException {
        script("schema.sql");
    }

    /**
     * Test setting up the primary keys.
     */
    public void testPrimaryKey() throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {
        script("primarykey.sql");
    }

    /**
     * Test setting up the foreign keys.
     */
    public void testForeignKey() throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {
        script("foreignkey.sql");
    }

    /**
     * Test setting up the remaining indexes.
     */
    public void testIndex() throws UnsupportedEncodingException, SQLException,
    PrivilegedActionException, IOException {
        script("index.sql");
    }

    /**
     * Run a Order Entry script.
     */
    private void script(String name) throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {

        String script = "org/apache/derbyTesting/system/oe/schema/" + name;
        int errorCount = runScript(script, "US-ASCII");
        assertEquals("Errors in script ", 0, errorCount);
    }
}
