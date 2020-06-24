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
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCPerfTestCase;

/**
 * Load the OE schema
 */
public class Schema extends JDBCPerfTestCase {

    private String scriptBase;

    /**
     * Create a test case with the given name.
     * 
     * @param name
     *            of the test case.
     */
    public Schema(String name) {
        super("testScript");
        scriptBase = name;
    }

    /**
     * junit tests to create schema
     * 
     * @return the tests to run
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("Order Entry- Schema");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        // Create Schema
//IC see: https://issues.apache.org/jira/browse/DERBY-2094
        addBaseSchema(suite);
        addConstraints(suite);
        return suite;
    }
    
    public static void addBaseSchema(BaseTestSuite suite) {
        // Create Schema
        suite.addTest(new Schema("schema.sql"));
        suite.addTest(new Schema("dataroutines.sql"));
//IC see: https://issues.apache.org/jira/browse/DERBY-2094
        suite.addTest(new Schema("delivery.sql"));
    }

    /**
     * Add constraint tests to suite.
     * 
     * @param suite
     */
    static void addConstraints(BaseTestSuite suite) {
        suite.addTest(new Schema("primarykey.sql"));
        suite.addTest(new Schema("foreignkey.sql"));
        suite.addTest(new Schema("index.sql"));

    }

    /**
     * Run a Order Entry script.
     */
    public void testScript() throws UnsupportedEncodingException,
    SQLException, PrivilegedActionException, IOException {

        String script = "org/apache/derbyTesting/system/oe/schema/" + scriptBase;
        int errorCount = runScript(script, "US-ASCII");
        assertEquals("Errors in script ", 0, errorCount);
    }
    
    public String getName() {
        return scriptBase;
    }
}
