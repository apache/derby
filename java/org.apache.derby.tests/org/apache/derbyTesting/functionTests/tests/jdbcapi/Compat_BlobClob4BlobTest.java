/*
 *
 * Derby - Class Compat_BlobClob4BlobTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test of JDBC blob and clob
 * Create a test suite running BlobClob4BlobTest, but in a server-client setting 
 * where the server has already been started.
 */

public class Compat_BlobClob4BlobTest extends BlobClob4BlobTest {

    /** Creates a new instance of Compat_BlobClob4BlobTest */
    public Compat_BlobClob4BlobTest(String name) {
        super(name);
    }

    /**
     * Set up the connection to the database.
     */
        
    public void setUp() throws  Exception { // IS NEVER RUN!
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /***                TESTS               ***/


    /**
     * Run the tests of BlobClob4BlobTest in server-client on an already started server.
     *
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("Compat_BlobClob4BlobTest");
        /* Embedded is not relevant for a running server....
         suite.addTest(
                TestConfiguration.embeddedSuite(BlobClob4BlobTest.class)); */
        suite.addTest(
                TestConfiguration.defaultExistingServerSuite(BlobClob4BlobTest.class, false));
                
        return (Test)suite; // Avoiding CleanDatabaseTestSetup and setLockTimeouts which both use embedded.


    }


}
