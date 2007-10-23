/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.SynonymTest
 *  
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

/**
 * Synonym testing using junit
 */
public class SynonymTest extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public SynonymTest(String name) {
        super(name);
    }

    /**
     * Create a suite of tests.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite(SynonymTest.class, "SynonymTest");
        return suite;
    }

    /**
     * The test makes sure that we correctly throw dependency exception when
     * user requests to drop a synonym which has dependent objects on it. Once
     * the dependency is removed, we should be able to drop the synonym.
     * @throws SQLException
     */
    public void testViewDependency() throws SQLException {
        Statement stmt = createStatement();  
        stmt.executeUpdate("create synonym mySyn for sys.systables");
        stmt.executeUpdate("create view v1 as select * from mySyn");
        stmt.executeUpdate("create view v2 as select * from v1");
        // Drop synonym should fail since it is used in two views.
        assertStatementError("X0Y23", stmt, "drop synonym mySyn");
        stmt.executeUpdate("drop view v2");
        // fails still because of view v1's dependency
        assertStatementError("X0Y23", stmt, "drop synonym mySyn");
        stmt.executeUpdate("drop view v1");
        stmt.executeUpdate("drop synonym mySyn");
        stmt.close();
    }
}
