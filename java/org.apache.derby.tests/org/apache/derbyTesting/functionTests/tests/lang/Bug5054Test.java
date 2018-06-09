/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Bug5054Test
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Where current of cursorname and case sensitivity
 */
public class Bug5054Test extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Bug5054Test(String name) {
        super(name);
    }

    /**
     * Sets the auto commit to false.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    /**
     * Returns the implemented tests.
     * 
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        BaseTestSuite suite = new BaseTestSuite("Bug5054Test");
        Test test = new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(Bug5054Test.class)) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt.executeUpdate("CREATE TABLE T1 (a integer, b integer)");
                stmt.executeUpdate("INSERT INTO T1 VALUES(1, 1)");
                stmt.executeUpdate("INSERT INTO T1 VALUES(2, 2)");
            }
        };
        suite.addTest(test);
        suite.addTest(TestConfiguration.clientServerDecorator(test));
        return suite;
    }

    /**
     * 
     * Test fix of use of delimited cursor name in DRDA.
     * @throws SQLException
     */
    public void testBugBug5054() throws SQLException {
        Statement stmt1;
        Statement stmt2;
        Statement stmt3;
        ResultSet rs;

        stmt1 = createStatement();
        stmt1.setCursorName("aBc");
        rs = stmt1.executeQuery("select * from t1 for update");
        rs.next();

        stmt2 = createStatement();
        stmt2.execute("update t1 set b=11 where current of \""
                + rs.getCursorName() + "\"");

        stmt3 = createStatement();
        rs = stmt3.executeQuery("SELECT * FROM T1");

        JDBC.assertFullResultSet(rs, 
                    new String[][]{{"1","11"},
                    {"2","2"}});

        rs.close();
    }
}
