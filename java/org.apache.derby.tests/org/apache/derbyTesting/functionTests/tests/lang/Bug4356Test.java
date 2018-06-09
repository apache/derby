/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.Bug4356Test
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Demonstrate subselect behavior with prepared statement.
 */
public class Bug4356Test extends BaseJDBCTestCase {

    /**
     * Basic constructor.
     */
    public Bug4356Test(String name) {
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
        return new CleanDatabaseTestSetup(TestConfiguration
                .embeddedSuite(Bug4356Test.class)) {
            protected void decorateSQL(Statement stmt) throws SQLException {
                stmt.executeUpdate("CREATE TABLE T1 (a integer, b integer)");
                stmt.executeUpdate("CREATE TABLE T2 (a integer)");
                stmt.executeUpdate("INSERT INTO T2 VALUES(1)");
            }
        };
    }

    /**
     * Bug only happens when autocommit is off.
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Check fix for Bug4356 - Prepared statement parameter buffers are not cleared between calls 
     * to executeUpdate() in the same transaction.
     * Using a prepared statement to insert data into a table using 
     * a sub select to get the data from a second table. The
     * prepared statement doesn't seem to clear it's buffers between 
     * execute statements within the same transaction.
     * @throws SQLException
     */
    public void testBug4356() throws SQLException {
        Statement stmt = createStatement();
        ResultSet rs;

        PreparedStatement ps = prepareStatement("INSERT INTO T1 VALUES (?,(select count(*) from t2 where a = ?)) ");

        ps.setInt(1, 1);
        ps.setInt(2, 1);
        ps.executeUpdate();

        ps.setInt(1, 2);
        ps.setInt(2, 2);
        ps.executeUpdate();

        commit();
     

        rs = stmt.executeQuery("SELECT * FROM T1");
        JDBC.assertFullResultSet(rs,new String[][] {{"1","1"},
               {"2","0"}});
 

        rs.close();
        stmt.close();
    }
}
