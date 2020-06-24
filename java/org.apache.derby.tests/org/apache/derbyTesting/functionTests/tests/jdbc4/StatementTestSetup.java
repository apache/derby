/*
 *
 * Derby - Class StatementTestSetup
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;

import junit.framework.Test;
import junit.extensions.TestSetup;

import java.sql.*;

/**
 *  Create the table necessary for running {@link StatementTest}.
 *
 *  @see StatementTest
 */
public class StatementTestSetup 
    extends BaseJDBCTestSetup {

    /**
     * Initialize database schema.
     * Uses the framework specified by the test harness.
     *
     * @see StatementTest
     */
    public StatementTestSetup(Test test) {
        super(test);
    }

    /**
     * Create the table and data needed for the test.
     *
     * @throws SQLException if database operations fail.
     *
     * @see StatementTest
     */
    protected void setUp()
        throws SQLException {
        Connection con = getConnection();
        // Create tables used by the test.
        Statement stmt = con.createStatement();
        // See if the table is already there, and if so, delete it.
        try {
            stmt.execute("select count(*) from stmtTable");
            // Only get here is the table already exists.
            stmt.execute("drop table stmtTable");
        } catch (SQLException sqle) {
            // Table does not exist, so we can go ahead and create it.
            assertEquals("Unexpected error when accessing non-existing table.",
                    "42X05",
                    sqle.getSQLState());
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        try {
            stmt.execute("drop function delay_st");
        }
        catch (SQLException se)
        {
            // ignore object does not exist error
            assertEquals( "42Y55", se.getSQLState() );
        }
        stmt.execute("create table stmtTable (id int, val varchar(10))");
        stmt.execute("insert into stmtTable values (1, 'one'),(2,'two')");
        // Check just to be sure, and to notify developers if the database
        // contents are changed at a later time.
        ResultSet rs = stmt.executeQuery("select count(*) from stmtTable");
        rs.next();
        assertEquals("Number of rows are not as expected", 
                2, rs.getInt(1));
        rs.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        stmt.execute
            (
             "create function delay_st(seconds integer, value integer) returns integer\n" +
             "parameter style java no sql language java\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.jdbcapi.SetQueryTimeoutTest.delay'"
             );
        stmt.close();
        con.commit();
    }

    /**
     * Clean up after the tests.
     * Deletes the table that was created for the tests.
     *
     * @throws SQLException if database operations fail.
     */
    protected void tearDown() 
        throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.execute("drop table stmtTable");
        stmt.close();
        con.commit();
        super.tearDown();
    }
   
} // End class StatementTestSetup
