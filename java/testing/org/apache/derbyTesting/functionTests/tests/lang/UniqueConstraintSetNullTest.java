/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UniqueConstraintSetNullTest
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
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Types;
import java.util.Enumeration;

import junit.framework.Test;
import junit.framework.TestFailure;
import junit.framework.TestResult;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test unique constraint
 */
public class UniqueConstraintSetNullTest extends BaseJDBCTestCase {
    
    /**
     * Basic constructor.
     */
    public UniqueConstraintSetNullTest(String name) {
        super(name);
    }
    
    /**
     * Returns the implemented tests.
     *
     * @return An instance of <code>Test</code> with the
     *         implemented tests to run.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("UniqueConstraintSetNullTest");
        suite.addTest(TestConfiguration.embeddedSuite(
                UniqueConstraintSetNullTest.class));
        return suite;
    }
    
    /**
     * Create table for test cases to use.
     */
    protected void setUp() throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table constraintest (" +
                "val1 varchar (20) not null, " +
                "val2 varchar (20))");
    }
    
    protected void tearDown() throws Exception {
        super.tearDown();
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("drop table constraintest");
        con.commit();
    }
    /**
     * Test the behaviour of unique constraint after making
     * column nullable.
     * @throws java.lang.Exception
     */
    public void testUpdateNullablity() throws Exception {
        Statement stmt = createStatement();
        //create constraint
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        //test the constraint without setting it to nullable
        PreparedStatement ps = prepareStatement("insert into " +
                "constraintest (val1) values (?)");
        ps.setString (1, "name1");
        ps.executeUpdate();
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        try {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
            fail ("null value in not null field!!");
        }
        catch (SQLException e){
            assertSQLState ("null value in non null field",
                    "23502", e);
        }
        stmt.executeUpdate("alter table constraintest alter column val1 null");
        //should work
        ps.setNull(1, Types.VARCHAR);
        ps.executeUpdate();
        //try another null
        ps.setNull(1, Types.VARCHAR);
        ps.executeUpdate();
        //try a duplicate non null should fail
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        //remove nulls from table and set the column back to non null
        stmt.executeUpdate("delete from constraintest where val1 is null");
        stmt.executeUpdate("alter table constraintest alter column " +
                "val1 not null");
        //try a duplicate non null key
        try {
            ps.setString (1, "name1");
            ps.execute();
            fail ("duplicate key in unique constraint!!!");
        }
        catch (SQLException e){
            assertSQLState ("duplicate key in unique constraint",
                    "23505", e);
        }
        try {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
            fail ("null value in not null field!!");
        }
        catch (SQLException e){
            assertSQLState ("null value in non null field",
                    "23502", e);
        }
    }
}
