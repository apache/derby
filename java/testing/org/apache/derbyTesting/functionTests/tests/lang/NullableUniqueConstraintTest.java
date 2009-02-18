/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NullableUniqueConstraintTest
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
public class NullableUniqueConstraintTest extends BaseJDBCTestCase {
    
    /**
     * Basic constructor.
     */
    public NullableUniqueConstraintTest(String name) {
        super(name);
    }
    
    /**
     * Returns the implemented tests.
     *
     * @return An instance of <code>Test</code> with the implemented tests to
     *         run.
     */
    public static Test suite() {
        TestSuite suite = new TestSuite("NullableUniqueConstraintTest");
        suite.addTest(TestConfiguration.defaultSuite(
                            NullableUniqueConstraintTest.class));
        return suite;
    }
    
    /**
     * Create table for test cases to use.
     */
    protected void setUp() throws Exception {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("create table constraintest (val1 varchar (20), " +
                "val2 varchar (20), val3 varchar (20), val4 varchar (20))");
    }
    
    protected void tearDown() throws Exception {
        Connection con = getConnection();
        con.commit ();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("drop table constraintest");
        stmt.close ();
        con.commit ();
        super.tearDown();
    }
    /**
     * Basic test of Unique Constraint using single part key.
     * @throws SQLException
     */
    public void testSingleKeyPartUniqueConstraint() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        //create unique constraint without not null
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1, val2) values (?, ?)");
        ps.setString(1, "test");
        ps.setString(2, "should pass");
        ps.execute();
        try {
            ps.setString(1, "test");
            ps.setString(2, "should fail");
            ps.execute();
            fail("duplicate key inserted expected '23505'");
        }
        catch (SQLException e) {
            assertSQLState("inserting duplicate", "23505", e);
        }
        ps.setNull(1, Types.VARCHAR);
        ps.setString(2, "should pass");
        ps.execute();
        ps.setNull(1, Types.VARCHAR);
        ps.setString(2, "should pass");
        ps.execute();
        //check if there are two record with val1=null
        ResultSet rs = stmt.executeQuery("select count (*) from " +
                "constraintest where val1 is null");
        rs.next();
        assertEquals("expected 2 rows", 2, rs.getInt(1));
        //try creating constraint with existing value
        stmt.execute("alter table constraintest drop constraint u_con");
        stmt.execute("delete from constraintest where val1 is null");
        con.commit ();
        ps.setString(1, "test");
        ps.setString(2, "removeit");
        ps.execute();
        //constraint dropped successfully
        //create constraint - must fail
        try {
            stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
            fail ("create unique constraint with duplicate key in " +
                    "table should fail");
        }
        catch (SQLException e) {
            assertSQLState("creating unique constraint when duplicate" +
                    " keys are present  duplicate", "23505", e);
        }
        //remove duplicate record
        stmt.execute ("delete from constraintest where val2 = 'removeit'");
        //should be fine now
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        con.commit();
        stmt.close ();
        ps.close();
    }
    
    /**
     * Basic test of Unique Constraint using multipart part key.
     * @throws SQLException
     */
    public void testMultipartKeyUniqueConstraint() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        //create unique constraint without not null
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1, val2, val3)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1, val2, val3, val4) values (?, ?, ?, ?)");
        ps.setString(1, "part1");
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        try {
            ps.setString(1, "part1");
            ps.setString(2, "part2");
            ps.setString(3, "part3");
            ps.setString(4, "should fail");
            ps.execute();
            fail("duplicate key inserted expected '23505'");
        }
        catch (SQLException e) {
            assertSQLState("inserting duplicate", "23505", e);
        }
        ps.setNull(1, Types.VARCHAR);
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        ps.setNull(1, Types.VARCHAR);
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        ps.setString(1, "part1");
        ps.setNull(2, Types.VARCHAR);
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        //check if there are two record with val1=null
        ResultSet rs = stmt.executeQuery("select count (*) from " +
                "constraintest where val1 is null");
        rs.next();
        assertEquals("expected 2 rows", 2, rs.getInt(1));
        //try creating constraint with existing value
        stmt.execute("alter table constraintest drop constraint u_con");
        con.commit ();
        ps.setString(1, "part1");
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "removeit");
        ps.execute();
        //constraint dropped successfully
        //create constraint - must fail
        try {
            stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1, val2, val3)");
            fail ("create unique constraint with duplicate key in " +
                    "table should fail");
        }
        catch (SQLException e) {
            assertSQLState("creating unique constraint when duplicate" +
                    " keys are present  duplicate", "23505", e);
        }
        //remove duplicate record
        stmt.execute ("delete from constraintest where val4 = 'removeit'");
        //should be fine now
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1, val2, val3)");
        con.commit();
        stmt.close ();
        ps.close();
    }
    
    /**
     * Inserts a duplicate key of a deleted key within same transaction.
     * @throws java.sql.SQLException
     */
    public void testWithDeletedKey() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        //create unique constraint without not null
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1, val2, val3)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1, val2, val3, val4) values (?, ?, ?, ?)");
        ps.setString(1, "part1");
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        //delete a record within transaction and try inserting same record
        con.setAutoCommit(false);
        stmt.executeUpdate("delete from constraintest where " +
                "val1 = 'part1' and val2 = 'part2' and val3 = 'part3'");
        //insert same record
        ps.setString(1, "part1");
        ps.setString(2, "part2");
        ps.setString(3, "part3");
        ps.setString(4, "should pass");
        ps.execute();
        stmt.close();
        ps.close();
        con.commit();
    }
    
    public void testDistinctQuery() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1) values (?)");
        //insert 5 null keys
        for (int i = 0; i < 5; i++) {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
        }
        
        //insert 5 null keys
        for (int i = 0; i < 5; i++) {
            ps.setString(1, String.valueOf(i));
            ps.executeUpdate();
        }
        ResultSet rs = stmt.executeQuery("select count (*) from constraintest");
        rs.next();
        assertEquals(10, rs.getInt(1));
        rs.close ();

        rs = stmt.executeQuery("select count (distinct (val1)) from " +
                "constraintest");
        rs.next();
        assertEquals(5, rs.getInt(1));
        rs.close ();
    }
    /**
     * Test null ordering of the key in order by query.
     * @throws java.sql.SQLException
     */
    public void testNullOrdering() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1) values (?)");
        //insert 5 null keys
        for (int i = 0; i < 5; i++) {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
        }
        
        //insert 5 non null keys
        for (int i = 0; i < 5; i++) {
            ps.setString(1, String.valueOf(i));
            ps.executeUpdate();
        }
        
        ResultSet rs = stmt.executeQuery("select val1 from constraintest " +
                            "order by val1 nulls last");
        //first 5 should be non null
        for (int i = 0; i < 5; i++) {
            rs.next();
            assertEquals (String.valueOf(i), rs.getString(1));
        }
        
        //next 5 should be null
        for (int i = 0; i < 5; i++) {
            rs.next();
            assertEquals (null, rs.getString(1));
        }
        rs.close ();
        rs = stmt.executeQuery("select val1 from constraintest " +
                            "order by val1 nulls first");
        //first 5 should be null
        for (int i = 0; i < 5; i++) {
            rs.next();
            assertEquals (null, rs.getString(1));
        }
        
        //next 5 should be null
        for (int i = 0; i < 5; i++) {
            rs.next();
            assertEquals (String.valueOf(i), rs.getString(1));
        }
        rs.close ();
    }
    
    /**
     * Tries to forces internal routibe to travel across
     * pages to check for duplicates. It first inserts large 
     * number of records assuming they occupy multiple pages 
     * in index and then tries to insert duplicates of each 
     * of them. Rrecords at the page boundry will require 
     * duplucate checking routine to check more than one page 
     * to look for locate. If that routine is not working properly 
     * duplucate will be inserted in tree.
     * @throws java.sql.SQLException
     */
    public void testComparisonAcrossPages() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        //create unique constraint without not null
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1, val2) values (?, ?)");
        for (int i = 0; i < 500; i++) {
            ps.setString(1, "" + i);
            ps.setString (2, "" + i);
            ps.execute();
        }
        
        for (int i = 0; i < 500; i++) {
            ps.setString(1, "" + i);
            ps.setString (2, "" + i);
            try {
                ps.execute();
                fail("duplicate key inserted expected '23505'");
            }
            catch (SQLException e) {
                assertSQLState("inserting duplicate", "23505", e);  
            }
        }
        //mark all records except for first, as deleted and try 
        //inserting duplicate. This will force comparison 
        //logic to scan all the records to find another rcord for 
        //comparison.
        con.setAutoCommit(false);
        assertEquals (499, stmt.executeUpdate (
                "delete from constraintest where val1 != '0'"));
        Savepoint deleted = con.setSavepoint("deleted");
        ps.setString(1, "0");
        ps.setString (2, "test");
        try {
            ps.execute();
            fail ("managed to insert a duplicate");
        }
        catch (SQLException e) {
            assertSQLState("inserting duplicate", "23505",  e);
        }
        //rollback to check point and try to insert a record 
        //at the middle
        con.rollback(deleted);
        ps.setString(1, "250");
        ps.setString(2, "test");
        ps.execute ();
        //rollback to check point and try 
        //inserting at end
        con.rollback(deleted);
        ps.setString(1, "499");
        ps.setString (2, "test");
        ps.execute ();

        ResultSet rs = stmt.executeQuery("select count (*) from constraintest");
        rs.next ();
        assertEquals(2, rs.getInt(1));
        
        con.rollback ();
        ps.close();
        stmt.close();
        ps.close();
    }
    
    /**
     * Checks is insert for updates uses deffered inserts or not. 
     * It inserts two part keys in the form of
     * part1 part2
     * 1        1
     * 1        2
     * 1        3
     * 2        1
     * 2        2
     * 2        3
     * 3        1
     * 3        2
     * 3        3
     * 
     * and then tries to update all the records so that the values 
     * part1 and part2 are interchanged. Internally updates are 
     * treated as delete and insert and unless inserts are deffered 
     * till all deletes are over, there will be unique constraint 
     * violation.
     * @throws java.sql.SQLException
     */
    public void testDefferedInsert() throws SQLException {
        Connection con = getConnection();
        Statement stmt = con.createStatement();
        //create unique constraint without not null
        stmt.executeUpdate("alter table constraintest add constraint " +
                "u_con unique (val1, val2)");
        PreparedStatement ps  = con.prepareStatement("insert into " +
                "constraintest (val1, val2) values (?, ?)");
        for (int i = 0; i < 5; i++) {
            for (int j = 0; j < 5; j++) {
                ps.setString(1, String.valueOf(i));
                ps.setString(2, String.valueOf(j));
                ps.executeUpdate();
            }
        }
        //interchange the values of val1 and val2
        //this will fail unless its handled by deffered inserts
        assertEquals("updating 25 records", 25, 
                stmt.executeUpdate("update constraintest set " +
                "val1 = val2, val2 = val1"));
    }

    /**
     * Test that repeatedly performing multi-row inserts and deletes spanning
     * multiple pages works correctly with nullable unique constraint. This
     * used to cause <tt>ERROR XSDA1: An attempt was made to access an out of
     * range slot on a page</tt> (DERBY-4027).
     */
    public void testMixedInsertDelete() throws SQLException {
        createStatement().execute(
                "alter table constraintest add constraint uc unique (val1)");
        PreparedStatement insert = prepareStatement(
                "insert into constraintest(val1) values ?");
        PreparedStatement delete = prepareStatement(
                "delete from constraintest");
        // The error happened most frequently in the second iteration, but
        // it didn't always, so we repeat it ten times to increase the
        // likelihood of triggering the bug.
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000; j++) {
                insert.setInt(1, j);
                insert.addBatch();
            }
            insert.executeBatch();
            assertEquals(1000, delete.executeUpdate());
        }
    }

    public static void main(String [] args) {
        TestResult tr = new TestResult();
        Test t = suite();
        t.run(tr);
        System.out.println(tr.errorCount());
        Enumeration e = tr.failures();
        while (e.hasMoreElements()) {
            ((TestFailure)e.nextElement ()).thrownException().printStackTrace();
        }
        System.out.println(tr.failureCount());
    }
}
