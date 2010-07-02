/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RenameTableTest

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Various tests for RENAME TABLE
 * 
 */
public class RenameTableTest extends BaseJDBCTestCase {
    public RenameTableTest(String name) {
        super(name);
    }

    public static Test suite() {
        return TestConfiguration.embeddedSuite(RenameTableTest.class);
    }

    protected void setUp() throws Exception {
        super.setUp();
        getConnection().setAutoCommit(false);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Tests that we can't rename a Non-Existing Table.
     * 
     * @exception SQLException
     */
    public void testRenameNonExistingTable() throws SQLException {
        Statement s = createStatement();
        assertStatementError("42Y55", s, "rename table notexists to notexists1");
    }

    /**
     * Tests that we can't rename a table with an existed table name
     * 
     * @exception SQLException
     */
    public void testExistedNameForRenameTable() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t1(c11 int not null primary key)");
        s.executeUpdate("create table t2(c21 int not null primary key)");
        assertStatementError("X0Y32", s, "rename table t1 to t2");
        s.executeUpdate("drop table t1");
        s.executeUpdate("drop table t2");
    }

    /**
     * Tests that we cannot rename a System Table.
     * 
     * @exception SQLException
     */
    public void testRenameSystemTable() throws SQLException {
        Statement s = createStatement();
        assertStatementError("42X62", s, "rename table sys.systables to fake");
    }

    /**
     * Tests that we cannot rename a View
     * 
     * @exception SQLException
     */
    public void testRenameTableWithViews() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t1(c11 int not null primary key)");
        s.executeUpdate("insert into t1 values 11");
        s.executeUpdate("create view v1 as select * from t1");
        assertStatementError("42Y62", s, "rename table v1 to fake");
        assertStatementError("X0Y23", s, "rename table t1 to fake");
        s.executeUpdate("drop view v1");
        s.executeUpdate("drop table t1");
    }

    // -- cannot rename a table when there is an open cursor on it
    // Bug 2994 ( https://issues.apache.org/jira/browse/DERBY-2994 )
    //
    /*
     * public void testRenameOpenCursoredTable() throws SQLException { Statement
     * s = createStatement(ResultSet.TYPE_FORWARD_ONLY ,
     * ResultSet.CONCUR_UPDATABLE); assertUpdateCount(s , 0 , "create table
     * t2(c21 int not null primary key)"); assertUpdateCount(s , 1 , "insert
     * into t2 values(21)"); assertUpdateCount(s , 1 , "insert into t2
     * values(22)");
     * 
     * ResultSet rs = s.executeQuery("select * from t2"); rs.next();
     * assertStatementError("X0X95" , s , "rename table t2 to fake"); }
     */
    // -- cannot rename a table when foreign key depends on it
    /**
     * We can't Rename a Table When there is a foreign key constraint depended
     * on it.
     * 
     * @exception SQLException
     */
    public void testRenameOnDependencies() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t4(c41 int not null primary key)");
        // -- create table with foreign key constraint
        s
                .executeUpdate("create table t5 (c51 int, constraint fk foreign key(c51) references t4)");
        assertStatementError("X0Y25", s, "rename table t4 to fake");
        // -- only dropping the fk constraint can allow the table to be renamed
        s.executeUpdate("alter table t5 drop constraint fk");
        // -- this statement should not fail
        s.executeUpdate("rename table t4 to realTab");
        s.executeUpdate("drop table t5");
        s.executeUpdate("drop table realTab");
    }

    /**
     * Tests that We can rename a table when there is an index defined on it
     * 
     * @exception SQLException
     */
    public void testRenameWithIndex() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t3(c31 int not null)");
        s.executeUpdate("create index i1_t3 on t3(c31)");
        // -- can rename a table when there is an index defined on it
        assertUpdateCount(s, 0, "rename table t3 to t3r");
        s.executeUpdate("drop table t3r");
    }

    /**
     * Test Rename Table With PreparedStatement.
     * 
     * @exception SQLException
     */
    public void testRenameWithPreparedStatement() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t3(c31 int not null primary key)");
        s.executeUpdate("insert into t3 values 31");
        s.executeUpdate("insert into t3 values 32");
        s.executeUpdate("insert into t3 values 33");
        PreparedStatement pstmt = prepareStatement("select * from t3 where c31 > ?");
        pstmt.setInt(1, 30);
        ResultSet rs = pstmt.executeQuery();
        rs.next();
        rs.close();
        // -- can rename with no errors
        assertUpdateCount(s, 0, "rename table t3 to t3r");
        // -- but the execute statement will fail
        pstmt.setInt(1, 30);
        try {
            ResultSet rs1 = pstmt.executeQuery();
            fail("Table/View t3 Doesn't exists:");
        } catch (SQLException e) {
            assertSQLState("42X05", e);
        }
        s.executeUpdate("drop table t3r");
    }

    // -- creating a table with triggers defined on it
    /**
     * Test that we can RENAME a TABLE when there is trigger defined on it.
     * 
     * @exception SQLException
     */
    public void testRenameTableWithTriggersOnIt() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table t6 (c61 int default 1)");
        s.executeUpdate("create table t7(c71 int)");
        // -- bug 5684
        s
                .executeUpdate("create trigger t7insert after insert on t7 referencing new as NEWROW for each row insert into t6 values(NEWROW.c71)");
        s.executeUpdate("insert into t7 values(1)");
        // -- bug 5683. Should fail
        assertStatementError("X0Y25", s, "rename table t7 to t7r");
        assertStatementError("42X05", s, "select * from t7r");
        ResultSet rs = s.executeQuery("select * from t7");
        rs.next();
        rs.close();
        s.executeUpdate("rename table t6 to t6r");
        assertStatementError("42X05", s, "insert into t7 values(3)");
        rs = s.executeQuery("select * from t6r");
        assertStatementError("42X05", s, "select * from t7r");
        // Clean Up
        s.executeUpdate("drop table t6r");
        s.executeUpdate("drop table t7");
    }

    /**
     * RENAME TABLE should fail when check constraints on it.
     * 
     * @exception SQLException
     */
    public void testRenameWithCheckConstraintsOnIt() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table tcheck (i int check(i>5))");
        assertStatementError("X0Y25", s, "rename table tcheck to tcheck1");
        s.executeUpdate("drop table tcheck");
        // - Rename should pass after dropping the check constriant
        s
                .executeUpdate("create table tcheck (i int, j int, constraint tcon check (i+j>2))");
        assertStatementError("X0Y25", s, "rename table tcheck to tcheck1");
        s.executeUpdate("alter table tcheck drop constraint tcon");
        s.executeUpdate("rename table tcheck to tcheck1");
        // select * from tcheck1;
        s.executeUpdate("drop table tcheck1");
    }

    /**
     * Tests that rename table invalidates stored statement plans (DERBY-4479).
     *
     * By issuing the *identical* create table statement after the rename,
     * we check to see whether the compiled statements from the first
     * create table statement were properly invalidated by the rename.
     * 
     * @exception SQLException
     */
    public void testRenameInvalidation_derby_4479()
        throws SQLException
    {
        getConnection().setAutoCommit(true);
        Statement s = createStatement();
        s.executeUpdate("create table a (x int not null primary key)");
        s.executeUpdate("rename table a to b");
        s.executeUpdate("create table a (x int not null primary key)");
        s.executeUpdate("drop table a");
        s.executeUpdate("drop table b");
    }

}
