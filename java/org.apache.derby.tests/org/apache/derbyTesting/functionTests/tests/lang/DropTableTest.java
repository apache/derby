/*
Derby - Class org.apache.derbyTesting.functionTests.tests.lang.DropTableTest

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

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.Test;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public final class DropTableTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public DropTableTest(String name)
    {
        super(name);
    }
    
    public static Test suite()
    {
        return TestConfiguration.defaultSuite(DropTableTest.class);
    }

    public void testDropTableWithConstraints() throws Exception
    {
        //test table with different constraints
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        // test simple table - all should work
        
        st.executeUpdate( "create table t1 ( a int)");
        st.executeUpdate( " drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // test table with unique constraint - all should work
        
        st.executeUpdate( "create table t1 (a int not null unique)");
        st.executeUpdate( " drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // test table with primary constraint - all should work
        
        st.executeUpdate( "create table t1 ( a int not null primary key)");
        st.executeUpdate( " drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // test table with check constraint - all should work
        
        st.executeUpdate( "create table t1 ( a int check(a > 0))");
        st.executeUpdate( " drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // test table with index - all should work
        
        st.executeUpdate( "create table t1 ( a int)");
        st.executeUpdate( " create index t1index on t1(a)");
        st.executeUpdate( " drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // test table with foreign key references;
        
        st.executeUpdate( "create table t1(a int not null primary key)");
        st.executeUpdate(
                " create table t2(a int constraint reft1a references t1(a))");
        
        // this should fail with a dependent constraint error
        
        assertStatementError("X0Y25", st, "drop table t1");
        
        // dropping dependent constraint
        
        st.executeUpdate( "alter table t2 drop constraint reft1a");
        
        // this should work since dependent constraint was dropped
        
        st.executeUpdate( "drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        
        // the following should work since no referential 
        // constraint is left
        
        st.executeUpdate( "insert into t2 values(1)");
        st.executeUpdate( " drop table t2");
    }

    public void testDropTableWithView() throws SQLException{
        // test table with view
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate( "create table t1(a int, b int)");
        st.executeUpdate( " create table t2(c int, d int)");
        st.executeUpdate( " create view vt1a as select a from t1");
        st.executeUpdate( " create view vt1b as select b from t1");
        st.executeUpdate( " create view vt1t2 as select * from t1, t2");
        st.executeUpdate( " create view vvt1a as select * from vt1a");
        st.executeUpdate( " create view vvvt1a as select * from vvt1a");
        
        // this should fail with view being a dependent object
        
        assertStatementError("X0Y23", st, "drop table t1");
        
        // dropping dependent views
        
        st.executeUpdate( "drop view vvvt1a");
        st.executeUpdate( " drop view vvt1a");
        st.executeUpdate( " drop view vt1t2");
        st.executeUpdate( " drop view vt1b");
        st.executeUpdate( " drop view vt1a");
        
        // this should work after dependent views were dropped
        
        st.executeUpdate( "drop table t1");
        
        // this shouldn't find the view
        
        assertStatementError("42X05", st, "select * from vt1a");
        assertStatementError("42X05", st, " select * from vt1b");
        assertStatementError("42X05", st, " select * from vt1t2");
        assertStatementError("42X05", st, " select * from vvt1a");
        assertStatementError("42X05", st, " select * from vvvt1a");
        st.executeUpdate( " drop table t2");
    }
    public void testDropTableWithPreparedStatement() throws SQLException{
        // test table with prepared statement
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate( "create table t1(a int)");
        PreparedStatement pSt = prepareStatement( "select * from t1");
        
        // this should work, statement will be invalidated and 
        // will fail when recompiled
        
        st.executeUpdate( "drop table t1");
        assertStatementError("42X05", pSt);
    }

    public void testDropTableWithTriggers() throws SQLException{
        // test table with triggers
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate( "create table t1(a int)");
        st.executeUpdate( " create table t2(a int)");
        st.executeUpdate(
            " create trigger t1trig after insert on t1 for each "
            + "row insert into t2 values(1)");
        
        // this should work - trigger should be deleted
        
        st.executeUpdate( "drop table t1");
        
        // t1 shouldn't be found
        
        assertStatementError("42X05", st, "select * from t1");
        st.executeUpdate( " drop table t2");
        
        // test table within the body of a trigger on another table
        
        st.executeUpdate( "create table t1(a int)");
        st.executeUpdate( " create table t2(a int)");
        st.executeUpdate(
            " create trigger t2trig after insert on t2 for each "
            + "row insert into t1 values(1)");
        
        // this should fail because t2trig depends on t1 (used to work
        // before DERBY-2041)
        assertStatementError("X0Y25", st, "drop table t1");

        // trigger should still work
        st.executeUpdate("insert into t2 values(1)");
        JDBC.assertSingleValueResultSet(
                st.executeQuery("select * from t1"), "1");
        JDBC.assertSingleValueResultSet(
                st.executeQuery("select * from t2"), "1");

        st.executeUpdate( " drop table t2");
        st.executeUpdate( " drop table t1");
    }
    
    public void testDropTableDropView() throws SQLException{
        // test drop view
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate( "create table t1(a int)");
        st.executeUpdate( " create view vt1 as select * from t1");
        st.executeUpdate( " create view vvt1 as select * from vt1");
        
        // these should fail
        
        assertStatementError("X0Y23", st, "drop view vt1");
        assertStatementError("42X01", st, " drop view vt1 restrict");
        assertStatementError("42X01", st, " drop view vt1 cascade");
        
        st.executeUpdate( "drop view vvt1"); // Clean up.
        st.executeUpdate( "drop view  vt1"); // Clean up.
        st.executeUpdate( "drop table  t1"); // Clean up.
    }

    public void testDropTableIndexesDropped() throws SQLException{
        // make sure that indexes are dropped for drop table
        
        Statement st = createStatement();
        setAutoCommit(false);
        
        st.executeUpdate( "create table t2(a int not null primary key)");
        st.executeUpdate(
            "create table reft2(a int constraint ref1 references t2)");
        
        // count should be 2
        
        JDBC.assertSingleValueResultSet( st.executeQuery(
            "select count(*) from (sys.sysconglomerates c), (sys.systables t) "
            + "where t.tableid = c.tableid and "
            + "t.tablename = 'REFT2'"), "2");
        
        // drop dependent referential constraint
        
        st.executeUpdate( "alter table reft2 drop constraint ref1");
        
        // should work since dependent constraint was previously 
        // dropped
        
        st.executeUpdate( "drop table t2");
        
        // count should be 1
        
        JDBC.assertSingleValueResultSet( st.executeQuery(
            "select count(*) "
            + "from (sys.sysconglomerates c), (sys.systables t) "
            + "where t.tableid = c.tableid and "
            + "t.tablename = 'REFT2'"), "1");

        rollback();
        
        // unsuccessful drop table should not affect open cursor 
        // beetle 4393
        
        st.executeUpdate(
            " create table T1 (i int, c varchar(255), d varchar(255))");
        st.executeUpdate( " insert into T1(i) values(1)");
        st.executeUpdate( " insert into T1(i) values(2)");
        
        Statement st1 = createStatement();
        st1.setCursorName("X1");
        
        ResultSet rs1 = st1.executeQuery( "select i from t1 for update of c"); 
        PreparedStatement pSt =
            prepareStatement("update t1 set c = CHAR(i) where current of X1");
        
        assertStatementError("X0X95",st,"drop table T1");
        
        rs1.next();      
        
        pSt.executeUpdate();
        
        ResultSet rs = st.executeQuery("select * from T1");
        String[] expColNames = new String[]{"I","C","D"};
        JDBC.assertColumnNames(rs, expColNames);
        
        String[][] expRS = new String[][]{
            {"1","1",null},
            {"2",null,null}
        };
        JDBC.assertFullResultSet(rs, expRS);
        
        st1.close();
        st.executeUpdate("drop table T1"); // Clean up

        //pretend all of the above didn't happen
        setAutoCommit(true);
    }
}
