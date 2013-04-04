/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.NullsTest

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
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for DERBY-6131: select from view with "upper" and "in" list 
 * throws a ClassCastException null value functionality.
 */
public class Derby6131 extends BaseJDBCTestCase {

    public Derby6131(String name) {
        super(name);
    }

    public static Test suite(){
        return TestConfiguration.defaultSuite(Derby6131.class);
    }

    public void setUp() throws SQLException{
        getConnection().setAutoCommit(false);
    }

    /**
     * Test the original user report of this issue:
     * <p>
     * the issue can be reproduced
     * 1. create table myTbl1 (name varchar(1000));
     * 2. create table myTbl2 (name varchar(1000));
     * 3. create view myView (name) as 
     *        select t1.name from myTbl1 t1 
     *        union all select t2.name from myTbl2 t2;
     * 4. select name from myView where upper(name) in ('AA', 'BB');
     * #4 failed with 
     *     "org.apache.derby.impl.sql.compile.SimpleStringOperatorNode 
     *     incompatible with org.apache.derby.impl.sql.compile.ColumnReference:
     *     java.lang.ClassCastException"
     *
     * If the view is created as 
     *    "create myView (name) as select t1.name from myTbl1 t1", 
     *    the query worked fine. 
     * <p>
     **/
    public void testOrigUserRepro()
        throws SQLException
    {
        Statement st = createStatement();

        // 1. create table myTbl1 (name varchar(1000));
        st.executeUpdate("create table myTbl1 (name varchar(1000))");

        // 2. create table myTbl2 (name varchar(1000));
        st.executeUpdate("create table myTbl2 (name varchar(1000))");

        // * 3. create view myView (name) as 
        //          select t1.name from myTbl1 t1 
        //              union all select t2.name from myTbl2 t2;
        st.executeUpdate(
            "create view myView (name) as " + 
                "select t1.name from myTbl1 t1 " + 
                    "union all select t2.name from myTbl2 t2");

        // 4. select name from myView where upper(name) in ('AA', 'BB');
        // #4 failed with 
        //    "org.apache.derby.impl.sql.compile.SimpleStringOperatorNode 
        //     incompatible with 
        //     org.apache.derby.impl.sql.compile.ColumnReference: 
        //     java.lang.ClassCastException"

        String sql = 
            "select name from myView where upper(name) in ('AA', 'BB')";

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ });

        dropView("myView");
        dropTable("myTbl1");
        dropTable("myTbl2");

        st.close();
    }

    /**
     * Test the original DERBY-6131 queries with some data to make sure
     * results look right in addtion to not getting an exception.
     * <p>
     **/
    public void testOrigUserReproWithData()
        throws SQLException
    {
        Statement st = createStatement();

        // 1. create table myTbl1 (name varchar(1000));
        st.executeUpdate("create table myTbl1 (name varchar(1000))");

        // 2. create table myTbl2 (name varchar(1000));
        st.executeUpdate("create table myTbl2 (name varchar(1000))");

        st.executeUpdate(
            "insert into myTbl1 values ('aA'), ('bB'), ('Cc'), ('Dd')");
        st.executeUpdate(
            "insert into myTbl2 values ('eE'), ('fF'), ('GG'), ('hh')");

        // * 3. create view myView (name) as 
        //          select t1.name from myTbl1 t1 
        //              union all select t2.name from myTbl2 t2;
        st.executeUpdate(
            "create view myView (name) as " + 
                "select t1.name from myTbl1 t1 " + 
                    "union all select t2.name from myTbl2 t2");

        // 4. select name from myView where upper(name) in ('AA', 'BB');
        // before fix #4 failed with 
        //    "org.apache.derby.impl.sql.compile.SimpleStringOperatorNode 
        //     incompatible with 
        //     org.apache.derby.impl.sql.compile.ColumnReference: 
        //     java.lang.ClassCastException"

        String sql = 
            "select name from myView where upper(name) in ('AA', 'BB')";

        // should match both values in IN-LIST
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
                {"aA"}, 
                {"bB"}
            });

        // same test using prepared statement with params rather than constants.
        String prepared_sql = 
            "select name from myView where upper(name) in (?, ?)";

        PreparedStatement pstmt = prepareStatement(prepared_sql);

        pstmt.setString(1, "AA");
        pstmt.setString(2, "BB");

        // should match both values in IN-LIST
        JDBC.assertFullResultSet(pstmt.executeQuery(),
            new String[][]{ 
                {"aA"}, 
                {"bB"}
            });

        // look for data across both parts of the union
        sql = "select name from myView where upper(name) in ('CC', 'HH')";

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
                {"Cc"}, 
                {"hh"}
            });

        // same test using prepared statement with params rather than constants.
        pstmt.setString(1, "CC");
        pstmt.setString(2, "HH");

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
                {"Cc"}, 
                {"hh"}
            });

        // negative test, should not match anything
        sql = "select name from myView where upper(name) in ('cc', 'hh')";

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
            });

        // same test using prepared statement with params rather than constants.
        pstmt.setString(1, "cc");
        pstmt.setString(2, "hh");

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
            });

        // test another function - user lower
        sql = "select name from myView where lower(name) in ('cc', 'hh')";

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
                {"Cc"}, 
                {"hh"}
            });

        // same test using prepared statement with params rather than constants.
        prepared_sql = 
            "select name from myView where upper(name) in (?, ?)";

        pstmt = prepareStatement(prepared_sql);

        pstmt.setString(1, "cc");
        pstmt.setString(2, "hh");

        // no data so just checking if no exception happens.
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{ 
                {"Cc"}, 
                {"hh"}
            });

        dropView("myView");
        dropTable("myTbl1");
        dropTable("myTbl2");

        st.close();
        pstmt.close();
    }
}
