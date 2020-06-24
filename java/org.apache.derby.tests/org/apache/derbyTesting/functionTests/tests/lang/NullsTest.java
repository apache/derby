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
 * Test case for nulls.sql. This test shows the current supported 
 * null value functionality.
 */
public class NullsTest extends BaseJDBCTestCase {

    public NullsTest(String name) {
        super(name);
    }

    public static Test suite(){
        return TestConfiguration.defaultSuite(NullsTest.class);
    }

    public void setUp() throws SQLException{
        getConnection().setAutoCommit(false);
    }

    public void testCreate() throws SQLException{
        Statement st = createStatement();

        // A column cannot be declared explicitly nullable.
        assertCompileError("42X01", "create table a(a1 int null)");
//IC see: https://issues.apache.org/jira/browse/DERBY-6075

        //Trying to define null and not null for a column
        String sql = "create table a(a1 int null not null)";
        assertStatementError("42X01", st, sql);

        //Same as above, except that it's in reverse order
        sql = "create table a(a1 int not null null)";
        assertStatementError("42X01", st, sql);

        //Defining primarykey column constraint for explicitly nullable column
        //gives error
        sql = "create table a1(ac1 int null primary key)";
        assertStatementError("42X01", st, sql);

        //Defining primarykey table constraint on explicitly nullable columns
        //give error
        sql = "create table a1(ac1 int null, ac2 int not null, " +
                "primary key(ac1,ac2))";
        assertStatementError("42X01", st, sql);

        //Say null twice should fail
        sql = "create table a2(ac1 int null null)";
        assertStatementError("42X01", st, sql);

        //Say not null, null and no null for a column. 
        //This is to make sure the flags stay proper for a column
        sql = "create table a3(ac1 int not null null not null)";
        assertStatementError("42X01", st, sql);

        //First statement says null and second one says not null. 
        //This is to make sure the flag for the first one doesn't 
        //affect the second one
        sql = "create table a3(ac1 int default null)";
        st.executeUpdate(sql);
        sql = "create table a4(ac1 int not null)";
        st.executeUpdate(sql);
        dropTable("a3");
        dropTable("a4");

        //One column says null and second one says not null
        sql = "create table a5(ac1 int default null," +
                " ac2 int not null)";
        st.executeUpdate(sql);
        dropTable("a5");

        //Statement1 says null, 2nd says nothing but says primary key
        sql = "create table a6(ac1 int default null)";
        st.executeUpdate(sql);
        sql = "create table a7(ac1 int not null primary key)";
        st.executeUpdate(sql);
        dropTable("a6");
        dropTable("a7");

        st.close();
    }

    public void testAlter() throws SQLException{
        Statement st = createStatement();

        //Alter table adding explicitly nullable column and primary key column
        //constraint on it fails
        String sql = "create table a(a1 int not null , a2 int not null)";
        st.executeUpdate(sql);
        sql = "alter table a add column a3 int null " +
                "constraint ap1 primary key";
        assertStatementError("42X01", st, sql);

        // Adding explicitly nullable column should fail.
        assertCompileError("42X01", "alter table a add column a3 int null");
//IC see: https://issues.apache.org/jira/browse/DERBY-6075

        //Alter table table level primary key constraint on nullable column
        //doesn't give an error
        sql = "alter table a add constraint ap1 primary key(a1,a2)";
        st.executeUpdate(sql);    
        dropTable("a");

        //Alter nullability on a unique column should fail
        sql = "create table a ( a int not null unique)";
        st.executeUpdate(sql);
        sql = "alter table a modify a null";
        assertStatementError("42X01", st, sql);
        dropTable("a");

        //It's different from sql script.
        //The content in sql maybe violate its original intent
        //( may be try with multiple not null columns).
        //Please check it.
        sql = "create table a (a1 int not null, a2 int, a3 int)";
        st.executeUpdate(sql);  
        sql = "insert into a values(1,1,1)";
        assertEquals(1, st.executeUpdate(sql));
        sql = "alter table a alter column a2 not null";
        st.executeUpdate(sql);
        sql = "alter table a alter column a3 not null";
        st.executeUpdate(sql);
        sql = "alter table a add constraint ap1 primary key(a1, a2, a3)";
        st.executeUpdate(sql);
        sql = "insert into a values(1, NULL, 1)";
        assertStatementError("23502", st, sql);
        dropTable("a");

        //Try with multiple null columns
        sql = "create table a (a1 int not null, a2 int default null, " +
                "a3 int default null)";
        st.executeUpdate(sql);
        sql = "insert into a values(1,NULL,1)";
        assertEquals(1, st.executeUpdate(sql));
        sql = "alter table a add constraint ap1 primary key(a1, a2, a3)";
        assertStatementError("42831", st, sql);
        dropTable("a");

        //Try adding a primary key where there is null data
        //this should error
        sql = "create table a (a1 int not null, a2 int)";
        st.executeUpdate(sql);
        sql = "insert into a values(1, NULL)";
        assertEquals(1, st.executeUpdate(sql));
        sql = "alter table a add constraint ap1 primary key(a1, a2)";
        assertStatementError("42831", st, sql);
        dropTable("a");

        //Try with multiple columns one of which is contained in a primary key
        //where there is null data this should error
        sql = "create table a (a1 int, a2 int, a3 int)";
        st.executeUpdate(sql);
        sql = "alter table a add constraint ap1 primary key(a1, a2, a3)";
        assertStatementError("42831", st, sql);
        dropTable("a");

        st.close();   
    }

    public void testInsert() throws SQLException{
        Statement st = createStatement();

        //Create table with not null column and unique key should work
        String sql = "create table a (a int not null constraint auniq unique)";
        st.executeUpdate(sql);
        //inert same value into unique column fails.
        sql = "insert into a values (1)";
        st.executeUpdate(sql);
        sql = "insert into a values (1)";
        assertStatementError("23505", st, sql);
        dropTable("a");

        //Create a table with a non-null column with 
        //a default value of null and verify that nulls are not allowed
        sql = "create table s (x int default null not null, y int)";
        st.executeUpdate(sql);
        sql = "insert into s (y) values(1)";
        assertStatementError("23502", st, sql);
        sql = "select * from s";
        JDBC.assertEmpty(st.executeQuery(sql));
        dropTable("s");

        st.close();
    }

    public void testInsertIntoTableWithNullAndNonNullColumns()
    throws SQLException{
        Statement st = createStatement();
        //-- create a table with null and non-null columns
        String sql = "create table t (i int, i_d int default null, " +
                "i_n int not null, s smallint, " +
                "s_d smallint default null, s_n smallint not null)";
        st.executeUpdate(sql);

        //-- insert non-nulls into null and non-null columns
        sql = "insert into t (i, i_d, i_n, s, s_d, s_n)" +
                " values (1, 1, 1, 1, 1, 1)";
        assertEquals(1, st.executeUpdate(sql));

        //-- insert nulls into those columns that take nulls
        sql = "insert into t values (null, null, 2, null, null, 2)";
        assertEquals(1, st.executeUpdate(sql));

        //-- insert a null as a default value 
        //into the first default null column
        sql = "insert into t (i, i_n, s, s_d, s_n)" +
                " values (3, 3, 3, 3, 3)";
        assertEquals(1, st.executeUpdate(sql));

        //-- insert a null as a default value 
        //into the other default null columns
        sql = "insert into t (i, i_d, i_n, s, s_n) " +
                "values (4, 4, 4, 4, 4)";
        assertEquals(1, st.executeUpdate(sql));

        //-- insert nulls as default values 
        //into all default null columns
        sql= "insert into t (i, i_n, s, s_n) " +
                "values (5, 5, 5, 5)";
        assertEquals(1, st.executeUpdate(sql));

        //-- attempt to insert default values 
        //into the columns that don't accept nulls
        sql = "insert into t (i, i_d, s, s_d) " +
                "values (6, 6, 6, 6)";
        assertStatementError("23502", st, sql);

        //-- insert default nulls into nullable columns 
        //that have no explicit defaults
        sql = "insert into t (i_d, i_n, s_d, s_n) " +
                "values (7, 7, 7, 7)";
        assertEquals(1, st.executeUpdate(sql));

        //-- attempt to insert an explicit null into a column 
        //that doesn't accept nulls
        sql = "insert into t values (8, 8, null, 8, 8, 8)";
        assertStatementError("23502", st, sql);

        //-- attempt to insert an explicit null
        //into the other columns that doesn't accept nulls
        sql = "insert into t values (9, 9, 9, 9, 9, null)";
        assertStatementError("23502", st, sql);

        sql = "select * from t";
        JDBC.assertFullResultSet(st.executeQuery(sql),
            new String[][]{
            {"1", "1", "1", "1", "1", "1",},
            {null, null, "2", null, null, "2"},
            {"3", null, "3", "3", "3", "3",},
            {"4", "4", "4", "4", null, "4",},
            {"5", null, "5", "5", null, "5",},
            {null, "7", "7", null, "7", "7",},
        });

        dropTable("t");
        st.close();
    }

    public void testISNullAndNot() throws SQLException{
        String sql = "create table u (c1 integer)";
        Statement st = createStatement();
        st.addBatch(sql);
        sql = "insert into u values null";
        st.addBatch(sql);
        sql = "insert into u values 1";
        st.addBatch(sql);
        sql = "insert into u values null";
        st.addBatch(sql);
        sql = "insert into u values 2";
        st.addBatch(sql);
        st.executeBatch();

        sql = "select * from u";
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            new String[][]{
            {null}, {"1"}, {null}, {"2"},
        });

        sql = "select * from u where c1 is null";
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            new String[][]{
            {null}, {null},
        });

        sql = "select * from u where c1 is not null";
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            new String[][]{
            {"1"}, {"2"},
        });
    
        st.close();

        sql = "select * from u where cast (? as varchar(1))" +
                " is null";
        PreparedStatement p1 = prepareStatement(sql);
        p1.setString(1, "a");
        JDBC.assertEmpty(p1.executeQuery());
        p1.close();

        sql = "select * from u where cast (? as varchar(1))" +
                " is not null";
        PreparedStatement p2 = prepareStatement(sql);
        p2.setString(1, "a");
        JDBC.assertFullResultSet(p2.executeQuery(), 
                new String[][]{
                {null}, {"1"}, {null}, {"2"}, 
            });
        p2.close();

        st = createStatement();

        sql = "select count(*) from u where c1 is null";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "2");

        sql = "insert into u select * from (values null) as X";
        assertEquals(1, st.executeUpdate(sql));
        sql = "select * from u";
        JDBC.assertFullResultSet(st.executeQuery(sql), 
            new String[][]{
            {null}, {"1"}, {null}, {"2"}, {null},
        });

        sql = "select count(*) from u where c1 is null";
        JDBC.assertSingleValueResultSet(st.executeQuery(sql), "3");

        dropTable("u");
        st.close();
    }
}
