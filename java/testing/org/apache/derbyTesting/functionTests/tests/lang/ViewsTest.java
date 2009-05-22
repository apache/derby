package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.Utilities;


public final class ViewsTest extends BaseJDBCTestCase {

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public ViewsTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        TestSuite suite = new TestSuite("views Test");
        suite.addTest(TestConfiguration.embeddedSuite(ViewsTest.class));
        return new CleanDatabaseTestSetup(suite);
    }

    public void test_views() throws Exception
    {
        ResultSet rs = null;
   
        Statement st = createStatement();

        String [][] expRS;
        String [] expColNames;


        //   Licensed to the Apache Software Foundation (ASF) 
        // under one or more   contributor license agreements.  See 
        // the NOTICE file distributed with   this work for 
        // additional information regarding copyright ownership.   
        // The ASF licenses this file to You under the Apache 
        // License, Version 2.0   (the "License"); you may not use 
        // this file except in compliance with   the License.  You 
        // may obtain a copy of the License at      
        // http://www.apache.org/licenses/LICENSE-2.0   Unless 
        // required by applicable law or agreed to in writing, 
        // software   distributed under the License is distributed 
        // on an "AS IS" BASIS,   WITHOUT WARRANTIES OR CONDITIONS 
        // OF ANY KIND, either express or implied.   See the 
        // License for the specific language governing permissions 
        // and   limitations under the License. tests for views set 
        // autocommit off
        
        getConnection().setAutoCommit(false);
        
        // create some tables
        
        st.executeUpdate(
            "create table t1(i int, s smallint, f float, dp "
            + "double precision)");
        
        st.executeUpdate(
            " create table t2(i int, s smallint, f float, dp "
            + "double precision)");
        
        st.executeUpdate(
            " create table insert_test (f float)");
        
        // create some views
        
        st.executeUpdate(
            "create view sv1 (s_was_i, dp_was_s, i_was_f, "
            + "f_was_dp) as select * from t1");
        
        st.executeUpdate(
            " create view sv2 as select * from t1");
        
        st.executeUpdate(
            " create view sv3 as select dp, f from t1 where i = s");
        
        st.executeUpdate(
            " create view sv4(i) as values 1, 2, 3");
        
        st.executeUpdate(
            " create view sv5 (c1) as select * from sv4");
        
        st.executeUpdate(
            " create view cv1 (t1_i, t2_s, t1_f, t2_dp) as "
            + "select t1.i, t2.s, t1.f, t2.dp from t1, t2 where "
            + "t1.i between t2.s and t2.i");
        
        st.executeUpdate(
            " create view cv2 as select * from sv1, sv3 where dp "
            + "= f_was_dp");
        
        st.executeUpdate(
            " create view cv3(i,s,f,dp) as select i, s, f, dp "
            + "from sv2 union select dp_was_s, s_was_i, f_was_dp, "
            + "i_was_f from sv1");
        
        st.executeUpdate(
            " create view cv4 (distinct_i) as select distinct i from t1");
        
        st.executeUpdate(
            " create view cv5(i,s) as select * from (select i, s "
            + "from cv3 where i = s) xyz");
        
        st.executeUpdate(
            " create view cv6 (c1, c2) as select a.c1 as x, b.c1 "
            + "as y from sv5 a, sv5 b where a.c1 <> b.c1");
        
        st.executeUpdate(
            " create view cv7 as select t.i, v.c1 from t1 t, cv6 "
            + "v where t.i = v.c1");
        
        st.executeUpdate(
            " create view cv8(col1, col2) as select 'Column 1',  "
            + "'Value = ' || cast(c1 as char(5)) from cv7 where 1 "
            + "in (select i from sv5)");
        
        // populate the tables
        
        st.executeUpdate(
            "insert into t1 values (1, 1, 1.0, 1.0)");
        
        st.executeUpdate(
            " insert into t1 values (1, 2, 3.0, 4.0)");
        
        st.executeUpdate(
            " insert into t1 values (8, 7, 6.0, 5.0)");
        
        st.executeUpdate(
            " insert into t2 values (1, 1, 1.0, 1.0)");
        
        st.executeUpdate(
            " insert into t2 values (1, 2, 3.0, 4.0)");
        
        st.executeUpdate(
            " insert into t2 values (8, 7, 6.0, 5.0)");
        
        // negative tests view with a parameter
        
        assertStatementError("42X98", st,
            "create view vneg as select * from t1 where i = ?");
        
        // drop view on table
        
        assertStatementError("X0Y16", st,
            "drop view t1");
        
        // drop table on view
        
        assertStatementError("42Y62", st,
            "drop table sv1");
        
        // views and tables share same name space
        
        assertStatementError("X0Y32", st,
            "create view sv1(i) as values 1");
        
        assertStatementError("X0Y32", st,
            " create table sv1 (c1 int)");
        
        assertStatementError("X0Y32", st,
            " create view t1(i) as values 1");
        
        // drop non-existant view
        
        assertStatementError("X0X05", st,
            "drop view notexists");
        
        // duplicate column name in view's column list
        
        assertStatementError("42Y13", st,
            "create view shouldntwork (c1, c2, c1) as select i, "
            + "s, f from t1");
        
        // # of columns in view's column list does not match that 
        // in view definition
        
        assertStatementError("42X56", st,
            "create view shouldntwork (c1, c2, c3) as select i, s from t1");
        
        assertStatementError("42X56", st,
            " create view shouldntwork (c1, c2, c3) as select i, "
            + "s, f, dp from t1");
        
        // try to drop a table out from under a view
        
        assertStatementError(new String[] {"X0Y23","X0Y23","X0Y23","X0Y23","X0Y23","X0Y23",
            "X0Y23","X0Y23","X0Y23","X0Y23"},st,
            "drop table t1");
        
        assertStatementError("X0Y23", st,
            " drop table t2");
        
        // try to drop a view out from under another view
        
        assertStatementError(new String[] {"X0Y23","X0Y23","X0Y23"}, st,
            "drop view sv1");
        
        assertStatementError("X0Y23", st,
            " drop view sv3");
        
        // try to drop a view out from under a cursor
        
        PreparedStatement ps_c1 = prepareStatement(
            "select * from cv8");
        
        ResultSet c1 = ps_c1.executeQuery();
        
        assertStatementError("X0X95", st,
            " drop view cv8");
        
        assertStatementError(new String[] {"X0Y23","X0Y23","X0Y23","X0X95"}, st,
            " drop view sv5");
        
        assertStatementError(new String[] {"X0Y23","X0Y23","X0Y23","X0Y23","X0X95"}, st,
            " drop view sv4");
        
        c1.close();
        ps_c1.close();
        
        // view updateability (No views are currently updateable)
        
        assertStatementError("42Y24", st,
            "insert into sv1 values 1");
        
        assertStatementError("42Y24", st,
            " delete from sv1");
        
        assertStatementError("42Y24", st,
            " update sv1 set s_was_i = 0");
        
        try {
        prepareStatement(
                "select * from sv1 for update of s_was_i");
            fail("statement ps_c2 should not have succeeded");
        } catch (SQLException se) {
            assertSQLState("42Y90",se);
        }
        
        // create index on a view
        
        assertStatementError("42Y62", st,
            "create index i1 on sv2(i)");
        
        // positive tests
        
        rs = st.executeQuery(
            "select * from sv1");
        
        expColNames = new String [] {"S_WAS_I", "DP_WAS_S", "I_WAS_F", "F_WAS_DP"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0"},
            {"1", "2", "3.0", "4.0"},
            {"8", "7", "6.0", "5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from sv2");
        
        expColNames = new String [] {"I", "S", "F", "DP"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0"},
            {"1", "2", "3.0", "4.0"},
            {"8", "7", "6.0", "5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from sv3");
        
        expColNames = new String [] {"DP", "F"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1.0", "1.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from sv4");
        
        expColNames = new String [] {"I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from sv5");
        
        expColNames = new String [] {"C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"2"},
            {"3"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv1");
        
        expColNames = new String [] {"T1_I", "T2_S", "T1_F", "T2_DP"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0"},
            {"1", "1", "3.0", "1.0"},
            {"8", "7", "6.0", "5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv2");
        
        expColNames = new String [] {"S_WAS_I", "DP_WAS_S", "I_WAS_F", "F_WAS_DP", "DP", "F"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0", "1.0", "1.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv3");
        
        expColNames = new String [] {"I", "S", "F", "DP"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0"},
            {"1", "2", "3.0", "4.0"},
            {"2", "1", "4.0", "3.0"},
            {"7", "8", "5.0", "6.0"},
            {"8", "7", "6.0", "5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv4 order by 1");
        
        expColNames = new String [] {"DISTINCT_I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv5");
        
        expColNames = new String [] {"I", "S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv6");
        
        expColNames = new String [] {"C1", "C2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "2"},
            {"1", "3"},
            {"2", "1"},
            {"2", "3"},
            {"3", "1"},
            {"3", "2"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv7");
        
        expColNames = new String [] {"I", "C1"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"},
            {"1", "1"},
            {"1", "1"},
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from cv8");
        
        expColNames = new String [] {"COL1", "COL2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"Column 1", "Value = 1"},
            {"Column 1", "Value = 1"},
            {"Column 1", "Value = 1"},
            {"Column 1", "Value = 1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select * from cv3) x order by 1,2");
        
        expColNames = new String [] {"I", "S", "F", "DP"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1", "1.0", "1.0"},
            {"1", "2", "3.0", "4.0"},
            {"2", "1", "4.0", "3.0"},
            {"7", "8", "5.0", "6.0"},
            {"8", "7", "6.0", "5.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select * from cv4) x order by 1");
        
        expColNames = new String [] {"DISTINCT_I"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1"},
            {"8"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        rs = st.executeQuery(
            " select * from (select * from cv5) x");
        
        expColNames = new String [] {"I", "S"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // verify that we can create and drop indexes on 
        // underlying tables
        
        st.executeUpdate(
            "create index i on t1(i)");
        
        st.executeUpdate(
            " drop index i");
        
        // verify the consistency of the indexes on the system 
        // catalogs
        
        rs = st.executeQuery(
            "select tablename, "
            + "SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename) from "
            + "sys.systables where CAST(tabletype AS CHAR(1)) = "
            + "'S' and CAST(tablename AS VARCHAR(128)) != 'SYSDUMMY1'");
        
        expColNames = new String [] {"TABLENAME", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SYSCONGLOMERATES", "1"},
            {"SYSTABLES", "1"},
            {"SYSCOLUMNS", "1"},
            {"SYSSCHEMAS", "1"},
            {"SYSCONSTRAINTS", "1"},
            {"SYSKEYS", "1"},
            {"SYSDEPENDS", "1"},
            {"SYSALIASES", "1"},
            {"SYSVIEWS", "1"},
            {"SYSCHECKS", "1"},
            {"SYSFOREIGNKEYS", "1"},
            {"SYSSTATEMENTS", "1"},
            {"SYSFILES", "1"},
            {"SYSTRIGGERS", "1"},
            {"SYSSTATISTICS", "1"},
            {"SYSTABLEPERMS", "1"},
            {"SYSCOLPERMS", "1"},
            {"SYSROUTINEPERMS", "1"},
            {"SYSROLES", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // test inserts from a view
        
        st.executeUpdate(
            "insert into insert_test select * from sv5");
        
        rs = st.executeQuery(
            " select * from insert_test");
        
        expColNames = new String [] {"F"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"1.0"},
            {"2.0"},
            {"3.0"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // drop the views
        
        st.executeUpdate(
            "drop view cv8");
        
        st.executeUpdate(
            " drop view cv7");
        
        st.executeUpdate(
            " drop view cv6");
        
        st.executeUpdate(
            " drop view cv5");
        
        st.executeUpdate(
            " drop view cv4");
        
        st.executeUpdate(
            " drop view cv3");
        
        st.executeUpdate(
            " drop view cv2");
        
        st.executeUpdate(
            " drop view cv1");
        
        st.executeUpdate(
            " drop view sv5");
        
        st.executeUpdate(
            " drop view sv4");
        
        st.executeUpdate(
            " drop view sv3");
        
        st.executeUpdate(
            " drop view sv2");
        
        st.executeUpdate(
            " drop view sv1");
        
        // drop the tables
        
        st.executeUpdate(
            "drop table t1");
        
        st.executeUpdate(
            " drop table t2");
        
        st.executeUpdate(
            " drop table insert_test");
        
        // verify the consistency of the indexes on the system 
        // catalogs
        
        rs = st.executeQuery(
            "select tablename, "
            + "SYSCS_UTIL.SYSCS_CHECK_TABLE('SYS', tablename) from "
            + "sys.systables where CAST(tabletype as CHAR(1)) = "
            + "'S' and CAST(tablename  as VARCHAR(128)) != 'SYSDUMMY1'");
        
        expColNames = new String [] {"TABLENAME", "2"};
        JDBC.assertColumnNames(rs, expColNames);
        
        expRS = new String [][]
        {
            {"SYSCONGLOMERATES", "1"},
            {"SYSTABLES", "1"},
            {"SYSCOLUMNS", "1"},
            {"SYSSCHEMAS", "1"},
            {"SYSCONSTRAINTS", "1"},
            {"SYSKEYS", "1"},
            {"SYSDEPENDS", "1"},
            {"SYSALIASES", "1"},
            {"SYSVIEWS", "1"},
            {"SYSCHECKS", "1"},
            {"SYSFOREIGNKEYS", "1"},
            {"SYSSTATEMENTS", "1"},
            {"SYSFILES", "1"},
            {"SYSTRIGGERS", "1"},
            {"SYSSTATISTICS", "1"},
            {"SYSTABLEPERMS", "1"},
            {"SYSCOLPERMS", "1"},
            {"SYSROUTINEPERMS", "1"},
            {"SYSROLES", "1"}
        };
        
        JDBC.assertFullResultSet(rs, expRS, true);
        
        // bug 2745
        
        st.executeUpdate(
            "CREATE TABLE orgtable ( name VARCHAR(255), "
            + "supervisorname VARCHAR(255), jobtitle VARCHAR(255) )");
        
        st.executeUpdate(
            " CREATE VIEW orgview AS SELECT name, "
            + "supervisorname, jobtitle FROM orgtable");
        
        rs = st.executeQuery(
            " SELECT name,jobtitle FROM orgview WHERE name IN "
            + "(SELECT supervisorname FROM orgview WHERE name LIKE "
            + "'WYATT%')");
        
        expColNames = new String [] {"NAME", "JOBTITLE"};
        JDBC.assertColumnNames(rs, expColNames);
        JDBC.assertDrainResults(rs, 0);
        
        st.executeUpdate(
            " drop view orgview");
        
        st.executeUpdate(
            " drop table orgtable");
        
        // reset autocommit
        
        getConnection().setAutoCommit(true);
        
        // DERBY-1304 view not getting dropped The second drop 
        // view statement fails before the patch
        
        st.executeUpdate(
            "CREATE SCHEMA TEST_SCHEMA");
        
        st.executeUpdate(
            " CREATE TABLE TEST_SCHEMA.T1 (TABLE_COLUMN LONG VARCHAR)");
        
        st.executeUpdate(
            " CREATE VIEW TEST_SCHEMA.V1 AS SELECT TABLE_COLUMN "
            + "AS VIEW_COLUMN FROM TEST_SCHEMA.T1");
        
        st.executeUpdate(
            " DROP VIEW TEST_SCHEMA.V1");
        
        st.executeUpdate(
            " DROP TABLE TEST_SCHEMA.T1");
        
        st.executeUpdate(
            " DROP SCHEMA TEST_SCHEMA RESTRICT");
        
        // reiterate
        
        st.executeUpdate(
            "CREATE SCHEMA TEST_SCHEMA");
        
        st.executeUpdate(
            " CREATE TABLE TEST_SCHEMA.T1 (TABLE_COLUMN LONG VARCHAR)");
        
        st.executeUpdate(
            " CREATE VIEW TEST_SCHEMA.V1 AS SELECT TABLE_COLUMN "
            + "AS VIEW_COLUMN FROM TEST_SCHEMA.T1");
        
        st.executeUpdate(
            " DROP VIEW TEST_SCHEMA.V1");
        
        st.executeUpdate(
            " DROP TABLE TEST_SCHEMA.T1");
        
        st.executeUpdate(
            " DROP SCHEMA TEST_SCHEMA RESTRICT");
        
        // DERBY-2018 expect error
        
        assertStatementError("42X07", st,
            "CREATE VIEW v1(c1) AS VALUES NULL");
        
        // DERBY-681
        
        st.executeUpdate(
            "create table o (name varchar(20), ord int)");
        
        st.executeUpdate(
            " create table a (ord int, amount int)");
        
        st.executeUpdate(
            " create view v1 (vx, vy) as select name, sum(ord) "
            + "from o where ord > 0 group by name, ord");
        
        st.executeUpdate(
            " create view v2 (vx, vy) as select name, sum(ord) "
            + "from o where ord > 0 group by name, ord having ord "
            + "<= ANY (select ord from a)");
        
        st.executeUpdate(
            " drop view v2");
        
        st.executeUpdate(
            " drop view v1");
        
        st.executeUpdate(
            " drop table a");
        
        st.executeUpdate(
            " drop table o");
        
        
        
        getConnection().rollback();
        st.close();
    }

   /**
    * DERBY-3270 Test that we can select from a view in another schema if the
    * default schema does not exist.
    *
    * @throws SQLException
    */
    public void testSelectViewFromOtherSchemaWithNoDefaultSchema()
            throws SQLException {
        Connection conn = openDefaultConnection("joe","joepass");
        Statement st = conn.createStatement();
        st.execute("create table mytable(a int)");
        st.execute("insert into mytable values (99)");
        st.execute("create view myview as select * from mytable");
        st.close();
        conn.close();
        Connection conn2 = openDefaultConnection("bill","billpass");
        Statement st2 = conn2.createStatement();
        ResultSet rs = st2.executeQuery("SELECT * FROM JOE.MYVIEW");
        JDBC.assertFullResultSet(rs,new String[][] {{"99"}});
        st2.executeUpdate("drop view joe.myview");
        st2.executeUpdate("drop table joe.mytable");
        st2.close();
        conn2.close();
   }

    /**
     * Make sure DatabaseMetaData.getColumns is correct when we have a view
     * created when there is an expression in the select list.
     * Also check the ResultSetMetaData
     * @throws SQLException
     */
    public void testViewMetaDataWithGeneratedColumnsDerby4230() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("create table A (id integer, data varchar(20), data2 integer)");
        s.executeUpdate("insert into A values (3, 'G', 5), (23, 'G', 4), (5, 'F', 1), (2, 'H', 4), (1, 'F', 5)");
        //DERBY-4230. Make sure DatabaseMetaData.getColumns does not include generated columns.
        // You need an expression in the select list.
        s.executeUpdate("create view V (data, num) as select data, data2 + 2 from A group by data, data2");
        DatabaseMetaData dmd = getConnection().getMetaData();
        ResultSet columns = dmd.getColumns(null, null, "V", null);
        String[][] expectedDBMetaRows = new String[][] {{"","APP","V","DATA","12","VARCHAR","20",null,null,null,"1","",null,null,null
            ,"40","1","YES",null,null,null,null,"NO"},
            {"","APP","V","NUM","4","INTEGER","10",null,"0","10","1","",null,null,null,null,"2","YES",null,null,null,null,"NO"}};  
        JDBC.assertFullResultSet(columns,expectedDBMetaRows);
        // Make sure ResultSetMetaData is right when selecting from view.
        // This wasn't a problem with DERBY-4230, but checking for good measure.
        ResultSet rs = s.executeQuery("SELECT * FROM V");        
        JDBC.assertColumnNames(rs, new String[] {"DATA","NUM"});
        JDBC.assertColumnTypes(rs, new int[] {java.sql.Types.VARCHAR, java.sql.Types.INTEGER});
        JDBC.assertNullability(rs,new boolean[] {true,true});
        // Finally check the results.
        String [][] expectedRows = new String[][] {{"F","3"},
            {"F","7"},
            {"G","6"},
            {"G","7"},
            {"H","6"}};
        JDBC.assertFullResultSet(rs, expectedRows);
        s.executeUpdate("DROP VIEW V");
        s.executeUpdate("DROP TABLE A");        
    }
}
