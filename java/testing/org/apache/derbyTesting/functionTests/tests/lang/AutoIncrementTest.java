/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AutoIncrementTest
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
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;

public class AutoIncrementTest extends BaseJDBCTestCase {
	public AutoIncrementTest(String name)
	{
		super (name);
	}
	/**
	 * converted from autoincrement.sql.  
	 * @throws SQLException
	 */
	private static void createSchemaObjects(Statement st)
	throws SQLException
	{
		
		st.executeUpdate("create table ai_zero (i int, a_zero int generated always as identity)");
		st.executeUpdate("create table ai_one (i int, a_one smallint generated always as identity)");
		st.executeUpdate("create table ai_two (i int, a_two int generated always as identity)");
		st.executeUpdate("create table ai_three (i int, a_three int generated always as identity)");
		st.executeUpdate("create table ai (i  int, autoinc int generated always as identity (start with 100))");
		st.executeUpdate("create table ai1 (i int, autoinc1 int generated always as identity (increment by 100))");
		st.executeUpdate("create table ai2 (i int,autoinc2 int generated always as identity (start with 101, increment by 100))"); 
		st.executeUpdate("create table ai3 (i int,a11 int generated always as identity (start with  0, increment by -1))");
		st.executeUpdate("create table ai4 (i int,a21 int generated always as identity (start with  +0, increment by -1))");
		st.executeUpdate("create table ai5 (i int, a31 int generated always as identity (start with  -1, increment by -1))");
		st.executeUpdate("create table ai6 (i int, a41 int generated always as identity (start with  -11, increment by +100))");
		//-- **** simple increment tests.
		st.executeUpdate("create table ai_short (i int,ais smallint generated always as identity (start with 0, increment by 2))");
		st.executeUpdate("create table ai_single1 (i int, a0 int generated always as identity (start with  -1, increment by -1))");
		st.executeUpdate("create table ai_single2 (i int, a1 smallint generated always as identity)");
		st.executeUpdate("create table ai_single3 (i int, a2 int generated always as identity (start with 0))");
		st.executeUpdate("create table ai_single4 (i int, a3 bigint generated always as identity(start with  -100, increment by 10))");


		st.executeUpdate("create table ai_test (x int generated always as identity (start with 2, increment by 2),y int)");

		st.executeUpdate("create table ai_single1conn (c char(100), a_odd int generated always as identity (start with 1, increment by 2))");
		st.executeUpdate("create table ai_single2conn (c char(100), a_even int generated always as identity (start with 0, increment by 2))");
		st.executeUpdate("create table ai_single3conn (c char(100), a_sum bigint generated always as identity (start with 1, increment by 2))");
		
		//-- triggers 
		st.executeUpdate("create table t1 (c1 int generated always as identity, name char(32))");
		st.executeUpdate("create table t2 (c2 int generated always as identity, name char(32))");
		st.executeUpdate("create trigger insert_trigger after insert on t1 for each row insert into t2 (name) values ('Bob Finocchio')");		
		st.executeUpdate("create table tab1(s1 int generated always as identity,lvl int)");
		st.executeUpdate("create table tab3 (c1 int)");
		st.executeUpdate("create trigger tab1_after1 after insert on tab3 referencing new as newrow for each row insert into tab1 (lvl) values 1,2,3");
		st.executeUpdate("create table tab1schema (i int, a1 int generated always as identity (start with -1, increment by -1))");
		st.executeUpdate("create table tab2schema (i int, a2 smallint generated always as identity (start with 1, increment by +1))");
		st.executeUpdate("create table tab3schema (i int, a1 int generated always as identity (start with 0, increment by -2))");
		st.executeUpdate("create table tab4schema (i int, a2 bigint generated always as identity (start with 0, increment by 2))");		

		st.executeUpdate("create table t1_1 (x int, s1 int generated always as identity)");
		st.executeUpdate("create table t2_1 (x smallint, s2 int generated always as identity (start with 0))");
		st.executeUpdate("create table t1_2 (s1 int generated always as identity)");
		st.executeUpdate("alter table t1_2 add column x int");		
		st.executeUpdate("create table t2_2 (s2 int generated always as identity (start with 2))");
		st.executeUpdate("alter table t2_2 add column x int");
		st.executeUpdate("create table t3_2 (s0 int generated always as identity (start with 0))");
		st.executeUpdate("alter table t3_2 add column x int");
		//-- test some more generated column specs
		st.executeUpdate("create table trigtest (s1 smallint generated always as identity, lvl int)");
		st.executeUpdate("create table t1_col (x char(2) default 'yy', y bigint generated always as identity)");
		//conn.setAutoCommit(false);
		st.executeUpdate("create table testme (text varchar(10), autonum int generated always as identity)");
		//conn.commit();		
		st.executeUpdate("create table ai_neg (x smallint generated always as identity, y int)");
		st.executeUpdate("create table ai_over1 (x int, y int generated always as identity (increment by 200000000))");
		st.executeUpdate("create table ai_over2 (x int, y smallint generated always as identity (start with  -32760, increment by -1))");
		st.executeUpdate("create table ai_over3 (x int, y int generated always as identity (start with  2147483646))");
		st.executeUpdate("create table ai_over4 (x int, y bigint generated always as identity(start with     9223372036854775805))");
		st.executeUpdate("create table base (x int)");
		// testing non-reserved keywords: generated, start, always
		// should be successful
		st.executeUpdate("create table always (a int)");
		st.executeUpdate("create table start (a int)");
		st.executeUpdate("create table generated (a int)");
		st.executeUpdate("create table idt1(c1 int generated always as identity, c2 int)");
		st.executeUpdate("create table autoinct2 (a int, b int generated always as identity)");
		st.executeUpdate("create table autoinct1(c1 int generated always as identity)");
		st.executeUpdate("create table autoinct3(c1 int generated always as identity (increment by 3))");
		st.execute("create table withinct1(i int, withinct1_autogen int generated always as identity)");
		st.execute("create table withinct2(i int, withinct2_autogen int generated by default as identity)");
		st.execute("create table withinctempt1(i int, withinct1_autogen int generated always as identity)");
		st.execute("create table withinctempt2(i int, withinct2_autogen int generated by default as identity)");
		st.execute("create table withinct3(i int, withinct3_autogen int generated always as identity(increment by 10))");
		st.execute("create table withinct4(i int, withinct4_autogen int generated by default as identity(increment by 10))");
		st.execute("create table variantt1 (c11 int generated always as identity (start with 101, increment by 3), c12 int)");
		st.execute("create table variantt2 (c21 int generated always as identity (start with 201, increment by 5), c22 int)");
		st.execute("create trigger variantt1tr1 after insert on variantt1 for each row insert into variantt2 (c22) values (1)");
		st.execute("create table restartt1 (rec11 int generated by default as identity(start with 2, increment by 2), c12 int)");
		st.execute("create table cycle1 (rec21 int generated by default as identity(start with 2, increment by 2), c32 int)");
		st.execute("create table t1lock(lockc11 int generated by default as identity (start with 1, increment by 1), c12 int)");
		st.execute("create unique index t1locki1 on t1lock(lockc11)");
		//-- Since RESTART is not a reserved keyword, we should be able to create a table with name RESTART
		st.execute("create table restart (c11 int)");
		st.execute("create table newTable (restart int)");
		st.execute("create table newTable2 (c11 int)");
		st.execute("alter table newTable2 add column RESTART int");
		st.execute("CREATE TABLE DERBY_1495 (testid INT GENERATED BY DEFAULT AS IDENTITY(START WITH 1, INCREMENT BY 1) NOT NULL,testcol2 INT NOT NULL)");
		st.execute("create table derby_1645 (testTableId INTEGER GENERATED BY DEFAULT AS IDENTITY NOT NULL,testStringValue VARCHAR(20) not null,constraint PK_derby_1645 primary key (testTableId))");
		st.execute("create table D1644 (d1644c1 int, d1644c2 int generated by default as identity)");
		st.execute("create table D1644_A (d1644_Ac1 int, d1644_Ac2 int generated by default as identity, c3 int)");
		st.execute("create table D1644_B (d1644_Bc1 int generated by default as identity)");
		st.execute("create table d4006 (x varchar(5) default 'abc')");
		st.execute("create table d4006_a (z int generated always as identity)");
		st.execute("create table d4419_t1(x int)");
		st.execute("insert into d4419_t1 values 1,2");
		st.execute("create table d4419_t2(x int)");
		st.execute("insert into d4419_t2 values 2,3");
		st.execute("create table d4419_t3(x int, y int generated always as identity)");
		st.execute("insert into d4419_t3(x) select * from d4419_t1 union select * from d4419_t2");
		st.execute("create table lockt1 (x int, yyyy int generated always as identity (start with  0))");
		st.execute("create view lock_table as select cast(username as char(8)) as username, cast(t.type as char(8)) as trantype,cast(l.type as char(8)) as type, cast(lockcount as char(3)) as cnt, mode, cast(tablename as char(12)) as tabname,state, status from  syscs_diag.lock_table l right outer join syscs_diag.transaction_table t on l.xid = t.xid  where t.type='UserTransaction' and l.lockcount is not null");
		st.execute("create table uniquet1(i int, t1_autogen int generated always as identity(start with 100, increment by 20))");
		st.execute("create table uniquet2(i int, t2_autogen int generated by default as identity(start with 100, increment by 20))");
		st.execute("create table uniquetempt1(i int, t1_autogen int generated always as identity(start with 100, increment by 20))");
		st.execute("create table uniquetempt2(i int, t2_autogen int generated by default as identity(start with 100, increment by 20))");
		st.execute("create table uniquet3(i int,uniquet3_autogen int generated by default as identity(start with 0, increment by 1) unique)");
		st.execute("create table uniquet4(i int,uniquet4_autogen int generated by default as identity(start with 0, increment by 1))");
		st.execute("create unique index idx_uniquet4_autogen on uniquet4(uniquet4_autogen)");
		st.execute("create table withinctempt3(i int, t1_autogen int generated always as identity(increment by 10))");
		st.execute("create table withinctempt4(i int, t2_autogen int generated by default as identity(increment by 10))");
	}
	public void testderbyIncrementTest() throws Exception
	{
		ResultSet rs;
		Statement s = createStatement();
		rs = s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME in ('A_ZERO','A_ONE', 'A_TWO', 'A_THREE') order by COLUMNNAME");
		String [][]expectedRows=
            {
				{"A_ONE","1","1","1"},
                {"A_THREE","1","1","1"},
                {"A_TWO","1","1","1"},
                {"A_ZERO","1","1","1"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testautoIncSysColTest()  throws Exception
	{
		ResultSet rs;
		String [][]expectedRows;
		
		Statement s = createStatement();
		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'AUTOINC'");
		expectedRows=new String[][]{{"100","100","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'AUTOINC1'");
		expectedRows=new String[][]{{"1","1","100"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'AUTOINC2'");
		expectedRows=new String[][]{{"101","101","100"}};
		JDBC.assertFullResultSet(rs,expectedRows);
	}
	public void testnegative() throws Exception
	{
		//-- try -ive numbers.
		ResultSet rs;
		
		
		Statement s = createStatement();
		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'A11'");
		String [][]expectedRows=new String[][]{{"0","0","-1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'A21'");
		expectedRows=new String[][]{{"0","0","-1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'A31'");
		expectedRows=new String[][]{{"-1","-1","-1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs = s.executeQuery("select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'A41'");
		expectedRows=new String[][]{{"-11","-11","100"}};
		JDBC.assertFullResultSet(rs,expectedRows);


	}
	
	public void testsimpleincrement() throws Exception
	{
		/*change*/
		ResultSet rs;
		Statement s = createStatement();
		s.executeUpdate("insert into ai_short (i) values (0)");
		s.executeUpdate("insert into ai_short (i) values (1)");
		s.executeUpdate("insert into ai_short (i) values (2)");
		s.executeUpdate("insert into ai_short (i) values (33)");
		rs = s.executeQuery("select * from ai_short order by i");
		String[][]expectedRows=new String[][]{{"0","0"},{"1","2"},{"2","4"},{"33","6"}};
		JDBC.assertFullResultSet(rs,expectedRows);
        vetSequenceState( "ai_short", 8, 0, 2 );
	}
    private void    vetSequenceState( String tableName, long currentValue, long startValue, long stepValue )
        throws Exception
    {
        Connection  conn = getConnection();
        String  sequenceName = IdentitySequenceTest.getIdentitySequenceName( conn, tableName );
        ResultSet   rs = conn.prepareStatement
            (
             "select s.startValue, s.increment\n" +
             "from sys.syssequences s\n" +
             "where sequenceName = '" + sequenceName + "'"
             ).executeQuery();
        String[][]  expectedRows = new String[][]
        {
            { Long.toString( startValue ), Long.toString( stepValue ) }
        };
        JDBC.assertFullResultSet( rs,expectedRows );

        rs = conn.prepareStatement
            (
             "values syscs_util.syscs_peek_at_identity( 'APP', '" + tableName.toUpperCase() + "' )"
             ).executeQuery();
        expectedRows = new String[][] { { Long.toString( currentValue ) } };
        JDBC.assertFullResultSet( rs,expectedRows );
    }
	public void testonegeneratedcolumn() throws Exception
	{
		//-- table with one generated column spec should succeed
		ResultSet rs;
		Statement s = createStatement();
		Integer i= 1;

		while (i.intValue()< 11)
		{
			String mysql="insert into ai_single1 (i) values ("+i.toString()+")";
			s.executeUpdate(mysql);
			mysql="insert into ai_single2 (i) values ("+i.toString()+")";
			s.executeUpdate(mysql);
			mysql="insert into ai_single3 (i) values ("+i.toString()+")";
			s.executeUpdate(mysql);
			mysql="insert into ai_single4 (i) values ("+i.toString()+")";
			s.executeUpdate(mysql);
			int j=i.intValue()+1;
			i= j;
		}
		rs = s.executeQuery("select a.i, a0, a1, a2, a3 from ai_single1 a join ai_single2 b on a.i = b.i join ai_single3 c on a.i = c.i join ai_single4 d on a.i = d.i order by a.i");
		String[][]expectedRows=new String[][]{{"1","-1","1","0","-100"},{"2","-2","2","1","-90"},
				{"3","-3","3","2","-80"},{"4","-4","4","3","-70"},
				{"5","-5","5","4","-60"},{"6","-6","6","5","-50"},
				{"7","-7","7","6","-40"},{"8","-8","8","7","-30"},
				{"9","-9","9","8","-20"},{"10","-10","10","9","-10"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("delete from ai_single1");
		s.executeUpdate("delete from ai_single2");
		s.executeUpdate("delete from ai_single3");
		s.executeUpdate("delete from ai_single4");
		s.executeUpdate("insert into ai_single1 (i) values (1)");
		s.executeUpdate("insert into ai_single2 (i) values (1)");
		s.executeUpdate("insert into ai_single3 (i) values (1)");
		s.executeUpdate("insert into ai_single4 (i) values (1)");
		rs=s.executeQuery("select a.i, a0, a1, a2, a3 from ai_single1 a join ai_single2 b on a.i = b.i join ai_single3 c on a.i = c.i join ai_single4 d on a.i = d.i");
		expectedRows=new String[][]{{"1","-11","11","10","0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		//-- table with more than one generated column spec should fail
		assertStatementError("428C1", s,"create table ai_multiple (i int, a0 int generated always as identity (start with  -1,increment by -1),a1 smallint generated always as identity,a2 int generated always as identity (start with  0),a3 bigint generated always as identity (start with  -100,increment by 10))");

	}
	public void testConnectionInfo() throws Exception
	{
		//-- **** connection info tests {basic ones}
		ResultSet rs;
		Statement s = createStatement();
		s.executeUpdate("insert into ai_test (y) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		String[][]expectedRows=new String[][]{{null}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_test (y) select y+10 from ai_test");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");		
		//-- try some more connection info tests
		expectedRows=new String[][]{{null}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single1conn (c) values ('a')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single2conn (c) values ('a')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single3conn (c) values ('a')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single1conn (c) values ('b')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single2conn (c) values ('b')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into ai_single3conn (c) values ('b')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"3"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testTrigger() throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		s.executeUpdate("insert into t1 (name) values ('Phil White')");
		rs=s.executeQuery("select * from t1");
		String[][]expectedRows=new String[][]{{"1","Phil White"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from t2");
		expectedRows=new String[][]{{"1","Bob Finocchio"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t2 (name) values ('Jean-Yves Dexemier')");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		
	}
	public void testSchema() throws Exception
	{
		//-- insert into multiple tables in different schema names with same tablename,column names
		//-- make sure 
		//-- lastAutoincrementValue shouldn't get confused.....
		ResultSet rs;
		Statement s=createStatement();
		s.executeUpdate("create schema BPP");
		s.executeUpdate("set schema BPP");
		s.executeUpdate("create table tab1schema (i int, a1 int generated always as identity (start with 100, increment by 1))");
		s.executeUpdate("create table tab2schema (i int, a2 bigint generated always as identity (start with 100, increment by -1))");
		s.executeUpdate("create table tab3schema (i int, a1 int generated always as identity (start with 100, increment by 2))");
		s.executeUpdate("create table tab4schema (i int, a2 smallint generated always as identity (start with 100, increment by -2))");
		s.executeUpdate("insert into APP.tab1schema (i) values (1)");
		s.executeUpdate("insert into APP.tab2schema (i) values (1)");
		s.executeUpdate("insert into APP.tab3schema (i) values (1)");
		s.executeUpdate("insert into APP.tab4schema (i) values (1)");
		s.executeUpdate("insert into tab1schema (i) values (1)");
		s.executeUpdate("insert into tab1schema (i) values (2)");
		s.executeUpdate("insert into tab2schema (i) values (1)");
		s.executeUpdate("insert into tab2schema (i) values (2)");
		s.executeUpdate("insert into tab3schema (i) values (1)");
		s.executeUpdate("insert into tab3schema (i) values (2)");
		s.executeUpdate("insert into tab4schema (i) values (1)");
		s.executeUpdate("insert into tab4schema (i) values (2)");
		rs=s.executeQuery("select a.i, a1, a2 from app.tab1schema a join app.tab2schema b on a.i = b.i");
		String[][]expectedRows=new String[][]{{"1","-1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select a.i, a1, a2 from app.tab3schema a join app.tab4schema b on a.i = b.i");
		expectedRows=new String[][]{{"1","0","0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select a.i, a1, a2 from tab1schema a join tab2schema b on a.i = b.i order by a.i");
		expectedRows=new String[][]{{"1","100","100"},{"2","101","99"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select a1, a2, a.i from tab3schema a join tab4schema b on a.i = b.i order by a1");
		expectedRows=new String[][]{{"100","100","1"},{"102","98","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"98"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("set schema APP");
		s.executeUpdate("drop table BPP.tab1schema");
		s.executeUpdate("drop table BPP.tab2schema");
		s.executeUpdate("drop table BPP.tab3schema");
		s.executeUpdate("drop table BPP.tab4schema");
		s.executeUpdate("drop schema BPP restrict");
		s.executeUpdate("insert into tab3 values null");
		rs=s.executeQuery("select * from tab1 order by s1");
		expectedRows=new String[][]{{"1","1"},{"2","2"},{"3","3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
        vetSequenceState( "TAB1", 4, 1, 1 );
		s.executeUpdate("create table tab2 (lvl int, s1  bigint generated always as identity)");
		s.executeUpdate("create trigger tab1_after2 after insert on tab3 referencing new as newrow for each row insert into tab2 (lvl) values 1,2,3");
		s.executeUpdate("insert into tab3 values null");
		rs=s.executeQuery("select * from tab2 order by lvl");
		expectedRows=new String[][]{{"1","1"},{"2","2"},{"3","3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
        vetSequenceState( "TAB2", 4, 1, 1 );
	}
	public void testadditionalSysCol() throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		s.executeUpdate("insert into t1_1 (x) values (1)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		String[][]expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t1_1 (x) values (2)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t2_1 (x) values (1)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t1_2 (x) values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t2_2 (x) values (1),(2),(3),(4),(5)");		
		s.executeUpdate("insert into t3_2 (x) values (1),(2),(3),(4),(5)");
		rs=s.executeQuery("select a.x, s1, s2, s0 from t1_2 a join t2_2 b on a.x = b.x join t3_2 c on a.x = c.x order by a.x");
		expectedRows=new String[][]{{"1","1","2","0"},{"2","2","3","1"},{"3","3","4","2"},{"4","4","5","3"},{"5","5","6","4"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into trigtest (lvl) values (0)");
		s.executeUpdate("insert into trigtest (lvl) values (1),(2)");
		s.executeUpdate("insert into trigtest (lvl) values (3),(4)");
		s.executeUpdate("insert into trigtest (lvl) values (5),(6)");
		s.executeUpdate("insert into trigtest (lvl) values (7),(8)");
		rs=s.executeQuery("select * from trigtest order by s1");
		expectedRows=new String[][]{{"1","0"},{"2","1"},{"3","2"},{"4","3"},{"5","4"},{"6","5"},{"7","6"},{"8","7"},{"9","8"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select count(*) from t1_2");
		expectedRows=new String[][]{{"5"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("delete from t1_2");
		s.executeUpdate("delete from t2_2");
		s.executeUpdate("delete from t3_2");
		s.executeUpdate("insert into t1_2 (x) values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t2_2 (x) values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t3_2 (x) values (1),(2),(3),(4),(5)");
		rs=s.executeQuery("select a.x, s1, s2, s0 from t1_2 a join t2_2 b on a.x = b.x join t3_2 c on a.x = c.x order by a.x");
		expectedRows=new String[][]{{"1","6","7","5"},{"2","7","8","6"},{"3","8","9","7"},{"4","9","10","8"},{"5","10","11","9"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t1_2 (x) values (6)");
		s.executeUpdate("insert into t2_2 (x) values (6)");
		s.executeUpdate("insert into t3_2 (x) values (6)");
		rs=s.executeQuery("select a.x, s1, s2, s0 from t1_2  a join t2_2 b on a.x = b.x join t3_2 c on a.x = c.x order by a.x");
		expectedRows=new String[][]{{"1","6","7","5"},{"2","7","8","6"},{"3","8","9","7"},{"4","9","10","8"},{"5","10","11","9"},{"6","11","12","10"}};
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"10"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("delete from t1_2");
		s.executeUpdate("delete from t2_2");
		s.executeUpdate("delete from t3_2");
		s.executeUpdate("insert into t1_2 (x) values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t2_2 (x) values (1),(2),(3),(4),(5)");
		s.executeUpdate("insert into t3_2 (x) values (1),(2),(3),(4),(5)");
		rs=s.executeQuery("select a.x, s1, s2, s0 from t1_2 a join t2_2 b on a.x = b.x join t3_2 c on a.x = c.x order by a.x");
		expectedRows=new String[][]{{"1","12","13","11"},{"2","13","14","12"},{"3","14","15","13"},{"4","15","16","14"},{"5","16","17","15"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into t1_2 (x) values (6)");
		s.executeUpdate("insert into t2_2 (x) values (6)");
		s.executeUpdate("insert into t3_2 (x) values (6)");
		rs=s.executeQuery("select a.x, s1, s2, s0 from t1_2 a join t2_2 b on a.x = b.x join t3_2 c on a.x = c.x order by a.x");
		expectedRows=new String[][]{{"1","12","13","11"},{"2","13","14","12"},{"3","14","15","13"},{"4","15","16","14"},{"5","16","17","15"},{"6","17","18","16"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"16"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testsyslocks()throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		setAutoCommit(false);
		s.execute("insert into lockt1 (x) values (1)");
		s.execute("insert into lockt1 (x) values (2)");
		rs=s.executeQuery("select * from lockt1 order by x");
		String[][]expectedRows=new String[][]{{"1","0"},{"2","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from lock_table order by username, trantype, type, cnt");
		//Utilities.showResultSet(rs);
		expectedRows=new String[][]
            {
                {"APP   ","UserTran","ROW     ","1  ","X","LOCKT1      ","GRANT","ACTIVE"},
                {"APP   ","UserTran","ROW     ","1  ","X","LOCKT1      ","GRANT","ACTIVE"},
                {"APP   ","UserTran","TABLE   ","2  ","IX","LOCKT1      ","GRANT","ACTIVE"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("delete from lockt1");
		commit();
		rs=s.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt");
		expectedRows=new String[][]{};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("set isolation serializable");
		rs=s.executeQuery("select columnname, autoincrementvalue from sys.syscolumns where columnname = 'YYYY'");
		expectedRows=new String[][]
            {
                {"APP     ","UserTran","TABLE   ","1   ","S   ","SYSCOLUMNS  ","GRANT","ACTIVE"}
            };
		rs=s.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt");
		expectedRows=new String[][]
            {
                {"APP     ","UserTran","TABLE   ","1  ","S","SYSCOLUMNS  ","GRANT","ACTIVE"}
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into lockt1 (x) values (3)");
		rs=s.executeQuery("select * from lock_table order by tabname, type desc, mode, cnt");
		expectedRows=new String[][]
            {
                {"APP     ","UserTran","TABLE   ","1  ","IX","LOCKT1      ","GRANT","ACTIVE"},
                {"APP     ","UserTran","ROW     ","1  ","X","LOCKT1      ","GRANT","ACTIVE"},
                {"APP     ","UserTran","TABLE   ","1  ","S","SYSCOLUMNS  ","GRANT","ACTIVE"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		commit();
		
	}
	public void testColoumnSpecs() throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		
		
		s.executeUpdate("insert into t1_col (x, y) values ('aa', default)");
		s.executeUpdate("insert into t1_col values ('bb', default)");
		s.executeUpdate("insert into t1_col (x) values default");
		s.executeUpdate("insert into t1_col (x) values null");
		//-- switch the order of the columns
		s.executeUpdate("insert into t1_col (y, x) values (default, 'cc')");
		rs=s.executeQuery("select * from t1_col order by y");
		String[][]expectedRows=new String[][]{{"aa","1"},{"bb","2"},{"yy","3"},{null,"4"},{"cc","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testbug3450() throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		PreparedStatement ps=prepareStatement("insert into testme (text) values ?");
		//PreparedStatement ps=conn.prepareStatement();
		ps.setString(1, "one");
		ps.execute();
		ps.setString(1, "two");;
		ps.execute();
		ps.setString(1, "three");;
		ps.execute();
		rs=s.executeQuery("select * from testme order by autonum");
		String[][]expectedRows=new String[][]{{"one","1"},{"two","2"},{"three","3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		//-- give exact query and make sure that the statment cache doesn't
		//-- mess up things.
		ps.setString(1, "four");
		ps.execute();
		ps.setString(1, "four");
		ps.execute();
		rs=s.executeQuery("select * from testme order by autonum");
		expectedRows=new String[][]{{"one","1"},{"two","2"},{"three","3"},{"four","4"},{"four","5"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("drop table testme");

	}
	public void testnegativeinvalidtype() throws Exception
	{
		//-- negative tests from autoincrementNegative.sql
		//-- negative bind tests.
		//-- invalid types
		ResultSet rs;
		Statement pst=createStatement();
		Statement s=createStatement(); 
		
		assertStatementError("42Z22",  pst,"create table ni (x int, y char(1) generated always as identity)");
		assertStatementError("42Z22", pst,"create table ni (x int, y decimal(5,2) generated always as identity)");
		assertStatementError("42Z22", pst,"create table ni (x int, y float generated always as identity (start with 1, increment by 1))");
		assertStatementError("42Z22", pst,"create table ni (s int, y varchar(10) generated always as identity)");
		assertStatementError("42Z21", pst,"create table ni (x int, y int generated always as identity (increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y int generated always as identity (start with 0, increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y smallint generated always as identity (increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y smallint generated always as identity (start with 0, increment by 0))");
		assertStatementError("42X01", pst,"create table ni (x int, y int generated always as identity (increment by 0)");
		assertStatementError("42Z21", pst,"create table ni (x int, y int generated always as identity (start with 0, increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y bigint generated always as identity (increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y bigint generated always as identity (start with 0, increment by 0))");
		assertStatementError("42Z21", pst,"create table ni (x int, y bigint generated always as identity (start with 0, increment by 0))");
		assertStatementError("22003", pst,"create table ni (x int, y smallint generated always as identity (start with 32768))");
		assertStatementError("22003", pst,"create table ni (x int, y smallint generated always as identity (start with -32769))");
		assertStatementError("22003", pst,"create table ni (x int, y int generated always as identity (start with  2147483648))");
		assertStatementError("22003", pst,"create table ni (x int, y int generated always as identity (start with  -2147483649))");
		assertStatementError("42X49", pst,"create table ni (x int, y int generated always as identity (start with  9223372036854775808))");
		assertStatementError("42X49", pst,"create table ni (x int, y bigint  generated always as identity (start with  -9223372036854775809))");
		s.executeUpdate("insert into ai_neg (y) values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(10)");
		rs=s.executeQuery("select * from ai_neg order by x");
		String[][]expectedRows=new String[][]{{"1","0"},{"2","1"},{"3","2"},{"4","3"},{"5","4"},{"6","5"},{"7","6"},{"8","7"},{"9","8"},{"10","9"},{"11","10"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("delete from ai_neg where y=8 OR y=4");
		s.executeUpdate("insert into ai_neg (y) values (11),(13),(14),(15),(17),(18),(19)");
		rs=s.executeQuery("select * from ai_neg order by x");
		expectedRows=new String[][]{{"1","0"},{"2","1"},{"3","2"},{"4","3"},{"6","5"},{"7","6"},{"8","7"},{"10","9"},{"11","10"},{"12","11"},{"13","13"},{"14","14"},{"15","15"},{"16","17"},{"17","18"},{"18","19"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("update ai_neg set y=-y");
		rs=s.executeQuery("select * from ai_neg order by x");
		expectedRows=new String[][]{{"1","0"},{"2","-1"},{"3","-2"},{"4","-3"},{"6","-5"},{"7","-6"},{"8","-7"},{"10","-9"},{"11","-10"},{"12","-11"},{"13","-13"},{"14","-14"},{"15","-15"},{"16","-17"},{"17","-18"},{"18","-19"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("update ai_neg set y=-y");
		rs=s.executeQuery("select * from ai_neg order by x");
		expectedRows=new String[][]{{"1","0"},{"2","1"},{"3","2"},{"4","3"},{"6","5"},{"7","6"},{"8","7"},{"10","9"},{"11","10"},{"12","11"},{"13","13"},{"14","14"},{"15","15"},{"16","17"},{"17","18"},{"18","19"}};
		JDBC.assertFullResultSet(rs,expectedRows);		
		s.executeUpdate("update ai_neg set y=4 where y=3"); // doubt WARNING 02000
		rs=s.executeQuery("select * from ai_neg order by x");
		expectedRows=new String[][]{{"1","0"},{"2","1"},{"3","2"},{"4","4"},{"6","5"},{"7","6"},{"8","7"},{"10","9"},{"11","10"},{"12","11"},{"13","13"},{"14","14"},{"15","15"},{"16","17"},{"17","18"},{"18","19"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("42Z23", pst,"insert into ai_neg values (1,2)");

	}
	public  void testOverflow()throws Exception
	{
		ResultSet rs;
		Statement pst=createStatement();
		Statement s=createStatement();
		assertStatementError("2200H", pst,"insert into ai_over1 (x) values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19)");
		assertStatementError("2200H", pst,"insert into ai_over1 (x) values (1)");		
		s.executeUpdate("insert into ai_over2 (x) values (1),(2),(3),(4),(5),(6),(7),(8)");
		assertStatementError("2200H", pst,"insert into ai_over2 (x) values (9),(10)");
		String[][]expectedRows=new String[][]{{"1","-32760"},{"2","-32761"},{"3","-32762"},{"4","-32763"},{"5","-32764"},{"6","-32765"},{"7","-32766"},{"8","-32767"}};
		rs=s.executeQuery("select * from ai_over2 order by x");
		JDBC.assertFullResultSet(rs,expectedRows);		
		s.executeUpdate("insert into ai_over3 (x) values (1)");
		s.executeUpdate("insert into ai_over3 (x) values (2)");
		rs=s.executeQuery("select * from ai_over3 order by x");
		expectedRows=new String[][]{{"1","2147483646"},{"2","2147483647"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("2200H", pst,"insert into ai_over3 (x) select x from ai_over3");			
		//bigint overflow check		
		s.executeUpdate("insert into ai_over4 (x) values (1),(2),(3)");
		assertStatementError("2200H", pst,"insert into ai_over4 (x) values (4)");
		rs=s.executeQuery("select * from ai_over4 order by x");
		expectedRows=new String[][]
            {
                {"1","9223372036854775805"},
                {"2","9223372036854775806"},
                {"3","9223372036854775807"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testIdentity()throws Exception
	{
		//-- IDENTITY_VAL_LOCAL function, same as DB2, beetle 5354
		ResultSet rs;
		Statement pst=createStatement();
		Statement s=createStatement();
		pst.executeUpdate("insert into base values (1),(2),(3),(4),(5),(6)");

        String[][] expectedRows = {
            {"1", "10"},
            {"2", "11"},
            {"3", "12"},
            {"4", "13"},
            {"5", "14"},
            {"6", "15"},
        };

        assertUpdateCount(pst, 0, "alter table base add column y "
                + "smallint generated always as identity (start with 10)");
        JDBC.assertFullResultSet(
                pst.executeQuery("select * from base order by x"),
                expectedRows);
        assertUpdateCount(pst, 0, "alter table base drop column y");

        assertUpdateCount(pst, 0, "alter table base add column y "
                + "int generated always as identity (start with 10)");
        JDBC.assertFullResultSet(
                pst.executeQuery("select * from base order by x"),
                expectedRows);
        assertUpdateCount(pst, 0, "alter table base drop column y");

        assertUpdateCount(pst, 0, "alter table base add column y "
                + "bigint generated always as identity (start with 10)");
        JDBC.assertFullResultSet(
                pst.executeQuery("select * from base order by x"),
                expectedRows);
        assertUpdateCount(pst, 0, "alter table base drop column y");

        assertUpdateCount(pst, 0, "alter table base add column y "
                + "bigint generated always as identity (start with 10)");
        JDBC.assertFullResultSet(
                pst.executeQuery("select * from base order by x"),
                expectedRows);
        assertUpdateCount(pst, 0, "alter table base drop column y");

		rs=pst.executeQuery("select * from base order by x");
        expectedRows = new String[][]{{"1"},{"2"},{"3"},{"4"},{"5"},{"6"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) values (8)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select IDENTITY_VAL_LOCAL()+1, IDENTITY_VAL_LOCAL()-1 from idt1");
		expectedRows=new String[][]{{"2","0"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) values (IDENTITY_VAL_LOCAL())");
		rs=s.executeQuery("select * from idt1 order by c1");
		expectedRows=new String[][]{{"1","8"},{"2","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select IDENTITY_VAL_LOCAL()+1, IDENTITY_VAL_LOCAL()-1 from idt1");
		expectedRows=new String[][]{{"3","1"},{"3","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) values (8), (9)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from idt1 order by c1");
		expectedRows=new String[][]{{"1","8"},{"2","1"},{"3","8"},{"4","9"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) select c1 from idt1");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from idt1 order by c1");
		expectedRows=new String[][]{{"1","8"},{"2","1"},{"3","8"},{"4","9"},{"5","1"},{"6","2"},{"7","3"},{"8","4"}};
        JDBC.assertFullResultSet(rs, expectedRows);
		s.executeUpdate("delete from idt1");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) select c1 from idt1");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into idt1(c2) values (8)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"9"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testdefaultautoincrement() throws Exception
	{
		//-- test cases for beetle 5404: inserting multiple rows of defaults into autoincrement column.
		ResultSet rs;
		Statement s=createStatement();
		Statement pst=createStatement();
		s.executeUpdate("insert into autoinct1 values (default)");
		rs=s.executeQuery("select * from autoinct1");
		String[][]expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);		
		assertStatementError("42Z23", pst,"insert into autoinct1 values (1), (1)");
		assertStatementError("42Z23", pst,"insert into autoinct1 values (1), (default)");
		assertStatementError("42Z23", pst,"insert into autoinct1 values (default), (1)");
		assertStatementError("42Z23", pst,"insert into autoinct1 values (default), (default), (default), (2)");
		assertStatementError("42Z23", pst,"insert into autoinct1 values (default), (default), (2)");
		assertStatementError("42Z23", pst,"insert into autoinct1 values (default), (default), (2), (default)");
		s.executeUpdate("insert into autoinct1 values (default), (default)");
		rs=s.executeQuery("select * from autoinct1 order by c1");
		expectedRows=new String[][]{{"1"},{"2"},{"3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into autoinct1 values (default), (default), (default)");
		rs=s.executeQuery("select * from autoinct1 order by c1");
		expectedRows=new String[][]{{"1"},{"2"},{"3"},{"4"},{"5"},{"6"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into autoinct1 values (default), (default), (default),(default)");
		rs=s.executeQuery("select * from autoinct1 order by c1");
		expectedRows=new String[][]{{"1"},{"2"},{"3"},{"4"},{"5"},{"6"},{"7"},{"8"},{"9"},{"10"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		s.executeUpdate("insert into autoinct2 values (1, default), (2, default)");
		rs=s.executeQuery("select * from autoinct2 order by a");
		expectedRows=new String[][]{{"1","1"},{"2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("42Z23", pst,"insert into autoinct2 values (1, default), (2, 2)");
		assertStatementError("42Z23", pst,"insert into autoinct2 values (1, default), (2, default), (2, 2)");
		assertStatementError("42Z23",pst,"insert into autoinct2 values (1, 2), (2, default), (2, default)");

		s.executeUpdate("insert into autoinct3 values (default)");
		rs=s.executeQuery("select * from autoinct3");
		expectedRows=new String[][]{{"1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into autoinct3 values (default)");
		rs=s.executeQuery("select * from autoinct3 order by c1");
		expectedRows=new String[][]{{"1"},{"4"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("42Z23",pst,"insert into autoinct3 values (1), (default)");
		assertStatementError("42Z23",pst,"insert into autoinct3 values (default), (1)");
		assertStatementError("42Z23",pst,"insert into autoinct3 values (default), (default), (default), (2)");
		assertStatementError("42Z23",pst,"insert into autoinct3 values (default), (default), (2)");
		assertStatementError("42Z23",pst,"insert into autoinct3 values (default), (default), (2), (default)");
		assertStatementError("42Z23",pst,"insert into autoinct3 select * from autoinct1");
		s.executeUpdate("insert into autoinct3 values (default), (default)");
		rs=s.executeQuery("select * from autoinct3 order by c1");
		expectedRows=new String[][]{{"1"},{"4"},{"7"},{"10"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into autoinct3 values (default), (default), (default)");
		rs=s.executeQuery("select * from autoinct3 order by c1");
		expectedRows=new String[][]{{"1"},{"4"},{"7"},{"10"},{"13"},{"16"},{"19"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("insert into autoinct3 values (default), (default), (default),(default)");
		rs=s.executeQuery("select * from autoinct3 order by c1");
		expectedRows=new String[][]{{"1"},{"4"},{"7"},{"10"},{"13"},{"16"},{"19"},{"22"},{"25"},{"28"},{"31"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.executeUpdate("drop table autoinct1");
		s.executeUpdate("drop table autoinct2");
		s.executeUpdate("drop table autoinct3");


	}
	public void testwithIncrement()throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();
		Statement pst=createStatement();
		s.execute("insert into withinct1(i) values(1)");
		s.execute("insert into withinct1(i) values(1)");
		rs=s.executeQuery("select * from withinct1 order by withinct1_autogen");
		String[][]expectedRows=new String[][]{{"1","1"},{"1","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into withinct2(i) values(1)");
		s.execute("insert into withinct2(i) values(1)");
		rs=s.executeQuery("select * from withinct2 order by withinct2_autogen");
		expectedRows=new String[][]{{"1","1"},{"1","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("42Z23",pst,"insert into withinctempt1(i,withinct1_autogen) values(2,1)");
		assertStatementError("42Z23",pst,"insert into withinctempt1(i,withinct1_autogen) values(2,2)");		
		s.execute("insert into withinctempt1(i) values(2)");
		s.execute("insert into withinctempt1(i) values(2)");
		rs=s.executeQuery("select * from withinctempt1 order by withinct1_autogen");
		expectedRows=new String[][]{{"2","1"},{"2","2"}};		
		//Utilities.showResultSet(rs);
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into withinctempt2(i,withinct2_autogen) values(2,1)");
		s.execute("insert into withinctempt2(i,withinct2_autogen) values(2,2)");		
		s.execute("insert into withinctempt2(i) values(2)");
		s.execute("insert into withinctempt2(i) values(2)");
		rs=s.executeQuery("select * from withinctempt2 order by withinct2_autogen, i");
		expectedRows=new String[][]
            {
                {"2","1"},
                {"2","1"},
                {"2","2"},
                {"2","2"}
            };		
		JDBC.assertFullResultSet(rs,expectedRows);		
		s.execute("insert into withinctempt3(i) values(1)");
		s.execute("insert into withinctempt3(i) values(1)");			
		rs=s.executeQuery("select * from withinctempt3 order by t1_autogen");
		expectedRows=new String[][]{{"1","1"},{"1","11"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into withinctempt4(i) values(1)");
		s.execute("insert into withinctempt4(i) values(1)");			
		rs=s.executeQuery("select * from withinctempt4 order by t2_autogen");
		expectedRows=new String[][]{{"1","1"},{"1","11"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("42Z23",s,"insert into withinct3(i,withinct3_autogen) values(2,1)");
		assertStatementError("42Z23",s,"insert into withinct3(i,withinct3_autogen) values(2,2)");
		s.execute("insert into withinct3(i) values(2)");			
		s.execute("insert into withinct3(i) values(2)");
		rs=s.executeQuery("select * from withinct3 order by withinct3_autogen");
		expectedRows=new String[][]{{"2","1"},{"2","11"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into withinct4(i,withinct4_autogen) values(2,1)");
		s.execute("insert into withinct4(i,withinct4_autogen) values(2,2)");
		s.execute("insert into withinct4(i) values(2)");
		s.execute("insert into withinct4(i) values(2)");
		rs=s.executeQuery("select * from withinct4 order by withinct4_autogen");
		expectedRows=new String[][]
            {
                {"2","1"},
                {"2","1"},
                {"2","2"},
                {"2","11"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testunique()throws Exception
	{	//--with unique constraint
		ResultSet rs;
		Statement s=createStatement();
		s.execute("insert into uniquet1(i) values(1)");
		s.execute("insert into uniquet1(i) values(1)");
		String[][]expectedRows=new String[][]{{"1","100"},{"1","120"}};
		rs=s.executeQuery("select * from uniquet1 order by t1_autogen");
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into uniquet2(i) values(1)");
		s.execute("insert into uniquet2(i) values(1)");
		expectedRows=new String[][]{{"1","100"},{"1","120"}};
		rs=s.executeQuery("select * from uniquet2 order by t2_autogen");
		JDBC.assertFullResultSet(rs,expectedRows);
		
		assertStatementError("42Z23",s,"insert into uniquetempt1(i,t1_autogen) values(2,1)");
		assertStatementError("42Z23",s,"insert into uniquetempt1(i,t1_autogen) values(2,2)");
		s.execute("insert into uniquetempt1(i) values(2)");
		s.execute("insert into uniquetempt1(i) values(2)");
		expectedRows=new String[][]{{"2","100"},{"2","120"}};
		rs=s.executeQuery("select * from uniquetempt1 order by t1_autogen");
		JDBC.assertFullResultSet(rs,expectedRows);
		
		s.execute("insert into uniquetempt2(i,t2_autogen) values(2,1)");
		s.execute("insert into uniquetempt2(i,t2_autogen) values(2,2)");
		s.execute("insert into uniquetempt2(i) values(2)");
		s.execute("insert into uniquetempt2(i) values(2)");
		expectedRows=new String[][]{{"2","1"},{"2","2"},{"2","100"},{"2","120"}};
		
		//assertStatementError("23505",pst,"insert into uniquet3(i,uniquet3_autogen) values(1,0)");
		s.execute("insert into uniquet3(i,uniquet3_autogen) values(1,0)");
		//assertStatementError("23505",pst,"insert into uniquet3(i,uniquet3_autogen) values(2,1)");
		s.execute("insert into uniquet3(i,uniquet3_autogen) values(2,1)");
		assertStatementError("23505",s,"insert into uniquet3(i) values(3)");
		assertStatementError("23505",s,"insert into uniquet3(i) values(4)");
		s.execute("insert into uniquet3(i) values(5)");		
		rs=s.executeQuery("select i,uniquet3_autogen from uniquet3 order by i");
		//Utilities.showResultSet(rs);

		expectedRows=new String[][]{{"1","0"},{"2","1"},{"5","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);


		//--with unique index
		s.execute("insert into uniquet4(i,uniquet4_autogen) values(1,0)");
		s.execute("insert into uniquet4(i,uniquet4_autogen) values(2,1)");
		assertStatementError("23505",s,"insert into uniquet4(i) values(3)");
		assertStatementError("23505",s,"insert into uniquet4(i) values(4)");
		s.execute("insert into uniquet4(i) values(5)");
		rs=s.executeQuery("select i,uniquet4_autogen from uniquet4 order by i");
		expectedRows=new String[][]{{"1","0"},{"2","1"},{"5","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);


	}
	public void testidvalconn()throws Exception
	{
		//-- test IDENTITY_VAL_LOCAL function with 2 different connections
		ResultSet rs;
		Connection conn1=openUserConnection("conn1");
		Statement conn1st=conn1.createStatement();
		conn1st.execute("create table idvalt1 (c11 int generated always as identity (start with 101, increment by 3), c12 int)");
		conn1st.execute("create table idvalt2 (c21 int generated always as identity (start with 201, increment by 5), c22 int)");
		rs=conn1st.executeQuery("values IDENTITY_VAL_LOCAL()");
		String[][]expectedRows=new String[][]{{null}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn1.commit();
		Connection conn2=openUserConnection("conn2");
		Statement conn2st=conn2.createStatement();
		rs=conn2st.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{null}};
		JDBC.assertFullResultSet(rs,expectedRows);
		//conn2st.executeUpdate("insert into idvalt2 (c22) values (1)");
		conn2st.execute("insert into conn1.idvalt2 (c22) values (1)");
		rs=conn2st.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"201"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn1=openUserConnection("conn1");
		rs=conn1st.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{null}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn1st.execute("insert into idvalt1 (c12) values (1)");
		rs=conn1st.executeQuery("values IDENTITY_VAL_LOCAL()");		
		expectedRows=new String[][]{{"101"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn2st=conn2.createStatement();
		rs=conn2st.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"201"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn2.commit();
		conn2st=conn2.createStatement();
		rs=conn2st.executeQuery("values IDENTITY_VAL_LOCAL()");
		expectedRows=new String[][]{{"201"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		conn1st.execute("drop table idvalt1");
		conn1st.execute("drop table idvalt2");

	}
	public void testidvalVariants()throws Exception
	{
		/*-- A table with identity column has an insert trigger which inserts into another table 
		-- with identity column. IDENTITY_VAL_LOCAL will return the generated value for the 
		-- statement table and not for the table that got modified by the trigger*/
		ResultSet rs;
		Statement s=createStatement();
		s.execute("insert into variantt1 (c12) values (1)");
		rs=s.executeQuery("values IDENTITY_VAL_LOCAL()");
		String[][]expectedRows=new String[][]{{"101"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from variantt1");
		expectedRows=new String[][]
            {
                {"101","1"}
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select * from variantt2");
		expectedRows=new String[][]{{"201","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}
	public void testrestart()throws Exception
	{
		//-- Test RESTART WITH syntax of ALTER TABLE for autoincrment columns
		ResultSet rs;
		Statement s=createStatement();
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'REC11'");

		String[][]expectedRows=new String[][]{{"REC11","2","2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into restartt1 values(2,2)");
		rs=s.executeQuery("select * from restartt1");

		expectedRows=new String[][]{{"2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'REC11'");
		expectedRows=new String[][]{{"REC11","2","2","2"}};									  
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into restartt1(c12) values(9999)");
		rs=s.executeQuery("select * from restartt1 order by c12");
		expectedRows=new String[][]
            {
                {"2","2"},
                {"2","9999"}
            };
		JDBC.assertFullResultSet(rs,expectedRows);
        vetSequenceState( "RESTARTT1", 4, 2, 2 );
		assertStatementError("42837",s,"alter table restartt1 alter column c12 RESTART WITH 2");
		assertStatementError("42X49",s,"alter table restartt1 alter column rec11 RESTART WITH 2.20");
		s.execute("alter table restartt1 alter column rec11 RESTART WITH 2");
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC	from sys.syscolumns where COLUMNNAME = 'REC11'");
		expectedRows=new String[][]{{"REC11","2","2","2"}};
		JDBC.assertFullResultSet(rs,expectedRows);

	}

	// Some simple tests of the new "[NO] CYCLE" variant of ALTER TABLE
	// added by DERBY-6903:
	//
	public void testDerby6903AlterCycleSimple()
			throws Exception
	{
		Statement s=createStatement();
		ResultSet rs;

		s.execute("alter table cycle1 alter column rec21 CYCLE");

		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC, AUTOINCREMENTCYCLE " +
				"       from sys.syscolumns where COLUMNNAME = 'REC21'");

		String[][] expectedRows = new String[][]{{"REC21","2","2","2","true"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		// Using alter table doesn't reset cycling option from true to false.
		s.execute("alter table cycle1 alter column rec21 RESTART WITH 10");

		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC, AUTOINCREMENTCYCLE " +
				"       from sys.syscolumns where COLUMNNAME = 'REC21'");

		expectedRows = new String[][]{{"REC21","10","10","2","true"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		// Using alter table doesn't reset cycling option from true to false.
		s.execute("alter table cycle1 alter column rec21 SET INCREMENT BY 50");

		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC, AUTOINCREMENTCYCLE " +
				"       from sys.syscolumns where COLUMNNAME = 'REC21'");

		expectedRows = new String[][]{{"REC21","50","10","50","true"}};
		JDBC.assertFullResultSet(rs,expectedRows);


	}


    // Some simple tests of the new "[NO] CYCLE" variant of ALTER TABLE
    // added by DERBY-6904:
    //
    public void testDerby6904AlterCycleSimple()
        throws Exception
    {
		Statement s=createStatement();
		ResultSet rs;

        // Some simple syntax errors:
		assertStatementError("42X01",s,"alter table restartt1 alter column c12 cycle cycle");
		assertStatementError("42X01",s,"alter table restartt1 alter column c12 no");
		assertStatementError("42X01",s,"alter table restartt1 alter column c12 restart cycle");
		assertStatementError("42X01",s,"alter table restartt1 alter column c12 restart with cycle");

        // c12 is not an autoincrement column:
		assertStatementError("42837",s,"alter table restartt1 alter column c12 cycle");
		assertStatementError("42837",s,"alter table restartt1 alter column c12 no cycle");

        // Demonstrate that we can change column rec11 from NO CYCLE to CYCLE
        // and back to NO CYCLE, verifying by looking at SYSCOLUMNS:
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTCYCLE " +
                          "       from sys.syscolumns where COLUMNNAME = 'REC11'");
		String[][]expectedRows = new String[][]{{"REC11","false"}};
		JDBC.assertFullResultSet(rs,expectedRows);

        s.execute("alter table restartt1 alter column rec11 cycle");

		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTCYCLE " +
                          "       from sys.syscolumns where COLUMNNAME = 'REC11'");
		expectedRows = new String[][]{{"REC11","true"}};
		JDBC.assertFullResultSet(rs,expectedRows);

        s.execute("alter table restartt1 alter column rec11 no cycle");

		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTCYCLE " +
                          "       from sys.syscolumns where COLUMNNAME = 'REC11'");
		expectedRows = new String[][]{{"REC11","false"}};
		JDBC.assertFullResultSet(rs,expectedRows);
    }

    // Some simple tests of the new "NO CYCLE" option on CREATE TABLE
    // added by DERBY-6906:
    //
    public void testDerby6906NoCycleSimple()
        throws Exception
    {
		Statement s=createStatement();

        // Some simple syntax errors:
        assertStatementError("42XAJ",s,
		    "create table derby6906 " +
            "(rec11 int generated by default as identity" +
            "       (start with 2, increment by 2, cycle cycle) )");
        assertStatementError("42X01",s,
		    "create table derby6906 " +
            "(rec11 int generated by default as identity" +
            "       (start with 2, increment by 2, no) )");
        assertStatementError("42XAJ",s,
		    "create table derby6906 " +
            "(rec11 int generated by default as identity" +
            "       (start with 2, increment by 2, cycle no cycle) )");

        // Demonstrate the ability to have the [NO] CYCLE clause:

		s.execute("create table derby6906 " +
            "(rec11 int generated by default as identity" +
            "       (start with 2, increment by 2,cycle) )");
		s.execute("drop table derby6906 ");

		s.execute("create table derby6906 " +
            "(rec11 int generated by default as identity" +
            "       (start with 2, increment by 2,no cycle) )");
		s.execute("drop table derby6906 ");
    }

	public void testlock()throws Exception
	{
		/*--following puts locks on system table SYSCOLUMNS's row for t1lock.c11
		--Later when a user tries to have the system generate a value for the
		--t1lock.c11, system can't generate that value in a transaction of it's own
		--and hence it reverts to the user transaction to generate the next value.
		--This use of user transaction to generate a value can be problematic if
		--user statement to generate the next value runs into statement rollback.
		--This statement rollback will cause the next value generation to rollback
		--too and system will not be able to consume the generated value. 
		--In a case like this, user can use ALTER TABLE....RESTART WITH to change the
		--start value of the autoincrement column as shown below.*/
		ResultSet rs;
		Statement s=createStatement();

		s.execute("insert into t1lock values(1,1)");
		rs=s.executeQuery("select * from t1lock");
		String[][]expectedRows=new String[][]{{"1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'LOCKC11'");
		expectedRows=new String[][]{{"LOCKC11","1","1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		assertStatementError("23505",s,"insert into t1lock(c12) values(3)");
        vetSequenceState( "T1LOCK", 2, 1, 1 );

		rs=s.executeQuery("select * from t1lock");
		expectedRows=new String[][]{{"1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("alter table t1lock alter column lockc11 restart with 2");
		rs=s.executeQuery("select COLUMNNAME, AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC from sys.syscolumns where COLUMNNAME = 'LOCKC11'");
		expectedRows=new String[][]{{"LOCKC11","2","2","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into t1lock(c12) values(3)");
		rs=s.executeQuery("select * from t1lock");
		expectedRows=new String[][]{{"1","1"},{"2","3"}};
		JDBC.assertFullResultSet(rs,expectedRows);
	}
	public void test_Derby14951465() throws Exception
	{
		ResultSet rs;
		Statement s=createStatement();		
		rs=s.executeQuery("SELECT	col.columndefault,col.autoincrementvalue, col.autoincrementstart,col.autoincrementinc FROM sys.syscolumns col INNER JOIN sys.systables tab ON col.referenceId = tab.tableid WHERE tab.tableName = 'DERBY_1495' AND ColumnName = 'TESTID'");
		String[][]expectedRows=new String[][]{{"GENERATED_BY_DEFAULT","1","1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("INSERT INTO DERBY_1495(TESTID, TESTCOL2) VALUES(2, 2)");
		s.execute("ALTER TABLE DERBY_1495 ALTER COLUMN TESTid RESTART WITH 3");
		rs=s.executeQuery("SELECT	col.columndefault,col.autoincrementvalue, col.autoincrementstart,col.autoincrementinc FROM sys.syscolumns col INNER JOIN sys.systables tab ON col.referenceId = tab.tableid WHERE tab.tableName = 'DERBY_1495' AND ColumnName = 'TESTID'");
		expectedRows=new String[][]{{"GENERATED_BY_DEFAULT","3","3","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);

		/*-- Similarly, verify that if we change the INCREMENT BY value for a
		-- GENERATED_BY_DEFAULT column, the column remains GENERATED_BY_DEFAULT
		-- and its START WITH value is preserved.*/

		rs=s.executeQuery("SELECT col.columndefault,col.autoincrementvalue, col.autoincrementstart,col.autoincrementinc FROM sys.syscolumns col INNER JOIN sys.systables tab ON col.referenceId = tab.tableid WHERE tab.tableName = 'DERBY_1645' AND ColumnName = 'TESTTABLEID'");
		expectedRows=new String[][]{{"GENERATED_BY_DEFAULT","1","1","1"}};
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("INSERT INTO derby_1645 (TESTTableId, TESTStringValue) VALUES (-1, 'test1')");
		s.execute("INSERT INTO derby_1645 (TESTTableId, TESTStringValue) VALUES (-2, 'test2')");
		s.execute("INSERT INTO derby_1645 (TESTTableId, TESTStringValue) VALUES (-3, 'test3')");
		s.execute("ALTER TABLE derby_1645 ALTER TESTTableId SET INCREMENT BY 50");
		rs=s.executeQuery("SELECT col.columndefault,col.autoincrementvalue, col.autoincrementstart,col.autoincrementinc FROM sys.syscolumns col INNER JOIN sys.systables tab ON col.referenceId = tab.tableid WHERE tab.tableName = 'DERBY_1645' AND ColumnName = 'TESTTABLEID'");
		expectedRows=new String[][]{{"GENERATED_BY_DEFAULT","53","1","50"}};
		s.execute("INSERT INTO derby_1645 (TESTStringValue) VALUES ('test53')");
		s.execute("INSERT INTO derby_1645 (TESTTableId, TEST" +"StringValue) VALUES (-999, 'test3')");
		s.execute("drop table derby_1645");

	}
	public void TESTD1644()throws Exception
	{
		/*-- Test cases related to DERBY-1644, which involve:
		--  a) multi-row VALUES clauses
		--  b) GENERATED BY DEFAULT autoincrement fields
		--  c) insert statements which mention only a subset of the table's columns
		-- First we have the actual case from the bug report. Then we have a number
		-- of other similar cases, to try to cover the code area in question*/
		ResultSet rs;
		Statement s=createStatement();
		s.execute("insert into D1644 (d1644c2) values default, 10");
		s.execute("insert into D1644 (d1644c2) values (11)");
		s.execute("insert into D1644 (d1644c2) values default");
		s.execute("insert into D1644 (d1644c2) values (default)");
		s.execute("insert into D1644 (d1644c2) values 12, 13, 14");
		s.execute("insert into D1644 (d1644c2) values 15, 16, default");
		s.execute("insert into D1644 values (17, 18)");
		s.execute("insert into D1644 values (19, default)");
		s.execute("insert into D1644 values (20, default), (21, 22), (23, 24), (25, default)");
		s.execute("insert into D1644 (d1644c2, d1644c1) values (default, 26)");
		s.execute("insert into D1644 (d1644c2, d1644c1) values (27, 28), (default, 29), (30, 31)");
		s.execute("insert into D1644 (d1644c2) values default, default, default, default");
		s.execute("insert into D1644 (d1644c2, d1644c1) values (default, 128),(default, 129),(default, 131)");
		rs=s.executeQuery("select * from D1644 order by d1644c1, d1644c2");
		String[][]expectedRows=new String[][]
            {
                {"17","18"},
                {"19","5"},
                {"20","6"},
                {"21","22"},
                {"23","24"},
                {"25","7"},
                {"26","8"},
                {"28","27"},
                {"29","9"},
                {"31","30"},
                {"128","14"},
                {"129","15"},
                {"131","16"},
                {null,"1"},
                {null,"2"},
                {null,"3"},
                {null,"4"},
                {null,"10"},
                {null,"10"},
                {null,"11"},
                {null,"11"},
                {null,"12"},
                {null,"12"},
                {null,"13"},
                {null,"13"},
                {null,"14"},
                {null,"15"},
                {null,"16"},
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into D1644_A (c3, d1644_Ac1, d1644_Ac2) values (1, 2, default)");
		s.execute("insert into D1644_A (c3, d1644_Ac1, d1644_Ac2) values (3,4,5), (6,7,default)");
		s.execute("insert into D1644_A (c3, d1644_Ac2) values (8, default), (9, 10)");
		rs=s.executeQuery("select * from D1644_A order by d1644_Ac1, d1644_Ac2");
		expectedRows=new String[][]
            {
                {"2","1","1"},
                {"4","5","3"},
                {"7","2","6"},
                {null,"3","8"},
                {null,"10","9"}
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("insert into D1644_B (d1644_Bc1) values default, 10");
		s.execute("insert into D1644_B values default, 10");
		rs=s.executeQuery("select * from D1644_B order by d1644_Bc1");
		expectedRows=new String[][]
            {
                {"1"},
                {"2"},
                {"10"},
                {"10"}
            };
		JDBC.assertFullResultSet(rs,expectedRows);


	}
	public void testDerby2902()throws Exception
	{
		/*-- Derby-2902: can't use LONG.MIN_VALUE as the start value for
		-- an identity column. These tests verify that values less than MIN_VALUE
		-- or greater than MAX_VALUE are rejected, but MIN_VALUE and MAX_VALUE
		-- themeselves are accepted.*/
		ResultSet rs;
		Statement s=createStatement();
		s.execute("insert into d4006 values default");
		s.execute("alter table d4006 alter column x with default null");
		s.execute("insert into d4006 values default");
		s.execute("alter table d4006 alter column x with default 'def'");
		s.execute("insert into d4006 values default");
		rs=s.executeQuery("select * from d4006 order by x");
		String[][]expectedRows=new String[][]
            {
                {"abc"},
                {"def"},
                {null},
            };
		JDBC.assertFullResultSet(rs,expectedRows);
		s.execute("alter table d4006 add column y int generated always as (-1)");
		assertStatementError("42XA7",s,"alter table d4006 alter column y default 42");
		assertStatementError("42XA7",s,"alter table d4006 alter column y default null");

		assertStatementError( "42XA7", s, "alter table d4006_a alter column z default 99" );
		assertStatementError( "42XA7", s, "alter table d4006_a alter column z default null" );
	}
	
	public static Test suite() {
		return new CleanDatabaseTestSetup(
            new BaseTestSuite(AutoIncrementTest.class, "AutoIncrementTest")) {
			protected void decorateSQL(Statement s)
			throws SQLException
			{
				createSchemaObjects(s);
			}
		};
	}
}
