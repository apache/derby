/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.RenameIndexTest

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
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class RenameIndexTest extends BaseJDBCTestCase
{
    public RenameIndexTest(String name)      {
        super(name);
    }
    public static Test suite() {
        
	return TestConfiguration.embeddedSuite(RenameIndexTest.class);
    }
    protected void setUp() throws Exception {
        super.setUp();
        getConnection().setAutoCommit(false);
     }
     protected void tearDown() throws Exception {
         super.tearDown();
     }
/**
 * Test that We cannot Rename a non-existing Index.
 *
 * @exception SQLException
 */
public void testRenameNonExistingIndex() throws SQLException {
	Statement s = createStatement();
	assertStatementError("42X65" , s , "rename index i1t1 to i1rt1");
}
/**
 * Test that We cannot Rename an Index With an existing Index name.
 *
 * @exception SQLException
 */
public void testExistingIndexName() throws SQLException {
	Statement s = createStatement();
	s.executeUpdate("create table t1(c11 int, c12 int)");
	s.executeUpdate("create index i1t1 on t1(c11)");
	s.executeUpdate("create index i2t1 on t1(c12)");
	assertStatementError("X0Y32" , s , "rename index i1t1 to i2t1");
	s.executeUpdate("drop table t1");
}
/**
 * Test that We cannot Rename a System Table's Index
 *
 * @exception SQLException
 */
//-- rename a system table's index
public void testRenameSystemTableIndex() throws SQLException {
	Statement s = createStatement();
	s.executeUpdate("set schema sys");
	// will fail because it is a system table
	assertStatementError("X0Y56" , s , "rename index syscolumns_index1 to newName");
	s.executeUpdate("set schema app");
}
/**
 * Test to RENAME an INDEX when view is on a table.
 * 
 * @exception SQLException
 */
public void testRenameIndexOfView() throws SQLException {
	Statement s = createStatement();
	s.executeUpdate("create table t1(c11 int, c12 int)");
	s.executeUpdate("create index t1i1 on t1(c11)");
	s.executeUpdate("create view v1 as select * from t1");
	ResultSet rs = s.executeQuery("select count(*) from v1");
	JDBC.assertSingleValueResultSet(rs , "0");
	//-- this succeeds with no exceptions
	assertUpdateCount(s , 0 , "rename index t1i1 to t1i1r");
	rs = s.executeQuery("select count(*) from v1");
        JDBC.assertSingleValueResultSet(rs , "0");
	s.executeUpdate("drop view v1");
	s.executeUpdate("drop table t1");
}
/**
 * Test RENAME INDEX when there is  a duplicate INDEX
 *
 * @exception SQLException
 */
public void testDuplicateIndexWithViews() throws SQLException
{
	//-- another test for views
	Statement s = createStatement();
	s.executeUpdate("create table t1(c11 int not null primary key, c12 int)");
	s.executeUpdate("create index i1t1 on t1(c11)");
	s.executeUpdate("create view v1 as select * from t1");
	
	assertStatementError("42X65" , s , "rename index i1t1 to i1rt1");
	assertUpdateCount(s , 0 , "drop view v1");
	//-- even though there is no index i1t1 it still doesn't fail
	assertUpdateCount(s , 0 , "create view v1 as select * from t1");
	//-- this succeeds with no exceptions
	ResultSet rs = s.executeQuery("select count(*) from v1");
        JDBC.assertSingleValueResultSet(rs , "0");
	assertStatementError("42X65" , s , "rename index i1rt1 to i1t1");
	s.executeUpdate("drop view v1");
	s.executeUpdate("drop table t1");
}
/*
 * -- cannot rename an index when there is an open cursor on it
 *	
 * @exception SQLException
 */
public void testRenameIndexWithOpenCursor() throws SQLException {
	Statement s = createStatement(ResultSet.TYPE_FORWARD_ONLY , ResultSet.CONCUR_UPDATABLE);	
	s.executeUpdate("create table t1(c11 int , c12 int)");
	s.executeUpdate("create index i1 on t1(c11)");
	s.executeUpdate("insert into t1 values(2 , 3)");
	s.executeUpdate("insert into t1 values(3 , 4)");
	ResultSet rs = s.executeQuery("select * from t1");
	rs.next();
	assertStatementError("X0X95" , createStatement() , "rename index i1 to i1r");
	rs.close();
	//-- following rename should pass because cursor c1 has been closed
	assertUpdateCount(s , 0 , "rename index i1 to i1r");
	s.executeUpdate("drop table t1");
}
/**
 * Test RENAME INDEX With Prepared Statement.
 *
 * @exception SQLException
 */
//-- creating a prepared statement on a table
public void testWithPreparedStatement() throws SQLException {
	Statement s = createStatement();	
	s.executeUpdate("create table t1(c11 int not null primary key, c12 int)");
	//-- bug 5685
	s.executeUpdate("create index i1 on t1(c11)");
	PreparedStatement pstmt = prepareStatement("select * from t1 where c11 > ?");
	pstmt.setInt(1 , 1);
	ResultSet rs = pstmt.executeQuery();
	rs.next();
	rs.close();
	assertStatementError("42X65" , s , "rename index i1 to i1r");
	//-- statement passes
	pstmt.setInt(1 , 1);
	rs = pstmt.executeQuery();
	rs.next();
	rs.close();
	pstmt.close();
	s.executeUpdate("drop table t1");
}
/**
 *  column with an index on it can be renamed
 *
 * @exception SQLException
 */
public void testRenameColumnWithIndex() throws SQLException {
	Statement s = createStatement();		
	s.executeUpdate("create table t3(c31 int not null primary key, c32 int)");
	s.executeUpdate("create index i1_t3 on t3(c32)");
	s.executeUpdate("rename index i1_t3 to i1_3r");
	//-- make sure that i1_t3 did get renamed. Following rename should fail, to prove that.
	assertStatementError("42X65" , s , "rename index i1_t3 to i1_3r");
	s.executeUpdate("drop table t3");
}
/**
 * Test the another feature with PreparedStatement.
 *
 * @exception SQLException
 */
//-- creating a prepared statement on a table
public void testDuplicateIndexWithPreparedStatement() throws SQLException {
	Statement s = createStatement();
	s.executeUpdate("create table t3(c31 int not null primary key, c32 int)");
	s.executeUpdate("create index i1_t3 on t3(c32)");
	PreparedStatement pstmt = prepareStatement("select * from t3 where c31 > ?");
	pstmt.setInt(1 , 1);
	ResultSet rs = pstmt.executeQuery();
	rs.close();
	//-- can rename with no errors
	assertUpdateCount(s , 0 , "rename index i1_t3 to i1_t3r");
	//execute p3 using 'values (1)';
	pstmt.setInt(1 , 1);
	rs = pstmt.executeQuery();
	rs.close();
	assertUpdateCount(s , 0 , "rename index i1_t3r to i1_t3");
	pstmt.close();
	s.executeUpdate("drop table t3");
  }
}
