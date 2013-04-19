/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.declareGlobalTempTableJavaJDBC30Test

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.*;

/**
 * Test for declared global temporary tables (introduced in Cloudscape 5.2)
 * and pooled connection close and jdbc 3.0 specific features
 * The jdbc3.0 specific featuers are holdable cursors, savepoints.
 * The rest of the temp table test are in declareGlobalTempTableJavaTest class. 
 * The reason for a different test class is that the holdability and savepoint 
 * support is under jdk14 and higher. But we want to be able to run the 
 * non-holdable tests under all the jdks we support and hence splitting the 
 * tests into two separate tests. 
 */


public class DeclareGlobalTempTableJavaJDBC30Test extends BaseJDBCTestCase {

    public DeclareGlobalTempTableJavaJDBC30Test(String name)
    {
        super(name);
    }	
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(TestConfiguration.embeddedSuite(
                DeclareGlobalTempTableJavaJDBC30Test.class));
        suite.addTest(TestConfiguration.clientServerDecorator(
                getClientSuite()));
        return suite;
    }
    
    /**
     * Return a suite of tests that are run with client only
     * 
     * @return A suite of tests being run with client only
     */
    private static Test getClientSuite() {
        TestSuite suite = new TestSuite("Client/Server");
        // skip the tests with more than 1 savepoint -  
        // see (lack of support described in) DERBY-3687
        // so, only do the following with network server/client: 
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testOnCommitPreserveRowsWithHoldability"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testSavepointRollbackbehaviour7"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testSavepointRollbackbehaviour8"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testSavepointRollbackbehaviour11"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testSavepointRollbackbehaviour12"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testTest4"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testPooledConnectionClosed"));
        
        // following 2 fail with network server; see DERBY-4373
        /*
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testOnCommitDeleteRowsWithHoldableOpenCursor"));
        suite.addTest(new DeclareGlobalTempTableJavaJDBC30Test(
            "testOnCommitDeleteRowsHoldabilityWithPreparedStatements"));
        */
        return suite;
    }
    
    protected void setUp() throws Exception {
        super.setUp();
        getConnection().setAutoCommit(false);
    }
    protected void tearDown() throws Exception {
        super.tearDown();
    }
    /**
     *  Tests that If a Global Temp table(with on commi delete rows) has open 
     *  cursors held on it 
     *  Data should be preserved in at commit time
     *
     *  @throws SQLException 
     */
    public  void  testOnCommitDeleteRowsWithHoldableOpenCursor() 
    throws SQLException {
        Statement s1 = getConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        s1.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit delete rows not logged");
        s1.executeUpdate("insert into session.t1 values(11, 1)");
        s1.executeUpdate("insert into session.t1 values(12, 2)");
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        //hold cursor open on t1. Commit should preserve the rows
        ResultSet rs1 = s1.executeQuery("select * from SESSION.t1");
        rs1.next();
        //cursor opened
        //Temp tables t2 & t3 with one held open cursor on them together. Data
        //should be preserved in t2 & t3 at commit time
        Statement s2 = getConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        s2.executeUpdate("declare global temporary table SESSION.t2(" +
                "c21 int, c22 int) on commit delete rows not logged");
        s2.executeUpdate("insert into session.t2 values(21, 1)");
        s2.executeUpdate("insert into session.t2 values(22, 2)");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t2"), "2");
        s2.executeUpdate("declare global temporary table SESSION.t3(" +
                "c31 int, c32 int) on commit delete rows not logged");
        s2.executeUpdate("insert into session.t3 values(31, 1)");
        s2.executeUpdate("insert into session.t3 values(32, 2)");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t3") , "2");
        //hold cursor open on t2 & t3. Commit should preseve the rows
        ResultSet rs23 = s2.executeQuery(
                "select * from SESSION.t2, SESSION.t3 where c22=c32"); 
        rs23.next(); 
        //cursor Opened
        Statement s3 = getConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        s3.executeUpdate("declare global temporary table SESSION.t4(" +
                "c41 int, c42 int) on commit delete rows not logged");
        s3.executeUpdate("insert into session.t4 values(41, 1)");
        s3.executeUpdate("insert into session.t4 values(42, 2)");
        ResultSet rs4 = s3.executeQuery("select count(*) from SESSION.t4");
        JDBC.assertSingleValueResultSet(rs4 , "2");
        //hold cursor open on t4 but close it before commit, 
        //Data should be deleted after commit
        rs4 = s3.executeQuery("select * from SESSION.t4");
        rs4.next();
        rs4.close();
        //rs4 closed before committing.
        commit();
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t3") , "2");
        JDBC.assertSingleValueResultSet(s3.executeQuery(
                "select count(*) from SESSION.t4") , "0");
        s1.executeUpdate("drop table SESSION.t1");
        s2.executeUpdate("drop table SESSION.t2");
        s3.executeUpdate("drop table SESSION.t3");   
        s3.executeUpdate("drop table SESSION.t4");
        rs1.close(); rs23.close(); rs4.close();
        commit();
        s1.close();  s2.close(); s3.close();
    }
    /**
     *  Test declared temporary table with ON COMMIT DELETE ROWS and holdable
     *  cursors on prepared statement
     *
     *  @throws SQLException 
     */
    public void testOnCommitDeleteRowsHoldabilityWithPreparedStatements() 
    throws SQLException {
        Statement s1 = createStatement();
        s1.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit delete rows not logged");
        s1.executeUpdate("insert into session.t1 values(11, 1)");
        s1.executeUpdate("insert into session.t1 values(12, 2)");
        //create a prepared statement with hold cursors over commit
        PreparedStatement ps1 = getConnection().prepareStatement(
                "select count(*) from SESSION.t1",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        ResultSet rs1 = ps1.executeQuery();
        JDBC.assertSingleValueResultSet(rs1 , "2");
        PreparedStatement ps2 = getConnection().prepareStatement(
                "select * from SESSION.t1", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
        //hold cursor open on t1. Commit should preserve the rows
        ResultSet rs11 = ps2.executeQuery();
        rs11.next();
        //Cursor was Opened.
        //Temp table t2 with one held cursor but it is closed before commit. 
        //Data should be deleted from t2 at commit time
        s1.executeUpdate("declare global temporary table SESSION.t2(" +
                "c21 int, c22 int) on commit delete rows not logged");
        s1.executeUpdate("insert into session.t2 values(21, 1)");
        s1.executeUpdate("insert into session.t2 values(22, 2)");
        //create a prepared statement with hold cursors over commit
        PreparedStatement ps3 = getConnection().prepareStatement(
                "select count(*) from SESSION.t2",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, 
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        ResultSet rs2 = ps3.executeQuery();
        JDBC.assertSingleValueResultSet(rs2 , "2");
        PreparedStatement ps4 = getConnection().prepareStatement(
                "select * from SESSION.t2", ResultSet.TYPE_FORWARD_ONLY, 		    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT );
        //hold cursor open on t2 but close it before commit
        rs2 = ps4.executeQuery();
        rs2.next();
        rs2.close();
        //commiting Point
        commit();
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        //Need to close the held cursor on t1 before t1 can be dropped
        rs11.close();
        rs1.close();
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        s1.executeUpdate("drop table SESSION.t1");
        s1.executeUpdate("drop table SESSION.t2");
    }
    /**
     *  Tests a temporary table with ON COMMIT PRESERVE ROWS and various 
     *  combinations of holdability.
     *  Temp table t1 with held open cursors on it. Data should be preserved,
     *  holdability shouldn't matter
     *
     *  @throws SQLException 
     */
    public void testOnCommitPreserveRowsWithHoldability() throws SQLException	{
        //create a statement with hold cursors over commit
        Statement s1 = getConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        s1.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        s1.executeUpdate("insert into session.t1 values(11, 1)");
        s1.executeUpdate("insert into session.t1 values(12, 2)");
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        //Opening the Cursor
        ResultSet rs1 = s1.executeQuery("select * from SESSION.t1");
        rs1.next();
        //create a statement without hold cursors.
        Statement s2 = createStatement();
        s1.executeUpdate("declare global temporary table SESSION.t2(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        s1.executeUpdate("insert into session.t2 values(11, 1)");
        s1.executeUpdate("insert into session.t2 values(12, 2)");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        //Opening The Cursor
        ResultSet rs2 = s2.executeQuery("select * from SESSION.t2");
        rs2.next();
        //commiting point
        commit();
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s2.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        s1.executeUpdate("drop table SESSION.t1");
        s2.executeUpdate("drop table SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 1 (3A)
     *  In the transaction Create savepoint1 and declare temp table t1. 
     *  Create savepoint 2, drop temp table t1, rollback savepoint 2 
     *  select should pass, rollback savepoint 1, select should fail.
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour1() throws SQLException {
        Statement s = createStatement();
        //Set The First savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        //create a temp table t1
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        PreparedStatement pStmt = prepareStatement(
                "insert into SESSION.t1 values (?, ?)");
        pStmt.setInt(1, 11);
        pStmt.setInt(2, 1);
        pStmt.execute();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "1");
        //Set The Second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        //drop the temp table t1
        s.executeUpdate("drop table SESSION.t1");
        assertStatementError("42X05" , s , "select * from SESSION.t1");
        //Rollback to the second savepoint - drop table Operation is rolled 
        //back hence we should have t1
        getConnection().rollback(savepoint2);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");	
        //Rollback to the First savepoint - create table Operation is also 
        //rolled back hence there is no t1
        getConnection().rollback(savepoint1);
        assertStatementError("42X05" , s , "select * from SESSION.t1");
    }
    /**
     *  Savepoint and Rollback behavior - 2 (3B)
     *  In the transaction Create savepoint1 and declare temp table t1. 
     *  Create savepoint2 and declare temp table t2
     *  Release savepoint 1 and select from temp table t1 and t2.
     *  Drop temp table t2(explicit drop), rollback transaction(implicit drop
     *  of t1) Select from temp table t1 and t2 will fail.
     *
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour2() throws SQLException {
        Statement s = createStatement();
        //Set First savepoint (save point - 1)
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        //Set Second savepoint (save point - 2)
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows not logged");
        //Release First savepoint
        getConnection().releaseSavepoint(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //Drop the Temp Table t2 Explicitly and t1 Implicitly(by rolling back)	
        s.executeUpdate("drop table SESSION.t2");
        rollback();
        assertStatementError("42X05" , s , "select * from SESSION.t1");
        assertStatementError("42X05" , s , "select * from SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 3 (3C)
     *  In the transaction Create savepoint1 and declare temp table t1. 
     *  Create savepoint2 and declare temp table t2.
     *  Release savepoint 1 and select from temp table t1 and t2 should pass.
     *  Create savepoint3 and rollback savepoint3(should not touch t1 and t2)
     *  select from temp tables t1 and t2 should pass.
     *  Rollback transaction and select from temp tables t1 and t2 should fail.
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour3() throws SQLException  {
        Statement s = createStatement();
        //set first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows  not logged");
        //set second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows  not logged");
        //release the first savepoint
        getConnection().releaseSavepoint(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //set third savepoint
        Savepoint savepoint3 = getConnection().setSavepoint();
        //rollback to third savepoint - (should not touch t1 and t2)
        getConnection().rollback(savepoint3);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //rollback the entire transaction - this rolls back create statements
        //also hence t1 , t2 got deleted
        rollback();
        assertStatementError("42X05" , s , "select * from SESSION.t1");
        assertStatementError("42X05" , s , "select * from SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 4 (3D)
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour4() throws SQLException {
        Statement s = createStatement();
        //set first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows  not logged");
        //set second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("drop table SESSION.t1");
        //rollback to second savepoint
        getConnection().rollback(savepoint2);
        JDBC.assertSingleValueResultSet(s.executeQuery("" +
                "select count(*) from SESSION.t1") , "0");
        //commit
        commit();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        s.executeUpdate("drop table SESSION.t1");
    }
    /**
     *  Savepoint and Rollback behavior - 5 - 3E
     *  Tests the basic definition of savepoint in combination with Rollback.
     *
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour5() throws SQLException {
        Statement s = createStatement();
        //set first savepoint 
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows  not logged");
        //set second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("drop table SESSION.t1");
        //rollback to first savepoint
        getConnection().rollback(savepoint1);
        assertStatementError("42X05" , s , "select * from SESSION.t1");
    }
    
    /**
     *  Savepoint and Rollback behavior - 6 - 3J
     *  In the transaction: declare temp table t1 with 2 columns and commit.
     *  Create savepoint1 and drop temp table t1 with 2 columns, declare temp
     *  table t1 but this time with 3 columns.
     *  Create savepoint2 and drop temp table t1 with 3 columns, rollback,
     *  select from temp table t1 here should have 2 columns
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour6() throws SQLException {
        String[] s1 = {"C11" , "C12"};
        String[] s2 = {"C11" , "C12" , "C13"};
        Statement s = createStatement();
        // declare temp table t1 with 2 columns
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows  not logged");
        s.executeUpdate("insert into SESSION.t1 values(11, 11)");
        ResultSet rs1 = s.executeQuery("select * from SESSION.t1");
        JDBC.assertColumnNames(rs1 , s1);
        //commiting point
        getConnection().commit();
        //set first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("drop table SESSION.t1");
        //declare temp table t1 with 3 columns
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int, c13 int not null) on commit preserve rows " +
                "not logged");
        s.executeUpdate("insert into SESSION.t1 values(22, 22, 22)");
        rs1 = s.executeQuery("select * from SESSION.t1");
        JDBC.assertColumnNames(rs1 , s2);
        //set second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("drop table SESSION.t1");
        //rollback the transaction
        rollback();
        rs1 = s.executeQuery("select * from SESSION.t1");
        JDBC.assertColumnNames(rs1 , s1);	
        s.executeUpdate("drop table SESSION.t1");
    }
    /**
     *  Savepoint and Rollback behavior - 7 - 3K
     *  tests the savepoint and rollback behaviour with update command
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour7() throws SQLException {
        Statement s = createStatement();
        //declare temp table t1 & t2, insert few rows and commit
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged " +
                "on rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows not logged");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into session.t2 values(21, 1)");
        commit();
        //committed - the first transaction has been completed
        //In the next transaction, insert couple more rows in t1 & t2
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        s.executeUpdate("insert into SESSION.t2 values(22, 2)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        //set first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("UPDATE SESSION.t1 SET c12 = 3 where c12>1");
        s.executeUpdate("UPDATE SESSION.t2 SET c22 = 3 where c22>2");
        //rollback to first savepoint
        getConnection().rollback(savepoint1);
        //Rollback to savepoint1 and we should loose all the rows in t1 temp
        //table t2 should also have no rows because attempt was made to modify 
        //it (even though nothing actually got modified in t2 in the savepoint)
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        commit();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        s.executeUpdate("drop table SESSION.t1");
        s.executeUpdate("drop table SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 8 - 3L
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour8() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows  not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows  not logged on " +
                "rollback delete rows");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into session.t2 values(21, 1)");
        commit();
        //insert couple more rows in t1 & t2 and
        //Create savepoint1 and update some rows in t1 and inspect the data
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        s.executeUpdate("insert into session.t2 values(22, 2)");
        //set first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("UPDATE SESSION.t1 SET c12 = 3 where c12>1");
        s.executeUpdate("UPDATE SESSION.t2 SET c22 = 3 where c22>3");
        //rollback to first savepoint
        getConnection().rollback(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery("select count(*) from SESSION.t1") , "0");
        rollback();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        //cleanUp
        s.executeUpdate("drop table SESSION.t1");
        s.executeUpdate("drop table SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 9 - 3M
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour9() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged " +
                "on rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(" +
                "c31 int, c32 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(" +
                "c41 int, c42 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into SESSION.t2 values(21, 1)");
        s.executeUpdate("insert into SESSION.t3 values(31, 1)");
        s.executeUpdate("insert into SESSION.t4 values(41, 1)");
        commit();
        //Beginning of second transaction
        //Insert a couple of more rows into t1 , t2 and t3
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        s.executeUpdate("insert into session.t2 values(22, 2)");
        s.executeUpdate("insert into session.t3 values(32, 2)");
        //set first savepoint                    
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("DELETE FROM SESSION.t1 where c12>1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "1");
        //set second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("DELETE FROM SESSION.t2 where c22>1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "1");
        //Release savepoint2 and now savepoint1 should keep track of changes 
        //made to t1 and t2
        getConnection().releaseSavepoint(savepoint2);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "1");
        // Rollback savepoint1 and should see no data in t1 and t2
        getConnection().rollback(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //Should see data in t3 since it was not touched in the savepoint that 
        //was rolled back
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t3") , "2");
        rollback();
        //Rolling back the transaction we should see no data in t1 and t2 and t3
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t3") , "0");
        //Should see data in t4 since it was not touched in the transaction that was rolled back
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t4") , "1");
        s.executeUpdate("drop table SESSION.t1");
        s.executeUpdate("drop table SESSION.t2");
        s.executeUpdate("drop table SESSION.t3");
        s.executeUpdate("drop table SESSION.t4");
    }
    /**
     *  Savepoint and Rollback behavior - 10 - 3N
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour10() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t3(" +
                "c31 int, c32 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t4(" +
                "c41 int, c42 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        s.executeUpdate("insert into SESSION.t2 values(21, 1)");
        s.executeUpdate("insert into SESSION.t2 values(22, 2)");
        s.executeUpdate("insert into SESSION.t3 values(31, 1)");
        s.executeUpdate("insert into SESSION.t4 values(41, 1)");
        commit();
        //In the next transaction, insert couple more rows in t3
        s.executeUpdate("insert into SESSION.t3 values(31, 2)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t3") , "2");
        //Create savepoint1 and delete some rows from t1
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("DELETE FROM SESSION.t1 where c12>1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "1");
        s.executeUpdate("DELETE FROM SESSION.t2 where c22>3");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        //Create savepoint2 and delete some rows from t2 
        Savepoint savepoint2 = getConnection().setSavepoint();
        s.executeUpdate("DELETE FROM SESSION.t2 where c22>1");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "1");
        //Rollback the transaction and should see no data in t1 and t2 and t3
        rollback();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t3") , "0");
        //Should see data in t4 since it was not touched in the transaction 
        //that was rolled back
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t4") , "1");
        s.executeUpdate("drop table SESSION.t1");
        s.executeUpdate("drop table SESSION.t2");
        s.executeUpdate("drop table SESSION.t3");
        s.executeUpdate("drop table SESSION.t4");
    }
    /**
     *  Savepoint and Rollback behavior - 11 - 3O
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour11() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) on commit preserve rows not logged on " +
                "rollback delete rows");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into SESSION.t2 values(21, 1)");
        commit();
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        //set the first savepoint here
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("insert into SESSION.t2 values(22, 2)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        //Rollback savepoint1; expect no data in t2 but t1 should have data
        getConnection().rollback(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //Commit the transaction; expect no data in t2 but t1 should have data
        commit();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        s.executeUpdate("drop table SESSION.t1");
        s.executeUpdate("drop table SESSION.t2");
    }
    /**
     *  Savepoint and Rollback behavior - 12 - 3P
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour12() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        s.executeUpdate("insert into SESSION.t1 values(11, 1)");
        s.executeUpdate("insert into SESSION.t1 values(12, 2)");
        commit();
        //set the first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        s.executeUpdate("insert into SESSION.t1 values(13, 3)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "3");
        //release the savepoint - now transaction should keep track of changes
        //made to t1
        getConnection().releaseSavepoint(savepoint1);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "3");
        //Rollback the transaction and should still see no data in t1
        rollback();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        s.executeUpdate("drop table SESSION.t1");
    }
    /**
     *  Savepoint and Rollback behavior - 13 - 3Q
     *  tests the rollback , savepoint behaviour with prepartedStatement
     *  
     *  @throws SQLException 
     */
    public void testSavepointRollbackbehaviour13() throws SQLException {
        Statement s = createStatement();
        s.executeUpdate("DECLARE GLOBAL TEMPORARY TABLE SESSION.t2(" +
                "c21 int, c22 int) not logged on commit preserve rows");
        PreparedStatement pStmtInsert = prepareStatement(
                "insert into SESSION.t2 values (?, ?)");
        pStmtInsert.setInt(1, 21);
        pStmtInsert.setInt(2, 1);
        pStmtInsert.execute();
        pStmtInsert.setInt(1, 22);
        pStmtInsert.setInt(2, 2);
        pStmtInsert.execute();
        pStmtInsert.setInt(1, 23);
        pStmtInsert.setInt(2, 2);
        pStmtInsert.execute();
        PreparedStatement pStmtUpdate = prepareStatement(
                "UPDATE SESSION.t2 SET c22 = 3 where c21=?");
        pStmtUpdate.setInt(1, 23);
        pStmtUpdate.execute();
        PreparedStatement pStmtDelete = prepareStatement(
                "DELETE FROM SESSION.t2 where c21 = ?");
        pStmtDelete.setInt(1, 23);
        pStmtDelete.execute();
        commit();//committing point
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        //set the first savepoint
        Savepoint savepoint1 = getConnection().setSavepoint();
        pStmtInsert.setInt(1, 23);
        pStmtInsert.setInt(2, 2);
        pStmtInsert.execute();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2")  , "3");
        //set the second savepoint
        Savepoint savepoint2 = getConnection().setSavepoint();
        pStmtUpdate.setInt(1, 23);
        pStmtUpdate.execute();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "3");
        //rollback savepoint2 and should loose all the data from t2");
        getConnection().rollback(savepoint2);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "0");
        //Create savepoint3, insert some rows into t2 using prepared statement
        Savepoint savepoint3 = getConnection().setSavepoint();
        pStmtInsert.setInt(1, 21);
        pStmtInsert.setInt(2, 1);
        pStmtInsert.execute();
        pStmtInsert.setInt(1, 22);
        pStmtInsert.setInt(2, 2);
        pStmtInsert.execute();
        pStmtInsert.setInt(1, 23);
        pStmtInsert.setInt(2, 333);
        pStmtInsert.execute();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "3");
        //Create savepoint4 and update row inserted in savepoint3 using prepared
        //statement and inspect the data in t2
        Savepoint savepoint4 = getConnection().setSavepoint();
        pStmtUpdate.setInt(1, 23);
        pStmtUpdate.execute();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "3");
        //release savepoint4
        getConnection().releaseSavepoint(savepoint4);
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "3");
        pStmtDelete.setInt(1, 23);
        pStmtDelete.execute();
        //Commit transaction and should see data in t2
        commit();
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.t2") , "2");
        s.executeUpdate("drop table SESSION.t2");
    }	
    /**
     *  Test declared temporary table with ON COMMIT DELETE ROWS and holdable 
     *  cursors and temp table as part of subquery
     *  Temp table t1 with no direct held cursor open on it. 
     *  Data should be deleted from t1 at commit time
     *  
     *  @throws SQLException 
     */
    public void testTest4() throws SQLException {
        Statement s1 = getConnection().createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                ResultSet.HOLD_CURSORS_OVER_COMMIT );
        try {
            s1.executeUpdate("drop table t1");
        }
        catch(SQLException e)
        {
            assertSQLState("42Y55" , e);
        }	
        s1.executeUpdate("create table t1(c11 int, c12 int)");
        s1.executeUpdate("declare global temporary table SESSION.t1(" +
                "c11 int, c12 int) on commit delete rows not logged");
        s1.executeUpdate("insert into session.t1 values(11, 1)");
        s1.executeUpdate("insert into session.t1 values(12, 2)");
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "2");
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from t1") , "0");
        //Insert into real table using temporary table data on a statement with
        //holdability set to true
        s1.executeUpdate("INSERT INTO T1 SELECT * FROM SESSION.T1");
        commit();
        //Temp table t1 will have no data after commit
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from SESSION.t1") , "0");
        //Physical table t1 will have 2 rows after commit
        JDBC.assertSingleValueResultSet(s1.executeQuery(
                "select count(*) from t1") , "2");
        s1.executeUpdate("drop table SESSION.t1");
        s1.executeUpdate("drop table t1");
    }
    /**
     *  Temporary tables declared in a pooled connection should get dropped 
     *  when that pooled connection is closed.
     *  
     *  @throws SQLException 
     */
    public void testPooledConnectionClosed() throws SQLException {

        ConnectionPoolDataSource dscsp = 
            J2EEDataSource.getConnectionPoolDataSource();
        //In the first connection handle to the pooled connection, create 
        //physical session schema, create table t1 in it
        PooledConnection pc = dscsp.getPooledConnection();
        Connection pcon = pc.getConnection();
        Statement s = pcon.createStatement();
        try {
            s.executeUpdate("CREATE schema SESSION");
        }
        catch(SQLException e)
        {
            assertSQLState("X0Y68" , e);
        }
        s.executeUpdate("CREATE TABLE SESSION.tx(c21 int)");
        s.executeUpdate("insert into SESSION.tx values(11)");
        s.executeUpdate("insert into SESSION.tx values(12)");
        s.executeUpdate("insert into SESSION.tx values(13)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.tx") , "3");
        //Declare temp table with same name as physical table in SESSION schema
        s.executeUpdate("declare global temporary table SESSION.tx(" +
                "c11 int, c12 int) on commit preserve rows not logged");
        s.executeUpdate("insert into SESSION.tx values(11,1)");
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.tx") , "1");
        commit();
        //Now close the connection handle to the pooled connection
        getConnection().close();
        //Do another getConnection() to get a new connection handle to the 
        //pooled connection
        s = getConnection().createStatement();
        //In this new handle, a select * from SESSION.tx should be looking at 
        //the physical session table
        JDBC.assertSingleValueResultSet(s.executeQuery(
                "select count(*) from SESSION.tx") , "3");
        s.executeUpdate("drop table SESSION.tx");
    }

    /**
     * Test that we don't get an NPE when re-using a PreparedStatement
     * on a temp table declared and then rolled back. See DERBY-6189.
     */
    public  void    test_derby_6189() throws Exception
    {
        Connection  conn = getConnection();

        conn.prepareStatement
            ( "DECLARE GLOBAL TEMPORARY TABLE SESSION.t6189( c21 int, c22 int) not logged on commit preserve rows" )
            .execute();        
        PreparedStatement pStmtInsert = conn.prepareStatement( "insert into SESSION.t6189 values (23, 1)" );

        pStmtInsert.execute();

        conn.rollback();

        try {
            pStmtInsert.execute();
            fail( "Should fail!" );
        }
        catch ( SQLException se)
        {
            assertEquals( "42X05", se.getSQLState() );
        }
    }
    
}
