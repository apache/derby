/*

   Derby - Class org.apache.derbyTesting.functionTests.store.OnlineBackupTest3

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

package org.apache.derbyTesting.functionTests.tests.store;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/*
 * This class tests online backup when jar actions
 * are running in parallel to the backup thread. 
 *
 * @version 1.0
 */

public class OnlineBackupTest3 {

    private static final String TEST_DATABASE_NAME = "wombat" ;
    private static final String BACKUP_PATH = "extinout/onlinebackuptest3";

    public static void main(String[] argv) throws Throwable {

        OnlineBackupTest3 test = new OnlineBackupTest3();
        ij.getPropertyArg(argv); 

        try {
            test.runTest();
        }
        catch (SQLException sqle) {
            dumpSQLException(sqle);
        } 
    }


    /*
     * Test online backup with unlogged jar operations running in parallel. 
     */
    private void runTest() throws Exception{
        logMessage("Begin Online Backup Test3");
        Connection conn = ij.startJBMS();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.execute("create table t1(a int ) ");
        stmt.execute("insert into t1 values(1)");
        stmt.execute("insert into t1 values(2)");
        stmt.execute("create table customer(id int , name varchar(100))");
        stmt.execute("insert into customer values(1, 'ABC')");
        stmt.execute("insert into customer values(2, 'XYZ')");
        String crproc = "create procedure addCustomer(id INT, name VARCHAR(100)) " +
            "MODIFIES SQL DATA " + 
            "external name " + 
            "'org.apache.derbyTesting.backupRestore.Customer.addCustomer' " + 
            " language java parameter style java ";
            
        stmt.execute(crproc);

        String dvfunc = "create function dv(P1 INT) RETURNS INT NO SQL " +
            " external name 'dbytesting.CodeInAJar.doubleMe' " + 
            " language java parameter style java " ;

        stmt.execute(dvfunc) ;
        conn.commit();
        
        logMessage("Initial Setup Complete");

        // perform install jar operation with 
        // online backup running in parallel.
        installJarTest();

        // perform remove jar operation with 
        // online backup running in parallel.
        removeJarTest();

        logMessage("End Online Backup Test3");
    }


    /**
     * Shutdown the datbase
     * @param  dbName  Name of the database to shutdown.
     */
    void shutdown(String dbName) {

        try{
            //shutdown
            TestUtil.getConnection(dbName, "shutdown=true");
        }catch(SQLException se){
            if (se.getSQLState() != null && se.getSQLState().equals("08006"))
                System.out.println("database shutdown properly");
            else
                dumpSQLException(se);
        }
    }

    /**
     * Write message to the standard output.
     */
    void logMessage(String   str){
        System.out.println(str);
    }

    /**
     * dump the SQLException to the standard output.
     */
    static private void dumpSQLException(SQLException sqle) {

        org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
        sqle.printStackTrace(System.out);
    }

    
    private int countRows(Connection conn, 
                          String tableName) 
        throws SQLException
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT count(*) from " +  tableName );
        rs.next();
        int noRows = rs.getInt(1);
        rs.close();
        s.close();
        return noRows;
    }

    /*
     * Test install jar running in parallel to backup and vice versa. 
     */
    void installJarTest() throws Exception{
        logMessage("Begin Install Jar Test");
        Connection conn1 = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        conn1.setAutoCommit(false);
        Statement conn1_stmt = conn1.createStatement();
        Connection conn2 = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        conn2.setAutoCommit(false);
        Statement conn2_stmt = conn2.createStatement();

        
        conn1_stmt.execute(
           "call sqlj.install_jar('extin/brtestjar.jar', 'math_routines', 0)");
        
        try {
            // followng backup call should fail because jar operation is pending 
           conn2_stmt.execute(
            "call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT('extinout/mybackup')");
        } catch (SQLException sqle) {
            //above statement should have failed. 
            org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
        }

        // invoke backup in another thread, it should block for the above install jar 
        // operation to install  'brtestjar.jar to commit.
        
        // start a  thread to perform online backup
        OnlineBackup backup = new OnlineBackup(TEST_DATABASE_NAME, BACKUP_PATH);
        Thread backupThread = new Thread(backup, "BACKUP1");
        backupThread.start();
        // wait for the backup to start
        backup.waitForBackupToBegin();
        logMessage("Backup-1 Started");

        // sleep for few seconds just to make sure backup thread has actually
        // gone into a wait state for unlogged actions to commit.
        java.lang.Thread.sleep(1000);
        
        // backup should not even start doing real work before the
        // unlogged transaction is commited
        if(!backup.isRunning())
            logMessage("Backup is not waiting for unlogged " +  
                       "install jar action to commit");

        //insert some rows that should appear in the backup.
        conn1_stmt.execute("insert into t1 values(3)");
        conn1_stmt.execute("insert into t1 values(4)");
        conn1_stmt.execute("insert into t1 values(5)");
        
        // set the database class with both the jars  installed above.
        conn1_stmt.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " + 
                           "'derby.database.classpath', " + 
                           "'APP.math_routines') " ) ;

        //commit the transaction with jar opearation that is blocking the backup.
        conn1.commit();
        logMessage("The transaction that was blocking the backup has ended");

        // wait for backup to finish. 
        backup.waitForBackupToEnd();
        backupThread.join();
        logMessage("Backup-1 Completed");
        
        // Case : jar op should block if backup is in progress
        // add a index that will block the backup until it is committted.
        conn1_stmt.execute("create index idx1 on customer(id)");
        conn1_stmt.execute("insert into t1 values(6)");
        
        // start a  thread to perform online backup
        backup = new OnlineBackup(TEST_DATABASE_NAME, BACKUP_PATH);
        backupThread = new Thread(backup, "BACKUP2");
        backupThread.start();
        // wait for the backup to start
        backup.waitForBackupToBegin();
        logMessage("Backup-2 Started");

        // sleep for few seconds just to make sure backup thread is actually
        // gone to a wait state for unlogged actions to commit.
        java.lang.Thread.sleep(1000);

        // backup should not even start doing real work before the
        // unlogged transaction is commited
        if(!backup.isRunning())
            logMessage("Backup is not waiting for unlogged " +  
                       "index action to commit");


        // add another jar file  , this one should block and 
        // should not get into the backup. Backup does not allow new 
        // jar operation if it is already waiting for backup blocking
        // to complete(commit/rollback). 

        AsyncStatementThread asyncJarActionThread = 
            new AsyncStatementThread(conn2, 
          "call sqlj.install_jar('extin/obtest_customer.jar', 'customer_app', 0)");
        asyncJarActionThread.start();
        logMessage("Started obtest_customer.jar addition in seperate thread");

        //sleep for few seconds to give a chance for the 
        //jar addition thread to get into action.
        java.lang.Thread.sleep(1000);

        //roll back the index op. Backup should proceed now.
        conn1.rollback();
        logMessage("The transaction that was blocking the backup has ended");

        // wait for backup to finish. 
        backup.waitForBackupToEnd();
        backupThread.join();
        logMessage("Backup-2 Completed");
        
        // wait for customer app jar installation to finish now. 
        asyncJarActionThread.join();
        logMessage("obtest_customer.jar addition is complete");

        // set the database class with both the jars  installed above.
        conn1_stmt.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " + 
                           "'derby.database.classpath', " + 
                           "'APP.customer_app:APP.math_routines') " ) ;
        
        conn1.commit();

        // second jar must have got installed after the backup. 
        // call a function in the custome_app jar
        conn1_stmt.execute("call addCustomer(3 , 'John')");
        conn1.commit();
        
        logMessage("No of rows in table t1: " + countRows(conn1, "T1"));
        logMessage("No of rows in table customer: " + 
                   countRows(conn1, "customer"));
        conn1.commit();
        conn2.commit();
        conn1_stmt.close();
        conn2_stmt.close();
        conn1.close();
        conn2.close();
        
        //shutdown the test db 
        shutdown(TEST_DATABASE_NAME);
        // restore the database from the backup and run some checks 
        backup.restoreFromBackup();
        logMessage("Restored From the Backup");
        Connection conn = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        Statement stmt = conn.createStatement();
        logMessage("No of rows in table t1: " + countRows(conn, "T1"));
        logMessage("No of rows in table customer: " + 
                   countRows(conn, "customer"));
        // execute select statement using the "dv" funciont.  
        stmt.execute("select dv(a) from t1");

        
        try {
            // set the database class with both the jars  installed above.
            stmt.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " + 
                           "'derby.database.classpath', " + 
                           "'APP.customer_app:APP.math_routines') " ) ;
            stmt.execute("call addCustomer(3 , 'John')"); 
        }catch(SQLException se) {
            //ignore for now. No sure way to 
            //check that jar did not get into backup 
            //without debug flags. 
        }

        stmt.close();
        conn.close();

        //shutdown the test db 
        shutdown(TEST_DATABASE_NAME);
        logMessage("End Of Install Jar Test.");

    }


    /*
     * Test remove jar running in parallel to backup and vice versa. 
     */
    void removeJarTest() throws Exception{
        logMessage("Begin Remove Jar Test");
        Connection conn1 = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        conn1.setAutoCommit(false);
        Statement conn1_stmt = conn1.createStatement();
        Connection conn2 = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        conn2.setAutoCommit(false);
        Statement conn2_stmt = conn2.createStatement();
        try {
            conn1_stmt.execute(
           "call sqlj.install_jar('extin/obtest_customer.jar', 'customer_app', 0)");
        }catch(SQLException se) {
            //it is ok if was jar already there.
        }

        // remove both the jars from the class path , 
        // so that we can remove them from the database. 
        conn1_stmt.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " + 
                        "'derby.database.classpath', '')") ;
        conn1.commit();

        conn1_stmt.execute(
           "call sqlj.remove_jar('APP.math_routines', 0)");
        
        // Case 0: backup call that is not waiting for unlogged 
        // opereation to complete should fail when a remove jar 
        // is not ended when backup started. 

        try {
            // followng backup call should fail because remove 
            // jar operation is pending 
           conn2_stmt.execute(
            "call SYSCS_UTIL.SYSCS_BACKUP_DATABASE_NOWAIT('extinout/mybackup')");
        } catch (SQLException sqle) {
            //above statement should have failed. 
            org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
        }


                
        // Case 1: backup should block because when a remove jar
        // is not ended when backup started. 

        // invoke backup in another thread, should block for 
        // the above remove jar  to commit.
        
        // start a  thread to perform online backup
        OnlineBackup backup = new OnlineBackup(TEST_DATABASE_NAME, BACKUP_PATH);
        Thread backupThread = new Thread(backup, "BACKUP3");
        backupThread.start();
        // wait for the backup to start
        backup.waitForBackupToBegin();
        logMessage("Backup-3 Started");

        // sleep for few seconds just to make sure backup thread is actually
        // gone to a wait state for unlogged actions to commit.
        java.lang.Thread.sleep(1000);

        // backup should not even start doing real work before the
        // unlogged transaction is commited
        if(!backup.isRunning())
            logMessage("Backup is not waiting for unlogged " +  
                       "remove jar action to commit");

        //insert some rows that should appear in the backup.
        conn1_stmt.execute("insert into t1 values(10)");
        conn1_stmt.execute("insert into t1 values(11)");

        //commit the transaction with jar opearation that is blocking the backup.
        conn1.commit();
        logMessage("The transaction that was blocking the backup has ended");
        
        // wait for backup to finish. 
        backup.waitForBackupToEnd();
        backupThread.join();

        logMessage("Backup-3 Completed");
        
        // Case 2: remove jar op should block if backup is in progress
        // add a index that will block the backup until it is committted.
        conn1_stmt.execute("create index idx1 on customer(id)");
        conn1_stmt.execute("insert into t1 values(12)");
        
        // start a  thread to perform online backup
        backup = new OnlineBackup(TEST_DATABASE_NAME, BACKUP_PATH);
        backupThread = new Thread(backup, "BACKUP4");
        backupThread.start();
        // wait for the backup to start
        backup.waitForBackupToBegin();
        logMessage("Backup-4 Started");

        // sleep for few seconds just to make sure backup thread is actually
        // gone to a wait state for unlogged actions to commit.
        java.lang.Thread.sleep(1000);

        // backup should not even start doing real work before the
        // unlogged transaction is commited
        if(!backup.isRunning())
            logMessage("Backup is not waiting for unlogged " +  
                       "index action to commit");


        // remove another jar file  , this one should block and 
        // should not get into the backup. Backup does not allow new 
        // jar operation if it is already waiting for backup blocking
        // to complete(commit/rollback). 

        AsyncStatementThread asyncJarActionThread = 
            new AsyncStatementThread(conn2, 
          "call sqlj.remove_jar('APP.customer_app', 0)");
        asyncJarActionThread.start();
        logMessage("Started obtest_customer.jar remove in seperate thread");

        //sleep for few seconds to give a chance for the 
        //jar addition thread to get into action.
        java.lang.Thread.sleep(1000);

        //roll back the index op. Backup should proceed now.
        conn1.rollback();
        logMessage("The transaction that was blocking the backup has ended");
        // wait for backup to finish. 
        backup.waitForBackupToEnd();
        backupThread.join();
        logMessage("Backup-4 Completed");

        // wait for customer app jar removal to finish now. 
        asyncJarActionThread.join();
        logMessage("obtest_customer.jar remove is complete");
        
        //this insert should not apprear on restore.
        conn1_stmt.execute("insert into t1 values(13)");

        logMessage("No of rows in table t1: " + countRows(conn1, "T1"));
        logMessage("No of rows in table customer: " + 
                   countRows(conn1, "customer"));
        conn1.commit();
        conn2.commit();
        conn1_stmt.close();
        conn2_stmt.close();
        conn1.close();
        conn2.close();
        
        //shutdown the test db 
        shutdown(TEST_DATABASE_NAME);
        // restore the database from the backup and run some checks 
        backup.restoreFromBackup();
        logMessage("Restored From the Backup");
        Connection conn = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        Statement stmt = conn.createStatement();
        logMessage("No of rows in table t1: " + countRows(conn, "T1"));
        logMessage("No of rows in table customer: " + 
                   countRows(conn, "customer"));

        // check if the jar removal was successful.
        // APP.math_routines should not be in backup.
        try {
            // set the database class path with the jar removed above, 
            // it should fail.
            stmt.execute("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY( " + 
                           "'derby.database.classpath', " + 
                           "'APP.math_routines') " ) ;
        }catch (SQLException sqle) {
            //above statement should have failed. 
            org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
        }
        

        stmt.close();
        conn.close();

        //shutdown the test db 
        shutdown(TEST_DATABASE_NAME);
        logMessage("End Of Remove Jar Test.");

    }


    /*
     * Run a sql statement in a seperate thread. 
     */
    class AsyncStatementThread extends Thread {
        Connection conn;
        String stmt;
    
        AsyncStatementThread(Connection conn, String stmt) {
            this.conn = conn;
            this.stmt = stmt;
        }

        public void run() {
            Statement aStatement = null;
            try {
                aStatement = conn.createStatement();
                aStatement.execute(stmt);
                aStatement.close();
                // commit here, it is possible that 
                // this thread may have got into action 
                // before the backup went into wait state.
                conn.commit();
            } catch (SQLException sqle) {
                org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
                sqle.printStackTrace(System.out);
            }
            aStatement = null;
        }
    }

}
