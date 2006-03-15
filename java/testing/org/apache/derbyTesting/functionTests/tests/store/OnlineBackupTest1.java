/*

   Derby - Class org.apache.derbyTesting.functionTests.store.OnlineBackupTest1

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

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
 * This class tests online backup when dml/ddl actions
 * are running in parallel to the backup thread. 
 *
 * @author <a href="mailto:suresh.thalamati@gmail.com">Suresh Thalamati</a>
 * @version 1.0
 */

public class OnlineBackupTest1 {

	private static final String TEST_DATABASE_NAME = "wombat" ;
	private static final String TEST_TABLE_NAME   =    "emp";
    private static final String TEST_TABLE_NAME_1 =    "emp_1";
    private static final String TEST_TABLE_NAME_2 =    "emp_2";
    private static final String BACKUP_PATH = "extinout/onlinebackuptest1";

	public static void main(String[] argv) throws Throwable {
		
        OnlineBackupTest1 test = new OnlineBackupTest1();
   		ij.getPropertyArg(argv); 

        try {
            test.runTest();
        }
        catch (SQLException sqle) {
			dumpSQLException(sqle);
		} 
    }


	/*
	 * Test online backup with unlogged operations. And DML/DDL's
	 * running in paralel to the backup. After the backup is complete restore
	 * the database from the backup and performs consistency checks on the
	 * database to make sure backup was good one.  
	 */
	private void runTest() throws Exception {
		logMessage("Begin Online Backup Test1");
		Connection conn = ij.startJBMS();
		conn.setAutoCommit(false);
		DatabaseActions dbActions = new DatabaseActions(conn);
		//create the test  tables. 
		dbActions.createTable(TEST_TABLE_NAME);
        dbActions.createTable(TEST_TABLE_NAME_1);
        dbActions.createTable(TEST_TABLE_NAME_2);
        conn.commit();

        // start first unlogged operation
		dbActions.startUnloggedAction(TEST_TABLE_NAME_1);
		logMessage("First Transaction with Unlogged Operation Started");

        // start second unlogged opearation
        Connection conn1 = ij.startJBMS();
		conn1.setAutoCommit(false);
		DatabaseActions dbActions1 = new DatabaseActions(conn1);
		dbActions1.startUnloggedAction(TEST_TABLE_NAME_2);
		logMessage("Second Transaction with Unlogged Operation Started");

        // setup threads.
        // start a  thread to perform online backup
		OnlineBackup backup = new OnlineBackup(TEST_DATABASE_NAME, BACKUP_PATH);
		Thread backupThread = new Thread(backup, "BACKUP");
        
        // run some dml actions in another thread
        Connection dmlConn = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        DatabaseActions dmlActions = 
            new DatabaseActions(DatabaseActions.DMLACTIONS, dmlConn);
		Thread dmlThread = new Thread(dmlActions, "DML_THREAD");
        
        // run some DDL create/drop tables in another thread
        Connection ddlConn = TestUtil.getConnection(TEST_DATABASE_NAME, null);
        
        DatabaseActions ddlActions = 
            new DatabaseActions(DatabaseActions.CREATEDROPS, ddlConn);
        Thread ddlThread = new Thread(ddlActions, "DDL_THREAD");

        try {
            // start a  thread to perform online backup
            backupThread.start();	
            // wait for the backup to start
            backup.waitForBackupToBegin();
            logMessage("BACKUP STARTED");

            // run some dml actions in another thread
            dmlThread.start();

            // run some DDL create/drop tables in another thread
            ddlThread.start();

            // sleep for few seconds just to make sure backup thread is actually
            // gone to a wait state for unlogged actions to commit and there is
            // some ddl and dml activity in progress. 
            java.lang.Thread.sleep(50000);
			
            // backup should not even start doing real work before the
            // unlogged transaction is commited
            if(!backup.isRunning())
                logMessage("Backup is not waiting for unlogged actions to commit");

            // end the unlogged work transaction.
            dbActions.endUnloggedAction(TEST_TABLE_NAME_1);
            // end the unlogged work transaction.
            dbActions1.endUnloggedAction(TEST_TABLE_NAME_2);
        
            backup.waitForBackupToEnd();

        }finally {
            //stop all threads activities.
            backupThread.join();
            dmlActions.stopActivity();
            ddlActions.stopActivity(); 
            dmlThread.join();
            ddlThread.join(); 
        }        
        // close the connections.
        conn.close();
        conn1.close();
        dmlConn.close();
        ddlConn.close() ;


		//shutdown the test db 
		shutdown(TEST_DATABASE_NAME);

		// restore the database from the backup and run some checks 
		backup.restoreFromBackup();
		logMessage("Restored From the Backup");
		runConsistencyChecker(TEST_DATABASE_NAME);
		logMessage("Consistency Check is Done");
		//shutdown the test db 
		shutdown(TEST_DATABASE_NAME);
		logMessage("End Online Backup Test1");
	}

	
	/**
	 * Run some consistency checks.
	 * @param  dbName  consistency checks are performed on this database.
	 */
	void runConsistencyChecker(String dbName) throws SQLException {
        Connection conn = TestUtil.getConnection(dbName, null);
		Statement stmt = conn.createStatement();
		stmt.execute("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP',  'EMP')");
        //check the data in the EMP table.
        DatabaseActions dbActions = new DatabaseActions(conn);
        dbActions.select(TEST_TABLE_NAME);
        dbActions.select(TEST_TABLE_NAME_1);
        dbActions.select(TEST_TABLE_NAME_2);
		conn.close();

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
	void logMessage(String   str)	{
			System.out.println(str);
	}

	
	/**
	 * dump the SQLException to the standard output.
	 */
	static private void dumpSQLException(SQLException sqle) {
		
		org.apache.derby.tools.JDBCDisplayUtil.	ShowSQLException(System.out, sqle);
		sqle.printStackTrace(System.out);
	}

	/*
	 * This class implements some DML and DDL operations to 
	 * run againest the datbase, when the backup is in progress. 
	 * Some of these operations can be  run in seperate threads in a
	 * loop until they are stopped  by some other thread. 
	 */
	
	class DatabaseActions implements Runnable {
 
		public static final int DMLACTIONS =   1;
		public static final int CREATEDROPS =  2;

		private static final int COMMIT =     1;
		private static final int ROLLBACK =   2;
		private static final int OPENTX =     3;

		private int     action = 0;
		private volatile boolean stopActivity = false ;
		private Connection conn;
	
		DatabaseActions(Connection conn) {
			this.conn = conn;
		};

		DatabaseActions(int action, Connection conn)	{
			this.action = action;
			this.conn = conn;
		}

		/**
		 * stops any actions that are looping on a differt threads.
		 */
		public void stopActivity() {
			stopActivity = true;
		}

		/**
		 * implementation of run() method in the Runnable interface, which
		 * is invoked when a thread is started using this class object. 
		 * <p>
		 * Performs DML ot DDL actions.
		 */
		 public void run() {
			try {
				conn.setAutoCommit(false);
				switch(action) {
					case DMLACTIONS :
						performDmlActions();
						break;
					case CREATEDROPS:
						performCreateDropTables() ;
						break;
				}
			} catch (SQLException sqle) {
				org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, sqle);
				sqle.printStackTrace(System.out);
			} 
		}

		
		/*
		 * Run insert, update, select on the test table in a loop.
		 */
		void performDmlActions() throws SQLException {
			
			while(!stopActivity) {
				insert(TEST_TABLE_NAME, 100, COMMIT, 10);
				insert(TEST_TABLE_NAME, 100, ROLLBACK, 10);
				update(TEST_TABLE_NAME, 50, ROLLBACK, 10);
				select(TEST_TABLE_NAME);
			}
		}


		
		/**
		 * start an Unlogged operation, but don't commit the transaction.
		 * @param  tableName  name of the table to start the unlogged operation.
		 * @exception SQLException if any database exception occurs.
		 */
		void startUnloggedAction(String tableName) throws SQLException {
			// load some data
			insert(tableName, 100, COMMIT, 10);
			// execute a unlogged database operation
			Statement s = conn.createStatement();
			
            // index creation does not log the index entries 
            s.executeUpdate("create index " + tableName + "_name_idx on " + 
                            tableName + "(name) ");
			s.close();
		}

		
		/**
		 * end an Unlogged operation, commit the transaction.
		 * @param  tableName  name of the table to end unlogged operation.
		 * @exception SQLException if any database exception occurs.
		 */
		void endUnloggedAction(String tableName) throws SQLException {
            // insert some rows, insert should be successful even if
            // backup is blocking for uncommitted unlogged operations. 
			insert(tableName, 1000, OPENTX, 10);
			conn.commit();
		}

				
		/**
		 * Create and Drop some tables.
		 * @exception SQLException if any database exception occurs.
		 */
		void performCreateDropTables() throws SQLException { 
			
			Statement s = conn.createStatement();
			while(!stopActivity) {
				for( int i = 0 ; i < 10; i++) {
					String tableName = "emp" + i ;
					createTable(tableName);
					//load some data
					insert(tableName, 100, OPENTX, 10);
					if((i % 2) == 0) {
						conn.commit();
                    }
					else
						conn.rollback();
				}

                //drop all the table that are created above.
				for( int i = 0 ; i < 10 ; i=i+2) {
					String tableName = "emp" + i ;
					s.executeUpdate("drop TABLE " + "emp" +i ); 
                    conn.commit();
				}
			}
            s.close();
		}


		
		/**
		 * Insert some rows into the specified table.
		 * @param  tableName  name of the table that rows are inserted.
		 * @param  rowCount   Number of rows to Insert.
		 * @param  txStaus    Transacton status commit/rollback/open.
		 * @param  commitCount After how many inserts commit/rollbacku should happen.
		 * @exception SQLException if any database exception occurs.
		 */
		void insert(String tableName, int rowCount, 
					int txStatus, int commitCount) throws SQLException {

			PreparedStatement ps = conn.prepareStatement("INSERT INTO " + 
														 tableName + 
														 " VALUES(?,?,?)");
			for (int i = 0; i < rowCount; i++) {
			
				ps.setInt(1, i); // ID
				ps.setString(2 , "skywalker" + i);
				ps.setFloat(3, (float)(i * 2000)); 
				ps.executeUpdate();
				if ((i % commitCount) == 0)
				{
					endTransaction(txStatus);
				}
			}

			endTransaction(txStatus);
			ps.close();
		}



		/**
		 * commit/rollback the transaction. 
		 * @param  txStaus    Transacton status commit/rollback/open.
		 * @exception SQLException if any database exception occurs.
		 */
		void endTransaction(int txStatus) throws SQLException
		{
			switch(txStatus){
			case COMMIT: 
				conn.commit();
				break;
			case ROLLBACK:
				conn.rollback();
				break;
			case OPENTX:
				//do nothing
				break;
			}
		}
		
		/**
		 * update some rows in the table.
		 * @param  tableName  name of the table that rows are updates.
		 * @param  rowCount   Number of rows to update.
		 * @param  txStaus    Transacton status commit/rollback/open.
		 * @param  commitCount After how many updates commit/rollback should
		 *                      happen.
		 * @exception SQLException if any database exception occurs.
		 */

		void update(String tableName, int rowCount, 
					int txStatus, int commitCount) throws SQLException
		{

			PreparedStatement ps = conn.prepareStatement("update " + tableName + 
								 " SET name = ?  where id=?");
		
			for (int i = 0; i < rowCount; i++) {
                ps.setString(1 ,  "moonwalker" + i);
				ps.setInt(2, i); // ID
				ps.executeUpdate();
				if ((i % commitCount) == 0)
				{
					endTransaction(txStatus);
				}
			}
			endTransaction(txStatus);
			ps.close();
		}


		/*
		 * read  the rows in the table. 
		 * @param  tableName  select operation is perfomed on this table.
		 * @exception SQLException if any database exception occurs.
		 */
		void select(String tableName) throws SQLException {
		
			Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery("SELECT ID, name from " +  
										  tableName + " order by id" );
			int count = 0;
			int id = 0;
			while(rs.next())
			{
				int tid = rs.getInt(1);
				String name = rs.getString(2);
 				if(name.equals("skywalker" + id) && tid!= id)
				{
					logMessage("DATA IN THE TABLE IS NOT AS EXPECTED");
					logMessage("Got :ID=" +  tid + " Name=:" + name);
					logMessage("Expected: ID=" + id + "Name=" + "skywalker" + id );
				}

				id++;
				count++;
			}
            
			rs.close();
			s.close();
            conn.commit();
		}

		/* 
		 * create the tables that are used by this test.
		 * @param  tableName  Name of the table to create.
		 * @exception SQLException if any database exception occurs.
		 */
		void createTable(String tableName) throws SQLException {

			Statement s = conn.createStatement();
			s.executeUpdate("CREATE TABLE " + tableName + 
							"(id INT," +
							"name CHAR(200),"+ 
							"salary float)");
			s.executeUpdate("create index " + tableName + "_id_idx on " + 
							tableName + "(id)");
			s.close();
		}

	}
}
