/*

   Derby - Class org.apache.derbyTesting.functionTests.store.LogDeviceTest

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
import java.io.File;
import java.io.IOException;

/*
 * This class tests create database with transaction 
 * log at non-default location specified as absolute path.
 * @version 1.0
 */

public class LogDeviceTest {

	private static final String TEST_DATABASE_NAME = "wombat" ;
    private static final String TEST_DATABASE_NAME1 = "wombat1" ;
	private static final String TEST_TABLE_NAME   =    "emp";
    private static final String LOG_PATH = "extinout/logDeviceTest_c1";
    private static final String LOG_PATH1 = "extinout/logDeviceTest_c2";

	public static void main(String[] argv) throws Throwable {
		
        LogDeviceTest test = new LogDeviceTest();
   		ij.getPropertyArg(argv); 

        try {
            test.runTest();
        }
        catch (SQLException sqle) {
			dumpSQLException(sqle);
		} 
    }

    /*
     * Returns the absolute path of the given path.
     */
    private String getFullPath(String path) throws IOException{
        File f = new File(path);
        return f.getCanonicalPath();
    }


    /*
     * create a directory.
     */
    private boolean createDir(String path) {
        File f = new File(path);
        return f.mkdirs();
    }



	/*
	 * Test database creation with log in non-default location.
	 */
	private void runTest() throws Exception {
		logMessage("Begin Log Device Test");

        // case 1: test logDevice property with absolute path

        Connection conn;
        String connAttr = "create=true;" + "logDevice=" + 
                           getFullPath(LOG_PATH);
        conn = TestUtil.getConnection(TEST_DATABASE_NAME, connAttr);
        conn.setAutoCommit(false);
        createTable(conn, TEST_TABLE_NAME);
        conn.commit();
        // just insert few rows and rollback and commit 
        // to make sure  tranaction log is working fine. 
        insert(conn, TEST_TABLE_NAME, 100);
        conn.commit();
        insert(conn, TEST_TABLE_NAME, 100);
        conn.rollback();
		// shutdown the test db 
		shutdown(TEST_DATABASE_NAME);
        
        // case 2: database creation on non-empty 
        // log dir location should fail.  

        
        try {
            // this database creation is specifying the same log 
            // location as the one above; so it should fail. 
            conn = TestUtil.getConnection(TEST_DATABASE_NAME1, 
                                          connAttr);
        }catch (SQLException se) {
            SQLException nse = se.getNextException();
            if (nse != null) {
                // expect to fail with log dir exists error.
                if (nse.getSQLState().equals("XSLAT"))
                    System.out.println("Failed with Expected error:" + 
                                       nse.getSQLState());
                else 
                    dumpSQLException(se);
            } else {
                dumpSQLException(se);
            }
        }
            
        // case 3: database creation on an empty log dir should pass. 

        // create a dummy log dir 
        createDir(getFullPath(LOG_PATH1) + 
                  File.separator + "log"); 
        connAttr = "create=true;" + "logDevice=" + 
                   getFullPath(LOG_PATH1);
        conn = TestUtil.getConnection(TEST_DATABASE_NAME1, 
                                      connAttr);
        // just insert few rows and rollback and commit 
        // to make sure  tranaction log is working fine. 
        conn.setAutoCommit(false);
        createTable(conn, TEST_TABLE_NAME);
        conn.commit();
        insert(conn, TEST_TABLE_NAME, 100);
		// shutdown the test db 
		shutdown(TEST_DATABASE_NAME1);
        
        // reconnect to the same database.
        conn = TestUtil.getConnection(TEST_DATABASE_NAME1, null);
        
		logMessage("End log device Test");
	}

		
	/**
	 * Shutdown the datbase
	 * @param  dbName  Name of the database to shutdown.
	 */
	private void shutdown(String dbName) {

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
	private void logMessage(String   str)	{
			System.out.println(str);
	}

	
	/**
	 * dump the SQLException to the standard output.
	 */
	static private void dumpSQLException(SQLException sqle) {
		
		org.apache.derby.tools.JDBCDisplayUtil.	ShowSQLException(System.out, sqle);
		sqle.printStackTrace(System.out);
	}

    /**
     * Insert some rows into the specified table.
     * @param  conn   connection to the database.    
     * @param  tableName  name of the table that rows are inserted.
     * @param  rowCount   Number of rows to Insert.
     * @exception SQLException if any database exception occurs.
     */
    private void insert(Connection conn, 
                        String tableName, 
                        int rowCount) throws SQLException {

        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + 
                                                     tableName + 
                                                     " VALUES(?,?,?)");
        for (int i = 0; i < rowCount; i++) {
			
            ps.setInt(1, i); // ID
            ps.setString(2 , "skywalker" + i);
            ps.setFloat(3, (float)(i * 2000)); 
            ps.executeUpdate();
		}
        ps.close();
    }


    /* 
     * create the tables that are used by this test.
     * @param  conn   connection to the database.
     * @param  tableName  Name of the table to create.
     * @exception SQLException if any database exception occurs.
     */
    private	void createTable(Connection conn, 
                             String tableName) throws SQLException {

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
