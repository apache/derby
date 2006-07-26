/*

   Derby - Class org.apache.derbyTesting.functionTests.store.ReEncryptCrashRecovery

   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.sanity.SanityManager;

/*
 * This class tests crash/recovery scenarions during  re-encryption of 
 * databas. Debug flags are used to simulate crashes during the 
 * re-encrytpion. 
 *
 *  Unlike the other recovery tests which do a setup and recovery as different
 *  tests, Incase of re-encryption crash/recovery can be simulated in one 
 *  test itself because re-encryption is done at boot time. When debug flags are
 *  set database boot itself fails. To test the recovery, it is just a matter 
 *  of clearing up the debug flag and rebooting the database. 
 *  
 * In Non debug mode, this tests just acts as a plain re-encryption test,
 * just testing re-encrytpion multiple times. 
 *
 * @author <a href="mailto:suresh.thalamati@gmail.com">Suresh Thalamati</a>
 * @version 1.0
 */

public class ReEncryptCrashRecovery
{

    private static final String TEST_DATABASE_NAME = "wombat_pwd" ;
    private static final String TEST_TABLE_NAME = "emp";
    private static final String OLD_PASSWORD = "xyz1234abc";
    private static final String NEW_PASSWORD = "new1234xyz";

	ReEncryptCrashRecovery() {
        
	}


    /*
	 * Test Re-encrytpion crash/recovery scenarios. 
	 */
	private void runTest() throws Exception {
		logMessage("Begin  ReEncryptCrashRecovery Test");
        createEncryptedDatabase();
        Connection conn = TestUtil.getConnection(TEST_DATABASE_NAME, 
                                                 null);
        createTable(conn, TEST_TABLE_NAME);
        //load some rows 
        insert(conn, TEST_TABLE_NAME, 100);
        conn.commit();
        //shutdown the test db 
		shutdown(TEST_DATABASE_NAME);
        
        // re-enryption crash/recovery test cases.
        crashBeforeCommit();
        recover_crashBeforeCommit();
        //shutdown the test db 
		shutdown(TEST_DATABASE_NAME);

        logMessage("End ReEncryptCrashRecovery Test");
    }


    /** *************************************************
     * Crash/recovery test scenarios during re-encryption.
     ****************************************************/

    /*
     * Attempt to re-encrypt the database and force it to fail 
     * using debug flags just before the commit. 
     */
    private void crashBeforeCommit() {
        // Re-encrytption crash before commit 
        setDebugFlag(TEST_REENCRYPT_CRASH_BEFORE_COMMT);

        SQLException sqle = null;
        try {
            reEncryptDatabase(OLD_PASSWORD, NEW_PASSWORD);
        }catch (SQLException se) {
            // re-encryption of the database should have failed,
            // at the specified debug flag.
            sqle = se;
        }

        // check that database boot failed at the set debug flag.
        verifyException(sqle, TEST_REENCRYPT_CRASH_BEFORE_COMMT);
        // clear the debug flag.
        clearDebugFlag(TEST_REENCRYPT_CRASH_BEFORE_COMMT);
    }

    
    /*
     * Recover the database that failied during re-encryption and 
     * perform some simple sanity check on the database. 
     */
    private void recover_crashBeforeCommit() throws SQLException{
        // starting recovery of database with failed Re-encrytpion
        // in debug mode;
        Connection conn = bootDatabase(OLD_PASSWORD);
        // verify the contents of the db are ok. 
        runConsistencyChecker(conn, TEST_TABLE_NAME);
        // insert some rows, this might fail if anyhing is 
        // wrong in the logging system setup.
        insert(conn, TEST_TABLE_NAME, 100);
        conn.close();
    }
    

    
    // Debug flags that needs to be set to simulate a crash 
    // at different points during re-encryption of the database. 
    // these flags should match the flags in the engine code;
    // these are redifined here to avoid pulling the engine code
    // into the tests. 

    
    /*
      Set to true if we want the re-encryption to crash just 
      before the commit.
	*/

	public static final String TEST_REENCRYPT_CRASH_BEFORE_COMMT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_BEFORE_COMMT" : null ;

    
    
    void setDebugFlag(String debugFlag) {
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_SET(debugFlag);
        }
    }

    void clearDebugFlag(String debugFlag) {
        if (SanityManager.DEBUG) {
            SanityManager.DEBUG_CLEAR(debugFlag);
        }
    }

    /*
     * verify that database boot failed when a debug flag is set. 
     */
    private void verifyException(SQLException sqle, String debugFlag) 
    {
        if (sqle != null) 
        {
            if (sqle.getSQLState() != null && 
                sqle.getSQLState().equals("XJ040")) 
            {
                // boot failed as expected with the  debug flag
            }else 
            {
                dumpSQLException(sqle);
            }
        } else {
            if (SanityManager.DEBUG) 
            {
                logMessage("Did not crash at " + debugFlag);
            }
        }
    }


    /* 
     * create the tables that are used by this test.
     * @param  conn  connection to the database.
     * @param  tableName  Name of the table to create.
     * @exception SQLException if any database exception occurs.
     */
    void createTable(Connection conn, 
                     String tableName) throws SQLException {

			Statement s = conn.createStatement();
			s.executeUpdate("CREATE TABLE " + tableName + 
							"(id INT," +
							"name CHAR(200))");
			s.executeUpdate("create index " + tableName + "_id_idx on " + 
							tableName + "(id)");
			s.close();
    }


    /**
	 * Run some consistency checks.
     * @param  conn  connection to the database.
	 * @param  tableName  consistency checks are performed on this table.
     * @exception SQLException if any database exception occurs.
	 */
	void runConsistencyChecker(Connection conn, 
                               String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.execute("values SYSCS_UTIL.SYSCS_CHECK_TABLE('APP',  'EMP')");
        // check the data in the EMP table.
        select(conn, tableName);
	}

    		
    /**
     * Insert some rows into the specified table.
     * @param  conn  connection to the database.
     * @param  tableName  name of the table that rows are inserted.
     * @param  rowCount   Number of rows to Insert.
     * @exception SQLException if any database exception occurs.
     */
    void insert(Connection conn, 
                String tableName, 
                int rowCount) throws SQLException 
    {

        PreparedStatement ps = conn.prepareStatement("INSERT INTO " + 
                                                     tableName + 
                                                     " VALUES(?,?)");
        int startId = findMax(conn, tableName);
        for (int i = startId; i < rowCount; i++) {
			
            ps.setInt(1, i); // ID
            ps.setString(2 , "skywalker" + i);
            ps.executeUpdate();
        }
        conn.commit();
        ps.close();
    }

    /**
    * find a max value on the give table. 
    * @param  conn  connection to the database.
    * @param  tableName  name of the table.
    * @exception SQLException if any database exception occurs.
    */
    private int findMax(Connection conn, 
                        String tableName) throws SQLException 
    {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT max(ID) from " +  
										  tableName);
        rs.next();
        return rs.getInt(1);
    }


    /*
     * read  the rows in the table. 
     * @param  conn  connection to the database.
     * @param  tableName  select operation is perfomed on this table.
     * @exception SQLException if any database exception occurs.
     */
    void select(Connection conn , 
                String tableName) throws SQLException 
    {
		
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
     * create an encrypted database.
     */
    private void createEncryptedDatabase() throws SQLException
    {
        TestUtil.getConnection(TEST_DATABASE_NAME, 
        "create=true;dataEncryption=true;bootPassword=" + 
                               OLD_PASSWORD);
    }

    /**
     * Re-encrypt the database. 
     * @param currentPassword  current boot password.
     * @param newPassword      new password to boot the database 
     *                         after successful re-encryption.
     * @exception SQLException if any database exception occurs.
     */
    private void reEncryptDatabase(String currentPassword, 
                                   String newPassword) 
        throws SQLException
    {
        // re-encrypt the database.
        String connAttrs = "bootPassword=" + currentPassword + 
                ";newBootPassword=" + newPassword;
        TestUtil.getConnection(TEST_DATABASE_NAME, connAttrs); 
    }

    
    /**
     * Encrypt an un-encrypted atabase. 
     * @param password boot password of the database.
     * @exception SQLException if any database exception occurs.
     */
    private void encryptDatabase(String password) 
        throws SQLException
    {
        //encrypt an existing database.
        String connAttrs = "dataEncryption=true;bootPassword=" +
            password ;

        TestUtil.getConnection(TEST_DATABASE_NAME, connAttrs); 
    }
    

    /**
     * Boot the database. 
     * @param password boot password of the database.
     * @exception SQLException if any database exception occurs.
     */
    Connection bootDatabase(String password) throws SQLException {
        
        return TestUtil.getConnection(TEST_DATABASE_NAME, 
                                      "bootPassword=" + password);
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
			if (se.getSQLState() == null || !(se.getSQLState().equals("08006")))
            {
                // database was not shutdown properly
				dumpSQLException(se);
            }
        }
    }

    /**
     * dump the SQLException to the standard output.
	 */
	private void dumpSQLException(SQLException sqle) {
		
		org.apache.derby.tools.JDBCDisplayUtil.	ShowSQLException(System.out, sqle);
		sqle.printStackTrace(System.out);
	}


    void logMessage(String str)
    {
        System.out.println(str);
    }
	
	
	public static void main(String[] argv) throws Throwable {
		
        ReEncryptCrashRecovery test = new ReEncryptCrashRecovery();
        try {
            test.runTest();
        }
        catch (SQLException sqle) {
			org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(
                System.out, sqle);
			sqle.printStackTrace(System.out);
		}
    }
}
