/*

   Derby - Class org.apache.derbyTesting.functionTests.store.ReEncryptCrashRecovery

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
import org.apache.derby.iapi.services.sanity.SanityManager;

/*
 * This class tests crash/recovery scenarions during  (re) encryption of 
 * database. Debug flags are used to simulate crashes during the 
 * encrytpion of an un-encrypted database and re-encryption of an encrypted
 * database with new password/key. 
 *
 *  Unlike the other recovery tests which do a setup and recovery as different
 *  tests, Incase of re-encryption crash/recovery can be simulated in one 
 *  test itself because re-encryption is done at boot time. When debug flags are
 *  set database boot itself fails. To test the recovery, it is just a matter 
 *  of clearing up the debug flag and rebooting the database. 
 *  
 * In Non debug mode, this tests does not do anything.
 *
 * @version 1.0
 */

public class ReEncryptCrashRecovery
{

    // database name used to test re-encryption of an encrypted database 
    // using a new boot password.
    private static final String TEST_REENCRYPT_PWD_DATABASE = "wombat_pwd_ren" ;
    // database name used to test encryption and un-encrypted database.
    // using a boot password.
    private static final String TEST_ENCRYPT_PWD_DATABASE = "wombat_pwd_en";


    // database name used to test re-encryption of an encrypted database 
    // using the external encryption key.
    private static final String TEST_REENCRYPT_KEY_DATABASE = "wombat_key_ren" ;
    // database name used to test encryption of un-encrypted database.
    // using external encryption key.
    private static final String TEST_ENCRYPT_KEY_DATABASE = "wombat_key_en";

    // flags to indicate type of mechanism used to test the (re)encryption
    private static final  int USING_KEY = 1;
    private static final  int USING_PASSWORD = 2;

    // flags to indicate the password/key to be used during recovery
    // on reboot after a crash.
    private static final int NONE = 1;
    private static final int OLD  = 2;
    private static final int NEW  = 3;

    // test table name.
    private static final String TEST_TABLE_NAME = "emp";

    private static final String OLD_PASSWORD = "xyz1234abc";
    private static final String NEW_PASSWORD = "new1234xyz";
    
    private static final String OLD_KEY = "6162636465666768";
    private static final String NEW_KEY = "5666768616263646";
    
    // the current database being tested.
    private String currentTestDatabase ;
    // the current encryption type being tested. 
    private int encryptionType; 

    // set the following to true, for this test 
    // spit out more status messages.
    private boolean verbose = false;


	ReEncryptCrashRecovery() {
        
	}


    /*
	 * Test (re)encrytpion crash/recovery scenarios. 
	 */
	private void runTest() throws Exception {
		logMessage("Begin  ReEncryptCrashRecovery Test");

        if (SanityManager.DEBUG) {
            if (verbose) 
                logMessage("Start testing re-encryption with Password");
            // test  crash recovery during re-encryption 
            // using the password mechanism.
            currentTestDatabase = TEST_REENCRYPT_PWD_DATABASE;
            encryptionType = USING_PASSWORD;
            runCrashRecoveryTestCases(true);

        
            if (verbose) 
            logMessage("Start Testing encryption with Password");

            // test crash recovery during databse encryption 
            // using the password mechanism.
            currentTestDatabase = TEST_ENCRYPT_PWD_DATABASE;
            encryptionType = USING_PASSWORD;
            // run crash recovery test cases. 
            runCrashRecoveryTestCases(false);


            if (verbose) {
                logMessage("Start Testing Encryption with external Key");
            }
            // test crash recovery during database encryption 
            // using the encryption key.
        
            currentTestDatabase = TEST_ENCRYPT_KEY_DATABASE;
            encryptionType = USING_KEY;
            runCrashRecoveryTestCases(false);

            if (verbose) 
                logMessage("Start Testing re-encryption with external Key");

            // test crash recovery dureing re-encryption 
            // using the encryption key.
        
            currentTestDatabase = TEST_REENCRYPT_KEY_DATABASE;
            encryptionType = USING_KEY;
            runCrashRecoveryTestCases(true);
        }
        logMessage("End ReEncryptCrashRecovery Test");
    }


    /**
     * run crash recovery test scenarios using the debug flags.
     * @param reEncrypt  <code> true </code> if testing re-encryption 
     *                   <colde> false </code> otherwise.
     */
    private void runCrashRecoveryTestCases(boolean reEncrypt) 
        throws SQLException
    {
        Connection conn;
        if (reEncrypt) 
            conn = createEncryptedDatabase();
        else 
            conn = createDatabase();

        createTable(conn, TEST_TABLE_NAME);
        //load some rows 
        insert(conn, TEST_TABLE_NAME, 100);
        conn.commit();
        conn.close();
		shutdown();

        // following cases of (re) encryption should be rolled back. 
        int passwordKey = (reEncrypt ? OLD : NONE );

        crash(reEncrypt, TEST_REENCRYPT_CRASH_BEFORE_COMMT);

        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_COMMT);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);

        
        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_COMMT);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        // retry (re)encryption and crash.
        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_COMMT);


        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);


        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY);
        // retry (re)encryption and crash.
        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);


        // following cases  (re) encryption should be successful, only 
        // cleanup is pending. 

        // crash after database is re-encrypted, but before cleanup. 
        // (re)encryption is complete, database should be bootable 
        // with a new password. 
        passwordKey = (reEncrypt ? NEW : OLD);
        crash(reEncrypt, TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT);
        crashInRecovery(passwordKey, 
                     TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP);

        recover(passwordKey);
        shutdown();
    }


    /*
     * Attempt to (re)encrypt the database and force it to crash 
     * at the given debug flag. 
     */
    private void crash(boolean reEncrypt, String debugFlag) 
    {
        if (verbose)
            logMessage("Testing : " + debugFlag);
        // set the debug flag to crash. 
        setDebugFlag(debugFlag);

        SQLException sqle = null;
        Connection conn;
        try {
            if (reEncrypt) 
                conn = reEncryptDatabase();
            else 
                conn = encryptDatabase();
                
        }catch (SQLException se) {
            // (re)encryption of the database should have failed,
            // at the specified debug flag.
            sqle = se;
        }

        // check that database boot failed at the set debug flag.
        verifyException(sqle, debugFlag);
        // clear the debug flag.
        clearDebugFlag(debugFlag);
    }


    /*
     * Crash in recovery of the database at the given 
     * debug flag.
     */
    private void crashInRecovery(int passwordKey, String debugFlag) 
        throws SQLException 
    {
        if (verbose) 
            logMessage("Testing : " + debugFlag);
        
        // set the debug flag to crash. 
        setDebugFlag(debugFlag);
        SQLException sqle = null;
        try {
            Connection conn = bootDatabase(passwordKey);
        } catch (SQLException se) {
            // recovery of the database 
            // shold have failed at the specified
            // debug flag.
            sqle = se;
        }
        // check that database boot failed at the set debug flag.
        verifyException(sqle, debugFlag);
        // clear the debug flag.
        clearDebugFlag(debugFlag);
    }   
    


    /*
     * Recover the database that failied during re-encryption and 
     * perform some simple sanity check on the database. 
     */
    private void recover(int passwordKey) 
        throws SQLException 
    {
        // starting recovery of database with failed Re-encrytpion
        // in debug mode;

        Connection conn = bootDatabase(passwordKey);

        // verify the contents of the db are ok. 
        runConsistencyChecker(conn, TEST_TABLE_NAME);
        // insert some rows, this might fail if anyhing is 
        // wrong in the logging system setup.
        insert(conn, TEST_TABLE_NAME, 100);
        conn.commit();
        conn.close();
    }   
    


    /** *************************************************
     * Crash/recovery test scenarios during 
     * encryption of an un-encrypted database.
     ****************************************************/
    


    // Debug flags that are to be set to simulate a crash 
    // at different points during (re)encryption of the database. 
    // these flags should match the flags in the engine code;
    // these are redifined here to avoid pulling the engine code
    // into the tests. 

    
    /*
      Set to true if we want the re-encryption to crash just 
      before the commit.
	*/

	public static final String TEST_REENCRYPT_CRASH_BEFORE_COMMT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_BEFORE_COMMT" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_COMMT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_COMMT" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_SWITCH_TO_NEWKEY" : null ;
    public static final String TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT  = 
        SanityManager.DEBUG ? "TEST_REENCRYPT_CRASH_AFTER_CHECKPOINT" : null ;
                                            
    public static final String 
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_LOGFILE_DELETE" : null;
    public static final String 
        TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_AFTER_RECOVERY_UNDO_REVERTING_KEY" : null;
    public static final String 
        TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP =
        SanityManager.DEBUG ?
        "TEST_REENCRYPT_CRASH_BEFORE_RECOVERY_FINAL_CLEANUP" : null;
    
    
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
        boolean expectedExcepion = false ;
        if (sqle != null) 
        {
            
            if (sqle.getSQLState() != null && 
                sqle.getSQLState().equals("XJ040")) 
            {
                // boot failed as expected with the  debug flag
                // now check if it failed with specifed debug flags.
                SQLException ne = sqle.getNextException();
                if (ne != null) {
                    String message = ne.getMessage();
                    // check if debug flag exists in the message
                    if (message.indexOf(debugFlag) != -1)
                    {
                        expectedExcepion = true;
                    }
                }
            }

            if (!expectedExcepion)
                dumpSQLException(sqle);
        } 
        else 
        {
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
        ps.close();
        conn.commit();
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
        int max = rs.getInt(1);
        rs.close();
        s.close();
        return max;
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
    private Connection createEncryptedDatabase() throws SQLException
    {
        String connAttrs = "";
        if (encryptionType == USING_PASSWORD) 
        {
            // create encrypted database.
            connAttrs = "create=true;dataEncryption=true;bootPassword=" +
                OLD_PASSWORD;
        }

        if (encryptionType == USING_KEY) 
        {
            // create an encrypted  database.
            connAttrs = "create=true;dataEncryption=true;encryptionKey=" +
                OLD_KEY;
        }
        
        return TestUtil.getConnection(currentTestDatabase, connAttrs); 
    }


    /*
     * create an un-encrypted database.
     */
    private Connection createDatabase() throws SQLException
    {
        return TestUtil.getConnection(currentTestDatabase,  
                                      "create=true" );
    }


    /**
     * Re-encrypt the database. 
     * @exception SQLException if any database exception occurs.
     */
    private Connection  reEncryptDatabase() throws SQLException
    {
        String connAttrs = "";
        if (encryptionType == USING_PASSWORD) 
        {
            // re-encrypt the database.
            connAttrs = "bootPassword=" + OLD_PASSWORD + 
                ";newBootPassword=" + NEW_PASSWORD;
        }

        if (encryptionType == USING_KEY) 
        {
            // re-encrypt the database.
            connAttrs = "encryptionKey=" + OLD_KEY + 
                ";newEncryptionKey=" + NEW_KEY;
        }
        
        if (verbose)
            logMessage("re-encrypting " + currentTestDatabase + 
                       " with " + connAttrs);

        return TestUtil.getConnection(currentTestDatabase, connAttrs); 
    }

    
    /**
     * Encrypt an un-encrypted atabase. 
     * @exception SQLException if any database exception occurs.
     */
    private Connection encryptDatabase() 
        throws SQLException
    {
        String connAttrs = "";
        if (encryptionType == USING_PASSWORD) 
        {
            //encrypt an existing database.
            connAttrs = "dataEncryption=true;bootPassword=" + OLD_PASSWORD;
        }
        if (encryptionType == USING_KEY) 
        {
            //encrypt an existing database.
            connAttrs = "dataEncryption=true;encryptionKey=" + OLD_KEY;
        }

        if (verbose)
            logMessage("encrypting " + currentTestDatabase + 
                       " with " + connAttrs);
        return TestUtil.getConnection(currentTestDatabase, connAttrs); 
    }
    

    /**
     * Boot the database. 
     * @param passwordKey the password/key to use.  
     * @exception SQLException if any database exception occurs.
     */
    Connection bootDatabase(int passwordKey)
        throws SQLException 
    {

        String connAttrs = "";
        if (encryptionType == USING_PASSWORD) 
        {
            if (passwordKey == NEW)
                connAttrs = "bootPassword=" + NEW_PASSWORD;
            else if (passwordKey == OLD)
                connAttrs = "bootPassword=" + OLD_PASSWORD;
        }

        
        if (encryptionType == USING_KEY) 
        {
            if (passwordKey == NEW)
                connAttrs = "encryptionKey=" + NEW_KEY;
            else if (passwordKey == OLD)
                connAttrs = "encryptionKey=" + OLD_KEY;
        }

        if (verbose)
            logMessage("booting " + currentTestDatabase + 
                   " with " + connAttrs);
        return TestUtil.getConnection(currentTestDatabase, connAttrs); 
    }



    /**
	 * Shutdown the datbase
	 */
	void shutdown() {

        if (verbose)
            logMessage("Shutdown " + currentTestDatabase);
		try{
			//shutdown
			TestUtil.getConnection(currentTestDatabase, "shutdown=true");
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
