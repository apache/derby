/*
 
 Derby - Class org.apache.derbyTesting.functionTests.tests.store.TestNoSyncs
 
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.*;
import org.apache.derbyTesting.functionTests.util.TestUtil;
import java.util.Properties;

/**
 * This program tests the system when the derby.system.durability property is
 * set to 'test'. 
 * <BR>
 * When the derby.system.durability is set to 'test', the system will not do 
 * any sync to the
 * disk and the recovery system will not work property. It is provided for
 * performance reasons and should ideally only be used when there is no
 * requirement for the database to recover.
 * <p>
 * When set, the system will not do any syncs, the cases namely - no sync of the
 * log file at each commit - no sync of the log file before data page is forced
 * to disk - no sync of page allocation when file is grown - no sync of data
 * writes during checkpoint
 * <p>
 * That means, when this property is set to 'test',
 *  - a commit no longer guarantees that the transaction's modification will 
 *    survive a system crash or
 * JVM termination 
 * - the database may not recover successfully upon restart 
 * - a near full disk at runtime may cause unexpected errors
 * - database may be in an inconsistent state
 * 
 * This program tests for 
 * 1. setting the derby.system.durability=test is actually not
 *    doing the syncs by timing inserts
 * 2. check if a warning message exists in derby.log 
 * 3. read log.ctrl file and check if the flag is set or not
 * 4. check if the log.ctrl file flag is not overwritten for the case when 
 * database booted with derby.system.durability=test set, then shutdown
 * and database booted without derby.system.durability=test
 * 
 * @version 1.0
 */
public class TestDurabilityProperty {
    public static void main(String[] args) {
        try {
            // Test 1: check if derby.system.durability=test
            // mode is not doing syncs 
            testNoSyncs(args);
            
            String derbyHome = System.getProperty("derby.system.home");
            // Test 2
            // Check if derby.log has the warning message
            report("Is warning message about derby.system.durability=test present in derby.log ="
                    + isMessageInDerbyLog(derbyHome));
            // Test 3
            // Check if marker is correctly written out to database
            markerInControlFile(derbyHome);
            
            // Test 4
            // shutdown database and boot database afresh without 
            // derby.system.durability set to test. In this case the derby.log 
            // and the log control file should still have the marker that this 
            // mode was once used to boot database.
            report(
                "2. shutdown database and reboot database without " +
                "derby.system.durability=test and test for marker in log.ctrl file");
            markerNotOverwritten(derbyHome);

        } catch (Throwable e) {
            report("FAIL -- unexpected exception: " + e);
            e.printStackTrace();
        }

    }

    /**
     * time inserts 
     * 
     * @param mode
     *            value for derby.system.durability property
     * @param create
     *            should table be created or not
     * @param autoCommit
     *            whether inserts should happen in autocommit mode or not
     * @return time taken to do inserts
     * @throws Exception
     */
    public static long timeTakenToInsert(String mode, boolean create,
            boolean autoCommit) throws Exception {
        System.setProperty("derby.system.durability", mode);
        Connection conn = org.apache.derby.tools.ij.startJBMS();

        if (create) {
            Statement s = conn.createStatement();
            s.execute("create table t1 (c1 int, c2 int)");
            s.close();
        }

        long timeTaken = doInserts(conn, autoCommit);

        try {
            conn.close();
//IC see: https://issues.apache.org/jira/browse/DERBY-949
            TestUtil.getConnection("","shutdown=true");
        } catch (SQLException sqle) {
            if ("XJ015".equals(sqle.getSQLState())) {
            }// ok database shutdown
            else {
                report(sqle.getSQLState());
                report("ERROR! during shutdown");
                sqle.printStackTrace();
            }
        }

        return timeTaken;
    }


    /**
     * Note doing inserts in autocommit mode is probably the worst case scenario
     * in terms of performance as each commit will involve a flush/sync to disk
     * but in case of the derby.system.durability=test mode, the syncs dont 
     * happen.
     */
    public static long doInserts(Connection conn,boolean autoCommit) throws Exception {
        PreparedStatement ps = conn
                .prepareStatement("insert into t1 values(?,?)");
        conn.setAutoCommit(autoCommit);
        long count = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < 500; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            count += ps.executeUpdate();
        }

        if (!autoCommit)
            conn.commit();
        long end = System.currentTimeMillis();
        if (count < 500)
            report(" FAIL!! all rows didnt get inserted ?");
        
        return (end - start);
    }

    /**
     * When derby.system.durability is set, a warning message is written out to
     * derby.log indicating that the property is set and that it does not
     * guarantee recoverability This test tests if a message is written out to
     * derby.log or not
     */
    public static boolean isMessageInDerbyLog(String derbyHome) throws Exception {
        BufferedReader reader = null;
        File derbylog = null;
        try {
            derbylog = new File(derbyHome, "derby.log");
            reader = new BufferedReader(new FileReader(derbylog));

            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.indexOf("derby.system.durability=test") != -1)
                    return true;

            }
            return false;
        } finally {
            if (reader != null) {
                reader.close();
            }
            derbylog = null;
        }
    }

    /**
     * if database is booted with derby.system.durability=test, 
     * a marker is written out into log control
     * file to recognize that the database was previously booted in this mode
     * Test if the marker byte is set correctly or not. See comments in
     * org.apache.derby.impl.store.log.LogToFile for IS_DURABILITY_TESTMODE_NO_SYNC_FLAG
     */
    public static void markerInControlFile(String derbyHome) throws Exception {
        RandomAccessFile controlFile = null;
        try {
            int testModeNoSyncMarkerPosition = 28;
            byte testModeNoSyncMarker = 0x2;
            controlFile = new RandomAccessFile(derbyHome
                    + "/wombat/log/log.ctrl", "r");
            controlFile.seek(testModeNoSyncMarkerPosition);
            report("log.ctrl file has durability testMode no sync marker value = "
                    + ((controlFile.readByte() & testModeNoSyncMarker) != 0) );
        } finally {
            if (controlFile != null)
                controlFile.close();

        }
    }

    /**
     * Test for case when database is booted without derby.system.durability=test
     * but previously has been booted with the derby.system.durability=test. In 
     * this scenario,the log control file should still have the marker to say
     * that this mode was set previously, and derby.log must also have a warning
     * message
     * @param derbyHome value of derby.system.home where the database is
     * @throws Exception
     */
    public static void markerNotOverwritten(String derbyHome) throws Exception
    {
        // shutdown database
        Connection conn = null;
        // unset property
        System.setProperty("derby.system.durability","");
        conn = org.apache.derby.tools.ij.startJBMS();
        conn.close();
        markerInControlFile(derbyHome);
        report("Is warning message about derby.system.durability=test present in derby.log ="
                + isMessageInDerbyLog(derbyHome));
    }
    
    /**
     * print message
     * @param msg to print out 
     */
    public static void report(String msg) {
        System.out.println(msg);
    }

    /**
     * Test if derby.system.durability=test property is broken or not. We time
     * inserts for 500 repeated inserts and make some approximate estimations on
     * how fast it should be. Since it is a timing based test, there might be
     * cases of a really really slow machine in some weird cases where this test
     * may have diffs in this part of the test.
     * 
     * So basically to determine if something is wrong, the following is done
     * to try best to eliminate issues with slow machines
     * 1)if inserts with autocommit on is fast enough (an estimated
     * bound)
     * 2)if not, then since no syncs happen , check if time taken to do
     * inserts for autocommit on and off are in proximity range 
     * 3)if 1 and 2 is not satisfied, then check time taken without this mode set
     * and with this mode set.
     * If they are in proximity range something could be wrong. It
     * might be good to check the machine configuration and environment when
     * test was running <BR>
     * Also note, although it would seem like a solution would be to bump the
     * estimated bound to a high limit, this might not help since on a really
     * good disk the inserts doing syncs might well be done within the bound and
     * thus would not be possible to know if this mode was possibly broken.
     *  
     */
    public static void testNoSyncs(String[] args) throws Exception {
        boolean debug = false;  // if set, prints out useful info when debugging test
        
        report("1. With derby.system.durability=test,"
                + "Test to see if syncs are not happening ");
        // use the ij utility to read the property file and
        // make the initial connection.
        org.apache.derby.tools.ij.getPropertyArg(args);

        boolean create = true;

        // Note we time inserts in normal all syncs case first even
        // though we may not require it if the inserts finish fast enough
        // But timing them here because once database is booted with
        // derby.system.durability=test there are no guarantees on consistency
        // of database so dont want to mess up numbers for the all syncs case

        // derby.system.durability is not test so it will default to
        // normal mode and autocommit=true
        long timeCommitOn = timeTakenToInsert("", create, true);
        String derbyHome = System.getProperty("derby.system.home");
        if (isMessageInDerbyLog(derbyHome))
            report("ERROR! System should not have been booted with"
                    + "derby.system.durability=test mode here");
        create = false;
        // derby.system.durability=test and autocommit=true
        long timeWithTestModeCommitOn = timeTakenToInsert("test", create, true);
        // derby.system.durability=test and autocommit=false
        long timeWithTestModeCommitOff = timeTakenToInsert("test", create, false);
      
        if (debug) {
            report("timeCommitOn = " + timeCommitOn);
            report("timeWithTestModeCommitOn = " + timeWithTestModeCommitOn);
            report("timeWithTestModeCommitOff = " + timeWithTestModeCommitOff);
        }

        // To run this, uncomment and build.
        // This check is disabled for normal test runs because we cannot
        // guarantee that some event on the machine might skew the inserts 
        // with one or another of the three settings, causing a false failure.
        // See DERBY-5865.
        /*
        // an approximation on the upper bound for time taken to do
        // inserts in autocommit mode with derby.system.durability=test mode
        long upperBound = 3000;

        // if it takes a lot of time to do the inserts then do extra checks
        // to determine if derby.system.durability=test mode is broken or not
        // because we cant be sure if inserts just took a long time
        // because of a really slow machine
        if (timeWithTestModeCommitOn > upperBound) {

            long proximityRange = 1000;

            // in derby.system.durability=test autocommit on or off should
            // be in same range since syncs are not happening
            if (Math.abs(timeWithTestModeCommitOn - timeWithTestModeCommitOff) > proximityRange) {
                // another approximation here (1.5 times of with testmode set)
                if (timeWithTestModeCommitOn > timeCommitOn
                        || (timeCommitOn < (1.5 * timeWithTestModeCommitOn))) {
                    report("FAIL -- derby.system.durability=test mode seems to be broken.");
                    report("-- In this mode one would expect that inserts with autocommit off and on "
                            + "would be in the same range as syncs are not happening but the difference "
                            + "here seems to be more than the approximate estimated range.");
                    report("-- Also comparing the time taken to do the inserts without this" +
                            " property set seems to be in the same"
                            + " range as with this property set.");
                    report("-- Please note this test times inserts and approximate estimates were " +
                            "considered to report this observation.");
                    report("timeCommitOn = " + timeCommitOn);
                    report("timeWithTestModeCommitOn = " + timeWithTestModeCommitOn);
                    report("timeWithTestModeCommitOff = " + timeWithTestModeCommitOff);
                }
            }
        }*/

    }
}
