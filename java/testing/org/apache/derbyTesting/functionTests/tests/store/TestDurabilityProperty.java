/*
 
 Derby - Class org.apache.derbyTesting.functionTests.tests.store.TestNoSyncs
 
 Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.
 
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
import java.sql.DriverManager;
import java.io.*;

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
 * @author Sunitha Kambhampati 
 * @version 1.0
 */
public class TestDurabilityProperty {
    public static void main(String[] args) {
        Connection conn = null;
        Statement s = null;
        PreparedStatement ps = null;
        try {
            report("1. create database with derby.system.durability=test mode");
            // use the ij utility to read the property file and
            // make the initial connection.
            org.apache.derby.tools.ij.getPropertyArg(args);
            System.setProperty("derby.system.durability","test");
            conn = org.apache.derby.tools.ij.startJBMS();

            s = conn.createStatement();
            s.execute("create table t1 (c1 int, c2 int)");
            s.close();

            // Test 1
            // this is a quick check incase someone breaks the
            // derby.system.durability=test
            long timeTaken = doInsertsInAutoCommit(conn);
            conn.close();

            int approxUpperBound = 3000; // approximate upper bound in
                                         // millisecond
            if (timeTaken > approxUpperBound) {
                report("FAIL -- derby.system.durability=test mode seems to be broken.");
                report(" Time to insert rows in test exceeded the usual limit.");
            }

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
     * Note doing inserts in autocommit mode is probably the worst case scenario
     * in terms of performance as each commit will involve a flush/sync to disk
     * but in case of the derby.system.durability=test mode, the syncs dont 
     * happen.
     * This test case times the inserts and assumes that the inserts on any
     * system will be less than three second for 500 inserts. Note this upper
     * bound on time is just an approximate estimation
     */
    public static long doInsertsInAutoCommit(Connection conn) throws Exception {
        PreparedStatement ps = conn
                .prepareStatement("insert into t1 values(?,?)");
        long count = 0;

        long start = System.currentTimeMillis();

        for (int i = 0; i < 500; i++) {
            ps.setInt(1, i);
            ps.setInt(2, i);
            count += ps.executeUpdate();
        }

        long end = System.currentTimeMillis();
        report("num successful inserts = " + count);

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
        try
        {
            conn = DriverManager.getConnection("jdbc:derby:;shutdown=true");
        }
        catch(Exception e)
        {
            report("expected exception for shutdown." + e.getMessage());
        }
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

}
