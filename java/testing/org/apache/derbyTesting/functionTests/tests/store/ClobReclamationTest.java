/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.ClobReclamationTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.store;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.Formatters;



/**
 * Verify that space gets reclaimed for multi-threaded Clob updates
 * 
 */
public class ClobReclamationTest {

    // Need to adjust NUM_THREADS and expectedNumAllocated.
    // For 2 threads expectedNumAllocated is 5
    // For 100 threads expectedNumAllocated is 201
    private static final int NUM_THREADS = 2;

    private static final int expectedNumAllocated = 5;

  
    /**
     * Two threads simultaneously updating a table. Thread 1 updates row 1 with
     * a long value (>32K) Thread 2 updates row with a short clob ("hello");
     * NUMALLOCATEDPAGES should be only 3 after each does 500 updates
     * 
     * @throws SQLException
     * @throws InterruptedException
     */
    public static void testMultiThreadedUpdate(Connection conn) throws SQLException,
            InterruptedException {

    	final String updateString = Formatters.repeatChar("a",33000);
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            final int key = i + 1;
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Connection conn = ij.startJBMS();
                        ClobReclamationTest.fiveHundredUpdates(conn,
                                updateString, key);
                    } catch (Exception  e) {
                        e.printStackTrace();
                    }
                }
            };
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].start();
        }
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }

        Statement s = conn.createStatement();
        // Check the space table 
        // Should not have grown.
        ResultSet rs = s.executeQuery("SELECT NUMALLOCATEDPAGES FROM "
                + " new org.apache.derby.diag.SpaceTable('APP','CLOBTAB') t"
                + " WHERE CONGLOMERATENAME = 'CLOBTAB'");
        rs.next();
        int numAllocated = rs.getInt(1);
        if (numAllocated == expectedNumAllocated)
        	System.out.println("PASS: NUMALLOCATEDPAGES =" + numAllocated);
        else
        	System.out.println("FAIL: NUMALLOCATEDPAGES =" + numAllocated);
        	
    }

    private static void fiveHundredUpdates(Connection conn,
            String updateString, int key) throws SQLException {
        PreparedStatement ps = conn
                .prepareStatement("UPDATE CLOBTAB SET C = ? WHERE I = ?");
        for (int i = 0; i < 500; i++) {
            ps.setString(1, updateString);
            ps.setInt(2, key);
            ps.executeUpdate();
        }
    }

    public static void main(String[] argv) throws SQLException, IllegalAccessException, ClassNotFoundException, InstantiationException, InterruptedException, IOException {
    ij.getPropertyArg(argv); 	
	Connection conn = ij.startJBMS();
	Statement s = conn.createStatement();
	s
	    .executeUpdate("CREATE TABLE CLOBTAB (I INT  PRIMARY KEY NOT NULL, c CLOB)");
	PreparedStatement ps = conn
	    .prepareStatement("INSERT INTO CLOBTAB VALUES(?,?)");
	String insertString = "hello";
	for (int i = 1; i <= NUM_THREADS; i++) {
	    ps.setInt(1, i);
	    ps.setString(2, insertString);
	    ps.executeUpdate();
	}
    testMultiThreadedUpdate(conn);

    }
}