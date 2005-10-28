/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.largedata.lobLengthTests

   Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.largedata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;

import java.io.ByteArrayInputStream;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
 * This test is part of the "largedata" suite because the use of
 * very large LOBs can require extra memory for the server JVM.
 * If this test was run as part of the normal 'derbyall' suite,
 * it would require that any developer running the derbyall suite
 * have a machine with a good deal of memory.  And since _every_
 * developer is encouraged to run 'derbyall' before submitting
 * any patches, that would mean that _every_ developer would
 * need a machine with lots of memory--and that's something we
 * do NOT want to require.
 * 
 * The specific JVM memory requirements for this test are set in the
 * properties file for this test (lobLengthTests_app.properties).
 * It started out as -mx128M -ms128M, but that could change in the
 * future as more test cases are added to this class.  If it's not
 * at least 128M, the result will be OutOfMemory exceptions when
 * running against Network Server.
 */

public class lobLengthTests {

    /**
     * Create an instance of this class and do the test.
     */
    public static void main(String [] args)
    {
        new lobLengthTests().go(args);
    }

    /**
     * Create a JDBC connection using the arguments passed
     * in from the harness, and then run the LOB length
     * tests.
     * @param args Arguments from the harness.
     */
    public void go(String [] args)
    {
        try {

            // use the ij utility to read the property file and
            // make the initial connection.
            ij.getPropertyArg(args);
            Connection conn = ij.startJBMS();

            // Add additional tests here.
            derby_121Test(conn);

        } catch (Exception e) {

            System.out.println("FAIL -- Unexpected exception:");
            e.printStackTrace(System.out);

        }
    }

    /**
     * There was a defect (DERBY-121) where the server and client
     * were processing lob lengths incorrectly.  For lob lengths
     * that are represented by 24 or more bits, the server and
     * Derby client were doing incorrect bit-shifting.  This
     * test makes sure that problem no longer occurs.
     */
    private static void derby_121Test(Connection conn)
        throws SQLException
    {
        System.out.println("Testing server read of lob length > 2^24 bytes.");

        boolean autoc = conn.getAutoCommit();
        conn.setAutoCommit(false);

        // Create a test table.
        Statement st = conn.createStatement();
        st.execute("create table lobTable100M(bl blob(100M))");

        PreparedStatement pSt = conn.prepareStatement(
            "insert into lobTable100M(bl) values (?)");

        // The error we're testing occurs when the server
        // is shifting bits 24 and higher of the lob's
        // length (in bytes).  This means that, in order
        // to check for the error, we have to specify a
        // lob length (in bytes) that requires at least
        // 24 bits to represent.  Thus for a blob the
        // length of the test data must be specified as
        // at least 2^24 bytes (hence the '16800000' in
        // the next line).
        byte [] bA = new byte[16800000];
        pSt.setBinaryStream(1,
            new java.io.ByteArrayInputStream(bA), bA.length);

        // Now try the insert; this is where the server processes
        // the lob length.
        try {
            pSt.execute();
            System.out.println("PASS.");
        } catch (Exception e) {
            System.out.println("FAIL -- unexpected exception:");
            e.printStackTrace(System.out);
        }

        // Clean up.
        try {
            st.execute("drop table lobTable100M");
        } catch (SQLException se) {}

        conn.setAutoCommit(autoc);
        return;

    }
}
