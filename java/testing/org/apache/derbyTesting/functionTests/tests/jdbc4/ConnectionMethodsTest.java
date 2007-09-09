/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ConnectionMethodsTest

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.util.ArrayList;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilePermission;
import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Blob;
import java.sql.Clob;
import javax.sql.DataSource;
import java.security.AccessController;
import java.security.*;
import org.apache.derbyTesting.junit.NetworkServerTestSetup;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derbyTesting.junit.TestConfiguration;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBCDataSource;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * This class is used to test the implementations of the JDBC 4.0 methods
 * in the Connection interface
 */
public class ConnectionMethodsTest extends BaseJDBCTestCase {

    FileInputStream is;

    public ConnectionMethodsTest(String name) {
        super(name);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("ConnectionMethodsTest");

        suite.addTest(baseSuite("ConnectionMethodsTest:embedded"));

        suite.addTest(
                TestConfiguration.clientServerDecorator(
                baseSuite("ConnectionMethodsTest:client")));
        return suite;
    }

    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(ConnectionMethodsTest.class, name);
        Test test = new SupportFilesSetup(suite, new String[] {"functionTests/testData/ConnectionMethods/short.txt"} );
        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {
                s.execute("create table clobtable2(n int,clobcol CLOB)");
                s.execute("create table blobtable2(n int,blobcol BLOB)");

            }
        };
    }
    /**
     * Test the createClob method implementation in the Connection interface
     *
     * @exception SQLException, FileNotFoundException, Exception if error occurs
     */
    public void testCreateClob() throws   SQLException,
            FileNotFoundException, IOException,
            Exception{

        Connection conn = getConnection();
        int b, c;
        Clob clob;

        Statement s = createStatement();

        PreparedStatement ps =
                prepareStatement("insert into clobtable2 (n, clobcol)" + " values(?,?)");
        ps.setInt(1,1000);
        clob = conn.createClob();

        try {
            is = (FileInputStream) AccessController.doPrivileged(
                    new PrivilegedExceptionAction() {
                public Object run() throws FileNotFoundException {
                    return new FileInputStream("extin/short.txt");
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of FileNotFoundException,
            // as only "checked" exceptions will be "wrapped" in a
            // PrivilegedActionException.
            throw (FileNotFoundException) e.getException();
        }
        OutputStream os = clob.setAsciiStream(1);
        ArrayList beforeUpdateList = new ArrayList();

        c = is.read();
        while(c>0) {
            os.write(c);
            beforeUpdateList.add(c);
            c = is.read();
        }
        ps.setClob(2, clob);
        ps.executeUpdate();

        Statement stmt = createStatement();
        ResultSet rs =
                stmt.executeQuery("select clobcol from clobtable2 where n = 1000");
        assertTrue(rs.next());

        clob = rs.getClob(1);
        assertEquals(beforeUpdateList.size(), clob.length());

        //Get the InputStream from this Clob.
        InputStream in = clob.getAsciiStream();
        ArrayList afterUpdateList = new ArrayList();

        b = in.read();

        while (b > -1) {
            afterUpdateList.add(b);
            b = in.read();
        }

        assertEquals(beforeUpdateList.size(), afterUpdateList.size());

        //Now check if the two InputStreams
        //match
        for (int i = 0; i < clob.length(); i++) {
            assertEquals(beforeUpdateList.get(i), afterUpdateList.get(i));
        }

        os.close();
        is.close();

    }
    /**
     * Test the createBlob method implementation in the Connection interface
     *
     * @exception  SQLException, FileNotFoundException, Exception if error occurs
     */
    public void testCreateBlob() throws   SQLException,
            FileNotFoundException,
            IOException,
            Exception{

        Connection conn = getConnection();
        int b, c;
        Blob blob;

        Statement s = createStatement();
        PreparedStatement ps =
                prepareStatement("insert into blobtable2 (n, blobcol)" + " values(?,?)");
        ps.setInt(1,1000);
        blob = conn.createBlob();

        try {
            is = (FileInputStream) AccessController.doPrivileged(
                    new PrivilegedExceptionAction() {
                public Object run() throws FileNotFoundException {
                    return new FileInputStream("extin/short.txt");
                }
            });
        } catch (PrivilegedActionException e) {
            // e.getException() should be an instance of FileNotFoundException,
            // as only "checked" exceptions will be "wrapped" in a
            // PrivilegedActionException.
            throw (FileNotFoundException) e.getException();
        }

        OutputStream os = blob.setBinaryStream(1);
        ArrayList beforeUpdateList = new ArrayList();

        int actualLength = 0;
        c = is.read();
        while(c>0) {
            os.write(c);
            beforeUpdateList.add(c);
            c = is.read();
            actualLength ++;
        }
        ps.setBlob(2, blob);
        ps.executeUpdate();

        Statement stmt = createStatement();
        ResultSet rs =
                stmt.executeQuery("select blobcol from blobtable2 where n = 1000");
        assertTrue(rs.next());

        blob = rs.getBlob(1);
        assertEquals(beforeUpdateList.size(), blob.length());

        //Get the InputStream from this Blob.
        InputStream in = blob.getBinaryStream();
        ArrayList afterUpdateList = new ArrayList();

        b = in.read();

        while (b > -1) {
            afterUpdateList.add(b);
            b = in.read();
        }

        assertEquals(beforeUpdateList.size(), afterUpdateList.size());

        //Now check if the two InputStreams
        //match
        for (int i = 0; i < blob.length(); i++) {
            assertEquals(beforeUpdateList.get(i), afterUpdateList.get(i));
        }

        os.close();
        is.close();
    }
    /**
     * Test the Connection.isValid method
     *
     * @exception SQLException, Exception if error occurs
     */
    public void testConnectionIsValid() throws SQLException, Exception {
       /*
        * Test illegal parameter values
        */
        Connection conn = getConnection();
        try {
            conn.isValid(-1);  // Negative timeout
            fail("FAIL: isValid(-1): Invalid argument execption not thrown");

        } catch (SQLException e) {
            assertSQLState("XJ081", e);
        }

       /*
        * Test with no timeout
        */
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0): returned false");
        }

       /*
        * Test with a valid timeout
        */
        if (!conn.isValid(1)) {
            fail("FAIL: isValid(1): returned false");
        }

       /*
        * Test on a closed connection
        */
        try {
            conn.close();
        } catch (SQLException e) {
            assertSQLState("08003", e);
        }

        if (conn.isValid(0)) {
            fail("FAIL: isValid(0) on closed connection: returned true");
        }

        /* Open a new connection and test it */
        conn = getConnection();
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0) on open connection: returned false");
        }

       /*
        * Test on stopped database
        */
        TestConfiguration.getCurrent().shutdownDatabase();

        /* Test if that connection is not valid */
        if (conn.isValid(0)) {
            fail("FAIL: isValid(0) on stopped database: returned true");
        }

        /* Start the database by getting a new connection to it */
        conn = getConnection();

        /* Check that a new connection to the newly started database is valid */
        if (!conn.isValid(0)) {
            fail("FAIL: isValid(0) on new connection: " +
                    "returned false");
        }

       /*
        * Test on stopped Network Server client
        */
        if ( !usingEmbedded() ) {

            TestConfiguration.getCurrent().stopNetworkServer();

            /* Test that the connection is not valid */
            if (conn.isValid(0)) {
                fail("FAIL: isValid(0) on stopped database: returned true");
            }

           /*
            * Start the network server and get a new connection and check that
            * the new connection is valid.
            */
            TestConfiguration.getCurrent().startNetworkServer();

            // Get a new connection to the database
            conn = getConnection();

            /* Check that a new connection to the newly started Derby is valid */
            if (!conn.isValid(0)) {
                fail("FAIL: isValid(0) on new connection: returned false");
            }
        }
    }
}
