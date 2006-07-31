/*
 *
 * Derby - Class BaseJDBCTestCase
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

import java.io.InputStream;
import java.io.IOException;
import java.io.Reader;
import java.sql.*;

/**
 * Base class for JDBC JUnit tests.
 * A method for getting a default connection is provided, along with methods
 * for telling if a specific JDBC client is used.
 */
public abstract class BaseJDBCTestCase
    extends BaseTestCase {

    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public BaseJDBCTestCase(String name) {
        super(name);
    }

    /**
     * Get connection to the default database.
     * If the database does not exist, it will be created.
     * A default username and password will be used for the connection.
     *
     * @return connection to default database.
     */
    public static Connection getConnection()
        throws SQLException {
        return CONFIG.getDefaultConnection();
    }
    

   
    /**
     * Tell if the client is embedded.
     *
     * @return <code>true</code> if using the embedded client
     *         <code>false</code> otherwise.
     */
     public static boolean usingEmbedded() {
         return CONFIG.getJDBCClient().isEmbedded();
     }
    
    /**
    * Tell if the client is DerbyNetClient.
    *
    * @return <code>true</code> if using the DerbyNetClient client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNetClient() {
        return CONFIG.getJDBCClient().isDerbyNetClient();
    }
    
    /**
    * Tell if the client is DerbyNet.
    *
    * @return <code>true</code> if using the DerbyNet client
    *         <code>false</code> otherwise.
    */
    public static boolean usingDerbyNet() {
        return CONFIG.getJDBCClient().isDB2Client();
    }

    /**
     * Assert equality between two <code>Blob</code> objects.
     * If both input references are <code>null</code>, they are considered
     * equal. The same is true if both blobs have <code>null</code>-streams.
     *
     * @param b1 first <code>Blob</code>.
     * @param b2 second <code>Blob</code>.
     * @throws AssertionFailedError if blobs are not equal.
     * @throws IOException if reading or closing a stream fails
     * @throws SQLException if obtaining a stream fails
     */
    public static void assertEquals(Blob b1, Blob b2)
            throws IOException, SQLException {
        if (b1 == null || b2 == null) {
            assertNull("Blob b2 is null, b1 is not", b1);
            assertNull("Blob b1 is null, b2 is not", b2);
            return;
        }
        assertEquals("Blobs have different lengths",
                     b1.length(), b2.length());
        InputStream is1 = b1.getBinaryStream();
        InputStream is2 = b2.getBinaryStream();
        if (is1 == null || is2 == null) {
            assertNull("Blob b2 has null-stream, blob b1 doesn't", is1);
            assertNull("Blob b1 has null-stream, blob b2 doesn't", is2);
            return;
        }
        long index = 1;
        int by1 = is1.read();
        int by2 = is2.read();
        do {
            // Avoid string concatenation for every byte in the stream.
            if (by1 != by2) {
                assertEquals("Blobs differ at index " + index,
                        by1, by2);
            }
            index++;
            by1 = is1.read();
            by2 = is2.read();
        } while ( by1 != -1 || by2 != -1);
        is1.close();
        is2.close();
    }

    /**
     * Assert equality between two <code>Clob</code> objects.
     * If both input references are <code>null</code>, they are considered
     * equal. The same is true if both clobs have <code>null</code>-streams.
     *
     * @param c1 first <code>Clob</code>.
     * @param c2 second <code>Clob</code>.
     * @throws AssertionFailedError if clobs are not equal.
     * @throws IOException if reading or closing a stream fails
     * @throws SQLException if obtaining a stream fails
     */
    public static void assertEquals(Clob c1, Clob c2)
            throws IOException, SQLException {
        if (c1 == null || c2 == null) {
            assertNull("Clob c2 is null, c1 is not", c1);
            assertNull("Clob c1 is null, c2 is not", c2);
            return;
        }
        assertEquals("Clobs have different lengths",
                     c1.length(), c2.length());
        Reader r1 = c1.getCharacterStream();
        Reader r2 = c2.getCharacterStream();
        if (r1 == null || r2 == null) {
            assertNull("Clob c2 has null-stream, clob c1 doesn't", r1);
            assertNull("Clob c1 has null-stream, clob c2 doesn't", r2);
            return;
        }
        long index = 1;
        int ch1 = r1.read();
        int ch2 = r2.read();
        do {
            // Avoid string concatenation for every char in the stream.
            if (ch1 != ch2) {
                assertEquals("Clobs differ at index " + index,
                        ch1, ch2);
            }
            index++;
            ch1 = r1.read();
            ch2 = r2.read();
        } while (ch1 != -1 || ch2 != -1);
        r1.close();
        r2.close();
    }

    /**
     * Assert that SQLState is as expected.
     * The expected SQLState is truncated to five characters if required.
     *
     * @param message message to print on failure.
     * @param expected the expected SQLState.
     * @param exception the exception to check the SQLState of.
     */
    public static void assertSQLState(String message, 
                                      String expected, 
                                      SQLException exception) {
        // Make sure exception is not null. We want to separate between a
        // null-exception object, and a null-SQLState.
        assertNotNull("Exception cannot be null when asserting on SQLState", 
                      exception);
        
        String state = exception.getSQLState();
        
        if ( state != null )
            assertTrue("The exception's SQL state must be five characters long",
                exception.getSQLState().length() == 5);
        
        if ( expected != null )
            assertTrue("The expected SQL state must be five characters long",
                expected.length() == 5);
        
        assertEquals(message, expected, state);
    }

    /**
     * Assert that SQLState is as expected.
     * The expected SQLState is truncated to five characters if required.
     *
     * @param expected the expected SQLState.
     * @param exception the exception to check the SQLState of.
     */
    public static void assertSQLState(String expected, SQLException exception) {
        assertSQLState("Unexpected SQL state.", expected, exception);
    }

} // End class BaseJDBCTestCase
