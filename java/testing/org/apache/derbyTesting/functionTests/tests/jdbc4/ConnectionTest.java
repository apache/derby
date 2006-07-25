/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ConnectionTest
 
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
package org.apache.derbyTesting.functionTests.tests.jdbc4;

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.TestDataSourceFactory;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.*;
import java.util.Properties;
import javax.sql.*;

/**
 * Tests for the JDBC 4.0 specific methods in the connection object(s).
 * 
 * Which connection implementation is tested, depends on what connection
 * object the <code>BaseJDBCTestCase.getConnection()</code>-method returns.
 * Currently, the property <code>derbyTesting.xa.single</code> can be set to
 * <code>true</code> to test the XA connection object, which happens to be the
 * same as the one used for poooled connections.
 * The connection returned also depends on which framework is being used.
 */
public class ConnectionTest
    extends BaseJDBCTestCase {

    /** 
     * Default connection used by the tests. 
     */
    protected Connection con = null;

    /**
     * Create a test with the given name.
     * 
     * @param name name of the test.
     */
    public ConnectionTest(String name) {
        super(name);
    }
    
    /**
     * Obtain a connection that the tests can use.
     */
    public void setUp() 
        throws SQLException {
        con = getConnection(); 
    }

    public void tearDown() 
        throws SQLException {
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        con = null;
    }
   
    //------------------------- T E S T  M E T H O D S ------------------------
    
    /**
     *
     * Tests the Embedded implementation for the createBlob method. The Embedded
     * server does'nt currently have the set methods implemented. Hence the 
     * create methods cannot be tested by inserting data into the empty LOB 
     * object. Here we do a simple test of checking that the length of the 
     * LOB object is 0.
     *
     * @throws SQLException upon failure in the createBlob or the length 
     *         methods.
     *
     */
    public void embeddedCreateBlob()
        throws SQLException {
            Blob blob = con.createBlob();
            //Check if the blob is empty
            if(blob.length() > 0)
                fail("The new Blob should not have more than zero bytes " +
                        "contained in it");
    }
    
    /**
     *
     * Tests the Embedded implementation for the createClob method. The Embedded
     * server does'nt currently have the set methods implemented. Hence the 
     * create methods cannot be tested by inserting data into the empty LOB 
     * object. Here we do a simple test of checking that the length of the 
     * LOB object is 0.
     *
     * @throws SQLException upon failure in the createClob or the length 
     *         methods.
     *
     */
    public void embeddedCreateClob()
        throws SQLException {
            Clob clob = con.createClob();
            //check if the Clob is empty
            if(clob.length() > 0)
                fail("The new Clob should not have a length of greater than " +
                        "zero");
    }

    public void testCreateArrayNotImplemented()
        throws SQLException {
        try {
            con.createArrayOf(null, null);
            fail("createArrayOf(String,Object[]) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    public void testCreateNClobNotImplemented()
        throws SQLException {
        try {
            con.createNClob();
            fail("createNClob() should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    /**
     * Simply test that the method is implemented.
     * [TODO] Write a better test when some (believed to be) JDK bugs have
     *        been resolved. Currently fails during query execution with 
     *        JDK 1.6.0_b85 because of some security manager problems. May be 
     *        fixed in a couple of beta releases.
     */
    public void testCreateQueryObjectIsImplemented()
        throws SQLException {
        con.createQueryObject(TestQuery.class);
        con.createQueryObject(TestQuery.class, con);
    }

    public void testCreateSQLXMLNotImplemented()
        throws SQLException {
        try {
            con.createSQLXML();
            fail("createSQLXML() should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    public void testCreateStructNotImplemented()
        throws SQLException {
        try {
            con.createStruct(null, null);
            fail("createStruct(String,Object[]) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }
    
    public void testGetClientInfo()
        throws SQLException {
        assertTrue("getClientInfo() must return an empty Properties object", 
                   con.getClientInfo().isEmpty());
    }
    
    public void testGetClientInfoString()
        throws SQLException {
        assertNull("getClientInfo(null) must return null",
                   con.getClientInfo(null));
        assertNull("getClientInfo(\"someProperty\") must return null",
                   con.getClientInfo("someProperty"));
    }

    /**
     * Tests that <code>isValid</code> is implemented and returns true
     * for the connection. This test is very limited but is tested
     * for all connection types. A more complete test of isValid is
     * found in the TestConnectionMethods.java test that is run for
     * embedded and network client connections.
     */
    public void testIsValidImplemented() throws SQLException {
        // Test with an infinite (0) timeout
        assertTrue(con.isValid(0));

        // Test with a 1 second timeout
        assertTrue(con.isValid(1));

        // Test with an illegal timeout
        try {
            con.isValid(-1);
        } catch (SQLException sqle) {
            assertSQLState("Incorrect SQL state when calling isValid(-1)",
                           "XJ081", sqle);
        }
    }

    /**
     * Tests that <code>getTypeMap()</code> returns an empty map when
     * no type map has been installed.
     * @exception SQLException if an error occurs
     */
    public void testGetTypeMapReturnsEmptyMap() throws SQLException {
        assertTrue(con.getTypeMap().isEmpty());
    }

    public void testIsWrapperReturnsFalse()
        throws SQLException {
        assertFalse(con.isWrapperFor(ResultSet.class));
    }

    public void testIsWrapperReturnsTrue()
        throws SQLException {
        assertTrue(con.isWrapperFor(Connection.class));
    }

    public void testSetClientInfoProperties()
        throws SQLException {
        con.setClientInfo(null);
        Properties p = new Properties();
        con.setClientInfo(p);

        p.setProperty("prop1", "val1");
        p.setProperty("prop2", "val2");
        try {
            con.setClientInfo(p);
            fail("setClientInfo(String,String) should throw "+
                 "SQLClientInfoException");
        } catch (SQLClientInfoException cie) {
            assertSQLState("SQLStates must match", "XCY02", cie);
            assertTrue("Setting property 'prop1' must fail with "+
                       "REASON_UNKNOWN_PROPERTY",
                       cie.getFailedProperties().
                       get("prop1").
                       equals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY));
             assertTrue("Setting property 'prop2' must fail with "+
                        "REASON_UNKNOWN_PROPERTY",
                        cie.getFailedProperties().
                        get("prop2").
                        equals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY));
        }
    }

    public void testSetClientInfoString()
        throws SQLException {
        con.setClientInfo(null, null);

        try {
            con.setClientInfo("foo", null);
            fail("setClientInfo(String, null) should throw "+
                 "NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            con.setClientInfo("name", "value");
            fail("setClientInfo(String,String) should throw "+
                 "SQLClientInfoException");
        } catch (SQLClientInfoException cie) {
            assertSQLState("SQLState must match 'unsupported'",
                           "XCY02", cie);
            assertTrue("Setting property 'name' must fail with "+
                       "REASON_UNKNOWN_PROPERTY",
                       cie.getFailedProperties().
                       get("name").
                       equals(ClientInfoStatus.REASON_UNKNOWN_PROPERTY));
        }
    }
    
    public void testUnwrapValid()
        throws SQLException {
        Connection unwrappedCon = con.unwrap(Connection.class);
        assertSame("Unwrap returned wrong object.", con, unwrappedCon);
    }

    public void testUnwrapInvalid()
        throws SQLException {
        try {
            ResultSet unwrappedRs = con.unwrap(ResultSet.class);
            fail("unwrap should have thrown an exception");
        } catch (SQLException sqle) {
            assertSQLState("Incorrect SQL state when unable to unwrap",
                           SQLStateConstants.UNABLE_TO_UNWRAP,
                           sqle);
        }
    }
        
    //------------------ E N D  O F  T E S T  M E T H O D S -------------------

    /**
     * Create suite containing client-only tests.
     */
    private static TestSuite clientSuite(String name) {
        TestSuite clientSuite = new TestSuite(name);
        return clientSuite; 
    }
    
    /**
     * Create suite containing embedded-only tests.
     */
    private static TestSuite embeddedSuite(String name) {
        TestSuite embeddedSuite = new TestSuite(name);
        embeddedSuite.addTest(new ConnectionTest(
                    "embeddedCreateBlob"));
        embeddedSuite.addTest(new ConnectionTest(
                    "embeddedCreateClob"));
        return embeddedSuite;
    }
    
    /**
     * Create a test suite containing tests for a JDB connection.
     *  In addition, separate suites for embedded- and client-only are added
     *  when appropriate.
     */
    public static Test suite() {
        TestSuite connSuite = 
            new TestSuite(ConnectionTest.class, "ConnectionTest suite");
        // Add client only tests
        // NOTE: JCC is excluded
        if (usingDerbyNetClient()) {
            connSuite.addTest(
                    clientSuite("ConnectionTest client-only suite"));
        }
        // Add embedded only tests
        if (usingEmbedded()) {
            connSuite.addTest(
                    embeddedSuite("ConnectionTest embedded-only suite"));
        }
        return connSuite;
    }
    
} // End class BaseJDBCTestCase
