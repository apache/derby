/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ConnectionTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
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

import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
     * Create a test with the given name.
     * 
     * @param name name of the test.
     */
    public ConnectionTest(String name) {
        super(name);
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
            Blob blob = getConnection().createBlob();
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
            Clob clob = getConnection().createClob();
            //check if the Clob is empty
            if(clob.length() > 0)
                fail("The new Clob should not have a length of greater than " +
                        "zero");
    }

    public void testCreateArrayNotImplemented()
        throws SQLException {
        try {
            getConnection().createArrayOf(null, null);
            fail("createArrayOf(String,Object[]) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    public void testCreateNClobNotImplemented()
        throws SQLException {
        try {
            getConnection().createNClob();
            fail("createNClob() should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    public void testCreateSQLXMLNotImplemented()
        throws SQLException {
        try {
            getConnection().createSQLXML();
            fail("createSQLXML() should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    public void testCreateStructNotImplemented()
        throws SQLException {
        try {
            getConnection().createStruct(null, null);
            fail("createStruct(String,Object[]) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }
    
    public void testGetClientInfo()
        throws SQLException {
        assertTrue("getClientInfo() must return an empty Properties object", 
                   getConnection().getClientInfo().isEmpty());
    }
    
    public void testGetClientInfoString()
        throws SQLException {
        assertNull("getClientInfo(null) must return null",
                   getConnection().getClientInfo(null));
        assertNull("getClientInfo(\"someProperty\") must return null",
                   getConnection().getClientInfo("someProperty"));
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
        assertTrue(getConnection().isValid(0));

        // Test with a 1 second timeout
        assertTrue(getConnection().isValid(1));

        // Test with an illegal timeout
        try {
            getConnection().isValid(-1);
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
        assertTrue(getConnection().getTypeMap().isEmpty());
    }
    
    /**
     * Tests that <code>getTypeMap()</code> returns the input map
     * @exception SQLException if an error occurs
     */
    public void testGetTypeMapReturnsAsExpected() throws SQLException {
        Statement s = getConnection().createStatement();
        int ret;
        try {
            ret = s.executeUpdate("DROP TABLE T1");
            ret = s.executeUpdate("DROP TYPE JAVA_UTIL_LIST RESTRICT");
        } catch (Exception e) {}
        
        ret = s.executeUpdate("CREATE TYPE JAVA_UTIL_LIST " +
                              "EXTERNAL NAME 'java.util.List'" +
                              "LANGUAGE JAVA");

        s.execute("CREATE TABLE T1 (A1 JAVA_UTIL_LIST)");
        
        PreparedStatement ps = getConnection().prepareStatement(
                "INSERT INTO T1(A1) VALUES (?)");
        
        ArrayList<String> lst = new ArrayList<String>();
        lst.add("First element");
        lst.add("Second element");
        
        ps.setObject(1, lst);
        ps.execute();     
        
        Map<String, Class<?>> map = getConnection().getTypeMap();
        try {
            map.put("JAVA_UTIL_LIST", List.class);
            fail("returned map should be immutable");
        } catch (UnsupportedOperationException uoe) {
            // Ignore expected exception
        }
        
        //Pass empty Map to setTypeMap(). It won't raise any erros because
        //the method does nothing when the Map is empty.
        java.util.Map<String,Class<?>> emptyMap = new java.util.HashMap<String,Class<?>>();
        getConnection().setTypeMap(emptyMap);

        // Create a non-empty map to test setTypeMap(). setTypeMap() raises
        // a feature not supported exception if the map isn't empty.
        map = new HashMap<String, Class<?>>();
        map.put("JAVA_UTIL_LIST", List.class);
        
        try {
            getConnection().setTypeMap(map);
            fail( "Should raise an Unimplemented Feature exception." );
        }
        catch (SQLException se)
        {
            assertEquals( SQLFeatureNotSupportedException.class.getName(), se.getClass().getName() );
        }
        
        ResultSet rs = s.executeQuery("select * from T1");
        assertTrue(rs.next());
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            Object o = rs.getObject(i);
            assertEquals(lst, o);
            //System.out.print(o + "(Type " + o.getClass().getName() + " )");
        }
        s.executeUpdate("DROP TABLE T1");
        s.executeUpdate("DROP TYPE JAVA_UTIL_LIST RESTRICT");
        s.close();
        ps.close();
    }

    public void testIsWrapperReturnsFalse()
        throws SQLException {
        assertFalse(getConnection().isWrapperFor(ResultSet.class));
    }

    public void testIsWrapperReturnsTrue()
        throws SQLException {
        assertTrue(getConnection().isWrapperFor(Connection.class));
    }

    public void testSetClientInfoProperties()
        throws SQLException {
        getConnection().setClientInfo(null);
        Properties p = new Properties();
        getConnection().setClientInfo(p);

        p.setProperty("prop1", "val1");
        p.setProperty("prop2", "val2");
        try {
            getConnection().setClientInfo(p);
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
        getConnection().setClientInfo(null, null);

        try {
            getConnection().setClientInfo("foo", null);
            fail("setClientInfo(String, null) should throw "+
                 "NullPointerException");
        } catch (NullPointerException npe) {}

        try {
            getConnection().setClientInfo("name", "value");
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
        Connection unwrappedCon = getConnection().unwrap(Connection.class);
        assertSame("Unwrap returned wrong object.", getConnection(), unwrappedCon);
    }

    public void testUnwrapInvalid()
        throws SQLException {
        try {
            ResultSet unwrappedRs = getConnection().unwrap(ResultSet.class);
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
        TestSuite connSuite = new TestSuite("ConnectionTest suite");

        TestSuite embedded = new TestSuite("ConnectionTest:embedded");
        embedded.addTestSuite(ConnectionTest.class);
        embedded.addTest(embeddedSuite("ConnectionTest:embedded-only"));
        connSuite.addTest(embedded);
        
        // repeat the embedded tests obtaining a connection from
        // an XA data source.
        embedded = new TestSuite("ConnectionTest:embedded XADataSource");
        embedded.addTestSuite(ConnectionTest.class);
        embedded.addTest(embeddedSuite("ConnectionTest:embedded-only XADataSource"));
        connSuite.addTest(TestConfiguration.connectionXADecorator(embedded));
        

        TestSuite client = new TestSuite("ConnectionTest:client");
        client.addTestSuite(ConnectionTest.class);
        client.addTest(clientSuite("ConnectionTest:client-only"));
        connSuite.addTest(TestConfiguration.clientServerDecorator(client));

        // repeat the client tests obtaining a connection from
        // an XA data source.
        client = new TestSuite("ConnectionTest:client XADataSource");
        client.addTestSuite(ConnectionTest.class);
        client.addTest(clientSuite("ConnectionTest:client-only XADataSource"));
        connSuite.addTest(
                TestConfiguration.clientServerDecorator(
                        TestConfiguration.connectionXADecorator(client)));

        return connSuite;
    }
    
} // End class BaseJDBCTestCase
