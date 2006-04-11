/*
 
   Derby - Class CallableStatementTest
 
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
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;

import java.sql.*;
import java.lang.reflect.Method;
import java.util.Vector;

/**
 * Tests of the <code>java.sql.CallableStatement</code> JDBC40 API.
 */
public class CallableStatementTest
    extends BaseJDBCTestCase {

    /** Default connection used by the tests. */
    private Connection con = null;
    /** Default callable statement used by the tests. */
    private CallableStatement cStmt = null;
    

    /**
     * Create a test with the given name.
     *
     * @param name name of the test.
     */
    public CallableStatementTest(String name) {
        super(name);
    }

    /**
     * Create a default callable statement and connection.
     *
     * @throws SQLException if creation of connection or callable statement
     *                      fail.
     */
    public void setUp() 
        throws SQLException {
        con = getConnection();
        cStmt = con.prepareCall("values 1");
    }

    /**
     * Close default callable statement and connection.
     *
     * @throws SQLException if closing of the connection or the callable
     *                      statement fail.
     */
    public void tearDown()
        throws SQLException {
        if (cStmt != null && !cStmt.isClosed()) {
            cStmt.close();
        }
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        cStmt = null;
        con = null;
    }
   
    public void testGetNClobIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getNClob(1);
            fail("CallableStatement.getNClob(int) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNClobStringNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNClob("some-parameter-name");
            fail("CallableStatement.getNClob(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNStringIntNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNString(1);
            fail("CallableStatement.getNString(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNStringStringNotImplemented() 
        throws SQLException {
        try {
            cStmt.getNString("some-parameter-name");
            fail("CallableStatement.getNString(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetCharacterStreamIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getCharacterStream(1);
            fail("CallableStatement.getCharacterStream(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetCharacterStreamStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getCharacterStream("some-parameter-name");
            fail("CallableStatement.getCharacterStream(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNCharacterStreamIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getNCharacterStream(1);
            fail("CallableStatement.getNCharacterStream(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNCharacterStreamStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getNCharacterStream("some-parameter-name");
            fail("CallableStatement.getNCharacterStream(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetBlobNotImplemented()
        throws SQLException {
        try {
            cStmt.setBlob("some-parameter-name", null);
            fail("CallableStatement.setBlob(String, Blob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testSetClobNotImplemented()
        throws SQLException {
        try {
            cStmt.setClob("some-parameter-name", null);
            fail("CallableStatement.setClob(String, Clob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNCharacterStreamNotImplemented()
        throws SQLException {
        try {
            cStmt.setNCharacterStream("some-parameter-name", null, 0l);
            fail("CallableStatement.setNCharacterStream(String,Reader,long) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNClobNClobNotImplemented()
        throws SQLException {
        try {
            cStmt.setNClob("some-parameter-name", null);
            fail("CallableStatement.setNClob(String, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNClobReaderNotImplemented()
        throws SQLException {
        try {
            cStmt.setNClob("some-parameter-name", null, 0l);
            fail("CallableStatement.setNClob(String, Reader, long) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetNStringNotImplemented()
        throws SQLException {
        try {
            cStmt.setNString("some-parameter-name", "some-value");
            fail("CallableStatement.setNString(String, String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
   
    public void testGetSQLXMLIntNotImplemented()
        throws SQLException {
        try {
            cStmt.getSQLXML(1);
            fail("CallableStatement.getSQLXML(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetSQLXMLStringNotImplemented()
        throws SQLException {
        try {
            cStmt.getSQLXML("some-parameter-name");
            fail("CallableStatement.getSQLXML(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testSetSQLXMLNotImplemented()
        throws SQLException {
        try {
            cStmt.setSQLXML("some-parameter-name", null);
            fail("CallableStatement.setSQLXML(String, SQLXML) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns true and we call unwrap
     * The test is right now being run in the embedded case only
     *
     */
    public void testisWrapperReturnsTrue() throws SQLException {
        //wrapper support is currently provided 
        //only for embedded side so return if 
        //running in DerbyNetClient framework
        if(usingDerbyNetClient())
            return;
        
        Class<CallableStatement> wrap_class = CallableStatement.class;
        
        //The if should return true enabling us  to call the unwrap method
        //without throwing  an exception
        if(cStmt.isWrapperFor(wrap_class)) {
            try {
                CallableStatement stmt1 =
                        (CallableStatement)cStmt.unwrap(wrap_class);
            }
            catch(SQLException sqle) {
                fail("Unwrap wrongly throws a SQLException");
            }
        } else {
            fail("isWrapperFor wrongly returns false");
        }
    }
    
    /**
     *
     * Tests the wrapper methods isWrapperFor and unwrap. Test
     * for the case when isWrapperFor returns false and we call unwrap
     * The test is right now being run in the embedded case only
     *
     */
    public void testisWrapperReturnsFalse() throws SQLException {
        //wrapper support is currently provided 
        //only for embedded side so return if 
        //running in DerbyNetClient framework
         if(usingDerbyNetClient())
            return;
         
        //test for the case when isWrapper returns false
        //using some class that will return false when
        //passed to isWrapperFor
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //returning false is the correct behaviour in this case
        //Generate a message if it returns true
        if(cStmt.isWrapperFor(wrap_class)) {
            fail("isWrapperFor wrongly returns true");
        } else {
            try {
                ResultSet rs1 = (ResultSet)
                cStmt.unwrap(wrap_class);
                fail("unwrap does not throw the expected " +
                        "exception");
            } catch (SQLException sqle) {
                //calling unwrap in this case throws an SQLException
                //check that this SQLException has the correct SQLState
                if(!SQLStateConstants.UNABLE_TO_UNWRAP.equals(sqle.getSQLState())) {
                    throw sqle;
                }
            }
        }
    }

    /**
     * Return suite with all tests of the class.
     */
    public static Test suite() {
        return (new TestSuite(CallableStatementTest.class,
                              "CallableStatementTest suite"));
    }
    
} // End class CallableStatementTest
