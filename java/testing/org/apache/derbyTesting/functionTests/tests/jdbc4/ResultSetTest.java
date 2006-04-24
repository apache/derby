/*
 
   Derby - Class ResultSetTest
 
   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.
 
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

import java.sql.*;

/**
 * Tests of JDBC4 features in ResultSet.
 */
public class ResultSetTest
    extends BaseJDBCTestCase {

    /** Default connection used by the tests. */
    private Connection con = null;
    /** Statement used to obtain default resultset. */
    private Statement stmt = null;
    /** Default resultset used by the tests. */
    private ResultSet rs = null;

    /**
     * Create test with given name.
     *
     * @param name name of the test.
     */
    public ResultSetTest(String name) {
        super(name);
    }

    public void setUp()
        throws SQLException {
        con = getConnection();
        stmt = con.createStatement();
        rs = stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");
        // Position on first result.
        rs.next();
    }

    public void tearDown()
        throws SQLException {
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
    }

    public void testGetNCharacterStreamIntNotImplemented()
        throws SQLException {
        try {
            rs.getNCharacterStream(1);
            fail("ResultSet.getNCharacterStream(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
   
    public void testGetNCharaterStreamStringNotImplemented()
        throws SQLException {
        try {
            rs.getNCharacterStream("some-column-name");
            fail("ResultSet.getNCharacterStream(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNClobNotIntImplemented()
        throws SQLException {
        try {
            rs.getNClob(1);
            fail("ResultSet.getNClob(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetNClobStringNotImplemented()
        throws SQLException {
        try {
            rs.getNClob("some-column-name");
            fail("ResultSet.getNClob(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNStringIntNotImplemented()
        throws SQLException {
        try {
            rs.getNString(1);
            fail("ResultSet.getNString(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testGetNStringStringNotImplemented()
        throws SQLException {
        try {
            rs.getNString("some-column-name");
            fail("ResultSet.getNString(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetSQLXMLIntNotImplemented()
        throws SQLException {
        try {
            rs.getSQLXML(1);
            fail("ResultSet.getSQLXML(int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testGetSQLXMLStringNotImplemented()
        throws SQLException {
        try {
            rs.getSQLXML("some-column-name");
            fail("ResultSet.getSQLXML(String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNCharacterStreamIntNotImplemented()
        throws SQLException {
        try {
            rs.updateNCharacterStream(1, null, 0);
            fail("ResultSet.updateNCharacterStream(int, Reader, int) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
   
    public void testUpdateNCharaterStreamStringNotImplemented()
        throws SQLException {
        try {
            rs.updateNCharacterStream("some-column-name", null, 0);
            fail("ResultSet.updateNCharacterStream(String, Reader, 0) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNClobNotIntImplemented()
        throws SQLException {
        try {
            rs.updateNClob(1, null);
            fail("ResultSet.updateNClob(int, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNClobStringNotImplemented()
        throws SQLException {
        try {
            rs.updateNClob("some-column-name", null);
            fail("ResultSet.updateNClob(String, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testUpdateNStringIntNotImplemented()
        throws SQLException {
        try {
            rs.updateNString(1, null);
            fail("ResultSet.updateNString(int, String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testUpdateNStringStringNotImplemented()
        throws SQLException {
        try {
            rs.updateNString("some-column-name", null);
            fail("ResultSet.updateNString(String, String) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateSQLXMLIntNotImplemented()
        throws SQLException {
        try {
            rs.updateSQLXML(1, null);
            fail("ResultSet.updateSQLXML(int, SQLXML) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateSQLXMLStringNotImplemented()
        throws SQLException {
        try {
            rs.updateSQLXML("some-column-name", null);
            fail("ResultSet.updateSQLXML(String, SQLXML) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public static Test suite() {
        return new TestSuite(ResultSetTest.class,
                             "ResultSetTest suite");
    }
}
