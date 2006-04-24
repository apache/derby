/*
 
   Derby - Class BlobTest
 
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

import java.sql.*;

/*
 * Tests of the JDBC 4.0 specific <code>Blob</code> methods.
 */
public class BlobTest
    extends BaseJDBCTestCase {

    /** Default Blob object used by the tests. */
    private Blob blob = null;
    /** Default connection used by the tests. */
    private Connection con = null;
    
    /**
     * Create the test with the given name.
     *
     * @param name name of the test.
     */
    public BlobTest(String name) {
        super(name);
    }
    
    public void setUp() 
        throws SQLException {
        con = getConnection();
        blob = BlobClobTestSetup.getSampleBlob(con);
    }

    public void tearDown()
        throws SQLException {
        blob = null;
        if (con != null && !con.isClosed()) {
            con.rollback();
            con.close();
        }
        con = null;
    }

    public void testFreeNotImplemented()
        throws SQLException {
        try {
            blob.free();
            fail("Blob.free() should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }
    
    public void testGetBinaryStringLongNotImplemented()
        throws SQLException {
        try {
            blob.getBinaryStream(5l, 10l);
            fail("Blob.getBinaryStream(long,long) should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // Do nothing, we are fine
        }
    }

    /**
     * Create test suite for this test.
     */
    public static Test suite() {
        return new BlobClobTestSetup(new TestSuite(BlobTest.class,
                                                   "BlobTest suite"));
    }

} // End class BlobTest
