/*
 
   Derby - Class BlobSetBytesBoundaryTest
    
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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Boundary tests for Blob.setBytes(). see DERBY-3898.
 *
 */
public class BlobSetBytesBoundaryTest extends BaseJDBCTestCase {

    private static final byte[] BOLB_CONTENT = "test".getBytes();;

    public BlobSetBytesBoundaryTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test suite = TestConfiguration.defaultSuite(
                BlobSetBytesBoundaryTest.class, false); 
        
        return new CleanDatabaseTestSetup(suite) {
            protected void decorateSQL(Statement stmt)
                    throws SQLException {
                    initializeBlobData(stmt);
            }
        };
    }
        
    public void testSetBytesWithTooLongLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(1, new byte[] {0x69}, 0, 2);
            fail("Wrong long length is not accepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesByBadLengthAndOffset() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);

        try {
            //length adding offset will be bigger than the length of the byte array.
            blob.setBytes(1, new byte[] {0x69, 0x4e, 0x47, 0x55}, 1, 4);
            fail("Wrong offset and length is not accepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }

        // Also check that we fail with the expected error if the sum of
        // offset and length is greater than Integer.MAX_VALUE.
        try {
            blob.setBytes(1, new byte[100], 10, Integer.MAX_VALUE);
            fail("setBytes() should fail when offset+length > bytes.length");
        } catch (SQLException sqle) {
            assertSQLState("XJ079", sqle);
        }

        stmt.close();
    }
    
    public void testSetBytesWithZeroLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        int actualLength = blob.setBytes(1, new byte[] {0x69}, 0, 0);
        assertEquals("return zero for zero length", 0, actualLength);            
        
        stmt.close();
    }
    
    public void testSetBytesWithNonPositiveLength() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try{
            blob.setBytes(1, new byte[] {0x69}, 0, -1);
            fail("Nonpositive Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ071", sqle);
        }
        
        stmt.close();
    }
        
    public void testSetBytesWithInvalidOffset() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(1, new byte[] {0xb}, -1, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        try {
            blob.setBytes(1, new byte[] {0xb}, 2, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        try {
            blob.setBytes(1, new byte[] {0xb, 0xe}, Integer.MAX_VALUE, 1);
            fail("Invalid offset Length is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ078", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesWithEmptyBytes() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        assertEquals(0, blob.setBytes(1, new byte[0]));
        
        stmt.close();
    }
    
    public void testSetBytesWithTooBigPos() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);

        try {
            blob.setBytes(Integer.MAX_VALUE, new byte[] {0xf});
            fail("Too big position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
        
        try {
            blob.setBytes(BOLB_CONTENT.length + 2, new byte[] {0xf});
            fail("Too big position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ076", sqle);
        }
        
        stmt.close();
    }
    
    public void testSetBytesWithNonpositivePos() throws SQLException {
        Statement stmt = getConnection().createStatement();
        ResultSet rs = stmt.executeQuery(
                "select dBlob, length from BlobTable");
        rs.next();
        Blob blob = rs.getBlob(1);
        
        try {
            blob.setBytes(0, new byte[] {0xf});
            fail("Nonpositive position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ070", sqle);
        }
        
        try {
            blob.setBytes(-1, new byte[] {0xf});
            fail("Nonpositive position is not sccepted!");
        } catch (SQLException sqle) {
            assertSQLState("XJ070", sqle);
        }
        
        stmt.close();
    }

    /**
     * Generates test data. 
     */
    private static void initializeBlobData(Statement stmt)
            throws SQLException {
        Connection con = stmt.getConnection();
        con.setAutoCommit(false);

        try {
            stmt.executeUpdate("drop table BlobTable");
        } catch (SQLException sqle) {
            assertSQLState("42Y55", sqle);
        }

        stmt.executeUpdate("create table BlobTable (dBlob Blob, length int)");

        PreparedStatement smallBlobInsert = con
                .prepareStatement("insert into BlobTable values (?,?)");
        // Insert just one record.
        
        smallBlobInsert.setBytes(1, BOLB_CONTENT );
        smallBlobInsert.setInt(2, BOLB_CONTENT.length);
        smallBlobInsert.executeUpdate();

        con.commit();
    }
}
