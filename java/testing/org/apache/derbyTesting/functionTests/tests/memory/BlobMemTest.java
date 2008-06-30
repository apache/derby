/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.BlobMemTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.memory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.harness.JavaVersionHolder;
import org.apache.derbyTesting.functionTests.tests.lang.SimpleTest;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class BlobMemTest extends BaseJDBCTestCase {

    private static final int LONG_BLOB_LENGTH = 18000000;
    private static final String LONG_BLOB_LENGTH_STRING= "18000000";
    private static final byte[] SHORT_BLOB_BYTES = new byte[] {0x01,0x02,0x03};

    public BlobMemTest(String name) {
        super(name);
    }

    public void tearDown() throws SQLException {
        rollback();
    }
    
    
    
    /**
     * Insert a blob and test length.    
     * 
     * @param lengthless  if true use the lengthless setBinaryStream api
     * 
     * @throws SQLException
     * @throws IOException 
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
    private void testBlobLength(boolean lengthless) throws SQLException, IOException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE BLOBTAB (K INT CONSTRAINT PK PRIMARY KEY, B BLOB(" + LONG_BLOB_LENGTH + "))");
        
        PreparedStatement ps = prepareStatement("INSERT INTO BLOBTAB VALUES(?,?)");
        // We allocate 16MB for the test so use something bigger than that.
        ps.setInt(1,1);
        LoopingAlphabetStream stream = new LoopingAlphabetStream(LONG_BLOB_LENGTH);
        if (lengthless) {
            Method m = null;
            try {
                Class c = ps.getClass();
                m = c.getMethod("setBinaryStream",new Class[] {Integer.TYPE,
                            InputStream.class});                
            } catch (NoSuchMethodException e) {
                // ignore method not found as method may not be present for 
                // jdk's lower than 1.6.
                println("Skipping lengthless insert because method is not available");
                return;                
            }
            m.invoke(ps, new Object[] {new Integer(2),stream});
        }
        else
            ps.setBinaryStream(2, stream,LONG_BLOB_LENGTH);
        ps.executeUpdate();
        // insert a zero length blob.
        ps.setInt(1, 2);
        ps.setBytes(2, new byte[] {});
        ps.executeUpdate();
        // insert a null blob.
        ps.setInt(1, 3);
        ps.setBytes(2,null);
        ps.executeUpdate();
        // insert a short blob
        ps.setInt(1, 4);
        ps.setBytes(2, SHORT_BLOB_BYTES);
        ps.executeUpdate();
        // Currently need to use optimizer override to force use of the index.
        // Derby should use sort avoidance and do it automatically, but there
        // appears to be a bug.
        ResultSet rs = s.executeQuery("SELECT K, LENGTH(B), B FROM BLOBTAB" +
                "-- DERBY-PROPERTIES constraint=pk\n ORDER BY K"); 
        rs.next();
        assertEquals(LONG_BLOB_LENGTH_STRING,rs.getString(2));
        // make sure we can still access the blob after getting length.
        // It should be ok because we reset the stream
        InputStream rsstream = rs.getBinaryStream(3);
        int len= 0;
        byte[] buf = new byte[32672];
        for (;;)  {
                int size = rsstream.read(buf);
                if (size == -1)
                        break;
                len += size;
                int expectedValue = ((len -1) % 26) + 'a';
                if (size != 0)
                    assertEquals(expectedValue,buf[size -1]);      
        }

        assertEquals(LONG_BLOB_LENGTH,len);
        // empty blob
        rs.next();
        assertEquals("0",rs.getString(2));
        byte[] bytes = rs.getBytes(3);
        assertEquals(0, bytes.length);
        // null blob
        rs.next();
        assertEquals(null,rs.getString(2));
        bytes = rs.getBytes(3);
        assertEquals(null,bytes);
        // short blob
        rs.next();
        assertEquals("3",rs.getString(2));
        bytes = rs.getBytes(3);
        assertTrue(Arrays.equals(SHORT_BLOB_BYTES, bytes));
        rs.close();         
        
        // Select just length without selecting the blob.
        rs = s.executeQuery("SELECT K, LENGTH(B)  FROM BLOBTAB " +
                "ORDER BY K");
        JDBC.assertFullResultSet(rs, new String [][] {{"1",LONG_BLOB_LENGTH_STRING},{"2","0"},
                {"3",null},{"4","3"}});
    }
    
    /**
     * Test the length after inserting with the setBinaryStream api 
     * that takes length.  In this case the length will be encoded at the
     * begining of the stream and the call should be fairly low overhead.
     * 
     * @throws SQLException
     * @throws IOException 
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
    public void testBlobLength() throws SQLException, IOException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        testBlobLength(false);
    }
    
    /**
     * Test the length after inserting the blob value with the lengthless
     * setBinaryStream api. In this case we will have to read the whole 
     * stream to get the length.
     * 
     * @throws SQLException
     * @throws IOException 
     * @throws InvocationTargetException 
     * @throws IllegalAccessException 
     * @throws IllegalArgumentException 
     */
    public void testBlobLengthWithLengthlessInsert() throws SQLException, IOException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {        
        testBlobLength(true);  
    }
       public static Test suite() {
        Test suite =  TestConfiguration.defaultSuite(BlobMemTest.class);
        Properties p = new Properties();
        // use small pageCacheSize so we don't run out of memory on the insert.
        p.setProperty("derby.storage.pageCacheSize", "100");
        return new SystemPropertyTestSetup(suite,p);
    }

      

}
