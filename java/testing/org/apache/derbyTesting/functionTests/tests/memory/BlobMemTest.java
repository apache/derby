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
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
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
    private void testBlobLength(boolean lengthless, int extraLen) throws SQLException, IOException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE BLOBTAB (K INT CONSTRAINT PK PRIMARY KEY, B BLOB(" + LONG_BLOB_LENGTH + "))");
        
        PreparedStatement ps = prepareStatement("INSERT INTO BLOBTAB VALUES(?,?)");
        // We allocate 16MB for the test so use something bigger than that.
        ps.setInt(1,1);
        int blobLen = LONG_BLOB_LENGTH + extraLen;
        LoopingAlphabetStream stream = new LoopingAlphabetStream(blobLen);
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
            ps.setBinaryStream(2, stream,blobLen);
        if (extraLen == 0)
        {
            ps.executeUpdate();
        }
        else
        {
            try
            {
                ps.executeUpdate();
                fail("Expected truncation error for blob too large");
            }
            catch (SQLException sqlE)
            {
                assertSQLState("Wrong SQL State for truncation", "22001", sqlE);
            }
            // extraLen > 0 is just a way to force the truncation error. Once
            // we've forced that error, we're done testing, so return.
            return;
        }
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
        testBlobLength(false, 0);
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
        testBlobLength(true, 0);  
    }
    /**
      * Simple test to excercise message 22001 as described in DERBY-961.
      */
    public void testBlobLengthTooLongDerby961() throws SQLException, IOException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {        
        testBlobLength(false, 10000);  
    }
       public static Test suite() {
        TestSuite suite =  new TestSuite();
        // Just add Derby-6096 embedded as it takes time to run
        suite.addTest(new BlobMemTest("xtestderby6096BlobhashJoin"));
        suite.addTest(TestConfiguration.defaultSuite(BlobMemTest.class));
        
        Properties p = new Properties();
        // use small pageCacheSize so we don't run out of memory on the insert.
        p.setProperty("derby.storage.pageCacheSize", "100");
        return new SystemPropertyTestSetup(suite,p);
    }

    /**
     * Tests that a blob can be safely occur multiple times in a SQL select and
     * test that large objects streams are not being materialized when cloned.
     * <p/>
     * See DERBY-4477.
     * @see org.apache.derbyTesting.functionTests.tests.jdbcapi.BLOBTest#testDerby4477_3645_3646_Repro
     * @see ClobMemTest#testDerby4477_3645_3646_Repro_lowmem_clob
     */
    public void testDerby4477_3645_3646_Repro_lowmem()
            throws SQLException, IOException {

        setAutoCommit(false);

        Statement s = createStatement();
        int blobsize = LONG_BLOB_LENGTH;

        s.executeUpdate(
            "CREATE TABLE T_MAIN(" +
            "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
            "V BLOB(" + blobsize + ") )");

        PreparedStatement ps = prepareStatement(
            "INSERT INTO T_MAIN(V) VALUES (?)");

        int blobLen = blobsize;
        LoopingAlphabetStream stream = new LoopingAlphabetStream(blobLen);
        ps.setBinaryStream(1, stream, blobLen);

        ps.executeUpdate();
        ps.close();

        s.executeUpdate("CREATE TABLE T_COPY ( V1 BLOB(" + blobsize +
                        "), V2 BLOB(" + blobsize + "))");

        // This failed in the repro for DERBY-3645 solved as part of
        // DERBY-4477:
        s.executeUpdate("INSERT INTO T_COPY SELECT  V, V FROM T_MAIN");

        // Check that the two results are identical:
        ResultSet rs = s.executeQuery("SELECT * FROM T_COPY");
        rs.next();
        InputStream is = rs.getBinaryStream(1);

        stream.reset();
        assertEquals(stream, is);

        is = rs.getBinaryStream(2);

        stream.reset();
        assertEquals(stream, is);
        rs.close();

        // This failed in the repro for DERBY-3646 solved as part of
        // DERBY-4477 (repro slightly rewoked here):
        rs = s.executeQuery("SELECT 'I', V, ID, V from T_MAIN");
        rs.next();

        is = rs.getBinaryStream(2);
        stream.reset();
        assertEquals(stream, is);

        is = rs.getBinaryStream(4);
        stream.reset();
        assertEquals(stream, is);

        // clean up
        stream.close();
        is.close();
        s.close();
        rs.close();

        rollback();
    }

    /**
     * Test that a BLOB that goes through the sorter does not get materialized
     * twice in memory. It will still be materialized as part of the sorting,
     * but the fix for DERBY-5752 prevents the creation of a second copy when
     * accessing the BLOB after the sorting.
     */
    public void testDerby5752DoubleMaterialization() throws Exception {
        setAutoCommit(false);

        Statement s = createStatement();
        s.execute("create table d5752(id int, b blob)");

        int lobSize = 1000000;

        // Insert a single BLOB in the table.
        PreparedStatement insert =
                prepareStatement("insert into d5752 values (1,?)");
        insert.setBinaryStream(1, new LoopingAlphabetStream(lobSize), lobSize);
        insert.execute();
        closeStatement(insert);

        Blob[] blobs = new Blob[15];

        // Repeatedly sort the table and keep a reference to the BLOB.
        for (int i = 0; i < blobs.length; i++) {
            ResultSet rs = s.executeQuery("select * from d5752 order by id");
            rs.next();
            // Used to get an OutOfMemoryError here because a new copy of the
            // BLOB was created in memory.
            blobs[i] = rs.getBlob(2);
            rs.close();
        }

        // Access the BLOBs here to make sure they are not garbage collected
        // earlier (in which case we wouldn't see the OOME in the loop above).
        for (int i = 0; i < blobs.length; i++) {
            assertEquals(lobSize, blobs[i].length());
        }
    }
    
    /**
     * 
     * DERBY-6096 Make blob hash join does not run out of memory.
     * Prior to fix blobs were estimated at 0. We will test with
     * 32K blobs even though the estimatedUsage is at 10k. The default
     * max memory per table is only 1MB.
     * 
     * @throws SQLException
     */
    public void xtestderby6096BlobhashJoin() throws SQLException {
        byte[] b = new byte[32000];
        Arrays.fill(b, (byte) 'a'); 
        Statement s = createStatement();
        s.execute("create table d6096(i int, b blob)");
        PreparedStatement ps = prepareStatement("insert into d6096 values (?, ?)");
        ps.setBytes(2, b);
        for (int i = 0; i < 2000; i++) {
            ps.setInt(1, i);
            ps.execute();
        }
        ResultSet rs = s.executeQuery("select * from d6096 t1, d6096 t2 where t1.i=t2.i");
        // just a single fetch will build the hash table and consume the memory.
        assertTrue(rs.next());
        // derby.tests.debug prints memory usage
        System.gc();
        println("TotalMemory:" + Runtime.getRuntime().totalMemory()
                + " " + "Free Memory:"
                + Runtime.getRuntime().freeMemory());
        rs.close();
    }

}
