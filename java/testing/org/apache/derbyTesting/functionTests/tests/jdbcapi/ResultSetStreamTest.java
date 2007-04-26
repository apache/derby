/**
 *  Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ResultSetStreamTest
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
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.ServerSocket;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.zip.CRC32;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ResultSetStreamTest extends BaseJDBCTestCase {

    public ResultSetStreamTest(String name) {
        super(name);

    }

    private static String filePath;

    private static String sep;

    public void testInsertData() throws SQLException, Exception {
        try {

            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws SQLException, FileNotFoundException,
                        IOException {
                    insertData();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            throw e.getException();
        }
    }

    /**
     * Test insert of data with setBinaryStream
     * 
     * @throws SQLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    private void insertData() throws SQLException, FileNotFoundException,
            IOException {
        Connection conn = getConnection();
        PreparedStatement ppw = conn
                .prepareStatement("insert into t2 (len, data) values (?, ?)");
        filePath = "extin";
        String userDir = System.getProperty("user.dir");
        sep = System.getProperty("file.separator");
        filePath = userDir + sep + filePath;
        File file = new File(filePath + sep + "littleclob.utf");
        int fileSize = (int) file.length();
        BufferedInputStream fileData = new BufferedInputStream(
                new FileInputStream(file));
        ppw.setInt(1, fileSize);
        ppw.setBinaryStream(2, fileData, fileSize);
        ppw.executeUpdate();
        fileData.close();

        file = new File(filePath + sep + "short.utf");
        fileSize = (int) file.length();
        fileData = new BufferedInputStream(new FileInputStream(file));
        ppw.setInt(1, fileSize);
        ppw.setBinaryStream(2, fileData, fileSize);
        ppw.executeUpdate();
        fileData.close();
        // null binary value
        ppw.setInt(1, -1);
        ppw.setBinaryStream(2, (java.io.InputStream) null, 0);
        ppw.executeUpdate();

        // value copied over from original Java object test.
        File rssg = new java.io.File(filePath + sep + "resultsetStream.gif");
        int rssgLength = (int) rssg.length();
        ppw.setInt(1, (int) rssgLength);
        fileData = new BufferedInputStream(new FileInputStream(rssg));
        ppw.setBinaryStream(2, fileData, rssgLength);
        ppw.executeUpdate();
        fileData.close();

        // check binary input streams of invalid length.
        // JDBC 3.0 tutorial says stream contents must match length.

        byte[] tooFew = new byte[234];

        ppw.setInt(1, 234);
        ppw.setBinaryStream(2, new java.io.ByteArrayInputStream(tooFew), 234); // matching length
        ppw.executeUpdate();


        ppw.setInt(1, 235);
        ppw.setBinaryStream(2, new java.io.ByteArrayInputStream(tooFew), 235); // too few bytes in stream
        try {
                ppw.executeUpdate();
                fail("FAIL - execute with setBinaryStream() with too few bytes succeeded");
        } catch (SQLException sqle) {       
                assertMisMatchStreamLength(sqle);
        }

        ppw.setInt(1, 233);
        ppw.setBinaryStream(2, new java.io.ByteArrayInputStream(tooFew), 233); // too many bytes
        try {
                ppw.executeUpdate();
                fail("FAIL - execute with setBinaryStream() with too many bytes succeeded");
        } catch (SQLException sqle) {
            assertMisMatchStreamLength(sqle);
        }

        ppw.close();

    }

    private void assertMisMatchStreamLength(SQLException sqle) {
        if (usingEmbedded()) {
            assertEquals("XSDA4",sqle.getSQLState());
            sqle = sqle.getNextException();
            assertSQLState("XJ001",sqle);
        } else {
            String state = sqle.getSQLState();
            assertTrue("SQLState not XN015 or XN017 as expected", "XN015".equals(state) || "XN017".equals(state));
        }
    }

    public void testBinaryStreamProcessing() throws SQLException, Exception {
        try {

            AccessController.doPrivileged(new PrivilegedExceptionAction() {
                public Object run() throws SQLException, FileNotFoundException,
                        IOException {
                    binaryStreamProcessing();
                    return null;
                }
            });
        } catch (PrivilegedActionException e) {
            throw e.getException();
        }
    }

    /**
     * Test getBinaryStream by comparing chksum of retrieved value.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void binaryStreamProcessing() throws SQLException, IOException {
        Connection conn = getConnection();
        Statement s = conn.createStatement();

        filePath = "extin";
        String userDir = System.getProperty("user.dir");
        sep = System.getProperty("file.separator");
        filePath = userDir + sep + filePath;

        File rssg = new java.io.File(filePath + sep + "resultsetStream.gif");
        int rssgLength = (int) rssg.length();
        ResultSet rs = s.executeQuery("select data from t2 where len = "
                + rssgLength);
        ResultSetMetaData met = rs.getMetaData();
        assertEquals(1, met.getColumnCount());

        while (rs.next()) {
            // JDBC columns use 1-based counting

            // get the first column as a stream

            InputStream is = rs.getBinaryStream(1);
            if (is == null) {
                fail("FAIL - getBinaryStream() return null");
                break;
            }

            // read the first 200 bytes from the stream and checksum them
            byte[] b200 = new byte[200];

            // no guaratees to read all 200 bytes in one read call.
            int count = 0;

            while (count < 200) {
                int r = is.read(b200, count, 200 - count);
                if (r == -1)
                    break;
                count += r;
            }

            if (count != 200) {
                fail("FAIL - failed to read 200 bytes from known file");
                break;
            }

            CRC32 cs = new CRC32();

            cs.reset();
            cs.update(b200);
            assertEquals("Incorrect checksum value", 3061553656L, cs.getValue());
            count = 200;
            for (; is.read() != -1; count++) {
            }
            assertEquals("unexpected size of file", 3470, count);
            rs.close();
            // check the stream is closed once another get call is made.
            rs = s.executeQuery("select data, len from t2 where len = "
                    + rssgLength);
            met = rs.getMetaData();
            assertEquals(2, met.getColumnCount());

            while (rs.next()) {
                // JDBC columns use 1-based counting

                // get the first column as a stream
                is = rs.getBinaryStream(1);
                if (is == null) {
                    fail("FAIL - getBinaryStream() return null");
                    break;
                }

                // read the first 200 bytes from the stream and checksum them
                b200 = new byte[200];

                // no guaratees to read all 200 bytes in one read call.
                count = 0;

                while (count < 200) {
                    int r = is.read(b200, count, 200 - count);
                    if (r == -1)
                        break;
                    count += r;
                }

                if (count != 200) {
                    fail("FAIL - failed to read 200 bytes from known file");
                    break;
                }

                cs = new CRC32();

                cs.reset();
                cs.update(b200);
                assertEquals("Incorrect checksum value", 3061553656L, cs
                        .getValue());
                assertEquals(3470, rs.getInt(2));
                try {
                    is.read();
                    fail("FAIL - stream was not closed after a get*() call. "
                                    + is.getClass());
                    break;
                } catch (IOException ioe) {
                    // yes, stream should be closed
                }

            }
            rs.close();

            // check a SQL null object gets a null stream
            rs = s.executeQuery("select data from t2 where len = -1");
            met = rs.getMetaData();
            assertEquals(1,met.getColumnCount());
            
           while (rs.next())
                {
               // JDBC columns use 1-based counting
               
               // get the first column as a stream

               is = rs.getBinaryStream(1);
                   if (is != null) {
                       fail("FAIL - getBinaryStream() did not return null for SQL null");
                       break;
                   }

                }
           
                rs.close();
                File file = new File(filePath + sep + "short.utf");
                int fileSize = (int) file.length();
                rs = s.executeQuery("select len, data from t2 where len = "
                                                                +  fileSize);
                rs.next();
                fileSize = rs.getInt(1);
                BufferedInputStream fileData = new BufferedInputStream(rs.getBinaryStream(2));
                int readCount = 0;
                while(true)
                {
                        int data = fileData.read();
                        if (data == -1) break;
                        readCount++;
                }
                fileData.close();
                assertEquals(56, fileSize);
                assertEquals(56,readCount);
        }
                
                rs.close();
                s.close();
        
    }
    final static String TEST_STRING_DATA = 
        "ABCDEFG" + 
        "\u00c0\u00c1\u00c2\u00c3\u00c4\u00c5" + 
        "\u00ff\u0100" + 
        "\u3042\u3044\u3046\u3048\u304a";
    
    public void testGetAsciiStream() throws SQLException, IOException {
        Connection conn = getConnection();
        PreparedStatement st;
        st = conn.prepareStatement("insert into t3(text_data) values(?)");
        st.setCharacterStream(1,
                              new StringReader(TEST_STRING_DATA),
                              TEST_STRING_DATA.length());
        st.executeUpdate();
        st = conn.prepareStatement("select " + 
                   "text_data as text_data_col1," + 
                   "text_data as text_data_col2 " + 
                   "from " + 
                   "t3");
        ResultSet rs = st.executeQuery();

        while(rs.next()){
            InputStream is = rs.getAsciiStream(1);
            int i = 0;
            for(int c = is.read(); c > -1; c = is.read()){
                int exp = (int) TEST_STRING_DATA.charAt(i++);
                if (exp > 255)
                    exp  = (int) 0x3f;
                assertEquals(exp,c);
    
            }
            Statement s = createStatement();
            s.executeUpdate("delete from t3");
        }

        
    }
    
    /**
     * test getCharacterStream against inserted data
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetCharacterStream() throws SQLException, IOException {
        Connection conn = getConnection();
        PreparedStatement st;
        st = conn.prepareStatement("insert into t3(text_data) values(?)");
        st.setCharacterStream(1,
                              new StringReader(TEST_STRING_DATA),
                              TEST_STRING_DATA.length());
        st.executeUpdate();
        st = conn.prepareStatement("select " + 
                   "text_data as text_data_col1," + 
                   "text_data as text_data_col2 " + 
                   "from " + 
                   "t3");
        ResultSet rs = st.executeQuery();

        while(rs.next()){
            Reader r = rs.getCharacterStream(1);
            int i = 0;
            for(int c = r.read(); c > -1; c = r.read()){
                int exp = (int) TEST_STRING_DATA.charAt(i++);
                assertEquals(exp,c);
            }
        }
        Statement s = createStatement();
        s.executeUpdate("delete from t3");
        
    }
    
    public static Test basesuite(String name) {
        TestSuite suite = new TestSuite(ResultSetStreamTest.class, name);
        Test test = new SupportFilesSetup(suite, new String[] {
                "functionTests/testData/ResultSetStream/littleclob.utf",
                "functionTests/testData/ResultSetStream/short.utf",
                "functionTests/testData/ResultSetStream/resultsetstream.gif" });

        return new CleanDatabaseTestSetup(test) {
            protected void decorateSQL(Statement s) throws SQLException {

                s.execute("create table t2 (len int, data LONG VARCHAR FOR BIT DATA)");
                s.execute("create table t3(text_data clob)");

            }
        };
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("ResultSetStreamTest");
        suite.addTest(basesuite("ResultSetStreamTest:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(basesuite("ResultSetStreamTest:client")));
        return suite;

    }
}
