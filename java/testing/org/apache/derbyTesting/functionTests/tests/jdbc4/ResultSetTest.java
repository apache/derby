/*
 
   Derby - Class ResultSetTest
 
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

import javax.xml.transform.Result;
import junit.extensions.TestSetup;
import junit.framework.*;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

import java.io.*;
import java.sql.*;

/**
 * Tests of JDBC4 features in ResultSet.
 *
 * Some utility methods have been introduced for the updateXXX test-methods.
 * This test also makes use of a TestSetup wrapper to perform one-time
 * setup and teardown for the whole suite.
 */
public class ResultSetTest
    extends BaseJDBCTestCase {

    private static final byte[] BYTES1 = {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

    private static final byte[] BYTES2 = {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

    /** 
     * Key used to identify inserted rows.
     * Use method <code>requestKey</code> to obtain it. 
     **/
    private static int insertKey = 0;

    /** Statement used to obtain default resultset. */
    private Statement stmt = null;
    /** Default resultset used by the tests. */
    private ResultSet rs = null;
    /** Default row identifier used by the tests. */
    private int key = -1;

    /**
     * Create test with given name.
     *
     * @param name name of the test.
     */
    public ResultSetTest(String name) {
        super(name);
    }

    protected void setUp()
        throws SQLException {
        key = requestKey();
        stmt = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

        rs = stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");

        // Position on first result.
        rs.next();
    }

    protected void tearDown()
        throws Exception {

        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }

        super.tearDown();
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

    public void testUpdateNCharacterStreamIntLengthLessNotImplemented()
        throws SQLException {
        try {
            rs.updateNCharacterStream(1, null);
            fail("ResultSet.updateNCharacterStream(int, Reader) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    public void testUpdateNCharacterStreamStringNotImplemented()
        throws SQLException {
        try {
            rs.updateNCharacterStream("some-column-name", null, 0);
            fail("ResultSet.updateNCharacterStream(String, Reader, 0) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNCharacterStreamStringLengthlessNotImplemented()
        throws SQLException {
        try {
            rs.updateNCharacterStream("some-column-name", null);
            fail("ResultSet.updateNCharacterStream(String, Reader) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNClobIntNotImplemented()
        throws SQLException {
        try {
            rs.updateNClob(1, (NClob)null);
            fail("ResultSet.updateNClob(int, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNClobIntLengthlessNotImplemented()
        throws SQLException {
        try {
            rs.updateNClob(1, (Reader)null);
            fail("ResultSet.updateNClob(int, Reader) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }

    public void testUpdateNClobStringNotImplemented()
        throws SQLException {
        try {
            rs.updateNClob("some-column-name", (NClob)null);
            fail("ResultSet.updateNClob(String, NClob) " +
                 "should not be implemented");
        } catch (SQLFeatureNotSupportedException sfnse) {
            // We are fine, do nothing.
        }
    }
    
    public void testUpdateNClobStringLengthlessNotImplemented()
        throws SQLException {
        try {
            rs.updateNClob("some-column-name", (Reader)null);
            fail("ResultSet.updateNClob(String, Reader) " +
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
    
    /**
     * This methods tests the ResultSet interface method
     * updateBinaryStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateBinaryStream()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongBit");
        ps_sb.setInt(1,key);
        ps_sb.setBinaryStream(2,is,BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = fetchUpd("dLongBit", key);
        rs1.next();
        rs1.updateBinaryStream(1,is_for_update,(int)BYTES2.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = fetch("dLongBit", key);
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(1);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<BYTES2.length;i++) {
            assertEquals("Error in updateBinaryStream",BYTES2[i],bytes_ret[i]);
        }
        rs1.close();
    }

    /**
     * This methods tests the ResultSet interface method
     * updateAsciiStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateAsciiStream()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR)");

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is,BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateAsciiStream(2,is_for_update,(int)BYTES2.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        InputStream is_ret = rs1.getAsciiStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<BYTES2.length;i++) {
            assertEquals("Error in updateAsciiStream",BYTES2[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

     /**
     * This methods tests the ResultSet interface method
     * updateCharacterStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateCharacterStream()
    throws Exception {
        String str = "Test data";
        String str_for_update = "Test data used for update";

        StringReader r = new StringReader(str);
        StringReader r_for_update = new StringReader
                (str_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongVarchar"); 
        ps_sb.setInt(1,key);
        ps_sb.setCharacterStream(2,r,str.length());
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        ResultSet rs1 = fetchUpd("dLongVarchar", key);
        rs1.next();
        rs1.updateCharacterStream(1,r_for_update,str_for_update.length());
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = fetch("dLongVarchar", key); 
        rs1.next();

        StringReader r_ret = (StringReader)rs1.getCharacterStream(1);

        char [] c_ret = new char[str_for_update.length()];

        r_ret.read(c_ret);

        String str_ret = new String(c_ret);

        assertEquals("Error in updateCharacterStream" +
            str_ret,str_for_update,str_ret);


        rs1.close();
    }

    /**
     * This methods tests the ResultSet interface method
     * updateBinaryStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateBinaryStreamStringParameterName()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongBit");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2,is,BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = fetchUpd("dLongBit", key);
        rs1.next();
        rs1.updateBinaryStream("dLongBit",is_for_update,(int)BYTES2.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = fetch("dLongBit", key);
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(1);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<BYTES2.length;i++) {
            assertEquals("Error in updateBinaryStream",BYTES2[i],bytes_ret[i]);
        }
        rs1.close();
    }

    /**
     * Test <code>updateBinaryStream</code> on a BINARY column, without
     * specifying length of inputstream.
     */
    public void testUpdateBinaryStreamLengthless()
            throws IOException, SQLException {
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
        // InputStream used for update.
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongBit");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2, is1);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        ResultSet rs1 = fetchUpd("dLongBit", key);
        rs1.next();
        rs1.updateBinaryStream(1, is2);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = fetch("dLongBit", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    /**
     * Test <code>updateBinaryStream</code> on a BLOB column, without
     * specifying length of inputstream.
     */
    public void testUpdateBinaryStreamLengthlessBlob()
            throws IOException, SQLException {
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
        // InputStream used for update.
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dBlob");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2, is1);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        ResultSet rs1 = fetchUpd("dBlob", key);
        rs1.next();
        rs1.updateBinaryStream(1, is2);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = fetch("dBlob", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    public void testUpdateBinaryStreamLengthlessParameterName()
            throws IOException, SQLException {
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
        // InputStream used for update.
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongBit");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2, is1);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        ResultSet rs1 = fetchUpd("dLongBit", key);
        rs1.next();
        rs1.updateBinaryStream("dLongBit", is2);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = fetch("dLongBit", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    /**
     * This methods tests the ResultSet interface method
     * updateAsciiStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateAsciiStreamStringParameterName()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongVarchar");
        ps_sb.setInt(1, key);
        ps_sb.setAsciiStream(2,is,BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = fetchUpd("dLongVarchar", key);
        rs1.next();
        rs1.updateAsciiStream("dLongVarchar",is_for_update,(int)BYTES2.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = fetch("dLongVarchar", key);
        rs1.next();
        InputStream is_ret = rs1.getAsciiStream(1);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<BYTES2.length;i++) {
            assertEquals("Error in updateAsciiStream",BYTES2[i],bytes_ret[i]);
        }
        rs1.close();
    }

    public void testUpdateAsciiStreamLengthless()
            throws IOException, SQLException {
        // Array to keep updated data fetched from the database.
        byte[] bytesRet = new byte[10];

        // Input Stream inserted initially.
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        // InputStream that is used for update.
        InputStream isForUpdate = new
                java.io.ByteArrayInputStream(BYTES2);

        // Prepared Statement used to insert the data.
        PreparedStatement ps_sb = prep("dLongVarchar");
        ps_sb.setInt(1, key);
        ps_sb.setAsciiStream(2, is, BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update the data.
        ResultSet rs1 = fetchUpd("dLongVarchar", key);
        rs1.next();
        rs1.updateAsciiStream(1, isForUpdate);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dLongVarchar", key);
        rs1.next();
        InputStream isRet = rs1.getAsciiStream(1);
        isRet.read(bytesRet);
        isRet.close();

        for (int i=0; i < BYTES2.length; i++) {
            assertEquals("Error in updateAsciiStream", BYTES2[i], bytesRet[i]);
        }
        rs1.close();
    }

    public void testUpdateAsciiStreamLengthlessParameterName()
            throws IOException, SQLException {
        // Array to keep updated data fetched from the database.
        byte[] bytesRet = new byte[10];

        // Input Stream inserted initially.
        InputStream is = new java.io.ByteArrayInputStream(BYTES1);

        // InputStream that is used for update.
        InputStream isForUpdate = new
                java.io.ByteArrayInputStream(BYTES2);

        // Prepared Statement used to insert the data.
        PreparedStatement ps_sb = prep("dLongVarchar");
        ps_sb.setInt(1, key);
        ps_sb.setAsciiStream(2, is, BYTES1.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update the data.
        ResultSet rs1 = fetchUpd("dLongVarchar", key);
        rs1.next();
        rs1.updateAsciiStream("dLongVarchar", isForUpdate);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dLongVarchar", key);
        rs1.next();
        InputStream isRet = rs1.getAsciiStream(1);
        isRet.read(bytesRet);
        isRet.close();

        for (int i=0; i < BYTES2.length; i++) {
            assertEquals("Error in updateAsciiStream", BYTES2[i], bytesRet[i]);
        }
        rs1.close();
    }

     /**
     * This methods tests the ResultSet interface method
     * updateCharacterStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateCharacterStreamStringParameterName()
    throws Exception {
        String str = "Test data";
        String str_for_update = "Test data used for update";

        StringReader r = new StringReader(str);
        StringReader r_for_update = new StringReader
                (str_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dLongVarchar");
        ps_sb.setInt(1, key);
        ps_sb.setCharacterStream(2,r,str.length());
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        ResultSet rs1 = fetchUpd("dLongVarchar", key);
        rs1.next();
        rs1.updateCharacterStream("dLongVarchar", 
                                  r_for_update,
                                  str_for_update.length());
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = fetch("dLongVarchar", key);
        rs1.next();

        StringReader r_ret = (StringReader)rs1.getCharacterStream(1);

        char [] c_ret = new char[str_for_update.length()];

        r_ret.read(c_ret);

        String str_ret = new String(c_ret);

        assertEquals("Error in updateCharacterStream" + str_ret,str_for_update,
            str_ret);

        rs1.close();
    }

    public void testUpdateCharacterStreamLengthless()
            throws IOException, SQLException {
        String str = "This is the (\u0FFF\u1234) test string";
        String strUpdated = "An updated (\u0FEF\u9876) test string";

        // Insert test data
        PreparedStatement psChar = prep("dLongVarchar");
        psChar.setInt(1, key);
        psChar.setCharacterStream(2, new StringReader(str));
        psChar.execute();
        psChar.close();

        // Update test data
        ResultSet rs = fetchUpd("dLongVarchar", key);
        rs.next();
        rs.updateCharacterStream(1, new StringReader(strUpdated));
        rs.updateRow();
        rs.close();

        // Verify that update took place and is correct.
        rs = fetch("dLongVarchar", key);
        rs.next();
        Reader updatedStr = rs.getCharacterStream(1);
        for (int i=0; i < strUpdated.length(); i++) {
            assertEquals("Strings differ at index " + i,
                    strUpdated.charAt(i),
                    updatedStr.read());
        }
        assertEquals("Too much data in stream", -1, updatedStr.read());
        updatedStr.close();
    }

    public void testUpdateCharacterStreamLengthlessParameterName()
            throws IOException, SQLException {
        String str = "This is the (\u0FFF\u1234) test string";
        String strUpdated = "An updated (\u0FEF\u9876) test string";

        // Insert test data
        PreparedStatement psChar = prep("dLongVarchar");
        psChar.setInt(1, key);
        psChar.setCharacterStream(2, new StringReader(str));
        psChar.execute();
        psChar.close();

        // Update test data
        ResultSet rs = fetchUpd("dLongVarchar", key);
        rs.next();
        rs.updateCharacterStream("dLongVarchar", new StringReader(strUpdated));
        rs.updateRow();
        rs.close();

        // Verify that update took place and is correct.
        rs = fetch("dLongVarchar", key);
        rs.next();
        Reader updatedStr = rs.getCharacterStream(1);
        for (int i=0; i < strUpdated.length(); i++) {
            assertEquals("Strings differ at index " + i,
                    strUpdated.charAt(i),
                    updatedStr.read());
        }
        assertEquals("Too much data in stream", -1, updatedStr.read());
        updatedStr.close();
    }

    /**
     * This methods tests the ResultSet interface method
     * updateClob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateClob()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");

        //first insert
        ps_sb.setInt(1,key);
        ps_sb.setAsciiStream(2,is1,BYTES1.length);
        ps_sb.executeUpdate();

        //second insert
        int key2 = requestKey();
        ps_sb.setInt(1,key2);
        ps_sb.setAsciiStream(2,is2,BYTES2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = fetchUpd("dClob", key);
        rs1.next();
        Clob clob = rs1.getClob(1);
        rs1.close();

        rs1 = fetchUpd("dClob", key2);
        rs1.next();
        rs1.updateClob(1,clob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateClob method is the same
        //data that we expected

        rs1 = fetch("dClob", key2);
        rs1.next();
        assertEquals(clob, rs1.getClob(1));
        rs1.close();
    }

    public void testUpdateClobLengthless()
            throws Exception {
        Reader r1 = new java.io.StringReader(new String(BYTES1));
        // InputStream for insertion.
        Reader r2 = new java.io.StringReader(new String(BYTES2));

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");
        ps_sb.setInt(1, key);
        ps_sb.setCharacterStream(2, r1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dClob", key);
        rs1.next();
        rs1.updateClob(1, r2);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dClob", key);
        rs1.next();
        assertEquals(new StringReader(new String(BYTES2)),
                     rs1.getCharacterStream(1));
        rs1.close();
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateBlob()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dBlob");

        //first insert
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2,is1,BYTES1.length);
        ps_sb.executeUpdate();

        //second insert
        int key2 = requestKey();
        ps_sb.setInt(1, key2);
        ps_sb.setBinaryStream(2,is2,BYTES2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = fetch("dBlob", key);
        rs1.next();
        Blob blob = rs1.getBlob(1);
        rs1.close();

        rs1 = fetchUpd("dBlob", key2);
        rs1.next();
        rs1.updateBlob(1,blob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBlob method is the same
        //data that we expected

        rs1 = fetch("dBlob", key2);
        rs1.next();
        assertEquals(blob, rs1.getBlob(1));
        rs1.close();
    }

    public void testUpdateBlobLengthless()
            throws Exception {
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
        // InputStream for insertion.
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dBlob");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2, is1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dBlob", key);
        rs1.next();
        rs1.updateBlob(1, is2);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dBlob", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    /**
     * This methods tests the ResultSet interface method
     * updateClob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateClobStringParameterName()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");

        //first insert
        ps_sb.setInt(1, key);
        ps_sb.setAsciiStream(2,is1,BYTES1.length);
        ps_sb.executeUpdate();

        //second insert
        int key2 = requestKey();
        ps_sb.setInt(1, key2);
        ps_sb.setAsciiStream(2,is2,BYTES2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = fetch("dClob", key);
        rs1.next();
        Clob clob = rs1.getClob(1);
        rs1.close();

        rs1 = fetchUpd("dClob", key2);
        rs1.next();
        rs1.updateClob("dClob",clob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateClob method is the same
        //data that we expected

        rs1 = fetch("dClob", key2);
        rs1.next();
        assertEquals(clob, rs1.getClob(1));
        rs1.close();
    }

    public void testUpdateClobLengthlessParameterName()
            throws Exception {
        Reader r1 = new java.io.StringReader(new String(BYTES1));
        // InputStream for insertion.
        Reader r2 = new java.io.StringReader(new String(BYTES2));

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");
        ps_sb.setInt(1, key);
        ps_sb.setCharacterStream(2, r1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dClob", key);
        rs1.next();
        rs1.updateClob("dClob", r2);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dClob", key);
        rs1.next();
        assertEquals(new StringReader(new String(BYTES2)),
                     rs1.getCharacterStream(1));
        rs1.close();
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateBlobStringParameterName()
    throws Exception {
        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dBlob");

        //first insert
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2,is1,BYTES1.length);
        ps_sb.executeUpdate();

        //second insert
        int key2 = requestKey();
        ps_sb.setInt(1, key2);
        ps_sb.setBinaryStream(2,is2,BYTES2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = fetch("dBlob", key);
        rs1.next();
        Blob blob = rs1.getBlob(1);
        rs1.close();

        rs1 = fetchUpd("dBlob", key2);
        rs1.next();
        rs1.updateBlob("dBlob",blob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBlob method is the same
        //data that we expected

        rs1 = fetch("dBlob", key2);
        rs1.next();
        assertEquals(blob, rs1.getBlob(1)); 
        rs1.close();
    }

    public void testUpdateBlobWithStreamLengthlessParameterName()
            throws Exception {
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
        // InputStream for insertion.
        InputStream is2 = new java.io.ByteArrayInputStream(BYTES2);

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dBlob");
        ps_sb.setInt(1, key);
        ps_sb.setBinaryStream(2, is1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dBlob", key);
        rs1.next();
        rs1.updateBlob("dBlob", is2);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dBlob", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    /************************************************************************
     **                        T E S T  S E T U P                           *
     ************************************************************************/

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
        embeddedSuite.addTest(new ResultSetTest(
                    "embeddedUpdateBlob"));
        embeddedSuite.addTest(new ResultSetTest(
                    "embeddedUpdateClob"));
        embeddedSuite.addTest(new ResultSetTest(
                    "embeddedUpdateClobStringParameterName"));
        return embeddedSuite;
    }

    public static Test suite() {
        TestSuite rsSuite = new TestSuite("ResultSetTest suite");

        TestSuite embedded = new TestSuite("ResultSetTest:embedded");
        embedded.addTestSuite(ResultSetTest.class);
        embedded.addTest(embeddedSuite("ResultSetTest:embedded-only"));
        rsSuite.addTest(decorateTestSuite(embedded));

        TestSuite client = new TestSuite("ResultSetTest:client");
        client.addTestSuite(ResultSetTest.class);
        client.addTest(clientSuite("ResultSetTest:client-only"));
        rsSuite.addTest(TestConfiguration.clientServerDecorator(
            decorateTestSuite(client)));

        return rsSuite;
    }

    private static Test decorateTestSuite(Test rsSuite) {
        // Wrap suite in a TestSetup-class.
        return new BaseJDBCTestSetup(rsSuite) {
                protected void setUp()
                        throws SQLException {
                    Connection con = getConnection();
                    Statement stmt = con.createStatement();
                    stmt.execute("create table UpdateTestTableResultSet (" +
                            "sno int not null unique," +
                            "dBlob BLOB," +
                            "dClob CLOB," +
                            "dLongVarchar LONG VARCHAR," +
                            "dLongBit LONG VARCHAR FOR BIT DATA)");
                    stmt.close();
               }

                protected void tearDown()
                        throws Exception {
                    Connection con = getConnection();
                    Statement stmt = con.createStatement();
                    stmt.execute("drop table UpdateTestTableResultSet");
                    stmt.close();
                    super.tearDown();
                }
            };
    }

    /*************************************************************************
     **                    U T I L I T Y  M E T H O D S                      *
     *************************************************************************/

    /**
     * Get a key that is used to identify an inserted row.
     * Introduced to avoid having to delete table contents after each test,
     * and because the order of the tests is not guaranteed.
     *
     * @return an integer in range [1, Integer.MAX_VALUE -1]
     */
    private static final int requestKey() {
        return ++insertKey;
    }

    /**
     * Prepare commonly used statement to insert a row.
     *
     * @param con connection to database
     * @param colName name of the column to insert into
     */
    private PreparedStatement prep(String colName)
            throws SQLException {
        return prepareStatement("insert into UpdateTestTableResultSet " +
                "(sno, " + colName + ") values (?,?)");
    }

    /**
     * Fetch the specified row for update.
     *
     * @param con connection to database
     * @param colName name of the column to fetch
     * @param key identifier for row to fetch
     * @return a <code>ResultSet</code> with zero or one row, depending on
     *      the key used
     */
    private ResultSet fetchUpd(String colName, int key)
            throws SQLException {
        Statement stmt = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                             ResultSet.CONCUR_UPDATABLE);
        return stmt.executeQuery("select " + colName +
                " from UpdateTestTableResultSet where sno = " + key +
                " for update");
    }

    /**
     * Fetch the specified row.
     *
     * @param con connection to database
     * @param colName name of the column to fetch
     * @param key identifier for row to fetch
     * @return a <code>ResultSet</code> with zero or one row, depending on
     *      the key used
     */
    private ResultSet fetch(String colName, int key)
            throws SQLException {
        Statement stmt = createStatement();
        return stmt.executeQuery("select " + colName +
                " from UpdateTestTableResultSet where sno = " + key);
    }
}
