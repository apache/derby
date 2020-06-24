/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.ResultSetTest
 
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import junit.framework.Test;
import org.apache.derby.iapi.types.HarmonySerialBlob;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestSetup;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Tests of JDBC4 and JDBC4.1 features in ResultSet.
 *
 * Some utility methods have been introduced for the updateXXX test-methods.
 * This test also makes use of a TestSetup wrapper to perform one-time
 * setup and teardown for the whole suite.
 */
public class ResultSetTest  extends Wrapper41Test
{
    private static final byte[] BYTES1 = {
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

    private static final byte[] BYTES2 = {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

    private static final String str1 =
        "I am the main Input string and I will be Updated";
//IC see: https://issues.apache.org/jira/browse/DERBY-2443

    private static final String str2 = "I am the string used to update";

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
        key = requestKey();
        stmt = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
//IC see: https://issues.apache.org/jira/browse/DERBY-1445

        rs = stmt.executeQuery("SELECT * FROM SYS.SYSTABLES");

        // Position on first result.
        rs.next();
    }

    protected void tearDown()
        throws Exception {

//IC see: https://issues.apache.org/jira/browse/DERBY-2707
        rs.close(); 
        stmt.close();

        rs = null;
        stmt = null;

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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

//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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

//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
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
//IC see: https://issues.apache.org/jira/browse/DERBY-1445
//IC see: https://issues.apache.org/jira/browse/DERBY-1445

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
    public void testUpdateClob()
    throws Exception {
        // Life span of Clob objects are limited by the transaction.  Need
        // autocommit off so Clob objects survive execution of next statement.
        getConnection().setAutoCommit(false);

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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
        // Life span of Clob objects are limited by the transaction.  Need
        // autocommit off so Clob objects survive execution of next statement.
        getConnection().setAutoCommit(false);

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
     * Test the Clob method that accepts a Input Stream and its length
     * as input parameter.
     *
     * @throws Exception
     */
    public void testUpdateClobwithLengthofIS()
//IC see: https://issues.apache.org/jira/browse/DERBY-2443
            throws Exception {
        Reader r1 = new java.io.StringReader(str1);
        // InputStream for insertion.
        Reader r2 = new java.io.StringReader(str2);

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");
        ps_sb.setInt(1, key);
        ps_sb.setCharacterStream(2, r1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dClob", key);
        rs1.next();
        rs1.updateClob(1, r2, str2.length());
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dClob", key);
        rs1.next();
        assertEquals(new StringReader(str2),
                     rs1.getCharacterStream(1));
        rs1.close();
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void testUpdateBlob()
    throws Exception {

        // Life span of Blob objects are limited by the transaction.  Need
        // autocommit off so Blob objects survive execution of next statement.
        getConnection().setAutoCommit(false);

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
     * tests the updateBlob that accepts a input stream and the length of the IS.
     *
     * @throws an Exception
     */
    public void testUpdateBlobWithLengthofIS()
//IC see: https://issues.apache.org/jira/browse/DERBY-2443
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
        rs1.updateBlob(1, is2, BYTES2.length);
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dBlob", key);
        rs1.next();
        assertEquals(new ByteArrayInputStream(BYTES2), rs1.getBinaryStream(1));
        rs1.close();
    }

    /**
     * Tests the updateBlob that accepts a input stream and the length of the IS
     * and the parameter name String.
     *
     * @throws an Exception
     */
    public void testUpdateBlobStringParameterNameWithLengthofIS()
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
        rs1.updateBlob("dBlob", is2, BYTES2.length);
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
    public void testUpdateClobStringParameterName()
    throws Exception {
        // Life span of Clob objects are limited by the transaction.  Need
        // autocommit off so Clob objects survive execution of next statement.
        getConnection().setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-2702

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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
     * Tests the updateClob that accepts a input stream and the length of the IS
     * and the parameter name String.
     *
     * @throws an Exception
     */
    public void testUpdateClobStringParameterNameWithLengthofIS()
//IC see: https://issues.apache.org/jira/browse/DERBY-2443
            throws Exception {
        Reader r1 = new java.io.StringReader(str1);
        // InputStream for insertion.
        Reader r2 = new java.io.StringReader(str2);

        // Prepared Statement used to insert the data
        PreparedStatement ps_sb = prep("dClob");
        ps_sb.setInt(1, key);
        ps_sb.setCharacterStream(2, r1);
        ps_sb.executeUpdate();
        ps_sb.close();

        // Update operation
        ResultSet rs1 = fetchUpd("dClob", key);
        rs1.next();
        rs1.updateClob("dClob", r2, str2.length());
        rs1.updateRow();
        rs1.close();

        // Query to see whether the data that has been updated.
        rs1 = fetch("dClob", key);
        rs1.next();
        assertEquals(new StringReader(str2),
                     rs1.getCharacterStream(1));
        rs1.close();
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void testUpdateBlobStringParameterName()
    throws Exception {
        // Life span of Blob objects are limited by the transaction.  Need
        // autocommit off so Blob objects survive execution of next statement.
        getConnection().setAutoCommit(false);
//IC see: https://issues.apache.org/jira/browse/DERBY-2496
//IC see: https://issues.apache.org/jira/browse/DERBY-2496
//IC see: https://issues.apache.org/jira/browse/DERBY-2702
//IC see: https://issues.apache.org/jira/browse/DERBY-2702

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(BYTES1);
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
//IC see: https://issues.apache.org/jira/browse/DERBY-1417

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1417
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

    /**
     * Tests that <code>ResultSet.getHoldability()</code> has the
     * correct behaviour.
     * 
     * @throws SQLException	Thrown if some unexpected error happens
     * @throws Exception	Thrown if some unexpected error happens
     */
    public void testGetHoldability() throws SQLException, Exception {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-2854
        Connection conn = getConnection();
        
        conn.setAutoCommit(false);
        
        // test default holdability
        Statement stmt = createStatement();
        ResultSet rs = stmt.executeQuery("values(1)");
        assertEquals("default holdability is HOLD_CURSORS_OVER_COMMIT", ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
        rs.close();
        try {
            rs.getHoldability();
            fail("getHoldability() should fail when closed");
        } catch (SQLException sqle) {
            assertSQLState("XCL16", sqle);
            // DERBY-4767, sample verification test for operation in XCL16 message.
            assertTrue(sqle.getMessage().indexOf("getHoldability") > 0);
        }
        
        // test explicitly set holdability
        final int[] holdabilities = {
            ResultSet.HOLD_CURSORS_OVER_COMMIT,
            ResultSet.CLOSE_CURSORS_AT_COMMIT,
        };
        for (int h=0; h < holdabilities.length; h++) {
            Statement s =
                    createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, holdabilities[h]);
            rs = s.executeQuery("values(1)");
            assertEquals("holdability " + holdabilityString(holdabilities[h]), holdabilities[h], rs.getHoldability());
            rs.close();
            s.close();
        }
        
        // test holdability of result set returned from a stored
        // procedure (DERBY-1101)
        stmt.execute("create procedure getresultsetwithhold(in hold int) " +
                "parameter style java language java external name " +
                "'org.apache.derbyTesting.functionTests.tests." +
                "jdbc4.ResultSetTest." +
                "getResultSetWithHoldability' " +
                "dynamic result sets 1 reads sql data");
        for (int statementHoldability=0; statementHoldability<holdabilities.length; statementHoldability++) {
            for (int procHoldability=0; procHoldability < holdabilities.length; procHoldability++) {
                CallableStatement cs =
                        prepareCall("call getresultsetwithhold(?)",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY,
                        holdabilities[statementHoldability]);
                cs.setInt(1, holdabilities[procHoldability]);
                cs.execute();
                rs = cs.getResultSet();
//IC see: https://issues.apache.org/jira/browse/DERBY-3305
                assertSame(cs, rs.getStatement());
                int holdability = rs.getHoldability();
                assertEquals("holdability of ResultSet from stored proc: " + holdabilityString(holdability), holdabilities[procHoldability], holdability);
                commit();
                
                try {
                    rs.next();
                    assertEquals("non-holdable result set not closed on commit", ResultSet.HOLD_CURSORS_OVER_COMMIT, holdability);
                } catch (SQLException sqle) {
                    assertSQLState("XCL16",sqle);
                    assertEquals("holdable result set closed on commit", ResultSet.CLOSE_CURSORS_AT_COMMIT, holdability);   
                }
                rs.close();
                cs.close();
            }
        }
        stmt.execute("drop procedure getresultsetwithhold");
        stmt.close();
        commit();
    }
    
    
    /**
     * Tests that <code>ResultSet.isClosed()</code> returns the
     * correct value in different situations.
     *
     * @throws SQLException	Thrown if some unexpected error happens
     */
    public void testIsClosed() throws SQLException{
        
        Statement stmt = createStatement();
        
        // simple open/read/close test
        ResultSet rs = stmt.executeQuery("values(1)");
        assertFalse("rs should be open", rs.isClosed());
        while (rs.next());
        assertFalse("rs should be open", rs.isClosed());
        rs.close();
        assertTrue("rs should be closed", rs.isClosed());
        
        // execute and re-execute statement
        rs = stmt.executeQuery("values(1)");
        assertFalse("rs should be open", rs.isClosed());
        ResultSet rs2 = stmt.executeQuery("values(1)");
        assertTrue("rs should be closed", rs.isClosed());
        assertFalse("rs2 should be open", rs2.isClosed());
        
        // re-execute another statement on the same connection
        Statement stmt2 = createStatement();
        rs = stmt2.executeQuery("values(1)");
        assertFalse("rs2 should be open" ,rs2.isClosed());
        assertFalse("rs should be open", rs.isClosed());
        
        // retrieve multiple result sets
        stmt.execute("create procedure retrieve_result_sets() " +
                "parameter style java language java external name " +
                "'org.apache.derbyTesting.functionTests.tests." +
                "jdbc4.ResultSetTest.threeResultSets' " +
                "dynamic result sets 3 reads sql data");
        stmt.execute("call retrieve_result_sets()");
        ResultSet[] rss = new ResultSet[3];
        int count = 0;
        do {
            rss[count] = stmt.getResultSet();
            assertFalse("rss[" + count + "] should be open", rss[count].isClosed());
            
            if (count > 0) {
                assertTrue("rss[" + (count-1) + "] should be closed", rss[count-1].isClosed());
            }
            ++count;
        } while (stmt.getMoreResults());
        assertEquals("expected three result sets", 3, count);
        stmt.execute("drop procedure retrieve_result_sets");
        
        // close statement
        rs = stmt2.executeQuery("values(1)");
        stmt2.close();
        assertTrue("rs should be closed", rs.isClosed());
        
        // close connection
        Connection conn2 = openDefaultConnection();
        stmt2 = conn2.createStatement();
        rs = stmt2.executeQuery("values(1)");
        conn2.close();
        assertTrue("rs should be closed", rs.isClosed());
        
        stmt.close();
        stmt2.close();
    }

    /**
     * Test that a {@code ResultSet} is marked as closed after commit if its
     * holdability is {@code CLOSE_CURSORS_AT_COMMIT} (DERBY-3404).
     */
    public void testIsClosedOnNonHoldableResultSet() throws SQLException {
        getConnection().setAutoCommit(false);
        getConnection().setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        ResultSet rs = createStatement().executeQuery(
            "SELECT TABLENAME FROM SYS.SYSTABLES");
        assertEquals("ResultSet shouldn't be holdable",
                     ResultSet.CLOSE_CURSORS_AT_COMMIT, rs.getHoldability());
        commit();
        assertTrue("Commit should have closed the ResultSet", rs.isClosed());
    }

    /**
     * Test that an exception is thrown when methods are called
     * on a closed result set (DERBY-1060).
     *
     * @throws SQLException	Thrown if some unexpected error happens
     */
    public void testExceptionWhenClosed() 
	throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, SQLException {
        
            // create a result set and close it
            Statement stmt = createStatement();
            ResultSet rs = stmt.executeQuery("values(1)");
            rs.close();

            // maps method name to parameter list
            HashMap<String, Class[]> params = new HashMap<String, Class[]>();
            // maps method name to argument list
            HashMap<String, Object[]> args = new HashMap<String, Object[]>();

            // methods with no parameters
            String[] zeroArgMethods = {
                "getWarnings", "clearWarnings", "getStatement",
                "getMetaData", "getConcurrency", "getHoldability",
                "getRow", "getType", "rowDeleted", "rowInserted",
                "rowUpdated", "getFetchDirection", "getFetchSize",
            };
            for (String name : zeroArgMethods) {
                params.put(name, null);
                args.put(name, null);
            }

            // methods with a single int parameter
            for (String name : new String[] { "setFetchDirection",
                                              "setFetchSize" }) {
                params.put(name, new Class[] { Integer.TYPE });
                args.put(name, new Integer[] { 0 });
            }

            // invoke the methods
            for (String name : params.keySet()) {
                    Method method =
                        rs.getClass().getMethod(name, params.get(name));
                    try {
                        method.invoke(rs, args.get(name));
			fail("Unexpected Failure: method.invoke(rs, " + 
					args.get(name) + ") should have failed.");
                    } catch (InvocationTargetException ite) {
                        Throwable cause = ite.getCause();
                        if (cause instanceof SQLException) {
                            SQLException sqle = (SQLException) cause;
                            String state = sqle.getSQLState();
                            // Should get SQL state XCL16 when the
                            // result set is closed
                            assertSQLState("XCL16", sqle);
                            continue;
                        }
                        throw ite;
                    }
                    fail("no exception thrown for " + name +
                                       "() when ResultSet is closed");
            }
            stmt.close();
        
    }
    /**
     * Tests the wrapper methods isWrapperFor and unwrap. There are two cases
     * to be tested
     * Case 1: isWrapperFor returns true and we call unwrap
     * Case 2: isWrapperFor returns false and we call unwrap
     *
     *
     * @throws SQLException	Thrown if some unexpected error happens
     */
    public void testWrapper() throws SQLException {
        PreparedStatement ps = prepareStatement("select count(*) from sys.systables");
        ResultSet rs = ps.executeQuery();
        Class<ResultSet> wrap_class = ResultSet.class;
        
        //The if succeeds and we call the unwrap method on the conn object        
        if(rs.isWrapperFor(wrap_class)) {
        	ResultSet rs1 = 
                	(ResultSet)rs.unwrap(wrap_class);
        }
        else {
        	assertFalse("isWrapperFor wrongly returns false", rs.isWrapperFor(wrap_class));
        } 
        //Being Test for Case2
        //test for the case when isWrapper returns false
        //using some class that will return false when 
        //passed to isWrapperFor
        Class<PreparedStatement> wrap_class1 = PreparedStatement.class;
        
        try {
            //returning false is the correct behaviour in this case
            //Generate a message if it returns true
            if(rs.isWrapperFor(wrap_class1)) {
                assertTrue("isWrapperFor wrongly returns true", rs.isWrapperFor(wrap_class1));
            }
            else {
                PreparedStatement ps1 = (PreparedStatement)
                                           rs.unwrap(wrap_class1);
                fail("unwrap does not throw the expected exception"); 
            }
        }
        catch (SQLException sqle) {
            //Calling unwrap in this case throws an 
            //SQLException ensure that this SQLException 
            //has the correct SQLState
            assertSQLState(SQLStateConstants.UNABLE_TO_UNWRAP, sqle);
        }
    }
    
    /************************************************************************
     **                        T E S T  S E T U P                           *
     ************************************************************************/

    public static Test suite() {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite rsSuite = new BaseTestSuite("ResultSetTest suite");
//IC see: https://issues.apache.org/jira/browse/DERBY-2443
        rsSuite.addTest(decorateTestSuite(TestConfiguration.defaultSuite
            (ResultSetTest.class,false)));
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
    
    /**
     * Convert holdability from an integer to a readable string.
     *
     * @param holdability an <code>int</code> value representing a holdability
     * @return a <code>String</code> value representing the same holdability
     *
     */
    private static String holdabilityString(int holdability) {
//IC see: https://issues.apache.org/jira/browse/DERBY-2854
        switch (holdability) {
        case ResultSet.HOLD_CURSORS_OVER_COMMIT:
            return "HOLD_CURSORS_OVER_COMMIT";
        case ResultSet.CLOSE_CURSORS_AT_COMMIT:
            return "CLOSE_CURSORS_AT_COMMIT";
        default:
            return "UNKNOWN HOLDABILITY";
        }
    }
    /**
     * Method that is invoked by <code>testIsClosed()</code> (as a
     * stored procedure) to retrieve three result sets.
     *
     * @param rs1 first result set
     * @param rs2 second result set
     * @param rs3 third result set
     * @exception SQLException if a database error occurs
     */
    public static void threeResultSets(ResultSet[] rs1,
                                       ResultSet[] rs2,
                                       ResultSet[] rs3)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement stmt1 = c.createStatement();
        rs1[0] = stmt1.executeQuery("values(1)");
        Statement stmt2 = c.createStatement();
        rs2[0] = stmt2.executeQuery("values(1)");
        Statement stmt3 = c.createStatement();
        rs3[0] = stmt3.executeQuery("values(1)");
        c.close();
    }
    /**
     * Method invoked by <code>testGetHoldability()</code> (as a stored
     * procedure) to retrieve a result set with a given holdability.
     *
     * @param holdability requested holdability
     * @param rs result set returned from stored procedure
     * @exception SQLException if a database error occurs
     */
    public static void getResultSetWithHoldability(int holdability,
                                                   ResultSet[] rs)
        throws SQLException
    {
        Connection c = DriverManager.getConnection("jdbc:default:connection");
        Statement s = c.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                        ResultSet.CONCUR_READ_ONLY,
                                        holdability);
        rs[0] = s.executeQuery("values (1), (2), (3)");
        c.close();
    }
    
    /**
     * EOFException when reading from blob's binary stream
     * and calling length() twice
     * 
     * Test with and without lengthless insert.
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testDerby1368() throws SQLException, IOException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1368
        testDerby1368(true);
        testDerby1368(false);
    }
    
    /**
     * EOFException when reading from blob's binary stream
     * and calling length() twice
     * 
     * @param lengthless Insert data with lengthless method.
     * @throws SQLException
     * @throws IOException 
     */
    public void testDerby1368 (boolean lengthless) throws SQLException, IOException 
    {
        Statement stmt = createStatement();
        stmt.execute("create table T1368 (ID char(32) PRIMARY KEY, DATA blob(2G) not null)");

        // add row  
        int length = 1024 * 1024;
        byte[] data = new byte[length]; 
        data[0] = 1; 
        data[1] = 2; 
        ByteArrayInputStream bais = new ByteArrayInputStream(data);

        PreparedStatement ps = prepareStatement("insert into T1368 (ID, DATA) values (?, ?)"); 
        
        ps.setString(1, "id"); 
        if (lengthless)
            ps.setBinaryStream(2, bais);
        else
            ps.setBinaryStream(2, bais,length);
        ps.execute(); 
        ps.close(); 

        // read row 
         
        ps = prepareStatement("select DATA from T1368 where ID = ?"); 
        ps.setString(1, "id"); 
        ResultSet rs = ps.executeQuery();          
        rs.next(); 
        Blob b = rs.getBlob(1); 

        
        // test output  
        assertEquals(length,b.length());
        InputStream in = b.getBinaryStream();
        assertEquals(1, in.read());
        //drain the stream
        while (in.read() != -1 );
        in.close(); 

        in = b.getBinaryStream(); 
        assertEquals(length,b.length());
        assertEquals(1, in.read());
 
        in.close(); 

        rs.close(); 
        stmt.executeUpdate("DROP TABLE T1368");
    }

    /**
     * Test the JDBC 4.1 extensions.
     */
    public  void    testJDBC4_1() throws Exception
    {
        Connection  conn = getConnection();
        PreparedStatement   ps = prepareStatement
            (
             conn,
//IC see: https://issues.apache.org/jira/browse/DERBY-4951
             "create function makeBlob( ) returns blob\n" +
             "language java parameter style java no sql deterministic\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.jdbc4.ResultSetTest.makeBlob'\n"
             );
        ps.execute();
        ps.close();

        vetDataTypeCount( conn );

        ps = prepareStatement
            (
             conn,
             "create table allTypes\n" +
             "(\n" +
             "    bigintCol bigint,\n" +
             "    blobCol blob,\n" +
             "    booleanCol boolean,\n" +
             "    charCol char(1),\n" +
             "    charForBitDataCol char(1) for bit data,\n" +
             "    clobCol clob,\n" +
             "    dateCol date,\n" +
             "    doubleCol double,\n" +
             "    floatCol float,\n" +
             "    intCol int,\n" +
             "    longVarcharCol long varchar,\n" +
             "    longVarcharForBitDataCol long varchar for bit data,\n" +
             "    numericCol numeric,\n" +
             "    realCol real,\n" +
             "    smallintCol smallint,\n" +
             "    timeCol time,\n" +
             "    timestampCol timestamp,\n" +
             "    varcharCol varchar( 2 ),\n" +
             "    varcharForBitDataCol varchar( 2 ) for bit data\n" +
             ")\n"
             );
        ps.execute();
        ps.close();

        ps = prepareStatement
            (
             conn,
             "insert into allTypes\n" +
             "(\n" +
             "    bigintCol,\n" +
             "    blobCol,\n" +
             "    booleanCol,\n" +
             "    charCol,\n" +
             "    charForBitDataCol,\n" +
             "    clobCol,\n" +
             "    dateCol,\n" +
             "    doubleCol,\n" +
             "    floatCol,\n" +
             "    intCol,\n" +
             "    longVarcharCol,\n" +
             "    longVarcharForBitDataCol,\n" +
             "    numericCol,\n" +
             "    realCol,\n" +
             "    smallintCol,\n" +
             "    timeCol,\n" +
             "    timestampCol,\n" +
             "    varcharCol,\n" +
             "    varcharForBitDataCol\n" +
             ")\n" +
             "values\n" +
             "(\n" +
             "    1,\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4951
             "    makeBlob(  ),\n" +
             "    true,\n" +
             "    'a',\n" +
             "    X'DE',\n" +
             "    'abc',\n" +
             "    date('1994-02-23'),\n" +
             "    1.0,\n" +
             "    1.0,\n" +
             "    1,\n" +
             "    'a',\n" +
             "    X'DE',\n" +
             "    1.0,\n" +
             "    1.0,\n" +
             "    1,\n" +
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
             "    ?,\n" +
             "    ?,\n" +
             "    'a',\n" +
             "    X'DE'\n" +
             ")\n"
             );
        ps.setTime(1, new Time(TIME_VALUE));
        ps.setTimestamp(2, new Timestamp(TIMESTAMP_VALUE));
        ps.executeUpdate();
        ps.close();

//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        ps = prepareStatement
            (
             conn,
             "insert into allTypes values " +
             "( null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null )"
             );
        ps.executeUpdate();
        ps.close();

        ps = prepareStatement( conn, "select * from allTypes order by bigintCol" );
        ResultSet   rs = ps.executeQuery();
        rs.next();
        examineJDBC4_1extensions( new Wrapper41( rs ), false );
        rs.next();
        examineJDBC4_1extensions( new Wrapper41( rs ), true );
        rs.close();
        ps.close();

        ps = prepareStatement( conn, "drop table allTypes" );
        ps.execute();
        ps.close();
        ps = prepareStatement( conn, "drop function makeBlob" );
        ps.execute();
        ps.close();
    }
    private void    vetDataTypeCount( Connection conn ) throws Exception
    {
        ResultSet rs = conn.getMetaData().getTypeInfo();
        int actualTypeCount = 0;
        while ( rs.next() ) { actualTypeCount++; }
        rs.close();

        //
        // If this assertion fails, that means that another data type has been added
        // to Derby. You need to add that datatype to the allTypes table created
        // by testJDBC4_1() and you need to add a verification case to examineJDBC4_1extensions().
        //
        assertEquals( 22, actualTypeCount );
    }

    /**
     * <p>
     * Function for making a Blob.
     * </p>
     */
    public  static  final   Blob    makeBlob()  throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4951
        return new HarmonySerialBlob( BINARY_VALUE );
    }
    
}

