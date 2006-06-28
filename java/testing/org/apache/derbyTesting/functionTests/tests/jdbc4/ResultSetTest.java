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

import javax.xml.transform.Result;
import junit.framework.*;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;

import java.io.*;
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
        stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);

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
            rs.updateNClob(1, (NClob)null);
            fail("ResultSet.updateNClob(int, NClob) " +
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
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR FOR BIT DATA)");
        //Initial set of bytes used to create the Binary Stream that has to
        //be inserted
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the BinaryStream that will be used in
        //the updateAsciiStream method
        byte[] bytes_for_update = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(bytes);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(bytes_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is,bytes.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateBinaryStream(2,is_for_update,(int)bytes_for_update.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes_for_update.length;i++) {
            assertEquals("Error in updateBinaryStream",bytes_for_update[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
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
        //Initial set of bytes used to create the Ascii Stream that has to
        //be inserted
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the AsciiStream that will be used in
        //the updateAsciiStream method
        byte[] bytes_for_update = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(bytes);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(bytes_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is,bytes.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateAsciiStream(2,is_for_update,(int)bytes_for_update.length);
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

        for(int i=0;i<bytes_for_update.length;i++) {
            assertEquals("Error in updateAsciiStream",bytes_for_update[i],bytes_ret[i]);
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
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR)");

        String str = "Test data";
        String str_for_update = "Test data used for update";

        StringReader r = new StringReader(str);
        StringReader r_for_update = new StringReader
                (str_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setCharacterStream(2,r,str.length());
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateCharacterStream(2,r_for_update,str_for_update.length());
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();

        StringReader r_ret = (StringReader)rs1.getCharacterStream(2);

        char [] c_ret = new char[str_for_update.length()];

        r_ret.read(c_ret);

        String str_ret = new String(c_ret);

        assertEquals("Error in updateCharacterStream" +
            str_ret,str_for_update,str_ret);


        rs1.close();

        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

    /**
     * This methods tests the ResultSet interface method
     * updateBinaryStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateBinaryStreamStringParameterName()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR FOR BIT DATA)");
        //Initial set of bytes used to create the Binary Stream that has to
        //be inserted
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the BinaryStream that will be used in
        //the updateAsciiStream method
        byte[] bytes_for_update = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(bytes);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(bytes_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is,bytes.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateBinaryStream("datacol",is_for_update,(int)bytes_for_update.length);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBinaryStream method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes_for_update.length;i++) {
            assertEquals("Error in updateBinaryStream",bytes_for_update[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

    /**
     * This methods tests the ResultSet interface method
     * updateAsciiStream
     *
     * @throws SQLException if some error occurs while calling the method
     */

    public void testUpdateAsciiStreamStringParameterName()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR)");
        //Initial set of bytes used to create the Ascii Stream that has to
        //be inserted
        byte[] bytes = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the AsciiStream that will be used in
        //the updateAsciiStream method
        byte[] bytes_for_update = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //Input Stream inserted initially
        InputStream is = new java.io.ByteArrayInputStream(bytes);

        //InputStream that is used for update
        InputStream is_for_update = new
                java.io.ByteArrayInputStream(bytes_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is,bytes.length);
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateAsciiStream("datacol",is_for_update,(int)bytes_for_update.length);
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

        for(int i=0;i<bytes_for_update.length;i++) {
            assertEquals("Error in updateAsciiStream",bytes_for_update[i],bytes_ret[i]);
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

    public void testUpdateCharacterStreamStringParameterName()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol LONG VARCHAR)");

        String str = "Test data";
        String str_for_update = "Test data used for update";

        StringReader r = new StringReader(str);
        StringReader r_for_update = new StringReader
                (str_for_update);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");
        ps_sb.setInt(1,1);
        ps_sb.setCharacterStream(2,r,str.length());
        ps_sb.executeUpdate();
        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        rs1.updateCharacterStream("datacol",r_for_update,str_for_update.length());
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateAsciiStream method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();

        StringReader r_ret = (StringReader)rs1.getCharacterStream(2);

        char [] c_ret = new char[str_for_update.length()];

        r_ret.read(c_ret);

        String str_ret = new String(c_ret);

        assertEquals("Error in updateCharacterStream" + str_ret,str_for_update,
            str_ret);

        rs1.close();

        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

    /**
     * This methods tests the ResultSet interface method
     * updateClob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateClob()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol Clob)");
        //Initial set of bytes used to create the Ascii Stream for the Clob
        //that has to be inserted
        byte[] bytes1 = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the AsciiStream for the Clob
        //that will be used in the updateClob method
        byte[] bytes2 = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(bytes1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(bytes2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");

        //first insert
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is1,bytes1.length);
        ps_sb.executeUpdate();

        //second insert
        ps_sb.setInt(1,2);
        ps_sb.setAsciiStream(2,is2,bytes2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        Clob clob = rs1.getClob(2);

        rs1.next();
        rs1.updateClob(2,clob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateClob method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        rs1.next();
        InputStream is_ret = rs1.getAsciiStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes1.length;i++) {
            assertEquals("Error in updateAsciiStream",bytes1[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateBlob()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol Blob)");
        //Initial set of bytes used to create the Binary Stream that has to
        //be inserted
        byte[] bytes1 = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the BinaryStream for the Blob
        //that will be used in the updateBlob method
        byte[] bytes2 = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(bytes1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(bytes2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");

        //first insert
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is1,bytes1.length);
        ps_sb.executeUpdate();

        //second insert
        ps_sb.setInt(1,2);
        ps_sb.setBinaryStream(2,is2,bytes2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        Blob blob = rs1.getBlob(2);

        rs1.next();
        rs1.updateBlob(2,blob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBlob method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes1.length;i++) {
            assertEquals("Error in updateBlob",bytes1[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

    /**
     * This methods tests the ResultSet interface method
     * updateClob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateClobStringParameterName()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol Clob)");
        //Initial set of bytes used to create the Ascii Stream for the Clob
        //that has to be inserted
        byte[] bytes1 = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the AsciiStream for the Clob
        //that will be used in the updateClob method
        byte[] bytes2 = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(bytes1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(bytes2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");

        //first insert
        ps_sb.setInt(1,1);
        ps_sb.setAsciiStream(2,is1,bytes1.length);
        ps_sb.executeUpdate();

        //second insert
        ps_sb.setInt(1,2);
        ps_sb.setAsciiStream(2,is2,bytes2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        Clob clob = rs1.getClob(2);

        rs1.next();
        rs1.updateClob("datacol",clob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateClob method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        rs1.next();
        InputStream is_ret = rs1.getAsciiStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes1.length;i++) {
            assertEquals("Error in updateAsciiStream",bytes1[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

     /**
     * This methods tests the ResultSet interface method
     * updateBlob
     *
     * @throws SQLException if some error occurs while calling the method
     */
    public void embeddedUpdateBlobStringParameterName()
    throws Exception {
        //create the table
        stmt.execute("create table UpdateTestTable_ResultSet (sno int, " +
                "datacol Blob)");
        //Initial set of bytes used to create the Binary Stream that has to
        //be inserted
        byte[] bytes1 = new byte[] {
            0x65, 0x66, 0x67, 0x68, 0x69,
            0x69, 0x68, 0x67, 0x66, 0x65
        };

        //Bytes that are used to create the BinaryStream for the Blob
        //that will be used in the updateBlob method
        byte[] bytes2 = new byte[] {
            0x69, 0x68, 0x67, 0x66, 0x65,
            0x65, 0x66, 0x67, 0x68, 0x69
        };

        //Byte array in which the returned bytes from
        //the Database after the update are stored. This
        //array is then checked to determine if it
        //has the same elements of the Byte array used for
        //the update operation

        byte[] bytes_ret = new byte[10];

        //1 Input Stream for insertion
        InputStream is1 = new java.io.ByteArrayInputStream(bytes1);

        //2 Input Stream for insertion
        InputStream is2 = new java.io.ByteArrayInputStream(bytes2);

        //Prepared Statement used to insert the data
        PreparedStatement ps_sb = con.prepareStatement
                ("insert into UpdateTestTable_ResultSet values(?,?)");

        //first insert
        ps_sb.setInt(1,1);
        ps_sb.setBinaryStream(2,is1,bytes1.length);
        ps_sb.executeUpdate();

        //second insert
        ps_sb.setInt(1,2);
        ps_sb.setBinaryStream(2,is2,bytes2.length);
        ps_sb.executeUpdate();

        ps_sb.close();

        //Update operation
        //use a different ResultSet variable so that the
        //other tests can go on unimpacted
        //we do not have set methods on Clob and Blob implemented
        //So query the first Clob from the database
        //update the second result set with this
        //Clob value

        ResultSet rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet for update");
        rs1.next();
        Blob blob = rs1.getBlob(2);

        rs1.next();
        rs1.updateBlob("datacol",blob);
        rs1.updateRow();
        rs1.close();

        //Query to see whether the data that has been updated
        //using the updateBlob method is the same
        //data that we expected

        rs1 = stmt.executeQuery
                ("select * from UpdateTestTable_ResultSet");
        rs1.next();
        rs1.next();
        InputStream is_ret = rs1.getBinaryStream(2);

        is_ret.read(bytes_ret);
        is_ret.close();

        for(int i=0;i<bytes1.length;i++) {
            assertEquals("Error in updateBlob",bytes1[i],bytes_ret[i]);
        }
        rs1.close();
        //delete the table
        stmt .execute("drop table UpdateTestTable_ResultSet");
    }

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
                    "embeddedUpdateBlobStringParameterName"));
        embeddedSuite.addTest(new ResultSetTest(
                    "embeddedUpdateClobStringParameterName"));
        return embeddedSuite;
    }

    public static Test suite() {
        TestSuite rsSuite =
            new TestSuite(ResultSetTest.class, "ResultSetTest suite");
        // Add client only tests
        // NOTE: JCC is excluded
        if (usingDerbyNetClient()) {
            rsSuite.addTest(
                    clientSuite("ResultSetTest client-only suite"));
        }
        // Add embedded only tests
        if (usingEmbedded()) {
            rsSuite.addTest(
                    embeddedSuite("ResultSetTest embedded-only suite"));
        }
        return rsSuite;
    }
}
