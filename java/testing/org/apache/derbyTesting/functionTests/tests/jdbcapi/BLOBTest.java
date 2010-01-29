/**
 *
 * Derby - Class BLOBTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import org.apache.derbyTesting.functionTests.util.TestInputStream;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.functionTests.util.streams.ReadOnceByteArrayInputStream;
import org.apache.derbyTesting.functionTests.util.streams.StringReaderWithLength;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;

import junit.framework.Test;
import junit.framework.TestSuite;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * Tests reading and updating binary large objects (BLOBs).
 */
final public class BLOBTest extends BaseJDBCTestCase
{
    /** 
     * Constructor
     * @param name name of test case (method).
     */
    public BLOBTest(String name) 
    {
        super(name);
    }

    
    /**
     * Tests updating a Blob from a scollable resultset, using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(3));
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt(1);
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset, using
     * positioned updates.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);

        rs.relative(0); // Necessary after a positioned update
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(3));
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, using
     * methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT * from " + 
                              BLOBDataModelSetup.getBlobTableName());
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt(1);
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt(1) + 11;
        final int newSize = rs.getInt(2) / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset produced by a
     * select query with projection. Updates are made using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetWithProjectUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data,val,length from " + 
                              BLOBDataModelSetup.getBlobTableName());
        println("Last");
        rs.last();
        
        final int newVal = rs.getInt(2) + 11;
        final int newSize = rs.getInt(3) / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob(1));
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, produced by 
     * a select query with projection. Updates are made using
     * result set update methods.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetWithProjectUsingResultSetMethods()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data,val,length from " + 
                              BLOBDataModelSetup.getBlobTableName());
        
        while (rs.next()) {
            println("Next");
            final int val = rs.getInt("VAL");
            if (val == BLOBDataModelSetup.bigVal) break;
        }
        
        final int newVal = rs.getInt("VAL") + 11;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithResultSetMethods(rs, newVal, newSize);
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a scollable resultset, produced by 
     * a select query with projection. Updates are made using
     * positioned updates
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromScrollableResultSetWithProjectUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data from " + 
                              BLOBDataModelSetup.getBlobTableName() + 
                              " WHERE val= " + BLOBDataModelSetup.bigVal);
        println("Last");
        rs.last();
        
        final int newVal = BLOBDataModelSetup.bigVal * 2;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);

        rs.relative(0); // Necessary after a positioned update
        
        println("Verify updated blob using result set");
        verifyBlob(newVal, newSize, rs.getBlob("DATA"));
        
        rs.close();
        stmt.close();
    }

    /**
     * Tests updating a Blob from a forward only resultset, produced by 
     * a select query with projection. Updates are made using
     * positioned updates.
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    public void testUpdateBlobFromForwardOnlyResultSetWithProjectUsingPositionedUpdates()
        throws SQLException, IOException
    {
        final Statement stmt = 
            createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_UPDATABLE);
        final ResultSet rs = 
            stmt.executeQuery("SELECT data from " + 
                              BLOBDataModelSetup.getBlobTableName() + 
                              " WHERE val = " + BLOBDataModelSetup.bigVal);
        rs.next();
        
        final int newVal =  BLOBDataModelSetup.bigVal * 2;
        final int newSize = BLOBDataModelSetup.bigSize / 2;
        testUpdateBlobWithPositionedUpdate(rs, newVal, newSize);
        
        rs.close();
        stmt.close();
    }
    
    
    /**
     * Tests updating the Blob using result set update methods.
     * @param rs result set, currently positioned on row to be updated
     * @param newVal new value in val column and blob data
     * @param newSize new size of Blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void testUpdateBlobWithResultSetMethods(final ResultSet rs,
                                                    final int newVal,
                                                    final int newSize) 
        throws SQLException, IOException
    {
        int val = rs.getInt("VAL");
        int size = rs.getInt("LENGTH");
        println("VerifyBlob");
        verifyBlob(val, size, rs.getBlob("DATA"));
        
        println("UpdateBlob");
        final TestInputStream newStream = new TestInputStream(newSize, newVal);
        
        rs.updateInt("VAL", newVal);
        rs.updateInt("LENGTH", newSize);
        rs.updateBinaryStream("DATA", newStream, newSize);
        rs.updateRow();
        
        println("Verify updated blob with another query");
        verifyNewValueInTable(newVal, newSize);
    }

    /**
     * Tests updating the Blob using positioned updates
     * @param rs result set, currently positioned on row to be updated
     * @param newVal new value in val column and blob data
     * @param newSize new size of Blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void testUpdateBlobWithPositionedUpdate(final ResultSet rs,
                                                    final int newVal,
                                                    final int newSize) 
        throws SQLException, IOException
    {
        final PreparedStatement preparedStatement = prepareStatement
            ("UPDATE " + BLOBDataModelSetup.getBlobTableName() +
             " SET val=?, length = ?, data = ? WHERE CURRENT OF " +
             rs.getCursorName());
        
        println("UpdateBlob");
        
        final TestInputStream newStream = new TestInputStream(newSize, newVal);
        
        preparedStatement.setInt(1, newVal);
        preparedStatement.setInt(2, newSize);
        preparedStatement.setBinaryStream(3, newStream, newSize);
        preparedStatement.executeUpdate();
        
        println("Verify updated blob with another query");
        verifyNewValueInTable(newVal, newSize);
    }
    
    /**
     * Tests that a stream value in a values clause can be cast to a BLOB.
     * <p>
     * See DERBY-4102 (test case resulted in a ClassCastException earlier).
     *
     * @throws IOException if something goes wrong
     * @throws SQLException if something goes wrong
     */
    public void testBlobCastInValuesClause()
            throws IOException, SQLException {
        // The length must be at least 32 KB.
        final int length = 38*1024;
        PreparedStatement ps = prepareStatement("values cast(? as blob)");
        ps.setBinaryStream(1, new LoopingAlphabetStream(length), length);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        Blob b = rs.getBlob(1);
        assertEquals(length, b.length());
        // Select some parts of the Blob, moving backwards.
        assertEquals(100, b.getBytes(32*1024-27, 100).length);
        assertEquals(1029, b.getBytes(19*1024, 1029).length);
        // Compare a fresh stream with the one from the Blob.
        assertEquals(new LoopingAlphabetStream(length), b.getBinaryStream());
        assertEquals(-1, b.position(new byte[] {(byte)'a', (byte)'A'}, 1));
        assertEquals(length, b.length());
        assertFalse(rs.next());
        rs.close();
    }


    /**
     * Tests that a lob can be safely occur multiple times in a SQL select.
     * <p/>
     * See DERBY-4477.
     * <p/>
     * @see org.apache.derbyTesting.functionTests.tests.memory.BlobMemTest#testDerby4477_3645_3646_Repro_lowmem
     * @see org.apache.derbyTesting.functionTests.tests.memory.ClobMemTest#testDerby4477_3645_3646_Repro_lowmem_clob
     */
    public void testDerby4477_3645_3646_Repro() throws SQLException, IOException {
        setAutoCommit(false);
        Statement s = createStatement();

        s.executeUpdate(
            "CREATE TABLE T_MAIN(" +
            "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
            "V BLOB(590473235) )");

        PreparedStatement ps = prepareStatement(
            "INSERT INTO T_MAIN(V) VALUES (?)");

        byte[] bytes = new byte[35000];

        for (int i = 0; i < 35000; i++) {
            bytes[i] = (byte)i;
        }

        ps.setBytes(1, bytes);
        ps.executeUpdate();
        ps.close();

        s.executeUpdate("CREATE TABLE T_COPY ( V1 BLOB(2M), V2 BLOB(2M))");

        // This failed in the repro for DERBY-3645 solved as part of
        // DERBY-4477:
        s.executeUpdate("INSERT INTO T_COPY SELECT  V, V FROM T_MAIN");

        // Check that the two results are identical:
        ResultSet rs = s.executeQuery("SELECT * FROM T_COPY");
        rs.next();
        String v1 = rs.getString(1);
        String v2 = rs.getString(2);
        assertEquals(v1.length(), v2.length());

        for (int i=0; i < v1.length(); i++) {
            assertEquals(v1.charAt(i), v2.charAt(i));
        }

        // Verify against a single select too (both above could be wrong..)
        rs = s.executeQuery("SELECT V from T_MAIN");
        rs.next();
        String v3 = rs.getString(1);
        assertEquals(v1.length(), v3.length());

        for (int i=0; i < v1.length(); i++) {
            assertEquals(v1.charAt(i), v3.charAt(i));
        }

        // This failed in the repro for DERBY-3646 solved as part of
        // DERBY-4477 (repro slightly rewoked here):
        rs = s.executeQuery("SELECT 'I', V, ID, V from T_MAIN");
        rs.next();

        InputStream s1 = rs.getBinaryStream(2);

        // JDBC says that the next getBinaryStream will close the s1 stream so
        // verify it now. Cf. DERBY-4521.

        for (int i = 0; i < 35000; i++) {
            assertEquals((byte)i, (byte)s1.read());
        }

        assertEquals(-1, s1.read());
        s1.close();

        InputStream s2 = rs.getBinaryStream(4);

        for (int i = 0; i < 35000; i++) {
            assertEquals((byte)i, (byte)s2.read());
        }

        assertEquals(-1, s2.read());
        s2.close();

        rs.close();

        rollback();
    }


    /**
     * Tests that a lob can be safely occur multiple times in a SQL select in
     * a trigger context.
     * <p/>
     * See DERBY-4477.
     */
    public void testDerby4477_2349_Repro() throws SQLException, IOException {

        setAutoCommit(false);

        Statement s = createStatement();

        s.executeUpdate("CREATE TABLE T_MAIN(" +
                "ID INT  GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                "V BLOB(590473235) )");
        s.executeUpdate("CREATE TABLE T_ACTION_ROW(ID INT, A CHAR(1), " +
                "V1 BLOB(590473235), V2 BLOB(590473235) )");
        s.executeUpdate("CREATE TABLE T_ACTION_STATEMENT(ID INT, A CHAR(1), " +
                "V1 BLOB(590473235), V2 BLOB(590473235) )");

        // ON INSERT copy the typed value V into the action table.
        // Use V twice to ensure there are no issues with values
        // that can be streamed.
        // Two identical actions,  per row and per statement.
        s.executeUpdate(
            "CREATE TRIGGER AIR AFTER INSERT ON T_MAIN " +
            "    REFERENCING NEW AS N FOR EACH ROW " +
            "    INSERT INTO T_ACTION_ROW(A, V1, ID, V2) " +
            "        VALUES ('I', N.V, N.ID, N.V)");

        s.executeUpdate(
            "CREATE TRIGGER AIS AFTER INSERT ON T_MAIN " +
            "    REFERENCING NEW_TABLE AS N FOR EACH STATEMENT " +
            "    INSERT INTO T_ACTION_STATEMENT(A, V1, ID, V2) " +
            "        SELECT 'I', V, ID, V FROM N");

        s.executeUpdate("INSERT INTO T_MAIN(V) VALUES NULL");

        s.close();
        actionTypesCompareMainToAction(1);

        int jdbcType = Types.BLOB;
        int precision = 590473235;

        Random r = new Random();

        String ins1 = "INSERT INTO T_MAIN(V) VALUES (?)";
        String ins3 = "INSERT INTO T_MAIN(V) VALUES (?), (?), (?)";

        PreparedStatement ps;
        ps = prepareStatement(ins1);
        setRandomValue(r, ps, 1, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(2);

        ps = prepareStatement(ins3);
        setRandomValue(r, ps, 1, jdbcType, precision);
        setRandomValue(r, ps, 2, jdbcType, precision);
        setRandomValue(r, ps, 3, jdbcType, precision);
        ps.executeUpdate();
        ps.close();

        actionTypesCompareMainToAction(5);

        rollback();
    }

    public static void setRandomValue(
        Random r,
        PreparedStatement ps,
        int column,
        int jdbcType,
        int precision) throws SQLException, IOException {

        Object val = getRandomValue(r, jdbcType, precision);

        if (val instanceof StringReaderWithLength) {
            StringReaderWithLength rd = (StringReaderWithLength) val;
            ps.setCharacterStream(column, rd, rd.getLength());
        } else if (val instanceof InputStream) {
            InputStream in = (InputStream) val;
            ps.setBinaryStream(column, in, in.available());
        } else {
            ps.setObject(column, val, jdbcType);
        }
    }

    public static Object getRandomValue(
        Random r,
        int jdbcType,
        int precision) throws IOException {

        switch (jdbcType) {
        case Types.BLOB:
            if (precision > 256*1024)
                precision = 256*1024;
            return new ReadOnceByteArrayInputStream(
                    randomBinary(r, r.nextInt(precision)));
        }

        fail("unexpected JDBC Type " + jdbcType);
        return null;
    }

    private static byte[] randomBinary(Random r, int len) {
        byte[] bb = new byte[len];
        for (int i = 0; i < bb.length; i++)
            bb[i] = (byte) r.nextInt();
        return bb;
     }

    private void actionTypesCompareMainToAction(
        int actionCount) throws SQLException, IOException {

        Statement s1 = createStatement();
        Statement s2 = createStatement();

        String sqlMain =
            "SELECT ID, V, V FROM T_MAIN ORDER BY 1";
        String sqlActionRow =
            "SELECT ID, V1, V2 FROM T_ACTION_ROW ORDER BY 1";
        String sqlActionStatement =
            "SELECT ID, V1, V2 FROM T_ACTION_STATEMENT ORDER BY 1";

        ResultSet rsMain = s1.executeQuery(sqlMain);
        ResultSet rsAction = s2.executeQuery(sqlActionRow);
        JDBC.assertSameContents(rsMain, rsAction);

        rsMain = s1.executeQuery(sqlMain);
        rsAction = s2.executeQuery(sqlActionStatement);
        JDBC.assertSameContents(rsMain, rsAction);


        assertTableRowCount("T_ACTION_ROW", actionCount);
        assertTableRowCount("T_ACTION_STATEMENT", actionCount);

        s1.close();
        s2.close();
    }


    /**
     * Verifies that the table has row with column val=newVal
     * and that it its data and size columns are consistent.
     * @param newVal value expected to be found in the val column of a row
     * @param newSize expected size of size column and size of blob
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void verifyNewValueInTable(final int newVal,
                                       final int newSize)
        throws IOException, SQLException
    {
        println("Verify new value in table: " + newVal);

        final Statement stmt = createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                   ResultSet.CONCUR_READ_ONLY);

        final ResultSet rs =
            stmt.executeQuery("SELECT * FROM " +
                              BLOBDataModelSetup.getBlobTableName() +
                              " WHERE val = " + newVal);

        println("Query executed, calling next");

        boolean foundVal = false;

        while (rs.next()) {
            println("Next called, verifying row");

            assertEquals("Unexpected value in val column",
                         newVal, rs.getInt(1));

            verifyBlob(newVal, newSize, rs.getBlob(3));
            foundVal = true;
        }
        assertTrue("No column with value= " + newVal + " found ", foundVal);

        rs.close();
        stmt.close();
    }


    /**
     * Verifies that the blob is consistent
     * @param expectedVal the InputStream for the Blob should return this value
     *                    for every byte
     * @param expectedSize the BLOB should have this size
     * @param blob the BLOB to check
     * @exception SQLException causes test to fail with error
     * @exception IOException causes test to fail with error
     */
    private void verifyBlob(final int expectedVal,
                            final int expectedSize,
                            final Blob blob)
        throws IOException, SQLException
    {
        final InputStream stream = blob.getBinaryStream();
        int blobSize = 0;
        for (int val = stream.read(); val!=-1; val = stream.read()) {
            blobSize++;

            // avoid doing a string-concat for every byte in blob
            if (expectedVal!=val) {
                assertEquals("Unexpected value in stream at position " +
                             blobSize,
                             expectedVal, val);
            }
        }
        stream.close();
        assertEquals("Unexpected size of stream ", expectedSize, blobSize);
    }

    /**
     * The suite decorates the tests of this class with 
     * a setup which creates and populates the data model.
     */
    public static Test suite() 
    {
        TestSuite mainSuite = new TestSuite(BLOBTest.class, "BLOBTest");
        return new BLOBDataModelSetup(mainSuite);
    }

    /**
     * The setup creates a Connection to the database.
     * @exception Exception any exception will cause test to fail with error.
     */
    public final void setUp() 
        throws Exception
    {
        getConnection().setAutoCommit(false);
    }
}
