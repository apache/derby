/*

Derby - Class org.apache.derbyTesting.functionTests.tests.largedata.LobLimitsTest

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.largedata;



import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.functionTests.util.PrivilegedFileOpsForTests;


/**
* This test is part of the "largedata" suite because this test tests data for
* lobs to the limits ( ie blob and clob can be 2G-1 maximum) and so this test
* may take considerable disk space as well as time to run. Hence it is not part
* of the derbyall suite but should ideally be run at some intervals to test if
* no regression has occurred.
*/

public class LobLimitsTest extends BaseJDBCTestCase {

   public LobLimitsTest(String name) {
        super(name);
    }

static boolean trace = false;

   static final int _2GB = 2 * 1024 * 1024* 1024 - 1;
   static final int _100MB = 100 * 1024 * 1024;
   static final long _4GB =  4*1024*1024*(1024L);
   static final int NUM_TRAILING_SPACES = 33*1024;
   
   
   static int BIGGEST_LOB_SZ= _2GB;

   public static void setBIGGEST_LOB_SZ(int bIGGESTLOBSZ) {
    BIGGEST_LOB_SZ = bIGGESTLOBSZ;
}

static int BIG_LOB_SZ = _100MB;

   public static void setBIG_LOB_SZ(int bIGLOBSZ) {
    BIG_LOB_SZ = bIGLOBSZ;
}
   static int MORE_DATA_THAN_COL_WIDTH= (_100MB)+1;

   public static void setMORE_DATA_THAN_COL_WIDTH(int mOREDATATHANCOLWIDTH) {
    MORE_DATA_THAN_COL_WIDTH = mOREDATATHANCOLWIDTH;
}

static final String DATAFILE = "extinout/byteLobLimits.dat";

   static final String CHARDATAFILE = "extinout/charLobLimits.txt";

  
    /**
     * Setup the schema and the blob sizes for the test.
     * @param s
     * @param biggestlobsz 
     * @param biglobsz
     * @throws SQLException
     */
    static void setupTables(Statement s, int biggestlobsz, int biglobsz ) throws SQLException {
       setBIGGEST_LOB_SZ(biggestlobsz);
       setBIG_LOB_SZ(biglobsz);
       setMORE_DATA_THAN_COL_WIDTH(biglobsz +1);
       println("BIGGEST_LOB_SZ=" + BIGGEST_LOB_SZ + " BIG_LOB_SZ=" + BIG_LOB_SZ);
       Connection conn = s.getConnection();
       conn.setAutoCommit(true);
       s.execute("CREATE TABLE BLOBTBL (ID INT NOT NULL PRIMARY KEY, "
               + "POS BIGINT, DLEN BIGINT, CONTENT BLOB(2G))");
       s.execute("CREATE TABLE CLOBTBL (ID INT NOT NULL PRIMARY KEY,"
               + "POS BIGINT, DLEN BIGINT, CONTENT CLOB(2G))");
       s.execute("CREATE TABLE BLOBTBL2 (ID INT NOT NULL PRIMARY KEY, "
               + "POS BIGINT, CONTENT BLOB("+BIG_LOB_SZ+"),DLEN BIGINT)");

       // Please dont change the clob column width,since tests use this width to 
       // test for truncation of trailing spaces.
       s.execute("CREATE TABLE CLOBTBL2 (ID INT NOT NULL PRIMARY KEY,"
               + "POS BIGINT, CONTENT CLOB("+BIG_LOB_SZ+"), DLEN BIGINT)"); 

   }

   public static Test suite() {
       // Right now run just with embeddded.
       Test suite = new CleanDatabaseTestSetup(
               new TestSuite(LobLimitsTest.class)) {
                   protected void decorateSQL(Statement s)
                           throws SQLException {
                       setupTables(s,_2GB, _100MB);
                   }
               };
               
       return new SupportFilesSetup(suite);
   }

   /**
    * tests specific for blobs
    * @param conn
    * @throws Exception
    */
   public void testBlob() throws Exception {
       Connection conn = getConnection();
       conn.setAutoCommit(false);
        PreparedStatement insertBlob =
                prepareStatement("INSERT INTO BLOBTBL values (?,?,?,?)");
        PreparedStatement selectBlob =
                prepareStatement("SELECT CONTENT,DLEN FROM BLOBTBL WHERE ID = ?");
        PreparedStatement insertBlob2 =
                prepareStatement("INSERT INTO BLOBTBL2 values (?,?,?,?)");
        PreparedStatement selectBlob2 =
                prepareStatement("SELECT CONTENT,DLEN FROM BLOBTBL2 WHERE ID = ?");
        PreparedStatement deleteBlob2 =
                prepareStatement("DELETE FROM BLOBTBL2");
        PreparedStatement deleteBlob =
                conn.prepareStatement("DELETE FROM BLOBTBL");
           // Test - 2Gb blob ( actually it is 2gb -1)
           // Note with setBinaryStream interface the maximum size for the
           // stream, can be max value for an int.
           // Also note, that lobs in derby currently  supports
           // maximum size of 2gb -1

           // first do insert blob of 2g, 2 rows
           insertBlob_SetBinaryStream("BlobTest #1", conn, insertBlob, BIGGEST_LOB_SZ,
                   0, 2, BIGGEST_LOB_SZ);
           // do a select to see if the inserts in test above went ok
           selectBlob("BlobTest #2", conn, selectBlob, BIGGEST_LOB_SZ, 0, 1);
           selectBlob("BlobTest #3", conn, selectBlob, BIGGEST_LOB_SZ, 1, 1);
           
           // now do a select of one of the 2gb rows and update another 2g row 
           // using the setBlob api, updated blob is of length 2gb
           // Fix for Bug entry -DERBY-599[setBlob should not materialize blob
           // into memory]
           selectUpdateBlob("BlobTest #4",conn,selectBlob,BIGGEST_LOB_SZ,0,1,1);
           // select row from blobtbl and then do insert into the blobtbl
           // using setBlob
           selectInsertBlob("BlobTest #4.1",conn,selectBlob,insertBlob,BIGGEST_LOB_SZ,0,3,1);
                              
           // Test - generate random data, write to a file, use it to insert
           // data into blob and then read back and compare if all is ok
           // currently in fvt ( derbyall), tests check for substrings etc and 
           // for small amounts of data.  This test will test for 100mb of blob data

           FileOutputStream fos = PrivilegedFileOpsForTests.getFileOutputStream(new File(DATAFILE));
           RandomByteStreamT r = new RandomByteStreamT(new java.util.Random(),
                   BIG_LOB_SZ);
           // write in chunks of 32k buffer
           byte[] buffer = new byte[32 * 1024];
           int count = 0;
           
           while((count=r.read(buffer))>=0)
               fos.write(buffer,0,count);

           fos.flush();
           fos.close();
           insertBlob2("BlobTest #5.1 ", conn, insertBlob2, BIG_LOB_SZ, 0, 1,
                   BIG_LOB_SZ, DATAFILE);
           selectBlob2("BlobTest #5.2 ", conn, selectBlob2, BIG_LOB_SZ, 0, 1,
                   DATAFILE);
           
           
           // update the 2gb row in blobtbl with the 100mb data and compare if the update
           // went ok. 
           selectUpdateBlob2("BlobTest #6",conn,selectBlob2,selectBlob,BIG_LOB_SZ,0,1,1,DATAFILE);
                       
           deleteTable(conn, deleteBlob2, 1);

       conn.commit();
       
       deleteTable(conn, deleteBlob, 3);
   }
   
   public void testBlobNegative () throws SQLException {
       // Negative Test, use setBlob api to insert a 4GB blob.
       Connection conn = getConnection();
       conn.setAutoCommit(false);
       PreparedStatement insertBlob = prepareStatement("INSERT INTO BLOBTBL values (?,?,?,?)");
       
       BlobImplT _4GbBlob = new BlobImplT(new RandomByteStreamT(new java.util.Random(),_4GB),_4GB);

       try
       {
           insertBlob_SetBlob("BlobTest #7 (setBlob with 4Gb blob",conn,insertBlob,_4GbBlob,
                   _4GB,0,1,0);
       }
       catch(SQLException sqle)
       {
           //DERBY DOES NOT SUPPORT INSERT OF 4GB BLOB
           assertSQLState("22003", sqle);
       }
       // ADD  NEW TESTS HERE
   }

   /**
    * tests using clobs
    * @throws Exception
    */
   public void testClob1() throws Exception {
       Connection conn = getConnection();
        setAutoCommit(false);
        PreparedStatement insertClob =
                prepareStatement("INSERT INTO CLOBTBL values (?,?,?,?)");
        PreparedStatement selectClob = 
            prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL WHERE ID = ?");

       // Test - 2Gb blob
       // Note with setCharacterStream interface the maximum size for the
       // stream has to be max value for a int which is (2GB -1 )
       // first do insert clob of 2g, 2 rows
       insertClob_SetCharacterStream("ClobTest #1", conn, insertClob,
               BIGGEST_LOB_SZ, 0, 2, BIGGEST_LOB_SZ);
       // do a select to see if the inserts in test above went ok
       selectClob("ClobTest #2", conn, selectClob, BIGGEST_LOB_SZ, 0, 1);
       selectClob("ClobTest #3", conn, selectClob, BIGGEST_LOB_SZ, 0, 1);
       // do a select and then update a row of 2gb size: uses getClob
       selectUpdateClob("ClobTest #4",conn,selectClob,BIGGEST_LOB_SZ,0,1,1);
           
   }
   
   /**
    * @throws Exception
    */
   public void testClob2() throws Exception {
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        PreparedStatement selectClob =
                prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL WHERE ID = ?");
        PreparedStatement insertClob2 =
                prepareStatement("INSERT INTO CLOBTBL2 values (?,?,?,?)");
        PreparedStatement selectClob2 =
                prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL2 WHERE ID = ?");

       setAutoCommit(false);
           // Test - generate random data, write to a file, use it to insert
           // data into clob and then read back and compare if all is ok
           // currently in fvt ( derbyall), tests check for substrings etc and 
           // for small amounts of data.  This test will test for 100mb of clob data
           writeToFile(CHARDATAFILE,new RandomCharReaderT(new java.util.Random(),BIG_LOB_SZ));
           insertClob2("ClobTest #5.1 ", conn, insertClob2, BIG_LOB_SZ, 0, 1,
                   BIG_LOB_SZ, CHARDATAFILE);
           selectClob2("ClobTest #5.2 ", conn, selectClob2, BIG_LOB_SZ, 0, 1,
                   CHARDATAFILE);

           // Disabled for now, this will materialize, will open 
           // jira for it.
           //updateClob2("ClobTest #8.1",conn,selectClob,BIG_LOB_SZ,0,0,10,1,CHARDATAFILE);

           // update the 2gb row in clobtbl with the 100mb data and compare if the update
           // went ok.
           selectUpdateClob2("ClobTest #8.2",conn,selectClob2,selectClob,BIG_LOB_SZ,0,1,1,CHARDATAFILE);

           // test for trailing space truncation
           // insert 100mb+33k of data which has 33k of trailing space,
           // into a column of 100mb
           // insert should be successful, select should retrieve 100mb of data
           
           // Generate random data and write to a file, this file will be used
           // in the verification process after inserts and updates.
           writeToFile(CHARDATAFILE,new RandomCharReaderT(new java.util.Random(),
                   (NUM_TRAILING_SPACES +BIG_LOB_SZ),NUM_TRAILING_SPACES));
           insertClob2("ClobTest #6.1 ", conn, insertClob2,BIG_LOB_SZ, 3, 1,
                   (NUM_TRAILING_SPACES +BIG_LOB_SZ), CHARDATAFILE);
           // select will retrieve data and verify the data inserted. 
           selectClob2("ClobTest #6.2 ", conn, selectClob2, BIG_LOB_SZ, 3, 1,
                   CHARDATAFILE);


       }
   
   public void testClobNegative() throws Exception {
       Connection conn = getConnection();
       conn.setAutoCommit(false);
        PreparedStatement insertClob =
                prepareStatement("INSERT INTO CLOBTBL values (?,?,?,?)");
        PreparedStatement selectClob =
                prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL WHERE ID = ?");
        PreparedStatement insertClob2 =
                prepareStatement("INSERT INTO CLOBTBL2 values (?,?,?,?)");
        PreparedStatement selectClob2 =
                prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL2 WHERE ID = ?");
        PreparedStatement deleteClob2 =
                prepareStatement("DELETE  from CLOBTBL2");
        PreparedStatement deleteClob = prepareStatement("DELETE  from CLOBTBL");

       negativeSpaceTruncationTest("ClobTest #7",conn);
       
       // Test - for stream contains a trailing non-space character
       // insert should throw an error
       writeToFile(CHARDATAFILE,new RandomCharReaderT(new java.util.Random(),MORE_DATA_THAN_COL_WIDTH));
              
           try {
               insertClob2("ClobTest #9.1 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH, CHARDATAFILE);
           }catch(SQLException sqle)
           {
               assertSQLState("XSDA4",sqle);               
           }
           // no row must be retrieved.
           selectClob2("ClobTest #9.2 ", conn, selectClob2, BIG_LOB_SZ, 4, 0,
                   CHARDATAFILE);

           try
           {
               insertClob2("ClobTest #10 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH +1 , CHARDATAFILE);
           }catch(SQLException sqle)
           {
               //NEGATIVE TEST - Expected Exception: truncation of non-blanks not allowed and
               //stream length is one greater than actual length of the stream 
               assertSQLState("XSDA4",sqle);
           }

           try
           {
               insertClob2("ClobTest #11 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH -1 , CHARDATAFILE);
           }catch(SQLException sqle)
           {
               //NEGATIVE TEST - Expected Exception: truncation of non-blanks not allowed and
               //stream length is one less than actual length of the stream
               assertSQLState("XSDA4", sqle);
           }
           deleteTable(conn, deleteClob2, 2);

       try {
           // give -ve streamlength
           insertClob_SetCharacterStream("ClobTest #12.1", conn, insertClob,
                   BIG_LOB_SZ, 4, 1, -1);
       } catch (SQLException sqle) {
           assertSQLState("XJ025", sqle);
       }

       selectClob("ClobTest #12.2", conn, selectClob,BIG_LOB_SZ, 4, 0);

       deleteTable(conn, deleteClob, 2);
       
       // Negative tests use the setClob API to insert a 4GB clob

       //long _4GB =  4*1024*1024*(1024L);

       ClobImplT _4GBClob = new ClobImplT(new RandomCharReaderT(new java.util.Random(),_4GB),_4GB);		

       try
       {
           insertClob_SetClob("ClobTest #13 (setClob with 4Gb clob",conn,insertClob,_4GBClob,
                   _4GB,0,1,0);
       }
       catch(SQLException sqle)
       {
           //DERBY DOES NOT SUPPORT INSERT OF 4GB CLOB 
           assertSQLState("22003",sqle);
       }

       // ADD NEW TESTS HERE
   }

   private static void negativeSpaceTruncationTest(String msg,Connection conn)
       throws Exception
   {
       PreparedStatement insertClob2 = conn
       .prepareStatement("INSERT INTO CLOBTBL2 values (?,?,?,?)");

       // Negative test, stream has trailing spaces but the stream length is one 
       // more than the actual length of the stream
       try
       {
           insertClob2(msg, conn, insertClob2,BIG_LOB_SZ, 4, 1,
               (NUM_TRAILING_SPACES +BIG_LOB_SZ - 1), CHARDATAFILE);
       }catch(SQLException sqle)
       {
           //EXPECTED EXCEPTION - stream has trailing spaces,but stream 
           //length is 1 less than actual length of stream
           assertSQLState("XSDA4",sqle);
       }

       try
       {
           insertClob2(msg, conn, insertClob2,BIG_LOB_SZ, 5, 1,
               (NUM_TRAILING_SPACES +BIG_LOB_SZ + 1), CHARDATAFILE);
       }catch(SQLException sqle)
       {
           //EXPECTED EXCEPTION - stream has trailing spaces,but stream 
           // length is 1 greater than actual length of stream
           assertSQLState("XSDA4", sqle);
       }
   }
   
 
   /**
    * insert blob
    * @param bloblen   length of blob to insert
    * @param start     start id value for insert
    * @param rows      insert rows number of rows
    * @param streamLength  stream length passed to setBinaryStream(,,length)
    */
   private static void insertBlob_SetBinaryStream(String testId,
           Connection conn, PreparedStatement ps, int bloblen, int start,
           int rows, int streamLength) throws SQLException {
       println("========================================");
       println("START " + testId + "insertBlob of size = "
               + bloblen);
       long ST = 0;
       ST = System.currentTimeMillis();

       int count = 0;
       java.util.Random random = new java.util.Random();
       for (int i = start; i < start + rows; i++) {
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(3, bloblen);
           ps.setBinaryStream(4, new RandomByteStreamT(random, bloblen),
                   streamLength);
           count += ps.executeUpdate();
       }
       conn.commit();
       println("Insert Blob (" + bloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));
       verifyTest(count, rows, " Rows inserted with blob of size (" + bloblen
               + ") =");
       println("========================================");

   }

   /**
    * insert blob, using a setBlob api.
    * @param bloblen
    *            length of blob to insert
    * @param blob
    *            blob to insert
    * @param start
    *            start id value for insert
    * @param rows
    *            insert rows number of rows
    * @param expectedRows
    *            rows expected to be inserted
    */
    private static void insertBlob_SetBlob(String testId, Connection conn,
            PreparedStatement ps, java.sql.Blob blob, long bloblen, int start,
            int rows, int expectedRows) throws SQLException {
        println("========================================");
        println("START " + testId + "insertBlob of size = "
                + bloblen);
        long ST = System.currentTimeMillis();
        int count = 0;
            for (int i = start; i < start + rows; i++) {
                ps.setInt(1, i);
                ps.setInt(2, 0);
                ps.setLong(3, bloblen);
                ps.setBlob(4, blob);
                count += ps.executeUpdate();
            }
            conn.commit();
            println("Insert Blob (" + bloblen + ")" + " rows= "
                        + count + " = "
                        + (long) (System.currentTimeMillis() - ST));

        verifyTest(count, expectedRows,
                " Rows inserted with blob of size (" + bloblen + ") =");
        println("========================================");

    }


   /**
    * select from blob table (BLOBTBL)
    * @param bloblen  select expects to retrieve a blob of this length
    * @param id       id of the row to retrieve
    * @param expectedRows  number of rows expected to match id
    */
   private static void selectBlob(String testId, Connection conn,
           PreparedStatement ps, int bloblen, int id, int expectedRows)
           throws SQLException {
       println("========================================");
       println("START " + testId + " - SELECT BLOB of size = "
               + bloblen);

       long ST = 0;
       ResultSet rs = null;
       ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Blob value = rs.getBlob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           assertEquals("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in BLOBTBL with ID="
                       + id, dlen, l);
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with blob of size(" + bloblen + ") =");
       println("Select Blob (" + bloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
       println("========================================");
   }

   /**
    * insert blob into BLOBTBL2
    * @param bloblen   length of blob to insert
    * @param start     id value for insert
    * @param rows      insert rows number of rows
    * @param streamLength  stream length passed to setBinaryStream(,,length)
    * @param file      filename to match retrieved data against
    */

   private static void insertBlob2(String testId, Connection conn,
           PreparedStatement ps, int bloblen, int start, int rows,
           int streamLength, String file) throws Exception {
       println("========================================");
       println("START " + testId + "insert Blob of size = "
               + bloblen);
       int count = 0;
       java.util.Random random = new java.util.Random();
       FileInputStream fis = null;

       long ST = 0;
       
       ST = System.currentTimeMillis();

       for (int i = start; i < start + rows; i++) {
           fis = PrivilegedFileOpsForTests.getFileInputStream(new File(file));
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(4, bloblen);
           ps.setBinaryStream(3, fis, streamLength);
           count += ps.executeUpdate();
           fis.close();
       }
       conn.commit();
       println("Insert Blob (" + bloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));
       verifyTest(count, rows, " Rows inserted with blob of size (" + bloblen
               + ") =");
       println("========================================");

   }

   /**
    * select from blob table (BLOBTBL2)
    * @param bloblen  select expects to retrieve a blob of this length
    * @param id       id of the row to retrieve
    * @param expectedRows  number of rows expected to match id
    * @param file  name of the file,against which the retrieved data is
    *              compared
    */
   private static void selectBlob2(String testId, Connection conn,
           PreparedStatement ps, int bloblen, int id, int expectedRows,
           String file) throws Exception {
       println("========================================");
       println("START " + testId + " - SELECT BLOB of size = "
               + bloblen);

       long ST = 0;
       ResultSet rs = null;
       ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Blob value = rs.getBlob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           assertEquals("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in BLOBTBL with ID="
                       + id, dlen, l);
           
           compareBlobToFile(value.getBinaryStream(), file);
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with blob of size(" + bloblen + ") =");

   
           println("Select Blob (" + bloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
           println("========================================");
   }

   /**
    * Basically this test will do an update using setBlob api -
    * select row from blobtbl and then update a row in blobtbl 
    * and verify updated data in blobtbl
    * @param    ps  select statement from which blob is retrieved
    * @param    bloblen updating value is of length bloblen
    * @param    id  id of the row retrieved, for the update
    * @param    updateId  id of the row that is updated
    * @param    expectedRows    to be updated
    */
   private static void selectUpdateBlob(String testId, Connection conn,
           PreparedStatement ps, int bloblen, int id, int updateId,
           int expectedRows) throws Exception {
       println("========================================");
       println("START " + testId + " - select and then update blob of size= "
               + bloblen + " - Uses getBlob api");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       assertEquals("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in BLOBTBL with ID=" + id,
                           dlen, l);
       
       PreparedStatement psUpd = conn
               .prepareStatement("update BLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setBlob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       println("Rows Updated = " + psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, updateId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Blob updatedValue = rs2.getBlob(1);
       assertEquals("FAIL - Retrieving the updated blob length does not match "+
                   "expected length = "+l +" found = "+ updatedValue.length(),
                   l, updatedValue.length());
       
       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       println("========================================");
   }

   /**
    * Basically this test will do an insert using setBlob api -
    * select row from blobtbl and then insert a row in blobtbl 
    * and verify updated data in blobtbl
    * @param    ps  select statement from which blob is retrieved
    * @param    bloblen updating value is of length bloblen
    * @param    id  id of the row retrieved, for the update
    * @param    insertId  id of the row that is inserted
    * @param    expectedRows    to be updated
    */
   private static void selectInsertBlob(String testId, Connection conn,
           PreparedStatement ps,PreparedStatement ins, int bloblen, int id, int insertId,
           int expectedRows) throws Exception {
       println("========================================");
       println("START " + testId + " - select and then insert blob of size= "
               + bloblen + " - Uses getBlob api to do select and setBlob for insert");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       assertEquals("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in BLOBTBL with ID=" + id,
                           dlen,l);
       ins.setInt(1,insertId);
       ins.setInt(2,0);
       ins.setLong(3,l);
       ins.setBlob(4,value);
       
       // assert one row updated
       assertEquals(1,ins.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, insertId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Blob insertedValue = rs2.getBlob(1);
       assertEquals("FAIL - Retrieving the updated blob length does not match "+
               "expected length = "+l +" found = "+ insertedValue.length(),
               l, insertedValue.length());
       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       println("========================================");
   }


   /**
    * Basically this test will do an update using setBinaryStream api and verifies the
    * updated data.  select row from blobtbl2 and then update a row in blobtbl 
    * and verify updated data in blobtbl
    * @param    bloblen updating value is of length bloblen
    * @param    id  id of the row retrieved, for the update
    * @param    updateId  id of the row that is updated
    * @param    expectedRows    to be updated  
    * @param file  name of the file,against which the updated data is
    *              compared
    */
   private static void selectUpdateBlob2(String testId, Connection conn,
           PreparedStatement ps,PreparedStatement sel,int bloblen, int id, int updateId,
           int expectedRows,String file) throws Exception {
       println("========================================");
       println("START " + testId + " - select and then update blob of size= "
               + bloblen + " - Uses getBlob and setBlob  api");

       ResultSet rs = null;
       
       // retrieve row from blobtbl2
       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       assertEquals("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
               + dlen + " for row in BLOBTBL2 with ID=" + id, dlen, l);
       
       PreparedStatement psUpd = conn
               .prepareStatement("update BLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setBlob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);
       // assert 1 row updated
       assertEquals(1, psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       sel.setInt(1, updateId);
       ResultSet rs2 = sel.executeQuery();
       rs2.next();
       Blob updatedValue = rs2.getBlob(1);
       assertEquals("FAIL - MISMATCH length of updated blob value : expected="+
               l+" found ="+updatedValue.length(), l, updatedValue.length());
       compareBlobToFile(updatedValue.getBinaryStream(),file);

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       println("========================================");

   }

   private static void compareBlobToFile(InputStream lobstream, String filename)
           throws Exception {
       FileInputStream file = PrivilegedFileOpsForTests.getFileInputStream(new File(filename));
       int l = 0;
       int b = 0;
       do {
           l = lobstream.read();
           b = file.read();
           assertEquals("FAIL -- MISMATCH in data stored (" + l + ") versus"+
                   "data retrieved from file(" + b +"). ", b,l);
       } while (l != -1 && b != -1);
   }

   private static void deleteTable(Connection conn, PreparedStatement ps,
           int expectedRows) throws SQLException {
       int count = ps.executeUpdate();
       conn.commit();
       verifyTest(count, expectedRows, "Rows deleted =");
   }

   
   /**
    * insert clob
    * @param cloblen   length of clob to insert
    * @param start     id value for insert
    * @param rows      insert rows number of rows
    * @param streamLength  stream length passed to setCharacterStream(...,length)
    */
   private static void insertClob_SetCharacterStream(String testId,
           Connection conn, PreparedStatement ps, int cloblen, int start,
           int rows, int streamLength) throws SQLException {
       println("========================================");
       println("START " + testId + "  -insertClob of size = "
               + cloblen);

       long ST = 0;
       java.util.Random random = new java.util.Random();
       int count = 0;
       ST = System.currentTimeMillis();

       for (int i = start; i < start + rows; i++) {
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(3, cloblen);
           ps.setCharacterStream(4, new RandomCharReaderT(random, cloblen),
                   streamLength);
           count += ps.executeUpdate();
       }
       conn.commit();
       println("Insert Clob (" + cloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));
       verifyTest(count, rows, "Rows inserted with clob of size (" + cloblen
               + ") = ");
       println("========================================");

   }

  /**
    * insert clob, using a setClob api.
    * @param cloblen
    *            length of clob to insert
    * @param clob
    *            clob to insert
    * @param start
    *            start id value for insert
    * @param rows
    *            insert rows number of rows
    * @param expectedRows
    *            rows expected to be inserted
   */
    private static void insertClob_SetClob(String testId, Connection conn,
            PreparedStatement ps, java.sql.Clob clob, long cloblen, int start,
            int rows, int expectedRows) throws SQLException {
        println("========================================");
        println("START " + testId + "insertClob of size = " + cloblen);
        long ST = 0;
        ST = System.currentTimeMillis();
        int count = 0;

        for (int i = start; i < start + rows; i++) {
            ps.setInt(1, i);
            ps.setInt(2, 0);
            ps.setLong(3, cloblen);
            ps.setClob(4, clob);
            count += ps.executeUpdate();
        }
        conn.commit();
        println("Insert Clob (" + cloblen + ")" + " rows= " + count + " = "
                + (long) (System.currentTimeMillis() - ST));

        verifyTest(count, expectedRows, " Rows inserted with clob of size ("
                + cloblen + ") =");
        println("========================================");

    }

   /**
    * select from clob table
    * @param cloblen  select expects to retrieve a clob of this length
    * @param id       id of the row to retrieve
    * @param expectedRows number of rows expected to match id
    */
   private static void selectClob(String testId, Connection conn,
           PreparedStatement ps, int cloblen, int id, int expectedRows)
           throws SQLException {
       println("========================================");
       println("START " + testId + " - SELECT CLOB of size = "
               + cloblen);

       long ST = 0;
       int count = 0;
       ResultSet rs = null;
       ST = System.currentTimeMillis();

       ps.setInt(1, id);
       rs = ps.executeQuery();
       while (rs.next()) {
           count++;
           Clob value = rs.getClob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           assertEquals("FAIL - MISMATCH LENGTHS GOT " + l
                   + " expected " + dlen + " for row in CLOBTBL with ID="
                   + id, l, dlen);
           }
       conn.commit();
       
       println("Select Clob (" + cloblen + ")" + " rows= "
               + expectedRows + " = "
               + (long) (System.currentTimeMillis() - ST));


       verifyTest(count, expectedRows,
               "Matched rows selected with clob of size(" + cloblen + ") =");
       println("========================================");

   }

   /**
    * insert clob into CLOBTBL2
    * @param cloblen   length of clob to insert
    * @param start     id value for insert
    * @param rows      insert rows number of rows
    * @param streamLength  stream length passed to setCharacterStream(pos,reader,streamLength)
    * @param file       name of the file that has data to be inserted
    */
   private static void insertClob2(String testId, Connection conn,
           PreparedStatement ps, int cloblen, int start, int rows,
           int streamLength, String file) throws Exception {
       println("========================================");
       println("START " + testId + "insert Clob of size = "
               + cloblen);
       int count = 0;
       FileReader reader = null;
       long ST = 0;
       ST = System.currentTimeMillis();
        try {
            for (int i = start; i < start + rows; i++) {
                reader = PrivilegedFileOpsForTests
                        .getFileReader(new File(file));
                println("Got reader for file " + file + " " + reader);
                ps.setInt(1, i);
                ps.setInt(2, 0);
                ps.setLong(4, cloblen);
                ps.setCharacterStream(3, reader, streamLength);
                count += ps.executeUpdate();
            }
        } finally {
            reader.close();
            println("Closed reader for file " + file + " " + reader);
        }
       conn.commit();
 
       println("Insert Clob (" + cloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));
       verifyTest(count, rows, " Rows inserted with clob of size (" + cloblen
               + ") =");
       println("========================================");

   }

   /**
    * select from clob table (CLOBTBL2)
    * @param cloblen  select expects to retrieve a clob of this length
    * @param id       id of the row to retrieve
    * @param expectedRows number of rows expected to match id
    * @param file  filename to compare the retrieved data against
    */
   private static void selectClob2(String testId, Connection conn,
           PreparedStatement ps, int cloblen, int id, int expectedRows,
           String file) throws SQLException, Exception {
       println("========================================");
       println("START " + testId + " - SELECT CLOB of size = "
               + cloblen);

       long ST = 0;
       ResultSet rs = null;

       ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Clob value = rs.getClob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           assertEquals("FAIL - MISMATCH LENGTHS GOT " + l
                   + " expected " + dlen + " for row in CLOBTBL2 with ID="
                   + id, l, cloblen);
           compareClobToFile(value.getCharacterStream(), file, cloblen);
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with clob of size(" + cloblen + ") =");

       
       println("Select Clob (" + cloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
       println("========================================");
   }

   /*
    * Basically this test will do an update using setClob api -
    *  select row from clobtbl and then update a row in clobtbl 
    * and verify updated data in clobtbl 
    */    
   private static void selectUpdateClob(String testId, Connection conn,
           PreparedStatement ps, int cloblen, int id, int updateId,
           int expectedRows) throws Exception {
       println("========================================");
       println("START " + testId + " - select and then update clob of size= "
               + cloblen + " - Uses setClob api");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Clob value = rs.getClob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       assertEquals("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
               + dlen + " for row in CLOBTBL with ID=" + id, dlen, l);

       PreparedStatement psUpd = conn
               .prepareStatement("update CLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setCharacterStream(1, value.getCharacterStream(), (int) l);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       assertEquals(1,psUpd.executeUpdate());
       
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, updateId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Clob updatedValue = rs2.getClob(1);
       assertEquals("FAIL - Retrieving the updated clob length does not match "+
               "expected length = "+l +" found = "+ updatedValue.length(),l, 
               updatedValue.length());

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       println("========================================");
   }

   
   /*
    * Basically this test will do an update using setBlob api and verifies the
    * updated data.  select row from clobtbl2 and then update a row in clobtbl 
    * and verify updated data in clobtbl against the data in the original file
    */
   private static void selectUpdateClob2(String testId, Connection conn,
           PreparedStatement ps, PreparedStatement sel,int cloblen, int id, int updateId,
           int expectedRows,String file) throws Exception {
       println("========================================");
       println("START " + testId + " - select and then update clob of size= "
               + cloblen + " - Uses setClob api");

       ResultSet rs = null;
       
       // retrieve row from clobtbl2
       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Clob value = rs.getClob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       assertEquals("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
               + dlen + " for row in CLOBTBL2 with ID=" + id, dlen, l);

       PreparedStatement psUpd = conn
               .prepareStatement("update CLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setClob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       assertEquals(1, psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       sel.setInt(1, updateId);
       ResultSet rs2 = sel.executeQuery();
       rs2.next();
       Clob updatedValue = rs2.getClob(1);
       assertEquals("FAIL - MISMATCH length of updated clob value , found="+
                   updatedValue.length() +",expected = "+l, l, updatedValue.length());
       compareClobToFile(updatedValue.getCharacterStream(),file,(int)l);

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       println("========================================");

   }

   /*
    * Basically this test will do an update using updateClob api and verifies the
    * updated data.  select row from clobtbl2 and then update a row in clobtbl 
    * and verify updated data in clobtbl against the data in the original file
    * @param updateRowId    id of the row that needs to be updated
    */
   private static void updateClob2(String testId, Connection conn,PreparedStatement sel,
           int cloblen, int id, int updateRowId,int updateIdVal,
           int expectedRows,String file) throws Exception {
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then update clob of size= "
               + cloblen + " - Uses updateClob api");

       
       PreparedStatement ps1 = conn.prepareStatement("SELECT * FROM CLOBTBL FOR UPDATE", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
       PreparedStatement ps = conn.prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL2 where ID =?");
        
       ResultSet rs = null;
       ps.setInt(1,id);
       // retrieve row from clobtbl2
       rs = ps.executeQuery();
       rs.next();
       Clob value = rs.getClob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       if (dlen != l) {
           System.out
                   .println("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in CLOBTBL2 with ID=" + id);
       }
       
       ResultSet rs1 = ps1.executeQuery();
       while (rs1.next()) {
            if (rs1.getInt(1) == updateRowId)
            {
                rs1.updateClob(4, value);
                rs1.updateInt(1, updateIdVal);
                rs1.updateInt(2, 0);
                rs1.updateLong(3, dlen);
                rs1.updateRow();
                break;
            }
        }
       // close resultsets
       conn.commit();
       rs.close();
       rs1.close();
       ps1.close();
       ps.close();
       
       // verify
       // now select and verify that update went through ok.
       sel.setInt(1, updateIdVal);
       ResultSet rs2 = sel.executeQuery();
       rs2.next();
       Clob updatedValue = rs2.getClob(1);
       
       if (updatedValue.length() != l)
       {
           System.out.println("FAIL - MISMATCH length of updated clob value , found="+
                   updatedValue.length() +",expected = "+l);
       }
       else
           compareClobToFile(updatedValue.getCharacterStream(),file,(int)l);


       System.out.println("========================================");

   }

   
   private static void compareClobToFile(Reader lobstream, String filename,int length)
           throws Exception {
       FileReader file = PrivilegedFileOpsForTests.getFileReader(
                   new  File(filename));
       int c1 = 0;
       int c2 = 0;
       long count = 0;
       do {
           c1 = lobstream.read();
           c2 = file.read();
           assertEquals("FAIL -- MISMATCH in data stored versus data retrieved at " + 
                   count + " " + c1 + " does not match " + c2,
                   c2, c1);
           count++;
           length--;
       } while (c1 != -1 && c2 != -1 && length > 0);
       file.close();
   }

   private static void verifyTest(int affectedRows, int expectedRows,
           String test) {
       assertEquals("FAIL --" + test + affectedRows
               + " , but expected rows =" + expectedRows, expectedRows,
               affectedRows);
       println(test + affectedRows);
   }
   
   private static void writeToFile(String file,Reader r)
       throws IOException
   {
       // does file exist, if so delete and write to a fresh file
       File f =new File(file);
       if (PrivilegedFileOpsForTests.exists(f)) {
           assertTrue(PrivilegedFileOpsForTests.delete(f));
       }
       FileWriter writer = PrivilegedFileOpsForTests.getFileWriter(f);
       println("Got FileWriter for " + file + " " + writer);
       // write in chunks of 32k buffer
       char[] buffer = new char[32 * 1024];
       int count = 0;
       
       while((count = r.read(buffer)) >=0)
           writer.write(buffer,0,count);
       writer.flush();
       writer.close();
       println("writer " + writer + " for file " + file + " closed");
   }
}

/**
 * Class to generate random byte data
 */
class RandomByteStreamT extends java.io.InputStream {
   private long length;

   private java.util.Random dpr;

   RandomByteStreamT(java.util.Random dpr, long length) {
       this.length = length;
       this.dpr = dpr;

   }

   public int read() {
       if (length <= 0)
           return -1;

       length--;
       return (byte) (dpr.nextInt() >>> 25);
   }

   public int read(byte[] data, int off, int len) {

       if (length <= 0)
           return -1;

       if (len > length)
           len = (int)length;

       for (int i = 0; i < len; i++) {
           // chop off bits and return a +ve byte value.
           data[off + i] = (byte) (dpr.nextInt() >>> 25);
       }

       length -= len;
       return len;
   }
}

/*
 * Class to generate random char data, generates 1,2,3bytes character.
 */
class RandomCharReaderT extends java.io.Reader {
   private long length;
   private long numTrailingSpaces;

   private java.util.Random dpr;

   RandomCharReaderT(java.util.Random dpr, long length) {
       this.length = length;
       this.dpr = dpr;
       this.numTrailingSpaces = 0;
   }

   RandomCharReaderT(java.util.Random dpr, long length,long numTrailingSpaces) {
       this.length = length;
       this.dpr = dpr;
       this.numTrailingSpaces = numTrailingSpaces;
   }

   private int randomInt(int min, int max) {
       return dpr.nextInt(max - min) + min;
   }

   private char getChar() {
       // return space for trailing spaces.
       if (length <= numTrailingSpaces)
       {
          return ' ';
       }
          
       double drand = dpr.nextDouble();
       char c = 'a';
       if (drand < 0.25)
           c = (char) randomInt((int) 'A', (int) 'Z');
       else if (drand < 0.5)
           switch (randomInt(1, 10)) {
           case 1:
               c = '\u00c0';
               break;
           case 2:
               c = '\u00c1';
               break;
           case 3:
               c = '\u00c2';
               break;
           case 4:
               c = '\u00ca';
               break;
           case 5:
               c = '\u00cb';
               break;
           case 6:
               c = '\u00d4';
               break;
           case 7:
               c = '\u00d8';
               break;
           case 8:
               c = '\u00d1';
               break;
           case 9:
               c = '\u00cd';
               break;
           default:
               c = '\u00dc';
               break;
           }
       else if (drand < 0.75)
           c = (char) randomInt((int) 'a', (int) 'z');
       else if (drand < 1.0)
           switch (randomInt(1, 10)) {
           case 1:
               c = '\u00e2';
               break;
           case 2:
               c = '\u00e4';
               break;
           case 3:
               c = '\u00e7';
               break;
           case 4:
               c = '\u00e8';
               break;
           case 5:
               c = '\u00ec';
               break;
           case 6:
               c = '\u00ef';
               break;
           case 7:
               c = '\u00f6';
               break;
           case 8:
               c = '\u00f9';
               break;
           case 9:
               c = '\u00fc';
               break;
           default:
               c = '\u00e5';
               break;
           }

       return c;

   }

   public int read() {
       if (length <= 0)
           return -1;

       length--;
       return getChar();
   }

   public int read(char[] data, int off, int len) {

       if (length <= 0)
           return -1;

       if (len > length)
           len = (int)length;

       for (int i = 0; i < len; i++) {
           data[off + i] = getChar();
           length -= 1;
       }

       return len;
   }

   public void close() {

   }
}

/**
 * Class used to simulate a 4GB Clob implementation to 
 * check whether derby implements such large Clobs correctly.
 * Derby throws an error if the clob size exceeds 2GB
 **/

class ClobImplT implements java.sql.Clob {
  long length;
  Reader myReader;
 
  public ClobImplT(Reader myReader,long length) {
      this.length = length;
      this.myReader = myReader;
  }

  public long length() throws SQLException {
      return length;
  }

  public String getSubString(long pos, int length) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public java.io.Reader getCharacterStream() throws SQLException {
      return myReader;
  }

  public java.io.InputStream getAsciiStream() throws SQLException {
      throw new SQLException("Not implemented");
  }

  public long position(String searchstr, long start) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public long position(Clob searchstr, long start) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public int setString(long pos, String str) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public int setString(long pos, String str, int offset, int len) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public java.io.OutputStream setAsciiStream(long pos) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public java.io.Writer setCharacterStream(long pos) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public void truncate(long len) throws SQLException {
      throw new SQLException("Not implemented");
  }

  public void free() throws SQLException {
      throw new SQLException("Not implemented");
  }

  public Reader getCharacterStream(long pos, long length) throws SQLException {
      throw new SQLException("Not implemented");
  }

}

/***
 * Class to simulate a 4Gb blob impl in order to test if Derby
 * handles such large blobs correctly. The main methods here are
 * only the length() and the getBinaryStream(). Rest are just
 * placeholders/dummy methods in order to implement the java.sql.Blob
 * interface
 * ----
 * Derby throws an error if the blob length exceeds the max range of
 * int. 
 */
class BlobImplT implements java.sql.Blob
{
    long length;
    InputStream myStream;
    
    public BlobImplT(InputStream is, long length)
    {
        this.myStream = is;
        this.length = length;
    }
    public InputStream getBinaryStream()
    throws SQLException
    {
        return myStream;
    }
    
    public byte[] getBytes()
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    public long length()
    throws SQLException
    {
        return length;
    }
    
    public long position(Blob pattern,long start)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    
    public long position(byte[] pattern,long start)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    public OutputStream setBinaryStream(long pos)
    throws SQLException
    
    {
        throw new SQLException("Not implemented"); 
    }
    
    public int setBytes(long pos,byte[] bytes)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    public int setBytes(long pos,byte[] bytes,int offset,int len)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    
    public void truncate(long len)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    
    public byte[] getBytes(long pos, int length)
    throws SQLException
    {
        throw new SQLException("Not implemented"); 
    }
    
}
