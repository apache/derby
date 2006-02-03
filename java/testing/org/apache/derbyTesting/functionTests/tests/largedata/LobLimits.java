/*

Derby - Class org.apache.derbyTesting.functionTests.tests.largedata.LobLimits

Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.largedata;

import java.sql.*;
import java.io.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
* This test is part of the "largedata" suite because this test tests data for
* lobs to the limits ( ie blob and clob can be 2G-1 maximum) and so this test
* may take considerable disk space as well as time to run. Hence it is not part
* of the derbyall suite but should ideally be run at some intervals to test if
* no regression has occurred.
*/

public class LobLimits {

   static boolean trace = false;
   static final int _2GB = 2 * 1024 * 1024* 1024 - 1;
   static final int _100MB = 100 * 1024 * 1024;
   static final int MORE_DATA_THAN_COL_WIDTH= (_100MB)+1;
   static final int NUM_TRAILING_SPACES = 33*1024;
   
   static PreparedStatement insertBlob = null;
   static PreparedStatement selectBlob = null;
   static PreparedStatement insertClob = null;
   static PreparedStatement selectClob = null;
   static PreparedStatement deleteBlob = null;
   static PreparedStatement deleteClob = null;
   static PreparedStatement insertBlob2 = null;
   static PreparedStatement selectBlob2 = null;
   static PreparedStatement insertClob2 = null;
   static PreparedStatement selectClob2 = null;
   static PreparedStatement deleteBlob2 = null;
   static PreparedStatement deleteClob2 = null;

   static final String DATAFILE = "byteLobLimits.dat";

   static final String CHARDATAFILE = "charLobLimits.txt";

   /**
    * setup prepared statements and schema for the tests
    * @param conn
    * @throws SQLException
    */
   private void setup(Connection conn) throws SQLException {
       System.out.println("-----------------------------------");
       System.out.println(" START setup");

       conn.setAutoCommit(true);
       // Create a test table.
       Statement s = conn.createStatement();
       try {
           s.execute("DROP TABLE BLOBTBL");
       } catch (Exception e) {
       }
       try {
           s.execute("DROP TABLE CLOBTBL");
       } catch (Exception e) {
       }
       try {
           s.execute("DROP TABLE BLOBTBL2");
       } catch (Exception e) {
       }
       try {
           s.execute("DROP TABLE CLOBTBL2");
       } catch (Exception e) {
       }

       s.execute("CREATE TABLE BLOBTBL (ID INT NOT NULL PRIMARY KEY, "
               + "POS BIGINT, DLEN BIGINT, CONTENT BLOB(2G))");

       insertBlob = conn
               .prepareStatement("INSERT INTO BLOBTBL values (?,?,?,?)");

       s.execute("CREATE TABLE CLOBTBL (ID INT NOT NULL PRIMARY KEY,"
               + "POS BIGINT, DLEN BIGINT, CONTENT CLOB(2G))");

       insertBlob = conn
               .prepareStatement("INSERT INTO BLOBTBL values (?,?,?,?)");
       selectBlob = conn
               .prepareStatement("SELECT CONTENT,DLEN FROM BLOBTBL WHERE ID = ?");

       insertClob = conn
               .prepareStatement("INSERT INTO CLOBTBL values (?,?,?,?)");

       selectClob = conn
               .prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL WHERE ID = ?");

       deleteBlob = conn.prepareStatement("DELETE FROM BLOBTBL");
       deleteClob = conn.prepareStatement("DELETE  from CLOBTBL");

       s.execute("CREATE TABLE BLOBTBL2 (ID INT NOT NULL PRIMARY KEY, "
               + "POS BIGINT, CONTENT BLOB("+_100MB+"),DLEN BIGINT)");

       insertBlob2 = conn
               .prepareStatement("INSERT INTO BLOBTBL2 values (?,?,?,?)");

       // Please dont change the clob column width,since tests use this width to 
       // test for truncation of trailing spaces.
       s.execute("CREATE TABLE CLOBTBL2 (ID INT NOT NULL PRIMARY KEY,"
               + "POS BIGINT, CONTENT CLOB("+_100MB+"), DLEN BIGINT)");

       insertBlob2 = conn
               .prepareStatement("INSERT INTO BLOBTBL2 values (?,?,?,?)");
       selectBlob2 = conn
               .prepareStatement("SELECT CONTENT,DLEN FROM BLOBTBL2 WHERE ID = ?");

       insertClob2 = conn
               .prepareStatement("INSERT INTO CLOBTBL2 values (?,?,?,?)");

       selectClob2 = conn
               .prepareStatement("SELECT CONTENT,DLEN FROM CLOBTBL2 WHERE ID = ?");
       System.out.println("-----------------------------------");
       System.out.println(" END setup");

       deleteBlob2 = conn.prepareStatement("DELETE FROM BLOBTBL2");
       deleteClob2 = conn.prepareStatement("DELETE  from CLOBTBL2");
       System.out.println("-----------------------------------");
       System.out.println(" END setup");

   }

   /**
    * Create an instance of this class and do the test.
    */
   public static void main(String[] args) {
       //trace = Boolean.getBoolean("trace");
       new LobLimits().runTests(args);

   }

   /**
    * Create a JDBC connection using the arguments passed in from the harness,
    * and then run the LOB tests.
    * 
    * @param args
    *            Arguments from the harness.
    */
   public void runTests(String[] args) {
       Connection conn = null;
       try {

           // use the ij utility to read the property file and
           // make the initial connection.
           ij.getPropertyArg(args);
           conn = ij.startJBMS();

           // do the initial setup,drop and create tables
           // prepare stmts
           setup(conn);

           conn.setAutoCommit(false);

           clobTests(conn);
           blobTests(conn);
           //cleanup
           cleanup(conn);
       } catch (Exception e) {
           System.out.println("FAIL -- Unexpected exception:");
           e.printStackTrace(System.out);
       }
   }

   /**
    * Close all prepared statements and connection
    * @param conn
    * @throws Exception
    */
   private void cleanup(Connection conn) throws Exception {
       insertBlob.close();
       selectBlob.close();
       selectClob.close();
       insertClob.close();
       deleteClob.close();
       deleteBlob.close();
       insertBlob2.close();
       selectBlob2.close();
       selectClob2.close();
       insertClob2.close();
       deleteBlob2.close();
       deleteClob2.close();
       conn.close();
       new File(DATAFILE).delete();
       new File(CHARDATAFILE).delete();
   }

   /**
    * tests specific for blobs
    * @param conn
    * @throws Exception
    */
   private static void blobTests(Connection conn) throws Exception {

       try {
           // Test - 2Gb blob ( actually it is 2gb -1)
           // Note with setBinaryStream interface the maximum size for the
           // stream, can be max value for an int.
           // Also note, that lobs in derby currently  supports
           // maximum size of 2gb -1

           // first do insert blob of 2g, 2 rows
           insertBlob_SetBinaryStream("BlobTest #1", conn, insertBlob, _2GB,
                   0, 2, _2GB);
           // do a select to see if the inserts in test above went ok
           selectBlob("BlobTest #2", conn, selectBlob, _2GB, 0, 1);
           selectBlob("BlobTest #3", conn, selectBlob, _2GB, 1, 1);
           
           // now do a select of one of the 2gb rows and update another 2g row 
           // using the setBlob api, updated blob is of length 2gb
           // Fix for Bug entry -DERBY-599[setBlob should not materialize blob
           // into memory]
           selectUpdateBlob("BlobTest #4",conn,selectBlob,_2GB,0,1,1);
           // select row from blobtbl and then do insert into the blobtbl
           // using setBlob
           selectInsertBlob("BlobTest #4.1",conn,selectBlob,insertBlob,_2GB,0,3,1);
                              
           // Test - generate random data, write to a file, use it to insert
           // data into blob and then read back and compare if all is ok
           // currently in fvt ( derbyall), tests check for substrings etc and 
           // for small amounts of data.  This test will test for 100mb of blob data

           FileOutputStream fos = new FileOutputStream(DATAFILE);
           RandomByteStream r = new RandomByteStream(new java.util.Random(),
                   _100MB);
           // write in chunks of 32k buffer
           byte[] buffer = new byte[32 * 1024];
           int count = 0;
           
           while((count=r.read(buffer))>=0)
               fos.write(buffer,0,count);

           fos.flush();
           fos.close();

           insertBlob2("BlobTest #5.1 ", conn, insertBlob2, _100MB, 0, 1,
                   _100MB, DATAFILE);
           selectBlob2("BlobTest #5.2 ", conn, selectBlob2, _100MB, 0, 1,
                   DATAFILE);
           
           
           // update the 2gb row in blobtbl with the 100mb data and compare if the update
           // went ok. 
           selectUpdateBlob2("BlobTest #6",conn,selectBlob2,selectBlob,_100MB,0,1,1,DATAFILE);
                       
           deleteTable(conn, deleteBlob2, 1);
           
       } catch (Exception e) {
           System.out.println("FAIL -- Unexpected exception:");
           e.printStackTrace(System.out);
       }

       conn.commit();
       
       deleteTable(conn, deleteBlob, 3);

       // Negative Test, use setBlob api to insert a 4GB blob.
       long _4GB =  4*1024*1024*(1024L);
       BlobImpl _4GbBlob = new BlobImpl(new RandomByteStream(new java.util.Random(),_4GB),_4GB);

       try
       {
           insertBlob_SetBlob("BlobTest #7 (setBlob with 4Gb blob",conn,insertBlob,_4GbBlob,
                   _4GB,0,1,0);
       }
       catch(SQLException sqle)
       {
           System.out.println("DERBY DOES NOT SUPPORT INSERT OF 4GB BLOB ");
           expectedException(sqle);
       }
       // ADD  NEW TESTS HERE
   }

   /**
    * tests using clobs
    * @param conn
    * @throws Exception
    */
   private static void clobTests(Connection conn) throws Exception {
       try {
           // Test - 2Gb blob
           // Note with setCharacterStream interface the maximum size for the
           // stream has to be max value for a int which is (2GB -1 )
           // first do insert clob of 2g, 2 rows
           insertClob_SetCharacterStream("ClobTest #1", conn, insertClob,
                   _2GB, 0, 2, _2GB);
           // do a select to see if the inserts in test above went ok
           selectClob("ClobTest #2", conn, selectClob, _2GB, 0, 1);
           selectClob("ClobTest #3", conn, selectClob, _2GB, 0, 1);
           // do a select and then update a row of 2gb size: uses getClob
           selectUpdateClob("ClobTest #4",conn,selectClob,_2GB,0,1,1);
           

           // Test - generate random data, write to a file, use it to insert
           // data into clob and then read back and compare if all is ok
           // currently in fvt ( derbyall), tests check for substrings etc and 
           // for small amounts of data.  This test will test for 100mb of clob data
           writeToFile(CHARDATAFILE,new RandomCharReader(new java.util.Random(),_100MB));
           insertClob2("ClobTest #5.1 ", conn, insertClob2, _100MB, 0, 1,
                   _100MB, CHARDATAFILE);
           selectClob2("ClobTest #5.2 ", conn, selectClob2, _100MB, 0, 1,
                   CHARDATAFILE);

           // Disabled for now, this will materialize, will open 
           // jira for it.
           //updateClob2("ClobTest #8.1",conn,selectClob,_100MB,0,0,10,1,CHARDATAFILE);

           // update the 2gb row in clobtbl with the 100mb data and compare if the update
           // went ok.
           selectUpdateClob2("ClobTest #8.2",conn,selectClob2,selectClob,_100MB,0,1,1,CHARDATAFILE);

           // test for trailing space truncation
           // insert 100mb+33k of data which has 33k of trailing space,
           // into a column of 100mb
           // insert should be successful, select should retrieve 100mb of data
           
           // Generate random data and write to a file, this file will be used
           // in the verification process after inserts and updates.
           writeToFile(CHARDATAFILE,new RandomCharReader(new java.util.Random(),
                   (NUM_TRAILING_SPACES +_100MB),NUM_TRAILING_SPACES));
           insertClob2("ClobTest #6.1 ", conn, insertClob2,_100MB, 3, 1,
                   (NUM_TRAILING_SPACES +_100MB), CHARDATAFILE);
           // select will retrieve data and verify the data inserted. 
           selectClob2("ClobTest #6.2 ", conn, selectClob2, _100MB, 3, 1,
                   CHARDATAFILE);

           negativeSpaceTruncationTest("ClobTest #7",conn);
           
           // Test - for stream contains a trailing non-space character
           // insert should throw an error
           writeToFile(CHARDATAFILE,new RandomCharReader(new java.util.Random(),MORE_DATA_THAN_COL_WIDTH));
           try
           {
               insertClob2("ClobTest #9.1 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH, CHARDATAFILE);
           }catch(SQLException sqle)
           {
               System.out.println("NEGATIVE TEST - Expected Exception: truncation of non-blanks not allowed");
               expectedException(sqle);
           }
           // no row must be retrieved.
           selectClob2("ClobTest #9.2 ", conn, selectClob2, _100MB, 4, 0,
                   CHARDATAFILE);

           try
           {
               insertClob2("ClobTest #10 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH +1 , CHARDATAFILE);
           }catch(SQLException sqle)
           {
               System.out.println("NEGATIVE TEST - Expected Exception: truncation of non-blanks not allowed and"+
                       " stream length is one greater than actual length of the stream ");
               expectedException(sqle);
           }

           try
           {
               insertClob2("ClobTest #11 ", conn, insertClob2,MORE_DATA_THAN_COL_WIDTH, 4, 1,
                       MORE_DATA_THAN_COL_WIDTH -1 , CHARDATAFILE);
           }catch(SQLException sqle)
           {
               System.out.println("NEGATIVE TEST - Expected Exception: truncation of non-blanks not allowed and"+
                       " stream length is one less than actual length of the stream ");
               expectedException(sqle);
           }
           deleteTable(conn, deleteClob2, 2);
       } catch (Exception e) {
           System.out.println("FAIL -- Unexpected exception:");
           e.printStackTrace(System.out);
       }

       try {
           // give -ve streamlength
           insertClob_SetCharacterStream("ClobTest #12.1", conn, insertClob,
                   _100MB, 4, 1, -1);
       } catch (SQLException sqle) {
           System.out.println("NEGATIVE TEST - Expected Exception:");
           expectedException(sqle);
       }

       selectClob("ClobTest #12.2", conn, selectClob,_100MB, 4, 0);

       deleteTable(conn, deleteClob, 2);
       
       // Negative tests use the setClob API to insert a 4GB clob

       long _4GB =  4*1024*1024*(1024L);

       ClobImpl _4GBClob = new ClobImpl(new RandomCharReader(new java.util.Random(),_4GB),_4GB);		

       try
       {
           insertClob_SetClob("ClobTest #13 (setClob with 4Gb clob",conn,insertClob,_4GBClob,
                   _4GB,0,1,0);
       }
       catch(SQLException sqle)
       {
           System.out.println("DERBY DOES NOT SUPPORT INSERT OF 4GB CLOB ");
           expectedException(sqle);
       }

       // ADD NEW TESTS HERE
   }

   private static void negativeSpaceTruncationTest(String msg,Connection conn)
       throws Exception
   {
       // Negative test, stream has trailing spaces but the stream length is one 
       // more than the actual length of the stream
       try
       {
           insertClob2(msg, conn, insertClob2,_100MB, 4, 1,
               (NUM_TRAILING_SPACES +_100MB - 1), CHARDATAFILE);
       }catch(SQLException sqle)
       {
           System.out.println("EXPECTED EXCEPTION - stream has trailing spaces,but stream "+
                   " length is 1 less than actual length of stream");
           expectedException(sqle);
       }

       try
       {
           insertClob2(msg, conn, insertClob2,_100MB, 5, 1,
               (NUM_TRAILING_SPACES +_100MB + 1), CHARDATAFILE);
       }catch(SQLException sqle)
       {
           System.out.println("EXPECTED EXCEPTION - stream has trailing spaces,but stream "+
                   " length is 1 greater than actual length of stream");
           expectedException(sqle);
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
       System.out.println("========================================");
       System.out.println("START " + testId + "insertBlob of size = "
               + bloblen);
       long ST = 0;
       if (trace)
           ST = System.currentTimeMillis();

       int count = 0;
       java.util.Random random = new java.util.Random();
       for (int i = start; i < start + rows; i++) {
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(3, bloblen);
           ps.setBinaryStream(4, new RandomByteStream(random, bloblen),
                   streamLength);
           count += ps.executeUpdate();
       }
       conn.commit();
       if (trace) {
           System.out.println("Insert Blob (" + bloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));

       }
       verifyTest(count, rows, " Rows inserted with blob of size (" + bloblen
               + ") =");
       System.out.println("========================================");

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
        System.out.println("========================================");
        System.out.println("START " + testId + "insertBlob of size = "
                + bloblen);
        long ST = 0;
        if (trace)
            ST = System.currentTimeMillis();
        int count = 0;

        try {
            
            for (int i = start; i < start + rows; i++) {
                ps.setInt(1, i);
                ps.setInt(2, 0);
                ps.setLong(3, bloblen);
                ps.setBlob(4, blob);
                count += ps.executeUpdate();
            }
            conn.commit();
            if (trace) {
                System.out.println("Insert Blob (" + bloblen + ")" + " rows= "
                        + count + " = "
                        + (long) (System.currentTimeMillis() - ST));

            }
        } catch (SQLException e) {
            verifyTest(count, expectedRows,
                    " Rows inserted with blob of size (" + bloblen + ") =");
            System.out.println("========================================");
            throw e;
        }

        verifyTest(count, expectedRows,
                " Rows inserted with blob of size (" + bloblen + ") =");
        System.out.println("========================================");

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
       System.out.println("========================================");
       System.out.println("START " + testId + " - SELECT BLOB of size = "
               + bloblen);

       long ST = 0;
       ResultSet rs = null;

       if (trace)
           ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Blob value = rs.getBlob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           if (dlen != l) {
               System.out.println("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in BLOBTBL with ID="
                       + id);
           }
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with blob of size(" + bloblen + ") =");

       if (trace) {
           System.out.println("Select Blob (" + bloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
       }
       System.out.println("========================================");
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
       System.out.println("========================================");
       System.out.println("START " + testId + "insert Blob of size = "
               + bloblen);
       int count = 0;
       java.util.Random random = new java.util.Random();
       FileInputStream fis = null;

       long ST = 0;
       if (trace)
           ST = System.currentTimeMillis();

       for (int i = start; i < start + rows; i++) {
           fis = new FileInputStream(file);
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(4, bloblen);
           ps.setBinaryStream(3, fis, streamLength);
           count += ps.executeUpdate();
           fis.close();
       }
       conn.commit();
       if (trace) {
           System.out.println("Insert Blob (" + bloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));

       }
       verifyTest(count, rows, " Rows inserted with blob of size (" + bloblen
               + ") =");
       System.out.println("========================================");

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
       System.out.println("========================================");
       System.out.println("START " + testId + " - SELECT BLOB of size = "
               + bloblen);

       long ST = 0;
       ResultSet rs = null;

       if (trace)
           ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Blob value = rs.getBlob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           if (dlen != l) {
               System.out.println("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in BLOBTBL with ID="
                       + id);
           } else
               compareBlobToFile(value.getBinaryStream(), file);
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with blob of size(" + bloblen + ") =");

       if (trace) {
           System.out.println("Select Blob (" + bloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
       }
       System.out.println("========================================");
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
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then update blob of size= "
               + bloblen + " - Uses getBlob api");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       if (dlen != l) {
           System.out
                   .println("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in BLOBTBL with ID=" + id);
       }

       PreparedStatement psUpd = conn
               .prepareStatement("update BLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setBlob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       System.out.println("Rows Updated = " + psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, updateId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Blob updatedValue = rs2.getBlob(1);

       if(updatedValue.length() != l)
           System.out.println("FAIL - Retrieving the updated blob length does not match "+
                   "expected length = "+l +" found = "+ updatedValue.length());

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       System.out.println("========================================");
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
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then insert blob of size= "
               + bloblen + " - Uses getBlob api to do select and setBlob for insert");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       if (dlen != l) {
           System.out
                   .println("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in BLOBTBL with ID=" + id);
       }
       
       ins.setInt(1,insertId);
       ins.setInt(2,0);
       ins.setLong(3,l);
       ins.setBlob(4,value);
       
       System.out.println("Rows Updated = " + ins.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, insertId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Blob insertedValue = rs2.getBlob(1);

       if(insertedValue.length() != l)
           System.out.println("FAIL - Retrieving the updated blob length does not match "+
                   "expected length = "+l +" found = "+ insertedValue.length());

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       System.out.println("========================================");
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
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then update blob of size= "
               + bloblen + " - Uses getBlob and setBlob  api");

       ResultSet rs = null;
       
       // retrieve row from blobtbl2
       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Blob value = rs.getBlob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       if (dlen != l) {
           System.out
                   .println("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in BLOBTBL2 with ID=" + id);
       }

       PreparedStatement psUpd = conn
               .prepareStatement("update BLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setBlob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       System.out.println("Rows Updated = " + psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       sel.setInt(1, updateId);
       ResultSet rs2 = sel.executeQuery();
       rs2.next();
       Blob updatedValue = rs2.getBlob(1);
       
       if (updatedValue.length() != l)
       {
           System.out.println("FAIL - MISMATCH length of updated blob value : expected="+
                   l+" found ="+updatedValue.length());
       }
       else
           compareBlobToFile(updatedValue.getBinaryStream(),file);

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       System.out.println("========================================");

   }

   private static void compareBlobToFile(InputStream lobstream, String filename)
           throws Exception {
       FileInputStream file = new FileInputStream(filename);
       int l = 0;
       int b = 0;
       do {
           l = lobstream.read();
           b = file.read();
           if (l != b)
           {
               System.out.println("FAIL -- MISMATCH in data stored versus"+
                       "data retrieved");
               break;
           }
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
       System.out.println("========================================");
       System.out.println("START " + testId + "  -insertClob of size = "
               + cloblen);

       long ST = 0;
       java.util.Random random = new java.util.Random();
       int count = 0;
       if (trace)
           ST = System.currentTimeMillis();

       for (int i = start; i < start + rows; i++) {
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(3, cloblen);
           ps.setCharacterStream(4, new RandomCharReader(random, cloblen),
                   streamLength);
           count += ps.executeUpdate();
       }
       conn.commit();
       if (trace) {
           System.out.println("Insert Clob (" + cloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));

       }
       verifyTest(count, rows, "Rows inserted with clob of size (" + cloblen
               + ") = ");
       System.out.println("========================================");

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
        System.out.println("========================================");
        System.out.println("START " + testId + "insertClob of size = "
                + cloblen);
        long ST = 0;
        if (trace)
           ST = System.currentTimeMillis();
        int count = 0;

        try {
            
            for (int i = start; i < start + rows; i++) {
                ps.setInt(1, i);
                ps.setInt(2, 0);
                ps.setLong(3, cloblen);
                ps.setClob(4, clob);
                count += ps.executeUpdate();
            }
            conn.commit();
            if (trace) {
                System.out.println("Insert Clob (" + cloblen + ")" + " rows= "
                        + count + " = "
                        + (long) (System.currentTimeMillis() - ST));

            }
        } catch (SQLException e) {
            verifyTest(count, expectedRows,
                    " Rows inserted with clob of size (" + cloblen + ") =");
            System.out.println("========================================");
            throw e;
        }

        verifyTest(count, expectedRows,
                " Rows inserted with clob of size (" + cloblen + ") =");
        System.out.println("========================================");

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
       System.out.println("========================================");
       System.out.println("START " + testId + " - SELECT CLOB of size = "
               + cloblen);

       long ST = 0;
       int count = 0;
       ResultSet rs = null;
       if (trace)
           ST = System.currentTimeMillis();

       ps.setInt(1, id);
       rs = ps.executeQuery();
       while (rs.next()) {
           count++;
           Clob value = rs.getClob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           if (dlen != l) {
               System.out.println("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in CLOBTBL with ID="
                       + id);
           }

       }
       conn.commit();
       if (trace) {
           System.out.println("Select Clob (" + cloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));

       }

       verifyTest(count, expectedRows,
               "Matched rows selected with clob of size(" + cloblen + ") =");
       System.out.println("========================================");

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
       System.out.println("========================================");
       System.out.println("START " + testId + "insert Clob of size = "
               + cloblen);
       int count = 0;
       FileReader reader = null;
       long ST = 0;
       if (trace)
           ST = System.currentTimeMillis();

       for (int i = start; i < start + rows; i++) {
           reader = new FileReader(file);
           ps.setInt(1, i);
           ps.setInt(2, 0);
           ps.setLong(4, cloblen);
           ps.setCharacterStream(3, reader, streamLength);
           count += ps.executeUpdate();
           reader.close();
       }
       conn.commit();
       if (trace) {
           System.out.println("Insert Clob (" + cloblen + ")" + " rows= "
                   + count + " = " + (long) (System.currentTimeMillis() - ST));

       }
       verifyTest(count, rows, " Rows inserted with clob of size (" + cloblen
               + ") =");
       System.out.println("========================================");

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
       System.out.println("========================================");
       System.out.println("START " + testId + " - SELECT CLOB of size = "
               + cloblen);

       long ST = 0;
       ResultSet rs = null;

       if (trace)
           ST = System.currentTimeMillis();

       int count = 0;
       ps.setInt(1, id);
       rs = ps.executeQuery();

       while (rs.next()) {
           count++;
           Clob value = rs.getClob(1);
           long l = value.length();
           long dlen = rs.getLong(2);
           if (cloblen != l) {
               System.out.println("FAIL - MISMATCH LENGTHS GOT " + l
                       + " expected " + dlen + " for row in CLOBTBL2 with ID="
                       + id);
           } else
               compareClobToFile(value.getCharacterStream(), file, cloblen);
       }
       conn.commit();

       verifyTest(count, expectedRows,
               "Matched rows selected with clob of size(" + cloblen + ") =");

       if (trace) {
           System.out.println("Select Clob (" + cloblen + ")" + " rows= "
                   + expectedRows + " = "
                   + (long) (System.currentTimeMillis() - ST));
       }
       System.out.println("========================================");
   }

   /*
    * Basically this test will do an update using setClob api -
    *  select row from clobtbl and then update a row in clobtbl 
    * and verify updated data in clobtbl 
    */    
   private static void selectUpdateClob(String testId, Connection conn,
           PreparedStatement ps, int cloblen, int id, int updateId,
           int expectedRows) throws Exception {
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then update clob of size= "
               + cloblen + " - Uses setClob api");

       ResultSet rs = null;

       ps.setInt(1, id);
       rs = ps.executeQuery();
       rs.next();
       Clob value = rs.getClob(1);
       long l = value.length();
       long dlen = rs.getLong(2);
       if (dlen != l) {
           System.out
                   .println("FAIL - MISMATCH LENGTHS GOT " + l + " expected "
                           + dlen + " for row in CLOBTBL with ID=" + id);
       }

       PreparedStatement psUpd = conn
               .prepareStatement("update CLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setCharacterStream(1, value.getCharacterStream(), (int) l);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       System.out.println("Rows Updated = " + psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       ps.setInt(1, updateId);
       ResultSet rs2 = ps.executeQuery();
       rs2.next();
       Clob updatedValue = rs2.getClob(1);

       if(updatedValue.length() != l)
           System.out.println("FAIL - Retrieving the updated clob length does not match "+
                   "expected length = "+l +" found = "+ updatedValue.length());

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       System.out.println("========================================");
   }

   
   /*
    * Basically this test will do an update using setBlob api and verifies the
    * updated data.  select row from clobtbl2 and then update a row in clobtbl 
    * and verify updated data in clobtbl against the data in the original file
    */
   private static void selectUpdateClob2(String testId, Connection conn,
           PreparedStatement ps, PreparedStatement sel,int cloblen, int id, int updateId,
           int expectedRows,String file) throws Exception {
       System.out.println("========================================");
       System.out.println("START " + testId + " - select and then update clob of size= "
               + cloblen + " - Uses setClob api");

       ResultSet rs = null;
       
       // retrieve row from clobtbl2
       ps.setInt(1, id);
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

       PreparedStatement psUpd = conn
               .prepareStatement("update CLOBTBL set content=?,dlen =? where id = ?");
       psUpd.setClob(1,value);
       psUpd.setLong(2, l);
       psUpd.setInt(3, updateId);

       System.out.println("Rows Updated = " + psUpd.executeUpdate());
       conn.commit();

       // now select and verify that update went through ok.
       sel.setInt(1, updateId);
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

       // close resultsets
       conn.commit();
       rs.close();
       rs2.close();
       psUpd.close();
       System.out.println("========================================");

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
       FileReader file = new FileReader(filename);
       int c1 = 0;
       int c2 = 0;
       long count = 0;
       do {
           c1 = lobstream.read();
           c2 = file.read();
           if (c1 != c2)
           {
               System.out.println("FAIL -- MISMATCH in data stored versus data retrieved at " + count);
               break;
           }
           count++;
           length--;
       } while (c1 != -1 && c2 != -1 && length > 0);
   }

   private static void expectedException(SQLException sqle) {

       while (sqle != null) {
           String sqlState = sqle.getSQLState();
           if (sqlState == null) {
               sqlState = "<NULL>";
           }
           System.out.println("EXPECTED SQL Exception: (" + sqlState + ") "
                   + sqle.getMessage());

           sqle = sqle.getNextException();
       }
   }

   private static void verifyTest(int affectedRows, int expectedRows,
           String test) {
       if (affectedRows != expectedRows)
           System.out.println("FAIL --" + test + affectedRows
                   + " , but expected rows =" + expectedRows);
       else
           System.out.println(test + affectedRows);
   }
   
   private static void writeToFile(String file,Reader r)
       throws IOException
   {
       // does file exist, if so delete and write to a fresh file
       File f =new File(file);
       if (f.exists())
           f.delete();
       FileWriter writer = new FileWriter(file);
       // write in chunks of 32k buffer
       char[] buffer = new char[32 * 1024];
       int count = 0;
       
       while((count = r.read(buffer)) >=0)
           writer.write(buffer,0,count);
       writer.flush();
       writer.close();
   }
}

/**
 * Class to generate random byte data
 */
class RandomByteStream extends java.io.InputStream {
   private long length;

   private java.util.Random dpr;

   RandomByteStream(java.util.Random dpr, long length) {
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
class RandomCharReader extends java.io.Reader {
   private long length;
   private long numTrailingSpaces;

   private java.util.Random dpr;

   RandomCharReader(java.util.Random dpr, long length) {
       this.length = length;
       this.dpr = dpr;
       this.numTrailingSpaces = 0;
   }

   RandomCharReader(java.util.Random dpr, long length,long numTrailingSpaces) {
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

class ClobImpl implements java.sql.Clob {
  long length;
  Reader myReader;
 
  public ClobImpl(Reader myReader,long length) {
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
class BlobImpl implements java.sql.Blob
{
    long length;
    InputStream myStream;
    
    public BlobImpl(InputStream is, long length)
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
