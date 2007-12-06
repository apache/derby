/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.bug3238

   Copyright 2007 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.io.IOException;
import java.io.Reader;

/**
 * Test fix for triggers on tables with lobs
 */
public class bug3238 {
 
  public static void main (String args[]) 
  { 
    try {
	/* Load the JDBC Driver class */
	// use the ij utility to read the property file and
	// make the initial connection.
	ij.getPropertyArg(args);
	Connection conn = ij.startJBMS();
	testBlobInTriggerTable(conn,1024);
	testBlobInTriggerTable(conn,16384);
	testBlobInTriggerTable(conn,1024 *32 -1);
	testBlobInTriggerTable(conn,1024 *32);
	testBlobInTriggerTable(conn,1024 *32+1);
	testBlobInTriggerTable(conn,1024 *64 -1);
	testBlobInTriggerTable(conn,1024 *64);
	testBlobInTriggerTable(conn,1024 *64+1);

	testClobInTriggerTable(conn,1024);
	testClobInTriggerTable(conn,16384);
	testClobInTriggerTable(conn,1024 *32 -1);
	testClobInTriggerTable(conn,1024 *32);
	testClobInTriggerTable(conn,1024 *32+1);
	testClobInTriggerTable(conn,1024 *64 -1);
	testClobInTriggerTable(conn,1024 *64);
	testClobInTriggerTable(conn,1024 *64+1);
	testUpdateTriggerOnClobColumn(conn);

        // no exception. we passed
        System.out.println("PASSED");
	conn.close();
    } catch (Exception e) {
		System.out.println("FAIL -- unexpected exception "+e);
		JDBCDisplayUtil.ShowException(System.out, e);
      	e.printStackTrace();
    }
  } 
   /**
     * Create a table with after update trigger on non-lob column.
     * Insert two blobs of size blobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testBlobInTriggerTable
     * 
     * @param conn connection to use
     * @param blobSize  size of blob to test.
     * @throws SQLException
     * @throws IOException
     */
    private static void testBlobInTriggerTable(Connection conn, int blobSize) throws SQLException, IOException {
    	

        String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = conn.createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), b_lob BLOB(50M), b_lob2 BLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        conn.commit();      

    	// --- add a blob
        PreparedStatement ps = conn.prepareStatement("INSERT INTO LOB1 VALUES (?, ?, ?)");
        
        ps.setString(1, blobSize +"");


        byte[] arr = new byte[blobSize];
        for (int i = 0; i < arr.length; i++)
            arr[i] = (byte)8;

        // - set the value of the input parameter to the input stream
        // use a couple blobs so we are sure it works with multiple lobs
        ps.setBinaryStream(2, new ByteArrayInputStream(arr) , blobSize);
        ps.setBinaryStream(3, new ByteArrayInputStream(arr) , blobSize);
        ps.execute();
        
        conn.commit();
        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
   
    }

 /**
     * Create a table with after update trigger on non-lob column.
     * Insert clob of size clobSize into table and perform update
     * on str1 column to fire trigger. Helper method called from 
     * testClobInTriggerTable
     * @param conn Connection
     * @param clobSize size of clob to test
     * @throws SQLException
     * @throws IOException
     */
    private static void testClobInTriggerTable(Connection conn,int clobSize) throws SQLException, IOException {
    	
    	// --- add a clob
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
        trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
        trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";

        Statement s = conn.createStatement();
        
        s.executeUpdate("create table LOB1 (str1 Varchar(80), c_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        conn.commit();      

       PreparedStatement ps = conn.prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        
        ps.setString(1, clobSize +"");


        char[] arr = new char[clobSize];
        for (int i = 0; i < arr.length; i++)
            arr[i] = 'a';

        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2, new CharArrayReader(arr) , clobSize);
        ps.execute();
        conn.commit();

        // Now executing update to fire trigger
        s.executeUpdate("update LOB1 set str1 = str1 || ' '");
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
    }


 /* 
     * Test an update trigger on a Clob column
     * 
     * @param conn Connection to use
     */
    private static void testUpdateTriggerOnClobColumn(Connection conn) throws SQLException, IOException, Exception
    {
    	Statement s = conn.createStatement();
    	String trig = " create trigger t_lob1 after update of str1 on lob1 ";
    	trig = trig + " REFERENCING OLD AS old NEW AS new FOR EACH ROW MODE DB2SQL ";
    	trig = trig + " insert into t_lob1_log(oldvalue, newvalue) values (old.str1, new.str1)";
    	s.executeUpdate("create table LOB1 (str1 Varchar(80), C_lob CLOB(50M))");
        s.executeUpdate("create table t_lob1_log(oldvalue varchar(80), newvalue varchar(80), chng_time timestamp default current_timestamp)");
        s.executeUpdate(trig);
        conn.commit();
        PreparedStatement ps = conn.prepareStatement("INSERT INTO LOB1 VALUES (?, ?)");
        int clobSize = 1024*64+1;
        ps.setString(1, clobSize +"");


        // - set the value of the input parameter to the input stream
        ps.setCharacterStream(2, makeCharArrayReader('a', clobSize), clobSize);
        ps.execute();
        conn.commit();


        PreparedStatement ps2 = conn.prepareStatement("update LOB1 set c_lob = ? where str1 = '" + clobSize + "'");
        ps2.setCharacterStream(1,makeCharArrayReader('b',clobSize), clobSize);
        ps2.executeUpdate();
        conn.commit();
        // 	--- reading the clob make sure it was updated
        ResultSet rs = s.executeQuery("SELECT * FROM LOB1 where str1 = '" + clobSize + "'");
        rs.next();
     
        Reader r = rs.getCharacterStream(2);
        int count = 0;
        int c;
        do {
        	c = r.read();        	 
        	if (c!= -1)
        	{
        		count++;
        		assertEquals('b',c);
        	}	
        } while (c != -1);
          
        assertEquals(clobSize,count);
        rs.close();
        s.executeUpdate("drop table lob1");
        s.executeUpdate("drop table t_lob1_log");
        
	  
    }

    /**
     * Make a CharArrayReader
     * @param c character to repeat	 
     * @param size size of array
     * @return CharArrayReader of specified character  repeating the specified character   
     */
    private static  CharArrayReader  makeCharArrayReader(char c, int size)
    {
   char[] arr = new char[size];
   for (int i = 0; i < arr.length; i++)
        	arr[i] = c;
    return new CharArrayReader(arr);
    }


    private static void assertEquals(int expected, int val) throws Exception
    {
	if (expected != val)
	    throw new Exception("value:" + val + " does not equal expected value:"+ expected);
    }

private void assertEquals(char expected, char  val) throws Exception
    {
	if (expected != val)
	    throw new Exception("value:" + val + " does not equal expected value:"+ expected);
    }


}

