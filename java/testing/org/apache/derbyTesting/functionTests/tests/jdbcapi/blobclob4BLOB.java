/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.blobclob4BLOB

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.Formatters;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/**
 * Test of JDBC blob and clob
 *
 * @author paulat
 */

public class blobclob4BLOB { 

	static String[] fileName;
	static long[] fileLength;
    static int numFiles;
    static int numRows;
    static int numStrings;
	static String[] unicodeStrings;
    static int numRowsUnicode;
    static String unicodeFileName;

	static boolean isDerbyNet = false;
	static boolean debug = true;
	private static final String START = "\nSTART: ";

	static
	{
		numFiles = 5;
		fileName = new String[numFiles];
		fileLength = new long[numFiles];

		fileName[0] = "extin/short.txt";	// set up a short (fit in one page) blob/clob
		fileName[1] = "extin/littleclob.txt"; // set up a long (longer than a page) blob/clob
		fileName[2] = "extin/empty.txt"; // set up a blob/clob with nothing in it
		fileName[3] = "extin/searchclob.txt"; // set up a blob/clob to search with
		fileName[4] = "extin/aclob.txt"; // set up a really long (over 300K) blob/clob

        numRows = 10;

        numStrings = 3;
        unicodeStrings = new String[numStrings];
        unicodeStrings[0] = "\u0061\u0062\u0063";    // abc
        unicodeStrings[1] = "\u0370\u0371\u0372";
        unicodeStrings[2] = "\u05d0\u05d1\u05d2";
        numRowsUnicode = 6;

        unicodeFileName = "extinout/unicodeFile.txt";
    }


	public static void main(String[] args)
    {
		System.out.println("Test blobclob starting");

		isDerbyNet = TestUtil.isNetFramework();

		try
        {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			Connection conn = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn.setAutoCommit(false);

            prepareCLOBMAIN(conn);
            prepareSearchClobTable(conn);
            prepareUnicodeTable(conn);
            prepareUnicodeFile(conn);
            // prepareBinaryTable(conn);

            setCharacterStreamTest(conn);

            // unicodeTest();
            // clobTestGroupfetch(conn);

            clobTest0(conn);
			clobTest11(conn);
			clobTest12(conn);
			clobTest2(conn);
            clobTest22(conn);
            clobTest3(conn);
            clobTest32(conn);
            clobTest4(conn);
            clobTest42(conn);
			clobTest51(conn);
			clobTest52(conn);
			clobTest53(conn);
			clobTest54(conn);
            clobTest6(conn);
            clobTest7(conn);

			clobTest8(conn);

			clobTest91(conn);
            clobTest92(conn);
            clobTest93(conn);
            clobTest94(conn);
            clobTest95(conn);
  
           // restart the connection
            conn = ij.startJBMS();
            conn.setAutoCommit(false);
            clobTest96(conn);

            prepareBlobTable(conn);
            prepareSearchBlobTable(conn);

            blobTest0(conn);
			blobTest2(conn);
            blobTest3(conn);
            blobTest4(conn);
            blobTest51(conn);
            blobTest52(conn);
            blobTest53(conn);
            blobTest54(conn);
            blobTest6(conn);
            blobTest7(conn);
			blobTest91(conn);
            blobTest92(conn);
            blobTest93(conn);
            blobTest94(conn);
            blobTest95(conn);
     
            // restart the connection
            conn = ij.startJBMS();
            conn.setAutoCommit(false);
            blobTest96(conn);

            clobTestSelfDestructive(conn);
            clobTestSelfDestructive2(conn);

            conn.commit();
            clobNegativeTest_Derby265(conn);
            blobNegativeTest_Derby265(conn);
            conn.close();
            System.out.println("FINISHED TEST blobclob :-)");

		}
        catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
			if (debug) e.printStackTrace();
		}
        catch (Throwable e)
        {
			System.out.println("xFAIL -- unexpected exception:" + e.toString());
//            e.fillInStackTrace();
            if (debug) e.printStackTrace();
		}
		System.out.println("Test blobclob finished\n");
    }


    private static void insertRow(PreparedStatement ps, String s)
        throws SQLException
    {
		ps.clearParameters();
        ps.setString(1, s);
        ps.setInt(2, s.length());
        ps.executeUpdate();
    }

    private static void insertRow(PreparedStatement ps, String s, int i)
        throws SQLException
    {
        ps.setString(1, s);
        ps.setInt(2, s.length());
        ps.setInt(3, i);
        ps.executeUpdate();
    }

    private static void insertRow(PreparedStatement ps, byte[] b)
        throws SQLException
    {
        ps.setBytes(1, b);
        ps.setInt(2, b.length);
        ps.executeUpdate();
    }

    /*
        Set up a table with all kinds of CLOB values,
        some short (less than 1 page), some long (more than 1 page)
        some very large (many pages).
        Table has 2 cols: the first is the value, the second is the length of
        the value.
        (Also sets the fileLength array.)
    */
    private static void prepareCLOBMAIN(Connection conn)
    {
        System.out.println(START +"prepareCLOBMAIN");
		ResultSet rs;
		Statement stmt;

		try {
			stmt = conn.createStatement();
			stmt.execute(
		// creating table small then add large column - that way forcing table to have default small page size, but have large rows.
                "create table testCLOB_MAIN (b integer)");
		stmt.execute("alter table testCLOB_MAIN add column a CLOB(1M)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testCLOB_MAIN (a, b) values(?,?)");

            // insert small strings
			insertRow(ps,"");
            insertRow(ps,"you can lead a horse to water but you can't form it into beverage");
            insertRow(ps,"a stitch in time says ouch");
            insertRow(ps,"here is a string with a return \n character");


            // insert larger strings using setAsciiStream
            for (int i = 0; i < numFiles; i++)
            {
                // prepare an InputStream from the file
                File file = new File(fileName[i]);
                fileLength[i] = file.length();
				/*
				System.out.println("inserting filename[" +i +
								   "]" + fileName[i] +
								   " length: " + fileLength[i]);
				*/
                InputStream fileIn = new FileInputStream(file);

                System.out.println("===> inserting " + fileName[i] + " length = "
				    				   + fileLength[i]);

                // insert a streaming column
				ps.setAsciiStream(1, fileIn, (int)fileLength[i]);
                ps.setInt(2, (int)fileLength[i]);
                ps.executeUpdate();
				ps.clearParameters();
                fileIn.close();
            }

            // insert a null
            ps.setNull(1, Types.CLOB);
            ps.setInt(2, 0);
            ps.executeUpdate();

            conn.commit();

            // set numRows
			rs = stmt.executeQuery("select count(*) from testCLOB_MAIN");
            int realNumRows = -1;
			if (rs.next())
                realNumRows = rs.getInt(1);
            if (realNumRows <= 0)
                System.out.println("FAIL. No rows in table testCLOB_MAIN");
            if (realNumRows != numRows)
                System.out.println("FAIL. numRows is incorrect");

        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			if (debug) e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
        //System.out.println("prepareCLOBMAIN finished");
    }



    /*
        Set up a table with clobs to search for
        most short (less than 1 page), some long (more than 1 page)
        some very large (many pages) ??
    */
    private static void prepareSearchClobTable(Connection conn)
    {
        System.out.println(START + "prepareSearchClobTable");
		ResultSet rs;
		Statement stmt;

		try
        {
			stmt = conn.createStatement();
			// creating table small then add large column - that way forcing table to have default small page size, but have large rows.
			stmt.execute("create table searchClob (b integer)");
			stmt.execute("alter table searchClob add column a CLOB(300k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into searchClob (a, b) values(?,?)");
            insertRow(ps,"horse");
            insertRow(ps,"ouch");
            insertRow(ps,"\n");
            insertRow(ps,"");
            insertRow(ps,"Beginning");
            insertRow(ps,"position-69");
            insertRow(ps,"I-am-hiding-here-at-position-5910");
            insertRow(ps,"Position-9907");

            // insert larger strings using setAsciiStream
            for (int i = 0; i < numFiles; i++)
            {
                // prepare an InputStream from the file
                File file = new File(fileName[i]);
                fileLength[i] = file.length();
                InputStream fileIn = new FileInputStream(file);

				/*
				System.out.println("inserting filename[" +i +
								   "]" + fileName[i] +
								   " length: " + fileLength[i]);
				*/
                System.out.println("===> inserting " + fileName[i] + " length = "
				    				   + fileLength[i]);

                // insert a streaming column

                ps.setAsciiStream(1, fileIn, (int)fileLength[i]);
                ps.setInt(2, (int)fileLength[i]);
                ps.executeUpdate();
				ps.clearParameters();
                fileIn.close();
            }

            // insert a null
            ps.setNull(1, Types.CLOB);
            ps.setInt(2, 0);
            ps.executeUpdate();

            conn.commit();
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			if (debug) e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
        System.out.println("prepareSearchClobTable finished");
    }


    /*
        Set up a table with unicode strings in it
        some short (less than 1 page), some long (more than 1 page)
        Table has 3 cols: the first is the value, the second is the length of
        the value, the third is the array index (or else -1 for the ones from files).
        (Also sets the fileLength array.)
        Try slurping the thing into a String.
    */
    private static void prepareUnicodeTable(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "prepareUnicodeTable");
		try {
			stmt = conn.createStatement();
			// creating table small then add large column - that way forcing table to have default small page size, but have large rows.
			stmt.execute("create table testUnicode (b integer, c integer)");
			stmt.execute("alter table testUnicode add column a CLOB(100k)");
            PreparedStatement ps = conn.prepareStatement(
            //    "insert into testUnicode values(?,?,?)");
                "insert into testUnicode (a, b, c)  values(?,?,?)");

            // insert small strings

            for (int i = 0; i < numStrings; i++)
            {
                insertRow(ps,unicodeStrings[i],i);
            }

            StringBuffer sb = new StringBuffer(5000);
            for (int i = 0; i < 5000; i++)
                sb.append('q');
            String largeString = new String(sb);

            // insert larger strings
            for (int i = 0; i < numStrings; i++)
            {
                insertRow(ps,unicodeStrings[i] + largeString + unicodeStrings[i] + "pppppppppp",i);
            }
            conn.commit();

            // set numRows
			rs = stmt.executeQuery("select count(*) from testUnicode");
            int realNumRows = -1;
			if (rs.next())
                realNumRows = rs.getInt(1);
            if (realNumRows <= 0)
                System.out.println("FAIL. No rows in table testUnicode");
            if (realNumRows != numRowsUnicode)
                System.out.println("FAIL. numRowsUnicode is incorrect");

        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}

    }


    /*
      Tests PreparedStatement.setCharacterStream
    */
    private static void setCharacterStreamTest(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "setCharacterStreamTest");
		try
        {
			stmt = conn.createStatement();
			// forcing table with default table space.
			stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096')");
			stmt.execute("create table testUnicode2 (a CLOB(100k))");
			stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','0')");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testUnicode2 values(?)");

            // insert large string using setCharacterStream
            // prepare an InputStream from the file
            File file = new File(unicodeFileName);
            InputStream fileIS = new FileInputStream(file);
            Reader filer = new InputStreamReader(fileIS,"UTF8");
            // insert a streaming column
            ps.setCharacterStream(1, filer, 5009);
            ps.executeUpdate();
            filer.close();
            conn.commit();

			rs = stmt.executeQuery("select a from testUnicode2");
            while (rs.next())
            {
                Clob clob = rs.getClob(1);
                System.out.println("Length of clob is " + clob.length());
                Reader r = clob.getCharacterStream();
                char[] buf = new char[3];
                for (int i = 0; i < numStrings; i++)
                {
                    r.read(buf);
                    if (unicodeStrings[i].equals(new String(buf)))
                        System.out.println("unicode string " + i + " matched");
                    else
                        System.out.println("unicode string " + i + " not matched");
                }
                for (int i = 0; i < 5000; i++)
                {
                    int c = r.read();
                    if (c == -1)
                    {
                        System.out.println("EOF reached at i = " + i);
                        break;
                    }
                    if ((char)c != 'p')
                    {
                        System.out.println("A p was missed, got a " + (char)c);
                    }
                }
                if (r.read() != -1)
                    System.out.println("EOF was missed");
                else
                    System.out.println("EOF matched");
            }
            conn.commit();

            System.out.println("setCharacterStreamTest finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
      Make a file with unicode stuff in it.
     */
    private static void prepareUnicodeFile(Connection conn)
    {
    	 System.out.println(START + "prepareUnicodeFile");
		try
        {
            File file = new File(unicodeFileName);
            OutputStream fos = new FileOutputStream(file);
            Writer filew = new OutputStreamWriter(fos,"UTF8");
            // FileWriter filew = new FileWriter(file);
            filew.write(unicodeStrings[0]);
            filew.write(unicodeStrings[1]);
            filew.write(unicodeStrings[2]);
            for (int i = 0; i < 5000; i++)
                filew.write('p');
            filew.close();

            InputStream fis = new FileInputStream(file);
            Reader filer = new InputStreamReader(fis,"UTF8");
            // FileReader filer = new FileReader(file);
            char bufs[][] = new char[numStrings][3];
            for (int i = 0; i < numStrings; i++)
            {
                filer.read(bufs[i]);
                String s = new String(bufs[i]);
                if (s.equals(unicodeStrings[i]))
                    System.out.println("unicode string " + i + " correct");
                else
                    System.out.println("FAILED: unicode string " + i + " incorrect");
            }
            for (int i = 0; i < 5000; i++)
                if (filer.read() != 'p')
                    System.out.println("Not a p : i = " + i);
            if (filer.read() != -1)
                System.out.println("Not EOF");
            filer.close();
            System.out.println("Finished prepareUnicodeFile");
        }
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }



    /*
        basic test of getAsciiStream
        also tests length
        need to run prepareCLOBMAIN first
    */
	private static void clobTest0(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "clobTest0");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			byte[] buff = new byte[128];
			// fetch row back, get the column as a clob.
            Clob clob;
            int clobLength, i = 0;
			while (rs.next()) {
                i++;
				// get the first column in select as a clob
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
				InputStream fin = clob.getAsciiStream();
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                clobLength = rs.getInt(2);

                if (columnSize != clobLength)
					System.out.println("test failed, columnSize should be " + clobLength
					   + ", but it is " + columnSize + ", i = " + i);
                if (columnSize != clob.length())
				{
					System.out.println("test failed, clob.length() should be " +  columnSize
					   + ", but it is " + clob.length() + ", i = " + i);
				}
			}
			
            conn.commit();
            System.out.println("clobTest0 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        basic test of getCharacterStream
        also tests length
        need to run prepareCLOBMAIN first
    */
	private static void clobTest11(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "clobTest1");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			met = rs.getMetaData();
			char[] buff = new char[128];
			// fetch row back, get the column as a clob.
            int i = 0, clobLength = 0;
			while (rs.next()) {
                i++;
				// get the first column as a clob
                Clob clob = rs.getClob(1);
                if (clob == null)
                    continue;
				Reader reader = clob.getCharacterStream();
				int columnSize = 0;
				for (;;) {
					int size = reader.read(buff);
					if (size == -1)
						break;
                    // System.out.println("the next buffer is :" + buff);
					columnSize += size;
				}
                clobLength = rs.getInt(2);
                if (columnSize != clobLength)
					System.out.println("test failed, columnSize should be " + clobLength
					   + ", but it is " + columnSize + ", i = " + i);
                if (columnSize != clob.length())
					System.out.println("test failed, clob.length() should be " +  columnSize
					   + ", but it is " + clob.length() + ", i = " + i);
				
			}
            conn.commit();
            System.out.println("clobTest11 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
            if (debug) e.printStackTrace();
		}
    }


    /*
        test of getCharacterStream on a table containing unicode characters
        need to run prepareUnicodeTable first
     */
	private static void clobTest12(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest12");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b,c from testUnicode");
            int i = 0, colLength = 0, arrayIndex = 0;
			while (rs.next())
            {
                i++;
                colLength = rs.getInt(2);
                arrayIndex = rs.getInt(3);
                Clob clob = rs.getClob(1);
                if (clob == null)
                {
                    System.out.println("row " + i + " is null, skipped");
                    continue;
                }
                Reader reader = clob.getCharacterStream();

				int columnSize = 0, c;
                String compareString = "";
				for (;;)
                {
					c = reader.read();
					if (c == -1)
						break;
                    if (columnSize < 3)
                        compareString += (char)c;
					columnSize ++;
				}
                if (compareString.equals(unicodeStrings[arrayIndex]))
                    System.out.println("Succeeded to match, row " + i);
                else
                {
                    System.out.println("Failed to match, row " + i +
                    ". compareString = " + compareString + ". arrayIndex = " +
                    arrayIndex + ". unicodeStrings[arrayIndex] = " +
                    unicodeStrings[arrayIndex]);

                }
                if (columnSize != colLength)
					System.out.println("test failed, columnSize should be " + colLength
					   + ", but it is " + columnSize + ", i = " + i);
                else
                    System.out.println("PASSED, row " + i + ", length was " + columnSize);
			}
            conn.commit();
            System.out.println("clobTest12 finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
            if (debug) e.printStackTrace();
		}
    }





    /*
    test getSubString
    need to run prepareCLOBMAIN first
    */
	private static void clobTest2(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest2");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
            int i = 0, clobLength = 0;
            Clob clob;
			while (rs.next())
            {
                i++;
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                blobclob4BLOB.printInterval(clob, 9905, 50, 0, i, clobLength);
                blobclob4BLOB.printInterval(clob, 5910, 150, 1, i, clobLength);
                blobclob4BLOB.printInterval(clob, 5910, 50, 2, i, clobLength);
                blobclob4BLOB.printInterval(clob, 204, 50, 3, i, clobLength);
                blobclob4BLOB.printInterval(clob, 68, 50, 4, i, clobLength);
                blobclob4BLOB.printInterval(clob, 1, 50, 5, i, clobLength);
                blobclob4BLOB.printInterval(clob, 1, 1, 6, i, clobLength);
                /*
                System.out.println(i + "(0) " + clob.getSubString(9905,50));
                System.out.println(i + "(1) " + clob.getSubString(5910,150));
                System.out.println(i + "(2) " + clob.getSubString(5910,50));
                System.out.println(i + "(3) " + clob.getSubString(204,50));
                System.out.println(i + "(4) " + clob.getSubString(68,50));
                System.out.println(i + "(5) " + clob.getSubString(1,50));
                System.out.println(i + "(6) " + clob.getSubString(1,1));
                */
                if (clobLength > 100)
                {
                    String res = clob.getSubString(clobLength-99,200);
                    System.out.println(i + "(7) ");
                    if (res.length() != 100)
                        System.out.println("FAIL : length of substring is " +
                            res.length() + " should be 100");
                    else
                        System.out.println(res);
                }
            }
            System.out.println("clobTest2 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test getSubString with unicode
    need to run prepareUnicodeTable first
    */
	private static void clobTest22(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest22");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b,c from testUnicode");
            int i = 0, clobLength = 0, arrayIndex = 0;
            Clob clob;
			while (rs.next())
            {
                i++;
                System.out.print("Row " + i + " : ");
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                arrayIndex = rs.getInt(3);
                if (clob.getSubString(1,3).equals(unicodeStrings[arrayIndex]))
                    System.out.println("Succeeded");
                else
                    System.out.println("Failed");
                if (clobLength > 5000)
                {
                    if (clob.getSubString(5004,3).equals(unicodeStrings[arrayIndex]))
                        System.out.println("Second time Succeeded");
                    else
                        System.out.println("Second time Failed");
                }
            }
            System.out.println("clobTest22 finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }



    /*
    test position with a String argument
    need to run prepareCLOBMAIN first
    */
	private static void clobTest3(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "clobTest3");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
            int i = 0, clobLength = 0;
            Clob clob;
			while (rs.next())
            {
                i++;
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
               clobLength = rs.getInt(2);
                if (clobLength > 20000)
                    continue;
                blobclob4BLOB.printPosition(i,"horse",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"ouch",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"\n",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"Beginning",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"Beginning",2,clob, clobLength);
                blobclob4BLOB.printPosition(i,"position-69",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"This-is-position-204",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",1,clob, clobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",5910,clob, clobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",5911,clob, clobLength);
                blobclob4BLOB.printPosition(i,"Position-9907",1,clob, clobLength);
            }
            System.out.println("clobTest3 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test position with a unicode String argument
    need to run prepareUnicodeTable first
    */
	private static void clobTest32(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest32");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b,c from testUnicode");
            int i = 0, clobLength = 0, arrayIndex = 0;
            long pos = 0;
            Clob clob;
			while (rs.next())
            {
                i++;
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                arrayIndex = rs.getInt(3);

                pos = clob.position(unicodeStrings[arrayIndex],1);
                if (pos == 1)
                    System.out.println("Succeeded: Found unicode string " + arrayIndex +
                    " at position " + pos + ",row " + i);
                else
                    System.out.println("Failed: Found unicode string " + arrayIndex +
                    " at position " + pos + ",row " + i);

                pos = clob.position(unicodeStrings[arrayIndex],4000);
                if (pos == 5004 || (pos == -1 && clobLength < 4000))
                    System.out.println("Succeeded: Found unicode string " + arrayIndex +
                    " at position " + pos + ",row " + i);
                else
                    System.out.println("Failed: Found unicode string " + arrayIndex +
                    " at position " + pos + ",row " + i);
            }
            System.out.println("clobTest32 finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test position with a Clob argument
    need to run prepareCLOBMAIN and prepareSearchClobTable first
    */
	private static void clobTest4(Connection conn)
    {
		ResultSet rs, rs2;
		Statement stmt, stmt2;
		System.out.println(START + "clobTest4");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
            int i = 0, clobLength = 0;
            Clob clob;
			while (rs.next())
            {
                i++;
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                if (clobLength > 20000)
                    {
                        System.out.println("testCLOB_MAIN row " + i + " skipped (too large)");
                        continue;
                    }
                // inner loop over table of clobs to search for
                // clobs
			    stmt2 = conn.createStatement();
			    rs2 = stmt2.executeQuery("select a,b from searchClob");
                int j = 0, clobLength2 = 0;
                Clob searchClob;
                String searchStr;
			    while (rs2.next())
                {
                    j++;
                    searchClob = rs2.getClob(1);
                    if (searchClob == null)
                        continue;
                    clobLength2 = rs2.getInt(2);
                    if (clobLength2 > 20000)
                    {
                        System.out.println("searchClob row " + j + " skipped (too large)");
                        continue;
                    }
                    if (clobLength2 < 150)
                        searchStr = rs2.getString(1);
                    else
                        searchStr = null;

                    printPositionClob(i,searchStr,1,clob,j,searchClob);
                }
            }
            System.out.println("clobTest4 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			if (debug) e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test position with a Clob argument containing unicode characters
    need to run prepareCLOBMAIN and prepareSearchClobTable first
    */
	private static void clobTest42(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		 System.out.println(START + "clobTest42");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b,c from testUnicode");
            Clob[] clobArray = new Clob[numRowsUnicode];
            int i = 0;
            long pos = 0;
			while (rs.next())
            {
                clobArray[i++] = rs.getClob(1);
            }

            for (int j = 0; j < 3; j++)
            {
                pos = clobArray[j+3].position(clobArray[j],1);
                if (pos == 1)
                    System.out.println("Succeeded: Found clob at position " + pos + ",row " + j);
                else
                    System.out.println("Failed: Found clob at position " + pos + ",row " + j);
                // pos = clobArray[i*2].position(clobArray[i*3],1);
            }
            System.out.println("clobTest42 finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }



    private static void printPositionClob(
        int rowNum,
        String searchStr,
        long position,
        Clob clob,
        int searchRowNum,
        Clob searchClob)
    {
        try
        {
            long result = clob.position(searchClob,position);
            if ("".equals(searchStr) && (result == 1)) {
				System.out.println("position(clob) FOUND @ 1 with empty search clob in clob of length " + clob.length());
                return;
			}
            if (result != -1)
            {
                System.out.print("Found ");
                if (searchStr != null)
                    System.out.print(searchStr);
                else
                    System.out.print("clob (row " + searchRowNum + ") ");
                System.out.println(" in row " + rowNum + " at position " + result);
            }
			else {
				System.out.println("position(clob) NOT FOUND " + rowNum + " searchStr " +
					(searchStr != null ? searchStr : ">150chars"));
			}
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
    }


    /* datatype tests */

    // make sure clobs work for small CLOB fields
    // also test length method
	private static void clobTest51(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest51");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testCLOB10 (a CLOB(10))");

            PreparedStatement ps = conn.prepareStatement(
                "insert into testCLOB10 values(?)");
            String val = "";
            for (int i = 0; i < 10; i++)
            {
                // insert a string
                ps.setString(1, val);
                ps.executeUpdate();
                val += "x";
            }

			rs = stmt.executeQuery("select a from testCLOB10");
			met = rs.getMetaData();
			byte[] buff = new byte[128];
            int j = 0;
			// fetch all rows back, get the columns as clobs.
			while (rs.next())
            {
				// get the first column as a clob
                Clob clob = rs.getClob(1);
                if (clob == null)
                    continue;
				InputStream fin = clob.getAsciiStream();
				int columnSize = 0;
				for (;;)
                {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                if (columnSize != j)
                    System.out.println("FAIL - Expected clob size : " + j +
                        " Got clob size : " + columnSize);
                if (clob.length() != j)
                    System.out.println("FAIL - Expected clob length : " + j +
                        " Got clob length : " + clob.length());
                j++;
			}
            System.out.println("clobTest51 finished");
        }
		catch (SQLException e) {
			if (isDerbyNet)
				System.out.println("EXPECTED SQL Exception: " + e.getMessage());
			else
				TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


   // make sure cannot get a clob from an int column
	private static void clobTest52(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;

		try {
			System.out.println(START + "clobTest52");
			stmt = conn.createStatement();
			System.out.println("create table testInteger (a integer)");
			stmt.execute("create table testInteger (a integer)");

            int i = 1;
            System.out.println("insert into testInteger values('158')");
            PreparedStatement ps = conn.prepareStatement("insert into testInteger values(158)");
            ps.executeUpdate();

			System.out.println("select a from testInteger");
			rs = stmt.executeQuery("select a from testInteger");
			met = rs.getMetaData();
			while (rs.next()) {
				// get the first column as a clob
				System.out.println("getClob(1)");
                Clob clob = rs.getClob(1);
                if (clob == null)
                    System.out.println("clob is null");
                else
                    System.out.println("clob is not null");
			}
            System.out.println("clobTest52 finished");
        }
		catch (SQLException e) {
			System.out.println("52: SQLException");
			if (isDerbyNet)
				System.out.println("EXPECTED SQL Exception: " + e.getMessage());
			else
				TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("52: Throwable");
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


   // test creating a clob column, currently this doesn't work since we don't
   // have a clob datatype (get a syntax error on the create table statement) 
	private static void clobTest53(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest53"); 
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testClobColumn (a clob(1K))");

            System.out.println("clobTest53 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    
    /*
        make sure setClob doesn't work on an int column
        need to run prepareCLOBMAIN first
    */
	private static void clobTest54(Connection conn)
    {
		ResultSet rs;
		Statement stmt1, stmt2;
		System.out.println(START + "clobTest54");
        try
        {
			stmt1 = conn.createStatement();
			stmt1.execute("create table testClob2 (a integer, b integer)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testClob2 values(?,?)");
			stmt2 = conn.createStatement();
			rs = stmt2.executeQuery("select a,b from testCLOB_MAIN");
            Clob clob;
            int clobLength;
			while (rs.next())
            {
				// get the first ncolumn as a clob
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                ps.setClob(1,clob);
                ps.setInt(2,clobLength);
                ps.executeUpdate();
			}
            rs.close();
            conn.commit();

            System.out.println("clobTest54 finished");
        }
		catch (SQLException e) {
			// Can't do a setClob on an int column. This is expected
				TestUtil.dumpSQLExceptions(e,true);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
		System.out.println("end clobTest54");
    }


    /*
    test raising of exceptions
    need to run prepareCLOBMAIN first
    */
	private static void clobTest6(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest6");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
            int i = 0, clobLength = 0;
            Clob clob;
			rs.next();
            clob = rs.getClob(1);
            if (clob == null)
                return;
            clobLength = rs.getInt(2);
            // 0 or negative position value
			if (isDerbyNet)
				System.out.println(" negative tests for clob.getSubstring won't run  for network server  until 5243 is fixed");
			if (! isDerbyNet)
			{

				try
				{
					clob.getSubString(0,5);
					System.out.println("FAIL = clob.getSubString(0,5)");
				}
				catch (SQLException e)
				{
				boolean isExpected = isOutOfBoundException(e);
													   
                TestUtil.dumpSQLExceptions(e, isExpected);
				}
			
            // negative length value
            try
            {
                clob.getSubString(1,-76);
				System.out.println("FAIL = getSubString(1,-76)");
            }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, isOutOfBoundException(e));
            }
            // zero length value
            try
            {
                clob.getSubString(1,0);
 				System.out.println("FAIL = getSubString(1,0)");
           }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, isOutOfBoundException(e));
            }
            // 0 or negative position value
            try
            {
                clob.position("xx",-4000);
				System.out.println("FAIL = position('xx',-4000)");
            }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, isOutOfBoundException(e));
            }
            // null pattern
            try
            {
                clob.position((String) null,5);
				System.out.println("FAIL = position((String) null,5)");
            }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, "XJ072".equals(e.getSQLState()));
            }
            // 0 or negative position value
            try
            {
                clob.position(clob,-42);
				System.out.println("FAIL = position(clob,-42)");
            }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, isOutOfBoundException(e));
            }
            // null pattern
            try
            {
                clob.position((Clob) null,5);
				System.out.println("FAIL = pposition((Clob) null,5)");
            }
            catch (SQLException e)
            {
                TestUtil.dumpSQLExceptions(e, "XJ072".equals(e.getSQLState()));
            }
            System.out.println("clobTest6 finished");
			}
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test setClob
        need to run prepareCLOBMAIN first
    */
	private static void clobTest7(Connection conn)
    {
		ResultSet rs, rs2;
		Statement stmt1, stmt2;
		System.out.println(START + "clobTest7");
        try
        {
			stmt1 = conn.createStatement();
			stmt1.execute("create table testClob7 (a CLOB(300k), b integer)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testClob7 values(?,?)");
			stmt2 = conn.createStatement();
			rs = stmt2.executeQuery("select a,b from testCLOB_MAIN");
            Clob clob;
            int clobLength;
			int rownum = 0;
			while (rs.next())
            {
				// get the first column as a clob
                clob = rs.getClob(1);
                if (clob == null)
                    continue;
                clobLength = rs.getInt(2);
                ps.setClob(1,clob);
                ps.setInt(2,clobLength);
                ps.executeUpdate();
			}
            rs.close();
            conn.commit();

			rs2 = stmt2.executeQuery("select a,b from testClob7");
            Clob clob2;
            int clobLength2, j = 0;
			while (rs2.next())
            {
                j++;
				// get the first column as a clob
                clob2 = rs2.getClob(1);
                if (clob2 == null)
                    continue;
                clobLength2 = rs2.getInt(2);
                if (clob2.length() != clobLength2)
                    System.out.println("FAILED at row " + j);
			}
            rs2.close();

            conn.commit();
            System.out.println("clobTest7 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			if (debug) e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

	/**
		Agressive test of position. 
	*/
  	private static void clobTest8(Connection conn)
    {
  		System.out.println(START + "clobTest8");
		try {
			Statement s = conn.createStatement();

			s.execute("CREATE TABLE C8.T8POS(id INT NOT NULL PRIMARY KEY, DD CLOB(1m), pos INT, L INT)");
			s.execute("CREATE TABLE C8.T8PATT(PATT CLOB(300k))");

			// characters used to fill the String
			char[] fill = new char[4];
			fill[0] = 'd';		// 1 byte UTF8 character (ASCII)
			fill[1] = '\u03a9';  // 2 byte UTF8 character (Greek)
			fill[2] = '\u0e14'; // 3 byte UTF8 character (Thai)
			fill[3] = 'j';		// 1 byte UTF8 character (ASCII)

			char[] base = new char[256 * 1024];

			for (int i = 0; i < base.length; i += 4) {

				base[i] = fill[0];
				base[i+1] = fill[1];
				base[i+2] = fill[2];
				base[i+3] = fill[3];

			}

			char[]  patternBase = new char[2 * 1024];
			for (int i = 0; i < patternBase.length; i += 8) {

				patternBase[i] = 'p';
				patternBase[i+1] = 'a';
				patternBase[i+2] = 't';
				patternBase[i+3] = '\u03aa';
				patternBase[i+4] = (char) i;   // changed value to keep pattern varyinh
				patternBase[i+5] = 'b';
				patternBase[i+6] = 'm';
				patternBase[i+7] = '\u0e15';

			}

			PreparedStatement ps = conn.prepareStatement("INSERT INTO C8.T8POS VALUES (?, ?, ?, ?)");
			PreparedStatement psp = conn.prepareStatement("INSERT INTO C8.T8PATT VALUES (?)");

			T8insert(ps, 1, base, 256, patternBase, 8, 100, true);
			T8insert(ps, 2, base, 3988, patternBase, 8, 2045, true);
			T8insert(ps, 3, base, 16321, patternBase, 8, 4566, true);
			T8insert(ps, 4, base, 45662, patternBase, 8, 34555, true);
			T8insert(ps, 5, base, 134752, patternBase, 8, 67889, true);
			T8insert(ps, 6, base, 303, patternBase, 8, 80, false);
			T8insert(ps, 7, base, 4566, patternBase, 8, 2086, false);
			T8insert(ps, 8, base, 17882, patternBase, 8, 4426, false);
			T8insert(ps, 9, base, 41567, patternBase, 8, 31455, false);
			String pstr = T8insert(ps, 10, base, 114732, patternBase, 8, 87809, false);			

			conn.commit();

			psp.setString(1, pstr);
			psp.executeUpdate();

			System.out.println("small string pattern");
			checkClob8(s, pstr);
			conn.commit();

			System.out.println("small java.sql.Clob pattern");
			ResultSet rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
			rsc.next();
			checkClob8(s, rsc.getClob(1));

			rsc.close();


			conn.commit();

			s.execute("DELETE FROM C8.T8POS");
			s.execute("DELETE FROM C8.T8PATT");


			T8insert(ps, 1, base, 256, patternBase, 134, 100, true);
			T8insert(ps, 2, base, 3988, patternBase, 134, 2045, true);
			T8insert(ps, 3, base, 16321, patternBase, 134, 4566, true);
			T8insert(ps, 4, base, 45662, patternBase, 134, 34555, true);
			T8insert(ps, 5, base, 134752, patternBase, 134, 67889, true);
			T8insert(ps, 6, base, 303, patternBase, 134, 80, false);
			T8insert(ps, 7, base, 4566, patternBase, 134, 2086, false);
			T8insert(ps, 8, base, 17882, patternBase, 134, 4426, false);
			T8insert(ps, 9, base, 41567, patternBase, 134, 31455, false);
			pstr = T8insert(ps, 10, base, 114732, patternBase, 134, 87809, false);

			conn.commit();
			psp.setString(1, pstr);
			psp.executeUpdate();
			conn.commit();


			System.out.println("medium string pattern");
			checkClob8(s, pstr);
			conn.commit();

			System.out.println("medium java.sql.Clob pattern");
			rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
			rsc.next();
			checkClob8(s, rsc.getClob(1));

			s.execute("DELETE FROM C8.T8POS");
			s.execute("DELETE FROM C8.T8PATT");

			T8insert(ps, 1, base, 256, patternBase, 679, 100, true);
			T8insert(ps, 2, base, 3988, patternBase, 679, 2045, true);
			T8insert(ps, 3, base, 16321, patternBase, 679, 4566, true);
			T8insert(ps, 4, base, 45662, patternBase, 679, 34555, true);
			T8insert(ps, 5, base, 134752, patternBase, 679, 67889, true);
			T8insert(ps, 6, base, 303, patternBase, 679, 80, false);
			T8insert(ps, 7, base, 4566, patternBase, 679, 2086, false);
			T8insert(ps, 8, base, 17882, patternBase, 679, 4426, false);
			T8insert(ps, 9, base, 41567, patternBase, 679, 31455, false);
			pstr = T8insert(ps, 10, base, 114732, patternBase, 679, 87809, false);

			conn.commit();
			psp.setString(1, pstr);
			psp.executeUpdate();
			conn.commit();


			System.out.println("long string pattern");
			checkClob8(s, pstr);
			conn.commit();

			System.out.println("long java.sql.Clob pattern");
			rsc = s.executeQuery("SELECT PATT FROM C8.T8PATT");
			rsc.next();
			checkClob8(s, rsc.getClob(1));

			s.execute("DELETE FROM C8.T8POS");
			s.execute("DELETE FROM C8.T8PATT");
			ps.close();
			psp.close();
			// 

			s.execute("DROP TABLE C8.T8POS");
			s.execute("DROP TABLE C8.T8PATT");

			s.close();

			conn.commit();

			System.out.println("complete clobTest8");


		} catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
			e.printStackTrace();
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace(System.out);
		}
	}

	private static void checkClob8(Statement s, String pstr) throws SQLException {
		
		ResultSet rs = s.executeQuery("SELECT ID, DD, POS, L FROM C8.T8POS ORDER BY 1");

		while (rs.next()) {

			int id = rs.getInt(1);

			System.out.print("@" + id + " ");

			java.sql.Clob cl = rs.getClob(2);

			int pos = rs.getInt(3);
			int len = rs.getInt(4);

			long clobPosition = cl.position(pstr, 1);
			if (clobPosition == (long) pos) {
				System.out.print(" position MATCH("+pos+")");
			} else {
				System.out.print(" position FAIL("+clobPosition+"!=" + pos +")");
			}

			System.out.println("");
		}
		rs.close();
	}
	private static void checkClob8(Statement s, Clob pstr) throws SQLException {
		ResultSet rs = s.executeQuery("SELECT ID, DD, POS, L FROM C8.T8POS ORDER BY 1");

		while (rs.next()) {

			int id = rs.getInt(1);

			System.out.print("@" + id + " ");

			java.sql.Clob cl = rs.getClob(2);

			int pos = rs.getInt(3);
			int len = rs.getInt(4);

			long clobPosition = cl.position(pstr, 1);
			if (clobPosition == (long) pos) {
				System.out.print(" position MATCH("+pos+")");
			} else {
				System.out.print(" position FAIL("+clobPosition+"!=" + pos +")");
			}

			System.out.println("");
		}
		rs.close();
	}

	private static String T8insert(PreparedStatement ps, int id, char[] base, int bl, char[] pattern, int pl, int pos, boolean addPattern)
		throws SQLException {

		StringBuffer sb = new StringBuffer();
		sb.append(base, 0, bl);

		// Assume the pattern looks like Abcdefgh
		// put together a block of misleading matches such as
		// AAbAbcAbcdAbcde

		int last = addPatternPrefix(sb, pattern, pl, 5, 10);

		if (last >= (pos / 2))
			pos = (last + 10) * 2;

		// now a set of misleading matches up to half the pattern width
		last = addPatternPrefix(sb, pattern, pl, pl/2, pos/2);

		if (last >= pos)
			pos = last + 13;

		// now a complete set of misleading matches
		pos = addPatternPrefix(sb, pattern, pl, pl - 1, pos);

		if (addPattern) {
			// and then the pattern
			sb.insert(pos, pattern, 0, pl);
		} else {
			pos = -1;
		}


		String dd = sb.toString();
		String pstr = new String(pattern, 0, pl);

		if (pos != dd.indexOf(pstr)) {
			System.out.println("FAIL - test confused pattern not at expected location");

			System.out.println("POS = " + pos + " index " + dd.indexOf(pstr));
			System.out.println("LENG " + dd.length());
			// System.out.println(sb.toString());
		}


		// JDBC uses 1 offset for first character
		if (pos != -1)
			pos = pos + 1;

		ps.setInt(1, id);
		ps.setString(2, dd);
		ps.setInt(3, pos); 
		ps.setInt(4, dd.length());
		ps.executeUpdate();

		return pstr;

	}

	private static int addPatternPrefix(StringBuffer sb, char[] pattern, int pl, int fakeCount, int pos) {

		for (int i = 0; i < fakeCount && i < (pl - 1); i++) {

			sb.insert(pos, pattern, 0, i + 1);
			pos += i + 1;
		}

		return pos;
	}

    /* advanced tests */

    // make sure clob is still around after we go to the next row,
    // after we close the result set, and after we close the statement
	private static void clobTest91(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest91");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			byte[] buff = new byte[128];
            Clob[] clobArray = new Clob[numRows];
            int[] clobLengthArray = new int[numRows];
            int j = 0;
			while (rs.next())
            {
                clobArray[j] = rs.getClob(1);
                clobLengthArray[j++] = rs.getInt(2);
            }
            rs.close();
            stmt.close();

            for (int i = 0; i < numRows; i++)
            {
                if (clobArray[i] == null)
                    {
                        System.out.println("row " + i + " is null, skipped");
                        continue;
                    }
				InputStream fin = clobArray[i].getAsciiStream();
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                if (columnSize != clobLengthArray[i])
					System.out.println("test failed, columnSize should be " +
                        clobLengthArray[i] + ", but it is " + columnSize + ", i = " + i);
                if (columnSize != clobArray[i].length())
					System.out.println("test failed, clobArray[i].length() should be " +  columnSize
					   + ", but it is " + clobArray[i].length() + ", i = " + i);
                System.out.println("done row " + i + ", length was " + clobLengthArray[i]);
            }
            System.out.println("clobTest91 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test locking
        need to run prepareCLOBMAIN fverirst
    */
	private static void clobTest92(Connection conn)
    {
		ResultSet rs;
		Statement stmt,stmt2;
		System.out.println(START + "clobTest92");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			// fetch row back, get the column as a clob.
            Clob clob = null, shortClob = null;
            int clobLength;
			while (rs.next())
            {
                clobLength = rs.getInt(2);
                if (clobLength == 10000)
                    clob = rs.getClob(1);
                if (clobLength == 26)
                    shortClob = rs.getClob(1);
			}
            rs.close();

            Connection conn2 = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn2.setAutoCommit(false);

            // update should go through since we don't get any locks on clobs
            // that are not long columns
            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testCLOB_MAIN set a = 'foo' where b = 26");
            if (shortClob.length() != 26)
                System.out.println("FAILED: clob length changed to " + shortClob.length());
            // should timeout waiting for the lock to do this
            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testCLOB_MAIN set b = b + 1 where b = 10000");

            conn.commit();
            conn2.rollback();
            System.out.println("clobTest92 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test locking with a long row + long column
    */
	private static void clobTest93(Connection conn)
    {
		ResultSet rs;
		Statement stmt, stmt2;
		System.out.println(START + "clobTest93");
        try
        {
			stmt = conn.createStatement();
			// creating table to fit within default 4k table size, then add large columns
			stmt.execute("create table testLongRowClob (a varchar(2000))");
			stmt.execute("alter table testLongRowClob add column b varchar(3000)");
			stmt.execute("alter table testLongRowClob add column c varchar(2000)");
			stmt.execute("alter table testLongRowClob add column d varchar(3000)");
			stmt.execute("alter table testLongRowClob add column e CLOB(400k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testLongRowClob values(?,?,?,?,?)");
            ps.setString(1,Formatters.padString("blaaa",2000));
            ps.setString(2,Formatters.padString("tralaaaa",3000));
            ps.setString(3,Formatters.padString("foodar",2000));
            ps.setString(4,Formatters.padString("moped",3000));
            File file = new File(fileName[1]);
            if (file.length() < 10000)
                System.out.println("ERROR: wrong file tested");
            InputStream fileIn = new FileInputStream(file);
            ps.setAsciiStream(5, fileIn, (int)file.length());
            ps.executeUpdate();
            fileIn.close();
            conn.commit();

			stmt = conn.createStatement();
			rs = stmt.executeQuery("select e from testLongRowClob");
			// fetch row back, get the column as a clob.
            Clob clob = null;
			while (rs.next())
                clob = rs.getClob(1);
            rs.close();

            Connection conn2 = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn2.setAutoCommit(false);
            // the following should timeout
            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testLongRowClob set e = 'smurfball' where a = 'blaaa'");

            conn.commit();
            conn2.commit();
            System.out.println("clobTest92 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    
    /*
        test accessing clob after commit
        need to run prepareCLOBMAIN first
    */
	private static void clobTest94(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest94");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			// fetch row back, get the column as a clob.
            Clob clob = null, shortClob = null;
            int clobLength;
            int i = 0;
			while (rs.next())
            {
				//System.out.println("ACCESSING ROW:" + i++);
                clobLength = rs.getInt(2);
                if (clobLength == 10000)
                    clob = rs.getClob(1);
                if (clobLength == 26)
                    shortClob = rs.getClob(1);
			}
            rs.close();
            conn.commit();

            // no problem accessing this after commit since it is in memory
            System.out.println("shortClob length after commit is " + shortClob.length());
            // these should all give blob/clob data unavailable exceptions
            try
            {
                clob.length();
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.getSubString(2,3);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.getAsciiStream();
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.position("foo",2);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.position(clob,2);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}

            System.out.println("clobTest94 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test accessing clob after closing the connection
        need to run prepareCLOBMAIN first
    */
	private static void clobTest95(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest95");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			// fetch row back, get the column as a clob.
            Clob clob = null, shortClob = null;
            int clobLength;
			while (rs.next())
            {
                clobLength = rs.getInt(2);
                if (clobLength == 10000)
                    clob = rs.getClob(1);
                if (clobLength == 26)
                    shortClob = rs.getClob(1);
			}
            rs.close();
			conn.commit();
            conn.close();

			try {
            // no problem accessing this after commit since it is in memory
            System.out.println("shortClob length after closing connection is " + shortClob.length());
			}
			catch (SQLException e)
			{
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
				
			}
            // these should all give blob/clob data unavailable exceptions
            try
            {
                clob.length();
            }
            catch (SQLException e)
            {
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.getSubString(2,3);
            }
            catch (SQLException e)
            {
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.getAsciiStream();
            }
            catch (SQLException e)
            {
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.position("foo",2);
            }
            catch (SQLException e)
            {
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                clob.position(clob,2);
            }
            catch (SQLException e)
            {
				
				if (isDerbyNet)
					System.out.println("EXPECTED SQL Exception: " + e.getMessage());
				else
					TestUtil.dumpSQLExceptions(e);
	    	}

            System.out.println("clobTest95 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test clob finalizer closes the container
        (should only release table and row locks that are read_committed)
        need to run prepareCLOBMAIN first
        NOTE: this test does not produce output since it needs to call the
        garbage collector whose behaviour is unreliable. It is in the test run to
        exercise the code (most of the time).
    */
	private static void clobTest96(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest96");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			byte[] buff = new byte[128];
            Clob[] clobArray = new Clob[numRows];
            int[] clobLengthArray = new int[numRows];
            int j = 0;
			while (rs.next())
            {
                clobArray[j] = rs.getClob(1);
                clobLengthArray[j++] = rs.getInt(2);
            }
            rs.close();
            stmt.close();

            for (int i = 0; i < numRows; i++)
            {
                clobArray[i] = null;
            }

            System.gc();
            System.gc();

            // System.out.println("after gc");
            // printLockTable(conn);

            System.out.println("clobTest96 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    /*
        test clob finalizer closes the container
        (should only release table and row locks that are read_committed)
        need to run prepareCLOBMAIN first
        NOTE: this test does not produce output since it needs to call the
        garbage collector whose behaviour is unreliable. It is in the test run to
        exercise the code (most of the time).
    */
    /*
     The bug here is that if we do 2 getBlobs on the same column, we reopen the
     container twice, but only remember the 2nd container. Then closing the
     container on one of the blobs causes the 2nd one not to work.
     (Also, closing both blobs leaves one container open.) 
    */

	private static void bug2(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "bug2");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			byte[] buff = new byte[128];
            Clob[] clobArray = new Clob[numRows*2];
            int[] clobLengthArray = new int[numRows*2];
            int j = 0;
			while (rs.next())
            {
                clobArray[j] = rs.getClob(1);
                clobLengthArray[j++] = rs.getInt(2);
                clobArray[j] = rs.getClob(1);
                clobLengthArray[j++] = rs.getInt(2);
            }
            rs.close();
            stmt.close();

            // null out clobs at all the even positions
            for (int i = 0; i < numRows*2; i=i+2)
            {
                clobArray[i] = null;
            }

            System.gc();
            System.gc();

            System.out.println("after gc");
            // printLockTable(conn);

            // access clobs at all the odd positions
            for (int i = 1; i < numRows*2+1; i=i+2)
            {
                if (clobArray[i].length() != clobLengthArray[i])
                    System.out.println("Error at array position " + i);
            }

            System.out.println("clobTest97 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    /*
        test locking
        need to run prepareCLOBMAIN first
    */
	private static void clobTestGroupfetch(Connection conn)
    {
		ResultSet rs;
		Statement stmt,stmt2;
		System.out.println(START + "clobTestGroupFetch");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN");
			// fetch row back, get the column as a clob.
            int clobLength;
			while (rs.next())
            {
                clobLength = rs.getInt(2);
                String s = rs.getString(1);
                printLockTable(conn);
			}
            rs.close();

            System.out.println("clobTestGroupFetch finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    This bug is happening because the clob.length() sets the stream to some
    position (probably to the end). Then when you do a getString() on the same
    column, you are using the same SQLChar object, and you try to call readExternal
    on the stream, but since it isn't at the beginning it doesn't work.
    */
	private static void bug(Connection conn)
    {
		ResultSet rs;
		Statement stmt, stmt2;
		System.out.println(START + "bug" );
        try
        {
			stmt = conn.createStatement();
			// creating table to fit within default 4k table size, then add large columns
			stmt.execute("create table testLongRowClob (a varchar(2000))");
			stmt.execute("alter table testLongRowClob add column b varchar(3000)");
			stmt.execute("alter table testLongRowClob add column c varchar(2000)");
			stmt.execute("alter table testLongRowClob add column d varchar(3000)");
			stmt.execute("alter table testLongRowClob add column e CLOB(400k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testLongRowClob values(?,?,?,?,?)");
            ps.setString(1,Formatters.padString("blaaa",2000));
            ps.setString(2,Formatters.padString("tralaaaa",3000));
            ps.setString(3,Formatters.padString("foodar",2000));
            ps.setString(4,Formatters.padString("moped",3000));
            File file = new File(fileName[1]);
            if (file.length() < 10000)
                System.out.println("ERROR: wrong file tested");
            InputStream fileIn = new FileInputStream(file);
            ps.setAsciiStream(5, fileIn, (int)file.length());
            ps.executeUpdate();
            fileIn.close();
            conn.commit();

            Connection conn2 = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn2.setAutoCommit(false);

            // printLockTable(conn2);
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select e from testLongRowClob");
            printLockTable(conn2);

			// fetch row back, get the column as a clob.
            Clob clob = null;
            int clobLength, i = 0;
			while (rs.next())
            {
                i++;
                clob = rs.getClob(1);
                System.out.println("got it");
                // bug doesn't happen if the below is commented out
                System.out.println("clob length is " + clob.length());
                System.out.println("the thing as a string is : \n" + rs.getString(1));
                printLockTable(conn2);
			}
            rs.close();
            System.out.println("After closing result set");
            printLockTable(conn2);

            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testLongRowClob set e = 'smurfball' where a = 'blaaa'");
            printLockTable(conn2);

            System.out.println("clob length is " + clob.length());

            conn.commit();
            conn2.commit();
            System.out.println("clobTest92 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    // test getAsciiStream, print out the result
    // this is just temporary, for comparison with getAsciiStream
	private static void clobTest9999(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTest9999");
		try {
			stmt = conn.createStatement();
			stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','4096')");
			stmt.execute("create table clobTest9999 (a CLOB(300k))");
			stmt.execute("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize','0')");

            int i = 4;
            // prepare an InputStream from the file
            File file = new File(fileName[i]);
            fileLength[i] = file.length();
            InputStream fileIn = new FileInputStream(file);

            System.out.println("===> testing " + fileName[i] + " length = "
								   + fileLength[i]);

            // insert a streaming column
            PreparedStatement ps = conn.prepareStatement("insert into clobTest9999 values(?)");
            ps.setAsciiStream(1, fileIn, (int)fileLength[i]);
            ps.executeUpdate();
            fileIn.close();

			rs = stmt.executeQuery("select a from clobTest9999");
			met = rs.getMetaData();
			// fetch row back, get the column as a clob.
			while (rs.next()) {
				// get the first column as a clob
                Clob clob = rs.getClob(1);
				InputStream fin = clob.getAsciiStream();
				int columnSize = 0;
				for (;;) {
					int j = fin.read();
					if (j == -1)
						break;
                    System.out.print((char) j);
				}
			}
            System.out.println("Finished clobTest9999");

        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    // test behaviour of system with self destructive user
    // update a long column underneath a clob
	private static void clobTestSelfDestructive(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTestSelfDestructive");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN where b = 10000");
			byte[] buff = new byte[128];
			// fetch row back, get the column as a clob.
            Clob clob = null;
            InputStream fin;
            int clobLength = 0, i = 0;
			if (rs.next())
            {
                i++;
                clobLength = rs.getInt(2);
				// get the first column as a clob
                clob = rs.getClob(1);
            }
            System.out.println("length of clob chosen is " + clobLength);
            fin = clob.getAsciiStream();
            int columnSize = 0;

            PreparedStatement ps = conn.prepareStatement(
                "update testCLOB_MAIN set a = ? where b = 10000");
            StringBuffer foo = new StringBuffer();
            for (int k = 0; k < 1000; k++)
                foo.append('j');
            ps.setString(1,foo.toString());
            ps.executeUpdate();

            System.out.println("After update");

			rs = stmt.executeQuery("select a from testCLOB_MAIN where b = 10000");
			while (rs.next())
            {
                int j = 1;
                String val = rs.getString(1);
                System.out.println("Row " + j + " value.substring(0,50) is " + val.substring(0,50));
                j++;
            }

            while (columnSize < 11000)
            {
                int size = fin.read(buff);
                if (size == -1)
                    break;
                columnSize += size;
				// printing the return from each read is very implementation dependent
            }
            System.out.println(columnSize + " total bytes read");

            if (columnSize != clobLength)
                System.out.println("test failed, columnSize should be " + clobLength
                    + ", but it is " + columnSize + ", i = " + i);
            if (columnSize != clob.length())
                System.out.println("test failed, clob.length() should be " +  columnSize
                    + ", but it is " + clob.length() + ", i = " + i);
            conn.rollback();
            System.out.println("clobTestSelfDestructive finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    // test behaviour of system with self destructive user
    // drop table and see what happens to the clob
    // expect an IOException when moving to a new page of the long column
	private static void clobTestSelfDestructive2(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "clobTestSelfDestructive2");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testCLOB_MAIN where b = 10000");
			byte[] buff = new byte[128];
			// fetch row back, get the column as a clob.
            Clob clob = null;
            InputStream fin;
            int clobLength = 0, i = 0;
			if (rs.next())
            {
                i++;
                clobLength = rs.getInt(2);
				// get the first column as a clob
                clob = rs.getClob(1);
            }
            System.out.println("length of clob chosen is " + clobLength);
            fin = clob.getAsciiStream();
            int columnSize = 0;

			stmt.executeUpdate("drop table testCLOB_MAIN");
            System.out.println("After drop");

            System.out.println("Expect to get an IOException, container has been closed");
            while (columnSize < 11000)
            {
                int size = fin.read(buff);
                if (size == -1)
                    break;
                columnSize += size;
				// printing the return from each read is very implementation dependent
            }
            System.out.println(columnSize + " total bytes read");

            conn.rollback();
            System.out.println("clobTestSelfDestructive2 finished");
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (java.io.IOException ioe)
		{
			System.out.println("EXPECTED IO Exception:" + ioe.getMessage());
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }



	private static void printLockTable(Connection conn)
    {
		ResultSet rs;
		Statement stmt,stmt2;
        try
        {
            System.out.println("\nLock table\n----------------------------------------");
			stmt = conn.createStatement();
			rs = stmt.executeQuery(
                "select xid,type,lockcount,mode,tablename,lockname,state from new LockTable() t where t.tableType <> 'S'");
            while (rs.next())
            {
                String xid = rs.getString("xid");
                String type = rs.getString("type");
                String lockcount = rs.getString("lockcount");
                String mode = rs.getString("mode");
                String tablename = rs.getString("tablename");
                String lockname = rs.getString("lockname");
                String state = rs.getString("state");
                System.out.println("Lock{xid = " + xid + ", type = " + type +
                    ", lockcount = " + lockcount + ", mode = " + mode +
                    ", tablename = " + tablename + ", lockname = " + lockname +
                    ", state = " + state + " } ");
            }
            System.out.println("----------------------------------------\n");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }






    private static void unicodeTest()
    {
    	System.out.println(START + "unicodeTest");
        try
        {
            // String to Unicode bytes
            byte[] unicodeArray = {0xfffffffe, 0xffffffff, 0x01, 0x68, 0x00, 0x65,
                            0x00, 0x6c, 0x00, 0x6c, 0x00, 0x6f};
            String str = new String (unicodeArray, "Unicode");
            System.out.println("First string is : " + str);
            // byte[] array = str.getBytes("UnicodeBigUnmarked");
            // Unicode bytes to String
            // double byte not supported
            // str = new String (array, "DoubleByte");
            // System.out.println("Second string is : " + str);

            byte[] uni2 = {0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x74, 0x68, 0x65, 0x72, 0x65};
            String suni2 = new String (uni2,"Unicode");
            System.out.println("uni2 is :" + uni2);

            String uni3 = "\u0020\u0021\u0023";
            System.out.println("uni3 is :" + uni3);
            String uni4 = "\u0061\u0062\u0063";
            System.out.println("uni4 is :" + uni4);
            System.out.println("uni4 equals abc ? " + uni4.equals("abc"));
            String uni5 = "\u0370\u0371\u0372";
            System.out.println("uni5 is :" + uni5);
            System.out.println("uni5 equals ??? ? " + uni5.equals("???"));
            System.out.println("uni5 equals uni5 ? " + uni5.equals(uni5));
            String uni6 = "\u05d0\u05d1\u05d2";
            System.out.println("uni6 is :" + uni6);
            System.out.println("uni5 equals uni6 ? " + uni5.equals(uni6));
            System.out.println("uni6 equals uni6 ? " + uni6.equals(uni6));

            FileWriter fw;

        }
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
            if (debug) e.printStackTrace();
		}
    }


    /*
        Set up a table with all kinds of blob values,
        some short (less than 1 page), some long (more than 1 page)
        some very large (many pages).
        Table has 2 cols: the first is the value, the second is the length of
        the value.
        (Also sets the fileLength array.)
    */
    private static void prepareBlobTable(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "prepareBlobTable");
		try {
			stmt = conn.createStatement();
			// creating table to fit within default 4k table size, then add large column
			stmt.execute("create table testBlob (b integer)");
			stmt.execute("alter table testBlob add column a blob(300k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testBlob (a, b) values(?,?)");

            // insert small strings
			insertRow(ps,"".getBytes());
            insertRow(ps,"you can lead a horse to water but you can't form it into beverage".getBytes());
            insertRow(ps,"a stitch in time says ouch".getBytes());
            insertRow(ps,"here is a string with a return \n character".getBytes());

            // insert larger strings using setAsciiStream
            for (int i = 0; i < numFiles; i++)
            {
                // prepare an InputStream from the file
                File file = new File(fileName[i]);
                fileLength[i] = file.length();
                InputStream fileIn = new FileInputStream(file);

                System.out.println("===> inserting " + fileName[i] + " length = "
				    				   + fileLength[i]);

                // insert a streaming column
                ps.setBinaryStream(1, fileIn, (int)fileLength[i]);
                ps.setInt(2, (int)fileLength[i]);
                ps.executeUpdate();
                fileIn.close();
            }

            // insert a null
            ps.setNull(1, Types.BLOB);
            ps.setInt(2, 0);
            ps.executeUpdate();

            conn.commit();

            // set numRows
			rs = stmt.executeQuery("select count(*) from testBlob");
            int realNumRows = -1;
			if (rs.next())
                realNumRows = rs.getInt(1);
            if (realNumRows <= 0)
                System.out.println("FAIL. No rows in table testCLOB_MAIN");
            if (realNumRows != numRows)
                System.out.println("FAIL. numRows is incorrect");

        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        Set up a table with binary values,
        Table has 2 cols: the first is the value, the second is the length of
        the value.
    */
    private static void prepareBinaryTable(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "prepareBinaryTable");
		try
        {
			stmt = conn.createStatement();
			stmt.execute("create table testBinary (a blob(80), b integer)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testBinary values(?,?)");

            // insert small strings
			insertRow(ps,"".getBytes());
            insertRow(ps,"you can lead a horse to water but you can't form it into beverage".getBytes());
            insertRow(ps,"a stitch in time says ouch".getBytes());
            insertRow(ps,"here is a string with a return \n character".getBytes());

            // insert a null
            // ps.setNull(1, Types.BINARY);
            // ps.setInt(2, 0);
            // ps.executeUpdate();

            conn.commit();
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e)
        {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }



    /*
        Set up a table with blobs to search for
        most short (less than 1 page), some long (more than 1 page)
        some very large (many pages) ??
    */
    private static void prepareSearchBlobTable(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "prepareSearchBlobTable");
		try {
			stmt = conn.createStatement();
			// creating table to fit within default 4k table size, then add large column
			stmt.execute("create table searchBlob (b integer)");
			stmt.execute("alter table searchBlob add column a blob(300k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into searchBlob (a, b) values(?,?)");
            insertRow(ps,"horse".getBytes());
            insertRow(ps,"ouch".getBytes());
            insertRow(ps,"\n".getBytes());
            insertRow(ps,"".getBytes());
            insertRow(ps,"Beginning".getBytes());
            insertRow(ps,"position-69".getBytes());
            insertRow(ps,"I-am-hiding-here-at-position-5910".getBytes());
            insertRow(ps,"Position-9907".getBytes());

            // insert larger blobs using setBinaryStream
            for (int i = 0; i < numFiles; i++)
            {
                // prepare an InputStream from the file
                File file = new File(fileName[i]);
                fileLength[i] = file.length();
                InputStream fileIn = new FileInputStream(file);

                System.out.println("===> inserting " + fileName[i] + " length = "
				    				   + fileLength[i]);

                // insert a streaming column
                ps.setBinaryStream(1, fileIn, (int)fileLength[i]);
                ps.setInt(2, (int)fileLength[i]);
                ps.executeUpdate();
                fileIn.close();
            }

            // insert a null
            ps.setNull(1, Types.BLOB);
            ps.setInt(2, 0);
            ps.executeUpdate();

            conn.commit();
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        basic test of getBinaryStream
        also tests length
        need to run prepareBlobTable first
    */
	private static void blobTest0(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest0");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			byte[] buff = new byte[128];
			// fetch row back, get the long varbinary column as a blob.
            Blob blob;
            int blobLength = 0, i = 0;
			while (rs.next()) {
                i++;
				// get the first column as a clob
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
				InputStream fin = blob.getBinaryStream();
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                blobLength = rs.getInt(2);
                if (columnSize != blobLength)
					System.out.println("test failed, columnSize should be " + blobLength
					   + ", but it is " + columnSize + ", i = " + i);
                if (columnSize != blob.length())
					System.out.println("test failed, blob.length() should be " +  columnSize
					   + ", but it is " + blob.length() + ", i = " + i);
			}
            conn.commit();
            System.out.println("blobTest0 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test getBytes
    need to run prepareBlobTable first
    */
	private static void blobTest2(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest2");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
            int i = 0, blobLength = 0;
            Blob blob;
			while (rs.next())
            {
                i++;
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
                blobLength = rs.getInt(2);
                blobclob4BLOB.printInterval(blob, 9905, 50, 0, i, blobLength);
                blobclob4BLOB.printInterval(blob, 5910, 150, 1, i, blobLength);
                blobclob4BLOB.printInterval(blob, 5910, 50, 2, i, blobLength);
                blobclob4BLOB.printInterval(blob, 204, 50, 3, i, blobLength);
                blobclob4BLOB.printInterval(blob, 68, 50, 4, i, blobLength);
                blobclob4BLOB.printInterval(blob, 1, 50, 5, i, blobLength);
                blobclob4BLOB.printInterval(blob, 1, 1, 6, i, blobLength);
                /*
                System.out.println(i + "(0) " + new String(blob.getBytes(9905,50)));
                System.out.println(i + "(1) " + new String(blob.getBytes(5910,150)));
                System.out.println(i + "(2) " + new String(blob.getBytes(5910,50)));
                System.out.println(i + "(3) " + new String(blob.getBytes(204,50)));
                System.out.println(i + "(4) " + new String(blob.getBytes(68,50)));
                System.out.println(i + "(5) " + new String(blob.getBytes(1,50)));
                System.out.println(i + "(6) " + new String(blob.getBytes(1,1)));
                */
                if (blobLength > 100)
                {
                    byte[] res = blob.getBytes(blobLength-99,200);
                    System.out.println(i + "(7) ");
                    if (res.length != 100)
                        System.out.println("FAIL : length of bytes is " +
                            res.length + " should be 100");
                    else
                        System.out.println(new String(res));
                }
            }
            System.out.println("blobTest2 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test position with a byte[] argument
    need to run prepareBlobTable first
    */
	private static void blobTest3(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest3");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
            int i = 0, blobLength = 0;
            Blob blob;
			while (rs.next())
            {
                i++;
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
               blobLength = rs.getInt(2);
                if (blobLength > 20000)
                    continue;
                blobLength = rs.getInt(2);
                blobclob4BLOB.printPosition(i,"horse",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"ouch",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"\n",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"Beginning",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"Beginning",2,blob, blobLength);
                blobclob4BLOB.printPosition(i,"position-69",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"This-is-position-204",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",1,blob, blobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",5910,blob, blobLength);
                blobclob4BLOB.printPosition(i,"I-am-hiding-here-at-position-5910",5911,blob, blobLength);
                blobclob4BLOB.printPosition(i,"Position-9907",1,blob, blobLength);
            }
            System.out.println("blobTest3 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    /*
    test position with a Blob argument
    need to run prepareBlobTable and prepareSearchBlobTable first
    */
	private static void blobTest4(Connection conn)
    {
		ResultSet rs, rs2;
		Statement stmt, stmt2;
		System.out.println(START + "blobTest4");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
            int i = 0, blobLength = 0;
            Blob blob;
			while (rs.next())
            {
                i++;
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
                blobLength = rs.getInt(2);
                if (blobLength > 20000)
                    {
                        System.out.println("testBlob row " + i + " skipped (too large)");
                        continue;
                    }
                // inner loop over table of blobs to search for
                // blobs
			    stmt2 = conn.createStatement();
			    rs2 = stmt2.executeQuery("select a,b from searchBlob");
                int j = 0, blobLength2 = 0;
                Blob searchBlob;
                String searchStr;
			    while (rs2.next())
                {
                    j++;
                    searchBlob = rs2.getBlob(1);
                    if (searchBlob == null)
                        continue;
                    blobLength2 = rs2.getInt(2);
                    if (blobLength2 > 20000)
                    {
                        System.out.println("searchBlob row " + j + " skipped (too large)");
                        continue;
                    }
                    if (blobLength2 < 150)
                        searchStr = new String(rs2.getBytes(1));
                    else
                        searchStr = null;

                    printPositionBlob(i,searchStr,1,blob,j,searchBlob);
                }
            }
            System.out.println("blobTest4 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    private static void printPositionBlob(
        int rowNum,
        String searchStr,
        long position,
        Blob blob,
        int searchRowNum,
        Blob searchBlob)
    {
    	
        try
        {
            long result = blob.position(searchBlob,position);
            if ((searchStr != null) && searchStr.equals("") && (result == 1))
                return;
            if (result != -1)
            {
                System.out.print("Found ");
                if (searchStr != null)
                    System.out.print(searchStr);
                else
                    System.out.print("blob (row " + searchRowNum + ") ");
                System.out.println(" in row " + rowNum + " at position " + result);
            }
        }
		catch (SQLException e)
        {
			TestUtil.dumpSQLExceptions(e);
		}
    }

    /* datatype tests */

    // make sure blobs work for regular varbinary fields
    // also test length method
	private static void blobTest51(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest51");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testVarbinary (a blob(13))");

            PreparedStatement ps = conn.prepareStatement(
                "insert into testVarbinary values(?)");
			String val = "";
			
            for (int i = 0; i < 10; i++)
            {
                // insert a string
                ps.setBytes(1, val.getBytes());
                ps.executeUpdate();
                val = val.trim() + "x";
            }

			rs = stmt.executeQuery("select a from testVarbinary");
			met = rs.getMetaData();
			byte[] buff = new byte[128];
            int j = 0;
			// fetch all rows back, get the columns as clobs.
			while (rs.next())
            {
				// get the first column as a clob
                Blob blob = rs.getBlob(1);
                if (blob == null)
                    continue;
				InputStream fin = blob.getBinaryStream();
				int columnSize = 0;
				for (;;)
                {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                if (columnSize != j)
                    System.out.println("FAIL - Expected blob size : " + j +
                        " Got blob size : " + columnSize);
                if (blob.length() != j)
                    System.out.println("FAIL - Expected blob length : " + j +
                        " Got blob length : " + blob.length());
                j++;
			}
            System.out.println("blobTest51 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


   // make sure cannot get a blob from an int column
	private static void blobTest52(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest52");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testInteger2 (a integer)");

            int i = 1;
            PreparedStatement ps = conn.prepareStatement("insert into testInteger2 values(158)");
            ps.executeUpdate();

			rs = stmt.executeQuery("select a from testInteger2");
			met = rs.getMetaData();
			while (rs.next()) {
				// get the first column as a clob
                Blob blob = rs.getBlob(1);
			}
            System.out.println("blobTest52 finished");
        }
		catch (SQLException e) {
			expectedExceptionForNSOnly(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


   // test creating a blob column, currently this doesn't work since we don't
   // have a blob datatype (get a syntax error on the create table statement)
	private static void blobTest53(Connection conn) {

		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest53");
		try {
			stmt = conn.createStatement();
			stmt.execute("create table testBlobColumn (a blob(1K))");

            System.out.println("blobTest53 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    /*
        make sure setBlob doesn't work for an int column
        need to run prepareBlobTable first
    */
	private static void blobTest54(Connection conn)
    {
		ResultSet rs;
		Statement stmt1, stmt2;
		System.out.println(START + "blobTest54");
		try
        {
			stmt1 = conn.createStatement();
			stmt1.execute("create table testBlob2 (a integer, b integer)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testBlob2 values(?,?)");
			stmt2 = conn.createStatement();
			rs = stmt2.executeQuery("select a,b from testBlob");
            Blob blob;
            int blobLength;
			while (rs.next())
            {
				// get the first column as a blob
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
                blobLength = rs.getInt(2);
                ps.setBlob(1,blob);
                ps.setInt(2,blobLength);
                ps.executeUpdate();
			}
            rs.close();
            conn.commit();

            System.out.println("blobTest54 finished");
        }
		catch (SQLException e) {
		    expectedExceptionForNSOnly(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
    test raising of exceptions
    need to run prepareBlobTable first
    */
	private static void blobTest6(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest6");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
            int i = 0, blobLength = 0;
            Blob blob;
			while (rs.next())
            {
                if (i > 0)
                    break;
                i++;
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
                blobLength = rs.getInt(2);
                // test end cases

                // 0 or negative position value
                try
                {
                    blob.getBytes(0,5);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isOutOfBoundException(e));
		        }
                // negative length value
                try
                {
                    blob.getBytes(1,-76);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isOutOfBoundException(e));
		        }
                // zero length value
                try
                {
                    blob.getBytes(1,0);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isOutOfBoundException(e));
		        }
                // 0 or negative position value
                try
                {
                    blob.position(new byte[0],-4000);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isOutOfBoundException(e));
		        }
                // null pattern
                try
                {
					// bug 5247 in network server (NPE)
                    blob.position((byte[]) null,5);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isNullSearchPattern(e));
		        }
                // 0 or negative position value
                try
                {
                    blob.position(blob,-42);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isOutOfBoundException(e));
		        }
                // null pattern
                try
                {
                    blob.position((Blob) null,5);
                }
        		catch (SQLException e)
                {
			        TestUtil.dumpSQLExceptions(e,isNullSearchPattern(e));
		        }
            }
            System.out.println("blobTest6 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			if (e instanceof NullPointerException)
			{
				if (isDerbyNet)
					System.out.println("NullPointerException: KNOWN JCC issue Bug 5247");
			}
			else 
			{
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
			}
		}
    }

    /*
        test setBlob
        need to run prepareBlobTable first
    */
	private static void blobTest7(Connection conn)
    {
		ResultSet rs, rs2;
		Statement stmt1, stmt2;
		System.out.println(START + "blobTest7");
        try
        {
			stmt1 = conn.createStatement();
			stmt1.execute("create table testBlobX (a blob(300K), b integer)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testBlobX values(?,?)");
			stmt2 = conn.createStatement();
			rs = stmt2.executeQuery("select a,b from testBlob");
            Blob blob;
            int blobLength;
			while (rs.next())
            {
				// get the first column as a blob
                blob = rs.getBlob(1);
                if (blob == null)
                    continue;
                blobLength = rs.getInt(2);
                ps.setBlob(1,blob);
                ps.setInt(2,blobLength);
                ps.executeUpdate();
			}
            rs.close();
            conn.commit();

			rs2 = stmt2.executeQuery("select a,b from testBlobX");
            Blob blob2;
            int blobLength2, j = 0;
			while (rs2.next())
            {
                j++;
				// get the first column as a clob
                blob2 = rs2.getBlob(1);
                if (blob2 == null)
                    continue;
                blobLength2 = rs2.getInt(2);
                if (blob2.length() != blobLength2)
                    System.out.println("FAILED at row " + j);
			}
            rs2.close();

            conn.commit();
            System.out.println("blobTest7 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("blobTest7 FAIL -- unexpected exception:" + e.toString());
            e.fillInStackTrace();
            if (debug) e.printStackTrace();
		}
    }


    /* advanced tests */

    // make sure blob is still around after we go to the next row,
    // after we close the result set, and after we close the statement
	private static void blobTest91(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest91");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			byte[] buff = new byte[128];
            Blob[] blobArray = new Blob[numRows];
            int[] blobLengthArray = new int[numRows];
            int j = 0;
			while (rs.next())
            {
                blobArray[j] = rs.getBlob(1);
                blobLengthArray[j++] = rs.getInt(2);
            }
            rs.close();
            stmt.close();

            for (int i = 0; i < numRows; i++)
            {
                if (blobArray[i] == null)
                    {
                        System.out.println("row " + i + " is null, skipped");
                        continue;
                    }
				InputStream fin = blobArray[i].getBinaryStream();
				int columnSize = 0;
				for (;;) {
					int size = fin.read(buff);
					if (size == -1)
						break;
					columnSize += size;
				}
                if (columnSize != blobLengthArray[i])
					System.out.println("test failed, columnSize should be " +
                        blobLengthArray[i] + ", but it is " + columnSize + ", i = " + i);
                if (columnSize != blobArray[i].length())
					System.out.println("test failed, blobArray[i].length() should be " +  columnSize
					   + ", but it is " + blobArray[i].length() + ", i = " + i);
                System.out.println("done row " + i + ", length was " + blobLengthArray[i]);
            }
            System.out.println("blobTest91 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test locking
        need to run prepareBlobTable first
    */
	private static void blobTest92(Connection conn)
    {
		ResultSet rs;
		Statement stmt,stmt2;
		System.out.println(START + "blobTest92");
		try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			// fetch row back, get the column as a blob.
            Blob blob = null, shortBlob = null;
            int blobLength;
			while (rs.next())
            {
                blobLength = rs.getInt(2);
                if (blobLength == 10000)
                    blob = rs.getBlob(1);
                if (blobLength == 26)
                    shortBlob = rs.getBlob(1);
			}
            rs.close();

            Connection conn2 = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn2.setAutoCommit(false);
            if (!TestUtil.isNetFramework())
            {
            // Note: Locks held until the end of transaction only for embedded.
            // Network Server cannot differentiate a getBlob from a getBytes so 
            // does not hold locks for blob calls (DERBY-255) 
            // The LOB is materialized on the client so we do not need to hold locks.
            // One ugly thing about this test is that these rows are used by other tests.
            // If this tests fails and the rows get updated, other tests can get 
            // NullPointer exceptions.	
            
            // Update should go through since we don't get any locks on blobs
            // that are not long columns
            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testBlob set a = null where b = 26");
            if (shortBlob.length() != 26)
                System.out.println("FAILED: blob length changed to " + shortBlob.length());
            // should timeout waiting for the lock to do this
            
            	stmt2 = conn2.createStatement();
            	stmt2.executeUpdate("update testBlob set b = b + 1 where b = 10000");
            	throw new Exception("FAIL: Should have gotten lock timeout");
            }
            else
            {
            	System.out.println("Locks not held by Network Server for Blobs since they are materialized on client");
            }
            conn.commit();
            conn2.commit();
            System.out.println("blobTest92 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test locking with a long row + long column
    */
	private static void blobTest93(Connection conn)
    {
		ResultSet rs;
		Statement stmt, stmt2;
		System.out.println(START + "blobTest93");
        try
        {
			stmt = conn.createStatement();
			// creating table to fit within default 4k table size, then add large columns
			stmt.execute("create table testLongRowBlob (a varchar(2000))");
			stmt.execute("alter table testLongRowBlob add column b varchar(3000)");
			stmt.execute("alter table testLongRowBlob add column c varchar(2000)");
			stmt.execute("alter table testLongRowBlob add column d varchar(3000)");
			stmt.execute("alter table testLongRowBlob add column e blob(300k)");
            PreparedStatement ps = conn.prepareStatement(
                "insert into testLongRowBlob values(?,?,?,?,?)");
            ps.setString(1,Formatters.padString("blaaa",2000));
            ps.setString(2,Formatters.padString("tralaaaa",3000));
            ps.setString(3,Formatters.padString("foodar",2000));
            ps.setString(4,Formatters.padString("moped",3000));
            File file = new File(fileName[1]);
            if (file.length() < 10000)
                System.out.println("ERROR: wrong file tested");
            InputStream fileIn = new FileInputStream(file);
            ps.setBinaryStream(5, fileIn, (int)file.length());
            ps.executeUpdate();
            fileIn.close();
            conn.commit();

			stmt = conn.createStatement();
			rs = stmt.executeQuery("select e from testLongRowBlob");
            Blob blob = null;
			while (rs.next())
                blob = rs.getBlob(1);
            rs.close();

            Connection conn2 = ij.startJBMS();
            // turn off autocommit, otherwise blobs/clobs cannot hang around
            // until end of transaction
            conn2.setAutoCommit(false);
            // the following should timeout
            stmt2 = conn2.createStatement();
            stmt2.executeUpdate("update testLongRowBlob set e = null where a = 'blaaa'");

            conn.commit();
            conn2.commit();
            System.out.println("blobTest93 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test accessing blob after commit
        need to run prepareCLOBMAIN first
    */
	private static void blobTest94(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest94");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			// fetch row back, get the column as a blob.
            Blob blob = null, shortBlob = null;
            int blobLength;
			while (rs.next())
            {
                blobLength = rs.getInt(2);
                if (blobLength == 10000)
                    blob = rs.getBlob(1);
                if (blobLength == 26)
                    shortBlob = rs.getBlob(1);
			}
            rs.close();
            conn.commit();

            // no problem accessing this after commit since it is in memory
            if (shortBlob != null)
            System.out.println("shortBlob length after commit is " + shortBlob.length());
            // these should all give blob/clob data unavailable exceptions

            try
            {
                blob.length();
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                blob.getBytes(2,3);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                blob.getBinaryStream();
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                blob.position("foo".getBytes(),2);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}
            try
            {
                blob.position(blob,2);
            }
            catch (SQLException e)
            {
    			TestUtil.dumpSQLExceptions(e);
	    	}

            System.out.println("blobTest94 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}

    }

    /*
        test accessing blob after closing the connection
        need to run prepareCLOBMAIN first
    */
	private static void blobTest95(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest95");
        try
        {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			// fetch row back, get the column as a blob.
            Blob blob = null, shortBlob = null;
            int blobLength;
			while (rs.next())
            {
                blobLength = rs.getInt(2);
                if (blobLength == 10000)
                    blob = rs.getBlob(1);
                if (blobLength == 26)
                    shortBlob = rs.getBlob(1);
			}
            rs.close();
			conn.rollback();
            conn.close();

			try {
				// no problem accessing this after commit since it is in memory
				System.out.println("shortBlob length after closing the connection is " + shortBlob.length());
			}
			catch (SQLException e)
			{
				expectedExceptionForNSOnly (e);
			}

				// these should all give blob/clob data unavailable exceptions
            try
            {
                blob.length();
            }
            catch (SQLException e)
            {
				expectedExceptionForNSOnly (e);
	    	}
            try
            {
                blob.getBytes(2,3);
            }
            catch (SQLException e)
            {
				expectedExceptionForNSOnly (e);
	    	}
            try
            {
                blob.getBinaryStream();
            }
            catch (SQLException e)
            {
				expectedExceptionForNSOnly (e);
	    	}
            try
            {
                blob.position("foo".getBytes(),2);
            }
            catch (SQLException e)
            {
				expectedExceptionForNSOnly (e);
	    	}
            try
            {
                blob.position(blob,2);
            }
            catch (SQLException e)
            {
				expectedExceptionForNSOnly (e);
	    	}

            // restart the connection
            conn = ij.startJBMS();
            conn.setAutoCommit(false);

            System.out.println("blobTest95 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }


    /*
        test blob finalizer closes the container
        (should only release table and row locks that are read_committed)
        need to run prepareCLOBMAIN first
        NOTE: this test does not produce output since it needs to call the
        garbage collector whose behaviour is unreliable. It is in the test run to
        exercise the code (most of the time).
    */
	private static void blobTest96(Connection conn)
    {
		ResultSet rs;
		Statement stmt;
		System.out.println(START + "blobTest96");
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select a,b from testBlob");
			byte[] buff = new byte[128];
            Blob[] blobArray = new Blob[numRows];
            int[] blobLengthArray = new int[numRows];
            int j = 0;
			while (rs.next())
            {
                blobArray[j] = rs.getBlob(1);
                blobLengthArray[j++] = rs.getInt(2);
            }
            rs.close();
            stmt.close();

            // printLockTable(conn);

            for (int i = 0; i < numRows; i++)
            {
                blobArray[i] = null;
            }

            // printLockTable(conn);

            System.gc();
            System.gc();

            // System.out.println("after gc");
            // printLockTable(conn);

            System.out.println("blobTest96 finished");
        }
		catch (SQLException e) {
			TestUtil.dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			if (debug) e.printStackTrace();
		}
    }

    
    
    /**
     * Test fix for derby-265.
     * Test that if getBlob is called after the transaction 
     * in which it was created is committed, a proper user error
     * is thrown instead of an NPE. 
     * Basically per the spec, getBlob is valid only for the duration of 
     * the transaction in it was created in
     * @param conn
     * @throws SQLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void blobNegativeTest_Derby265(Connection conn)
            throws SQLException, FileNotFoundException,IOException {
    	System.out.println(START + "blobTestNegativeTest_Derby265");
    	// basically setup the tables for clob and blob
        Statement s = conn.createStatement();
        s.execute("create table \"MAPS_BLOB\"(MAP_ID int, MAP_NAME varchar(20),REGION varchar(20),AREA varchar(20), PHOTO_FORMAT varchar(20),PICTURE blob(2G))");
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("insert into \"MAPS_BLOB\" values(?,?,?,?,?,?)");
        
        for (int i = 0; i < 3; i++) {
            FileInputStream fis = new FileInputStream(fileName[4]);
            ps.setInt(1, i);
            ps.setString(2, "x" + i);
            ps.setString(3, "abc");
            ps.setString(4, "abc");
            ps.setString(5, "abc");
            ps.setBinaryStream(6, new java.io.BufferedInputStream(fis), 300000);
            ps.executeUpdate();
            fis.close();
        }
        conn.commit();

        conn.setAutoCommit(true);
        System.out.println("-----------------------------");

        s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s.execute("SELECT \"MAP_ID\", \"MAP_NAME\", \"REGION\", \"AREA\", \"PHOTO_FORMAT\", \"PICTURE\" FROM \"MAPS_BLOB\"");
        ResultSet rs1 = s.getResultSet();
        Statement s2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s2.executeQuery("SELECT \"MAP_ID\", \"MAP_NAME\", \"REGION\", \"AREA\", \"PHOTO_FORMAT\", \"PICTURE\" FROM \"MAPS_BLOB\"");
        ResultSet rs2 = s2.getResultSet();
        rs2.next();

        Blob b2 = rs2.getBlob(6);
        rs1.next();
        Blob b1 = rs1.getBlob(6);
        try {
            rs1.close();
            rs2.next();
            rs2.getBlob(6);
        } catch (SQLException sqle) {
        	String sqlstate = sqle.getSQLState();
        	boolean expected = (sqlstate != null && 
        				(sqlstate.equals("XJ073") || sqlstate.equals("XCL30")));
            	TestUtil.dumpSQLExceptions(sqle,expected);            	
        }
        finally {
            rs2.close();
            s2.close();
            s.close();
            ps.close();
        }

    }

    /**
     * Test fix for derby-265.
     * Test that if getClob is called after the transaction 
     * in which it was created is committed, a proper user error
     * is thrown instead of an NPE. 
     * Basically per the spec, getClob is valid only for the duration of 
     * the transaction in it was created in
     * @param conn
     * @throws SQLException
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void clobNegativeTest_Derby265(Connection conn)
            throws SQLException, FileNotFoundException,IOException {

    	System.out.println(START + "clobNegativeTest_Derby265");
    	// basically setup the tables for clob 
        Statement s = conn.createStatement();
        s.execute("create table \"MAPS\"(MAP_ID int, MAP_NAME varchar(20),REGION varchar(20),AREA varchar(20), PHOTO_FORMAT varchar(20),PICTURE clob(2G))");
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("insert into \"MAPS\" values(?,?,?,?,?,?)");
        for (int i = 0; i < 3; i++) {
            FileReader fr = new FileReader(fileName[4]);
            ps.setInt(1, i);
            ps.setString(2, "x" + i);
            ps.setString(3, "abc");
            ps.setString(4, "abc");
            ps.setString(5, "abc");
            ps.setCharacterStream(6, new java.io.BufferedReader(fr),300000);
            ps.executeUpdate();
            fr.close();
        }
        conn.commit();

        conn.setAutoCommit(true);
        System.out.println("-----------------------------");
        s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s.execute("SELECT \"MAP_ID\", \"MAP_NAME\", \"REGION\", \"AREA\", \"PHOTO_FORMAT\", \"PICTURE\" FROM \"MAPS\"");
        ResultSet rs1 = s.getResultSet();
        Statement s2 = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        s2.executeQuery("SELECT \"MAP_ID\", \"MAP_NAME\", \"REGION\", \"AREA\", \"PHOTO_FORMAT\", \"PICTURE\" FROM \"MAPS\"");
        ResultSet rs2 = s2.getResultSet();
        rs2.next();

        Clob b2 = rs2.getClob(6); // should be fine
        rs1.next();
        Clob b1 = rs1.getClob(6);
        try {
            rs1.close(); // this commits the transaction
            rs2.next();
            rs2.getClob(6); // no longer valid
        } catch (SQLException sqle) {
        	String sqlstate = sqle.getSQLState();
        	boolean expected = (sqlstate != null && 
        				(sqlstate.equals("XJ073") || sqlstate.equals("XCL30")));
            	TestUtil.dumpSQLExceptions(sqle,expected);
        }	
        finally {
            rs2.close();
            s2.close();
            s.close();
            ps.close();
        }

    }
    static void printInterval(Clob clob, long pos, int length,
        int testNum, int iteration, int clobLength)
    {
		if (pos > clobLength)
			System.out.println("CLOB getSubString " + pos + " > " + clobLength);
        try
        {
			/*
			System.out.println("printInterval(" + clob + "," + pos +"," +
						   length +"," + testNum + "," + iteration + "," +
						   clobLength + ")");
			*/
			String ss = clob.getSubString(pos,length);

            System.out.println(iteration + "(" + testNum + ") (len " + length + ") " + ss);
			if (ss.length() > length)
				System.out.println("FAIL getSubString("+pos+","+length+") returned a string of length " + ss.length());

			long l1 = clob.length();
			if (l1 != clobLength) {
				System.out.println("CHECK - test has mismatched lengths " + l1 + " != " + clobLength);
			}
			if (pos > clobLength)
				System.out.println("CLOB FAIL - NO ERROR ON getSubString POS TOO LARGE " + pos + " > " + clobLength);


        }
		catch (SQLException e)
        {
			String state = e.getSQLState();
			boolean expected = false;


			if (pos < 1 || pos > clobLength)
			{
				if (isOutOfBoundException(e))
					expected = true;
			} 
			else
			{
				System.out.println("FAIL -- unexpected exception:" + e.toString());
			}
			TestUtil.dumpSQLExceptions(e,expected);
		}
		
		catch (Exception e)
		{
			// Known bug.  JCC 5914.  
			if ((pos > clobLength) && isDerbyNet && (e.getMessage() != null &&
													e.getMessage().indexOf("String index out of range") >= 0))
				System.out.println("EXPECTED Out of bounds exception");
			else
			{
				System.out.println("FAIL -- unexpected exception:" + e.toString());
	            if (debug) e.printStackTrace();
			}
		}
    }

    static void printInterval(Blob blob, long pos, int length,
        int testNum, int iteration, long blobLength)
    {
		if (pos > blobLength)
			System.out.println("testing Blob.getBytes() with pos " + pos + " > " + blobLength);
        try
        {
            System.out.println(iteration + "(" + testNum + ") " +
                new String(blob.getBytes(pos,length)));

			long l1 = blob.length();
			if (l1 != blobLength) {
				System.out.println("CHECK - test has mismatched lengths " + l1 + " != " + blobLength);
			}
			if (pos > blobLength)
				System.out.println("FAIL testing Blob.getBytes() with pos " + pos + " > " + blobLength);
        }
		catch (SQLException e)
        {
			String state = e.getSQLState();
			boolean expected = false;

			if (pos < 1 || pos > blobLength)
				expected = isOutOfBoundException(e);

			TestUtil.dumpSQLExceptions(e, expected);
		}
		catch (Exception e)
		{
			if ((pos > blobLength) && isDerbyNet)
				System.out.println("Known JCC Bug 5914");
		}
    }
    static void printPosition(
        int rowNum,
        String searchStr,
        long position,
        Clob clob,
		long clobLength)
    {

        try
        {

            long result = clob.position(searchStr,position);

            System.out.println("Found " + searchStr + " in row " + rowNum +
                    " starting from position " + position + " at position " +
                    (result == -1 ? " NOTFOUND " : Long.toString(result)));

			long l1 = clob.length();
			if (l1 != clobLength) {
				System.out.println("CHECK - test has mismatched lengths " + l1 + " != " + clobLength);
			}


        }
		catch (SQLException e)
        {
			String state = e.getSQLState();
			boolean expected = false;

			if (position < 1 || position > clobLength)
				expected = isOutOfBoundException(e);

			if (searchStr == null)
				if ("XJ072".equals(state))
					expected = true;

			if ("".equals(searchStr))
				if ("XJ078".equals(state))
					expected = true;
	
				
			TestUtil.dumpSQLExceptions(e, expected);
			e.printStackTrace();
		}
    }

    static void printPosition(
        int rowNum,
        String searchStr,
        long position,
        Blob blob, int blobLength)
    {
        try
        {
            long result = blob.position(searchStr.getBytes(),position);
            if ((searchStr == "") && (result == 1))
                return;
            if (result != -1)
                System.out.println("Found " + searchStr + " in row " + rowNum +
                    " starting from position " + position + " at position " +
                    result);

			long l1 = blob.length();
			if (l1 != blobLength) {
				System.out.println("CHECK - test has mismatched lengths " + l1 + " != " + blobLength);
			}
        }
		catch (SQLException e)
        {
			String state = e.getSQLState();
			boolean expected = false;

			if (position < 1 || position > blobLength)
				expected = isOutOfBoundException(e);

			if (searchStr == null)
				if ("XJ072".equals(state))
					expected = true;

			if ("".equals(searchStr))
				if ("XJ078".equals(state))
					expected = true;
	
				
			TestUtil.dumpSQLExceptions(e, expected);
		}
    }
	/**
	 * In network server we expect an exception.
	 * In embedded we don't
	 */
	 
	static private void expectedExceptionForNSOnly (SQLException se)
	{
		TestUtil.dumpSQLExceptions(se, isDerbyNet);
	}

	static private boolean isOutOfBoundException(SQLException se)
	{
		String sqlState = se.getSQLState();
		String msg = se.getMessage();
		if ("XJ070".equals(sqlState) ||
			"XJ071".equals(sqlState) ||
			"XJ076".equals(sqlState) ||
			(sqlState  == null && 
			 ((msg.indexOf("Index Out Of Bound") != -1) ||
			  (msg.indexOf("Invalid position") != -1))))
			return true;
			
		return false;
	}

	static private boolean isNullSearchPattern(SQLException se)
	{
		String sqlState = se.getSQLState();
		if ("XJ072".equals(sqlState) ||
			(sqlState  == null &&
			 se.getMessage().indexOf("Search pattern cannot be null") != -1))
			return true;
			
		return false;
	}
}










