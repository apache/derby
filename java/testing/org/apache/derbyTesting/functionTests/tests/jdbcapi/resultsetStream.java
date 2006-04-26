/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.resultsetStream

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.PreparedStatement;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

import java.io.InputStream;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.util.zip.CRC32;
import java.io.Reader;
import java.io.StringReader;

/**
 * Test of JDBC result set Stream calls.
 *
 * @author djd
 */


public class resultsetStream { 
    
	public static void main(String[] args) {
		Connection con;
		ResultSetMetaData met;
		ResultSet rs;
		Statement stmt;

		System.out.println("Test resultsetStream starting");

		try
		{
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			con = ij.startJBMS();

			stmt = con.createStatement();

			stmt.execute("create table t2 (len int, data LONG VARCHAR FOR BIT DATA)");
			PreparedStatement ppw = con.prepareStatement(
				"insert into t2 (len, data) values (?, ?)");
			String filePath = "extin";
			String sep = System.getProperty("file.separator");
			boolean exists = (new File("extin", "littleclob.utf")).exists();
			if (!exists)
			{
				String userDir = System.getProperty("user.dir");
					filePath = userDir + sep + ".." + sep + filePath;
			}
			File file = new File(filePath + sep + "littleclob.utf");
			int fileSize = (int) file.length();
			BufferedInputStream fileData = new BufferedInputStream(new FileInputStream(file));
			ppw.setInt(1, fileSize);
			ppw.setBinaryStream(2, fileData, fileSize);
			ppw.executeUpdate();

			file = new File(filePath + sep + "short.utf");
			fileSize = (int) file.length();
			fileData = new BufferedInputStream(new FileInputStream(file));
			ppw.setInt(1, fileSize);
			ppw.setBinaryStream(2, fileData, fileSize);
			ppw.executeUpdate();
			// null binary value
			ppw.setInt(1, -1);
			ppw.setBinaryStream(2, (java.io.InputStream) null, 0);
			ppw.executeUpdate();

			// value copied over from original Java object test.
			File rssg = new java.io.File(filePath + sep + "resultsetStream.gif");
			int rssgLength = (int) rssg.length();
			ppw.setInt(1, (int) rssgLength);
			ppw.setBinaryStream(2, new FileInputStream(rssg), rssgLength);
			ppw.executeUpdate();

			// try binary stream processing on a known file.
			rs = stmt.executeQuery("select data from t2 where len = "
									+ rssgLength);
			met = rs.getMetaData();
			System.out.println("getColumnCount(): "+  met.getColumnCount());
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as a stream
				try {

					InputStream is = rs.getBinaryStream(1);
					if (is == null) {
						System.out.println("FAIL - getBinaryStream() return null");
					    break;
					}

					// read the first 200 bytes from the stream and checksum them
					byte[] b200 = new byte[200];

					// no guaratees to read all 200 bytes in one read call.
					int count = 0;

					while (count < 200) {
						int r = is.read(b200, count, 200-count);
						if (r == -1)
							break;
						count += r;
					}

					if (count != 200){
						System.out.println("FAIL - failed to read 200 bytes from known file");
						break;
					}

					CRC32 cs = new CRC32();

					cs.reset();
					cs.update(b200);

					System.out.println("Checksum of first 200 bytes " + cs.getValue());

					count = 200;
					for (;  is.read() != -1; count++) {
					}
					System.out.println("Size of file = " + count);
					is.close();
				}
				catch (Throwable e) {
					System.out.println(
						"FAIL - exection while processing valid file");
					if (e instanceof SQLException)
					JDBCDisplayUtil.ShowSQLException(System.out, (SQLException)e);
				}
			}
			rs.close();

			// check the stream is closed once another get call is made.
			rs = stmt.executeQuery("select data, len from t2 where len = "
									+ rssgLength);
			met = rs.getMetaData();
			System.out.println("getColumnCount(): "+ met.getColumnCount());
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as a stream
				try {

					InputStream is = rs.getBinaryStream(1);
					if (is == null) {
						System.out.println("FAIL - getBinaryStream() return null");
					    break;
					}

					// read the first 200 bytes from the stream and checksum them
					byte[] b200 = new byte[200];

					// no guaratees to read all 200 bytes in one read call.
					int count = 0;

					while (count < 200) {
						int r = is.read(b200, count, 200-count);
						if (r == -1)
							break;
						count += r;
					}

					if (count != 200){
						System.out.println("FAIL - failed to read 200 bytes from known file");
						break;
					}

					CRC32 cs = new CRC32();

					cs.reset();
					cs.update(b200);

					System.out.println("Checksum of first 200 bytes " + cs.getValue());

					System.out.println("second columns is " + rs.getInt(2));

					System.out.println("FAILS DUE TO BUG 5710");
					try {
						is.read();
						System.out.println("FAIL - stream was not closed after a get*() call. " + is.getClass());
						break;
					} catch (IOException ioe) {
						// yes, stream should be closed
					}
				}
				catch (Throwable e) {
					System.out.println(
						"FAIL - exection while processing valid file");
					if (e instanceof SQLException)
					JDBCDisplayUtil.ShowSQLException(System.out, (SQLException)e);
				}
			}
			rs.close();

			// check a SQL null object gets a null stream
			rs = stmt.executeQuery("select data from t2 where len = -1");
			met = rs.getMetaData();
			System.out.println("getColumnCount(): "+ met.getColumnCount());
			while (rs.next())
			{
				// JDBC columns use 1-based counting

				// get the first column as a stream

				InputStream is = rs.getBinaryStream(1);
				if (is != null) {
					System.out.println("FAIL - getBinaryStream() did not return null for SQL null");
					break;
				}

			}
			rs.close();

			rs = stmt.executeQuery("select len, data from t2 where len = "
									+ fileSize);
			rs.next();
			fileSize = rs.getInt(1);
			fileData = new BufferedInputStream(rs.getBinaryStream(2));
			int readCount = 0;
			while(true)
			{
				int data = fileData.read();
				if (data == -1) break;
				readCount++;
			}
			fileData.close();
			System.out.println("len=" + fileSize);
			System.out.println("number of reads=" + readCount);

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
				System.out.println("FAIL - execute with setBinaryStream() with too few bytes succeeded");
			} catch (SQLException sqle) {
				org.apache.derbyTesting.functionTests.util.TestUtil.dumpSQLExceptions(sqle, true);
			}

			ppw.setInt(1, 233);
			ppw.setBinaryStream(2, new java.io.ByteArrayInputStream(tooFew), 233); // too many bytes
			try {
				ppw.executeUpdate();
				System.out.println("FAIL - execute with setBinaryStream() with too many bytes succeeded");
			} catch (SQLException sqle) {
				org.apache.derbyTesting.functionTests.util.TestUtil.dumpSQLExceptions(sqle, true);
			}


			ppw.close();
			rs.close();
			
			TestOfGetAsciiStream.executeTestOfGetAsciiStream(con);

			cleanUp(stmt);
			stmt.close();
			con.close();

		}
		catch (SQLException e) {
			dumpSQLExceptions(e);
		}
		catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception:" + e.toString());
			e.printStackTrace();
		}

		System.out.println("Test resultsetStream finished");
    }

	static private void dumpSQLExceptions (SQLException se) {
		System.out.println("FAIL -- unexpected exception: " + se.toString());
		while (se != null) {
			System.out.print("SQLSTATE("+se.getSQLState()+"):");
			se = se.getNextException();
		}
	}
	
	private static void cleanUp(Statement stmt) throws SQLException {
		String[] testObjects = { " table t3", "table t2" };
		TestUtil.cleanUpTest(stmt, testObjects);
	}
	
	static class TestOfGetAsciiStream {
		
		final static String TEST_STRING_DATA = 
			"ABCDEFG" + 
			"\u00c0\u00c1\u00c2\u00c3\u00c4\u00c5" + 
			"\u00ff\u0100" + 
			"\u3042\u3044\u3046\u3048\u304a";
				
		
		static private void executeTestOfGetAsciiStream(Connection conn) throws SQLException {
			
			System.out.println("Test of getAsciiStream");
			createTestTable(conn);
			executeTestRun(conn);

		}
		

		static private void createTestTable(Connection conn) throws SQLException {

			PreparedStatement st = null;
			
			try{
				st = conn.prepareStatement("create table t3(text_data clob)");
				st.executeUpdate();
				
			}finally{
				if(st != null)
					st.close();
				
			}
			
		}
		

		static private void executeTestRun(Connection conn) throws SQLException {
			
			insertTestData(conn);
			printTestData(conn);
			
		}


		static private void insertTestData(Connection conn) throws SQLException {
			
			PreparedStatement st = null;
			
			try{
				
				st = conn.prepareStatement("insert into t3(text_data) values(?)");
				st.setCharacterStream(1,
						      new StringReader(TEST_STRING_DATA),
						      TEST_STRING_DATA.length());
				st.executeUpdate();
				
			}finally{
				if(st != null)
					st.close();
				
			}

		}


		static private void printTestData(Connection conn) throws SQLException {
			
			PreparedStatement st = null;
			ResultSet rs = null;
			
			try{
				st = conn.prepareStatement("select " + 
							   "text_data as text_data_col1," + 
							   "text_data as text_data_col2 " + 
							   "from " + 
							   "t3");
				rs = st.executeQuery();
				
				while(rs.next()){
					printTestDataInARowViaStream(rs);
					printTestDataInARowViaReader(rs);
				}
				
			}catch(IOException e){
				System.out.println("FAIL -- unexpected IOException: " + e.toString());
				e.printStackTrace();
				
			}finally{
				if(rs != null){
					rs.close();
				}

				if(st != null){
					st.close();
				}
				
			}
		}
		
		
		static private void printTestDataInARowViaStream(ResultSet rs) throws SQLException, 
										      IOException{
			
			InputStream is = null;

			try{
				is = rs.getAsciiStream(1);
			
				for(int c = is.read();
				    c > -1;
				    c = is.read()){
					
					System.out.print(getCharacterCodeString((char) c));
				}
				
				System.out.println();
				
			}finally{
				if(is != null)
					is.close();
			}
				
		}


		static private void printTestDataInARowViaReader(ResultSet rs) throws SQLException, 
										      IOException{

			Reader reader = null;
			
			try{
				reader = rs.getCharacterStream(2);

				for(int c = reader.read();
				    c > -1;
				    c = reader.read()){
					
					System.out.print(getCharacterCodeString((char) c));

				}
			
				System.out.println();
				
			}finally{
				if(reader != null){
					reader.close();
				}
			}
			
		}
		
		
		private static String getCharacterCodeString(char c){

			String hexString = Integer.toHexString((int) c);
			
			while(hexString.length() < 4){
				hexString = "0" + hexString;
			}
			
			return "U+" + hexString;
		}
		
	}

}
