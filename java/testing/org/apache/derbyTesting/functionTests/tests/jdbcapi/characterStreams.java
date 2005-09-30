/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.characterStreams

   Copyright 2002, 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

import java.io.*;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class characterStreams { 

	private static boolean isDerbyNet;

	public static void main(String[] args) {

		isDerbyNet = TestUtil.isNetFramework();
		if (isDerbyNet) {
			System.out.println("SKIP TEST FOR NOW");
			return;
		}
		boolean		passed = true;
		Connection	conn = null;
		try {
			System.out.println("Test characterStreams starting");

			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.createStatement().executeUpdate("create table charstream(id int GENERATED ALWAYS AS IDENTITY primary key, c char(25), vc varchar(32532), lvc long varchar)");

			setStreams(conn);

			conn.close();

		} catch (SQLException se) {
			passed = false;
			JDBCDisplayUtil.ShowSQLException(System.out, se);
			se.printStackTrace(System.out);
		} catch (Throwable e) {
			System.out.println("FAIL -- unexpected exception caught in main():\n");
			System.out.println(e.getMessage());
			e.printStackTrace(System.out);
			passed = false;
		}

		if (passed)
			System.out.println("PASS");

		System.out.println("Test characterStreams finished");
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
	static void setStreams(Connection conn) throws Exception {

		ResultSet rs;

		PreparedStatement psi = conn.prepareStatement("insert into charstream(c, vc, lvc) values(?,?,?)");
		PreparedStatement psq = conn.prepareStatement("select id, c, {fn length(c)} AS CLEN, cast (vc as varchar(25)) AS VC, {fn length(vc)} AS VCLEN, cast (lvc as varchar(25)) AS LVC, {fn length(lvc)} AS LVCLEN from charstream where id > ? order by 1");

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);

		// test setAsciiStream into CHAR
		System.out.println("\nTest setAsciiStream into CHAR");
		int maxid = getMaxId(conn);
		setAscii(psi, 1);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
	

		// Show results as various streams
		PreparedStatement psStreams = conn.prepareStatement("SELECT id, c, vc, lvc from charstream where id > ? order by 1");
		psStreams.setInt(1, maxid);
		rs = psStreams.executeQuery();
		showResultsAsciiStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStreamBlock(rs);

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);
		// test setAsciiStream into VARCHAR
		System.out.println("\nTest setAsciiStream into VARCHAR");
		maxid = getMaxId(conn);
		setAscii(psi, 2);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
		psStreams.setInt(1, maxid);
		rs = psStreams.executeQuery();
		showResultsAsciiStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStreamBlock(rs);

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);
		// test setAsciiStream into LONG VARCHAR
		System.out.println("\nTest setAsciiStream into LONG VARCHAR");
		maxid = getMaxId(conn);
		setAscii(psi, 3);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);
		psStreams.setInt(1, maxid);
		rs = psStreams.executeQuery();
		showResultsAsciiStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStream(rs);
		rs = psStreams.executeQuery();
		showResultsCharacterStreamBlock(rs);

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);

		// test setCharacterStream into CHAR
		System.out.println("\nTest setCharacterStream into CHAR");
		maxid = getMaxId(conn);
		setCharacter(psi, 1);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);

		// test setCharacterStream into VARCHAR
		System.out.println("\nTest setCharacterStream into VARCHAR");
		maxid = getMaxId(conn);
		setCharacter(psi, 2);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);

		psi.setString(1, null);
		psi.setString(2, null);
		psi.setString(3, null);

		// test setCharacterStream into LONG VARCHAR
		System.out.println("\nTest setCharacterStream into LONG VARCHAR");
		maxid = getMaxId(conn);
		setCharacter(psi, 3);
		psq.setInt(1, maxid);
		rs = psq.executeQuery();
		JDBCDisplayUtil.DisplayResults(System.out, rs, conn);

		// now insert long values using streams and check them programatically.
		PreparedStatement psDel = conn.prepareStatement("DELETE FROM charstream");
		PreparedStatement psq2 = conn.prepareStatement("select c, vc, lvc from charstream");

		// now insert long values using streams and check them programatically.
		System.out.println("setAsciiStream(LONG ASCII STREAMS)");
		checkAsciiStreams(psDel, psi, psq2, 18, 104, 67);
		checkAsciiStreams(psDel, psi, psq2, 25, 16732, 14563);
		checkAsciiStreams(psDel, psi, psq2, 1, 32433, 32673);
		checkAsciiStreams(psDel, psi, psq2, 0, 32532, 32700);

		System.out.println("setCharacterStream(LONG CHARACTER STREAMS WITH UNICODE)");
		checkCharacterStreams(psDel, psi, psq2, 14, 93, 55);
		checkCharacterStreams(psDel, psi, psq2, 25, 19332, 18733);
		checkCharacterStreams(psDel, psi, psq2, 1, 32433, 32673);
		checkCharacterStreams(psDel, psi, psq2, 0, 32532, 32700);
	}

	private static int getMaxId(Connection conn) throws SQLException {

		ResultSet rs = conn.createStatement().executeQuery("select max(id) from charstream");
		rs.next();
		int maxid = rs.getInt(1);
		rs.close();
		return maxid;
	}

	private static void setAscii(PreparedStatement ps, int targetCol) throws Exception {

		// correct byte count
		System.out.println("CORRECT NUMBER OF BYTES IN STREAM");
		ps.setAsciiStream(targetCol, new java.io.ByteArrayInputStream("Lieberman ran with Gore".getBytes("US-ASCII")), 23);
		ps.executeUpdate();

		// less bytes than stream contains. JDBC 3.0 indicates it should throw an exception
		// (in Tutorial & reference book)
		System.out.println("MORE BYTES IN STREAM THAN PASSED IN VALUE");
		try {
			ps.setAsciiStream(targetCol, new java.io.ByteArrayInputStream("against Republicans George W. Bush ".getBytes("US-ASCII")), 19);
			ps.executeUpdate();
			System.out.println("FAIL - MORE BYTES IN ASCII STREAM THAN SPECIFIED LENGTH - ACCEPTED");
		} catch (SQLException sqle) {
			System.out.println("MORE BYTES IN ASCII STREAM THAN SPECIFIED LENGTH - REJECTED ");
            expectedException(sqle);
		}

		// more bytes than the stream contains
		// JDBC 3.0 changed to indicate an exception should be thrown. (in Tutorial & reference book)
		System.out.println("LESS BYTES IN STREAM THAN PASSED IN VALUE");
		try {
			ps.setAsciiStream(targetCol, new java.io.ByteArrayInputStream("and Dick Cheney.".getBytes("US-ASCII")), 17);
			ps.executeUpdate();
			System.out.println("FAIL - LESS BYTES IN ASCII STREAM THAN SPECIFIED LENGTH - ACCEPTED");
		} catch (SQLException sqle) {
			System.out.println("LESS BYTES IN ASCII STREAM THAN SPECIFIED LENGTH - REJECTED ");
            expectedException(sqle);
		}

		// null
		System.out.println("NULL ASCII STREAM");
		ps.setAsciiStream(targetCol, null, 1);
		ps.executeUpdate();

	}

	private static void setCharacter(PreparedStatement ps, int targetCol) throws Exception {

		// correct character count
		ps.setCharacterStream(targetCol, new java.io.StringReader("A Mississippi Republican"), 24);
		ps.executeUpdate();

		ps.setCharacterStream(targetCol, new java.io.StringReader("Lott has apologized"), 19);
		ps.executeUpdate();

		// less bytes than stream contains.
		try {
			ps.setCharacterStream(targetCol, new java.io.StringReader("for comments he made at"), 20);
			ps.executeUpdate();
			System.out.println("FAIL - MORE CHARACTERS IN READER THAN SPECIFIED LENGTH - ACCEPTED");
		} catch (SQLException sqle) {
			System.out.println("MORE CHARACTERS IN READER THAN SPECIFIED LENGTH - REJECTED ");
            expectedException(sqle);
		}

		// more bytes than the stream contains,
		// JDBC 3.0 changed to indicate an exception should be thrown.
		try {
			ps.setCharacterStream(targetCol, new java.io.StringReader("a birthday party"), 17);
			ps.executeUpdate();
			System.out.println("FAIL - LESS CHARACTERS IN READER THAN SPECIFIED LENGTH - ACCEPTED");
		} catch (SQLException sqle) {
			System.out.println("LESS CHARACTERS IN READER STREAM THAN SPECIFIED LENGTH - REJECTED ");
            expectedException(sqle);
		}

		// null
		ps.setCharacterStream(targetCol, null, 1);
		ps.executeUpdate();

	}

	private static void showResultsAsciiStream(ResultSet rs) throws SQLException, java.io.IOException {

		System.out.println("Results from ASCII stream");
		while (rs.next()) {
			System.out.print(rs.getInt(1));
			System.out.print(",");
			showAsciiStream(rs.getAsciiStream(2));
			System.out.print(",");
			showAsciiStream(rs.getAsciiStream(3));
			System.out.print(",");
			showAsciiStream(rs.getAsciiStream(4));
			System.out.println("");
		}
		rs.close();
	}

	private static void showAsciiStream(java.io.InputStream is) throws java.io.IOException {

		if (is == null) {
			System.out.print("<NULL>");
			return;
		}

		StringBuffer sb = new StringBuffer();
		for (;;) {

			int b = is.read();
			if (b == -1)
				break;

			sb.append((char) b);
		}

		System.out.print(sb.toString());
	}
	private static void showResultsCharacterStream(ResultSet rs) throws SQLException, java.io.IOException {

		System.out.println("Results from Character Stream (read char) stream");
		while (rs.next()) {
			System.out.print(rs.getInt(1));
			System.out.print(",");
			showCharacterStream(rs.getCharacterStream(2));
			System.out.print(",");
			showCharacterStream(rs.getCharacterStream(3));
			System.out.print(",");
			showCharacterStream(rs.getCharacterStream(4));
			System.out.println("");
		}
		rs.close();
	}
	private static void showResultsCharacterStreamBlock(ResultSet rs) throws SQLException, java.io.IOException {

		System.out.println("Results from Character Stream (read block) stream");
		while (rs.next()) {
			System.out.print(rs.getInt(1));
			System.out.print(",");
			showCharacterStreamBlock(rs.getCharacterStream(2));
			System.out.print(",");
			showCharacterStreamBlock(rs.getCharacterStream(3));
			System.out.print(",");
			showCharacterStreamBlock(rs.getCharacterStream(4));
			System.out.println("");
		}
		rs.close();
	}
	private static void showCharacterStream(java.io.Reader r) throws java.io.IOException {

		if (r == null) {
			System.out.print("<NULL>");
			return;
		}

		StringBuffer sb = new StringBuffer();
		for (;;) {

			int b = r.read();
			if (b == -1)
				break;

			sb.append((char) b);
		}

		System.out.print(sb.toString());
	}
	private static void showCharacterStreamBlock(java.io.Reader r) throws java.io.IOException {

		if (r == null) {
			System.out.print("<NULL>");
			return;
		}

		char[] buf = new char[2];

		StringBuffer sb = new StringBuffer();
		for (;;) {

			int read = r.read(buf, 0, buf.length);
			if (read == -1)
				break;

			sb.append(buf, 0, read);
		}

		System.out.print(sb.toString());
	}

	private static void checkAsciiStreams(PreparedStatement psDel, PreparedStatement psi, PreparedStatement psq2,
				int cl, int vcl, int lvcl)
				throws SQLException, java.io.IOException {

		psDel.executeUpdate();

		// now insert long values using streams and check them programatically.
		psi.setAsciiStream(1, new c3AsciiStream(cl), cl);
		psi.setAsciiStream(2, new c3AsciiStream(vcl), vcl);
		psi.setAsciiStream(3, new c3AsciiStream(lvcl), lvcl);
		psi.executeUpdate();

		ResultSet rs = psq2.executeQuery();
		rs.next();

		InputStream is = rs.getAsciiStream(1);
		System.out.print("AS-CHAR-" + cl + " ");
		c3AsciiStream.check(is, cl, 25);
		System.out.println("DONE");

		is = rs.getAsciiStream(2);
		System.out.print("AS-VARCHAR-" + vcl + " ");
		c3AsciiStream.check(is, vcl, -1);
		System.out.println("DONE");

		is = rs.getAsciiStream(3);
		System.out.print("AS-LONG VARCHAR-" + lvcl + " ");
		c3AsciiStream.check(is, lvcl, -1);
		System.out.println("DONE");

		rs.close();
		
		rs = psq2.executeQuery();
		rs.next();

		Reader r = rs.getCharacterStream(1);
		System.out.print("CS-CHAR-" + cl + " ");
		c3AsciiStream.check(r, cl, 25);
		System.out.println("DONE");

		r = rs.getCharacterStream(2);
		System.out.print("CS-VARCHAR-" + vcl + " ");
		c3AsciiStream.check(r, vcl, -1);
		System.out.println("DONE");

		r = rs.getCharacterStream(3);
		System.out.print("CS-LONG VARCHAR-" + lvcl + " ");
		c3AsciiStream.check(r, lvcl, -1);
		System.out.println("DONE");

		rs.close();

		// and check as Strings

		rs = psq2.executeQuery();
		rs.next();

		r = new java.io.StringReader(rs.getString(1));
		System.out.print("ST-CHAR-" + cl + " ");
		c3AsciiStream.check(r, cl, 25);
		System.out.println("DONE");

		r = new java.io.StringReader(rs.getString(2));
		System.out.print("ST-VARCHAR-" + vcl + " ");
		c3AsciiStream.check(r, vcl, -1);
		System.out.println("DONE");

		r = new java.io.StringReader(rs.getString(3));
		System.out.print("ST-LONG VARCHAR-" + lvcl + " ");
		c3AsciiStream.check(r, lvcl, -1);
		System.out.println("DONE");

		rs.close();
		}

	private static void checkCharacterStreams(PreparedStatement psDel, PreparedStatement psi, PreparedStatement psq2,
				int cl, int vcl, int lvcl)
				throws SQLException, java.io.IOException {

		psDel.executeUpdate();

		psi.setCharacterStream(1, new c3Reader(cl), cl);
		psi.setCharacterStream(2, new c3Reader(vcl), vcl);
		psi.setCharacterStream(3, new c3Reader(lvcl), lvcl);
		psi.executeUpdate();

		ResultSet rs = psq2.executeQuery();
		rs.next();

		InputStream is = rs.getAsciiStream(1);
		System.out.print("AS-CHAR-" + cl + " ");
		c3Reader.check(is, cl, 25);
		System.out.println("DONE");

		is = rs.getAsciiStream(2);
		System.out.print("AS-VARCHAR-" + vcl + " ");
		c3Reader.check(is, vcl, -1);
		System.out.println("DONE");

		is = rs.getAsciiStream(3);
		System.out.print("AS-LONG VARCHAR-" + lvcl + " ");
		c3Reader.check(is, lvcl, -1);
		System.out.println("DONE");

		rs.close();
		
		rs = psq2.executeQuery();
		rs.next();

		Reader r = rs.getCharacterStream(1);
		System.out.print("CS-CHAR-" + cl + " ");
		c3Reader.check(r, cl, 25);
		System.out.println("DONE");

		r = rs.getCharacterStream(2);
		System.out.print("CS-VARCHAR-" + vcl + " ");
		c3Reader.check(r, vcl, -1);
		System.out.println("DONE");

		r = rs.getCharacterStream(3);
		System.out.print("CS-LONG VARCHAR-" + lvcl + " ");
		c3Reader.check(r, lvcl, -1);
		System.out.println("DONE");

		rs.close();

		// check converting them into Strings work
		rs = psq2.executeQuery();
		rs.next();

		String suv = rs.getString(1);
		r = new java.io.StringReader(suv);
		System.out.print("ST-CHAR-" + cl + " ");
		c3Reader.check(r, cl, 25);
		System.out.println("DONE");

		suv = rs.getString(2);
		r = new java.io.StringReader(suv);
		System.out.print("ST-VARCHAR-" + vcl + " ");
		c3Reader.check(r, vcl, -1);
		System.out.println("DONE");

		suv = rs.getString(3);
		r = new java.io.StringReader(suv);
		System.out.print("ST-LONG VARCHAR-" + lvcl + " ");
		c3Reader.check(r, lvcl, -1);
		System.out.println("DONE");

		rs.close();

		}
}

class c3AsciiStream extends java.io.InputStream {

	private final int size;
	private int count;
	c3AsciiStream(int size) {
		this.size = size;
	}
	public int read(byte[] buf, int off, int length) {
		if (count >= size)
			return -1;

		if (length > (size - count))
			length = (size - count);

		// ensure the readers don't always get a full buffer,
		// makes sure they are not assuming the buffer will be filled.

		if (length > 20)
			length -= 17;

		for (int i = 0; i < length ; i++) {
			buf[off + i] = (byte) count++;
		}

		return length;
	}

	private byte[] rd = new byte[1];
	public int read() {

		int read = read(rd, 0, 1);
		if (read == -1)
			return -1;
		return rd[0] & 0xFF;
	}

	public void close() {
	}

	static void check(InputStream is, int length, int fixedLen) throws java.io.IOException {

		InputStream orig = new c3AsciiStream(length);

		int count = 0;
		for (;;) {

			int o = orig == null ? (count == fixedLen ? -2 : 0x20) : orig.read();
			int c = is.read();
			if (o == -1) {
				orig = null;
				if (fixedLen != -1 && fixedLen != length)
					o = ' ';
			}
			if (o == -2)
				o = -1;

			if ((byte) o != (byte) c) {
				System.out.print("F@" + count +"("+((byte)o)+","+((byte)c)+")");
			}
			if (orig == null) {
				if (fixedLen == -1)
					break;
			}

			if (c == -1 && fixedLen != -1)
				break;
			
			count++;
		}
		if (fixedLen != -1)
			length = fixedLen;

		if (count != length) {
			System.out.print("FAIL-LEN" + count + " expected " + length);
		}
		is.close();
	}
	static void check(Reader r, int length, int fixedLen) throws java.io.IOException {

		InputStream orig = new c3AsciiStream(length);

		int count = 0;
		for (;;) {

			int o = orig == null ? (count == fixedLen ? -2 : 0x20) : orig.read();
			int c = r.read();
			if (o == -1) {
				orig = null;
				if (fixedLen != -1 && fixedLen != length)
					o = ' ';
			}
			if (o == -2)
				o = -1;

			if (o != c) {
				System.out.print("F@" + count +"("+o+","+c+")");
			}
			if (orig == null) {
				if (fixedLen == -1)
					break;
			}

			if (c == -1 && fixedLen != -1)
				break;
			
			count++;
		}
		if (fixedLen != -1)
			length = fixedLen;

		if (count != length) {
			System.out.print("FAIL-LEN" + count + " expected " + length);
		}
		r.close();
	}
}

class c3Reader extends java.io.Reader {

	private final int size;
	private int count;
	c3Reader(int size) {
		this.size = size;
	}
	public int read(char[] buf, int off, int length) {
		if (count >= size)
			return -1;

		if (length > (size - count))
			length = (size - count);

		// ensure the readers don't always get a full buffer,
		// makes sure they are not assuming the buffer will be filled.

		if (length > 20)
			length -= 17;

		for (int i = 0; i < length ; i++) {
			char c;
			switch (count % 3) {
			case 0:
				c = (char) (count & 0x7F); // one byte UTF8
				break;
			case 1:
				c = (char) ((count + 0x7F) & 0x07FF); // two byte UTF8
				break;
			default:
			case 2:
				c = (char) (count + 0x07FF); // three byte UTF8
				break;

			}
			buf[off + i] = c;
			count++;
		}
		return length;
	}

	public void close() {
	}
	static void check(InputStream is, int length, int fixedLen) throws java.io.IOException {

		Reader orig = new c3Reader(length);

		int count = 0;
		for (;;) {

			int o = orig == null ? (count == fixedLen ? -2 : 0x20) : orig.read();
			int c = is.read();
			if (o == -1) {
				orig = null;
				if (fixedLen != -1 && fixedLen != length)
					o = ' ';
			}
			if (o == -2)
				o = -1;

			if (o != -1) {
				if (o <= 255)
					o = o & 0xFF; // convert to single byte exended ASCII
				else
					o = '?'; // out of range character.
			}

			if (o != c) {
				System.out.print("F@" + count +"("+o+","+c+")");
			}
			if (orig == null) {
				if (fixedLen == -1)
					break;
			}

			if (c == -1 && fixedLen != -1)
				break;
			
			count++;
		}
		if (fixedLen != -1)
			length = fixedLen;

		if (count != length) {
			System.out.print("FAIL-LEN" + count + " expected " + length);
		}
		is.close();
	}
	static void check(Reader r, int length, int fixedLen) throws java.io.IOException {

		Reader orig = new c3Reader(length);

		int count = 0;
		for (;;) {

			int o = orig == null ? (count == fixedLen ? -2 : 0x20) : orig.read();
			int c = r.read();
			if (o == -1) {
				orig = null;
				if (fixedLen != -1 && fixedLen != length)
					o = ' ';
			}
			if (o == -2)
				o = -1;

			if (o != c) {
				System.out.print("F@" + count +"("+o+","+c+")");
			}
			if (orig == null) {
				if (fixedLen == -1)
					break;
			}

			if (c == -1 && fixedLen != -1)
				break;
			
			count++;
		}
		if (fixedLen != -1)
			length = fixedLen;

		if (count != length) {
			System.out.print("FAIL-LEN" + count + " expected " + length);
		}
		r.close();
	}
}
