/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.streams

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;

import org.apache.derby.tools.ij;
import org.apache.derby.tools.JDBCDisplayUtil;

/**
  This tests streams, and when we should not materialize it. Beetle entry 4896, 4955.

  Some of the code comes from conn/largeStreams.java.  But this program figures out whether
  a stream is materialized or not in a different way.  Part of the reason is that this test
  should be run nightly to catch regressions and shouldn't require too much disk space.  It
  figures out whether a stream is materialized or not by comparing the stack levels of different
  cases.  The stack level is when reading the last byte of the stream.  According to the current
  code, the stack is about 10 levels deeper when reading the stream from store per page (not
  materialized before hand), comparing to the case when materializing from sql language layer.
  We don't expect this to change dramatically for some time.  And this can always be adjusted
  when needed.

  For bug 5592 - match db's limits for long varchar which is 32700. In order to enforce that limit
  we now materialize the stream to make sure we are not trying to overstuff data in long varchar.
  Because of this, I had to make some changes into the stack level checking for long varchars.
 */

public class streams {

	private static int pkCount;
	private static Connection conn;

	public static void main(String[] args) {
		System.out.println("Test streams starting");

		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
			conn = ij.startJBMS();

			conn.setAutoCommit(true);

			setup();

			conn.setAutoCommit(false);

			doWork();

			conn.setAutoCommit(true);

			teardown();

			conn.close();

		} catch (Throwable e) {
			System.out.println("FAIL: exception thrown:");
			JDBCDisplayUtil.ShowException(System.out,e);
		}

		System.out.println("Test streams finished");
	}

	static void setup() throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("call SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.storage.pageSize', '2048')");

		verifyCount(
			stmt.executeUpdate("create table t1 (id int, pid int, lvc long varchar, lvb long varchar for bit data)"),
			0);
		verifyCount(
			stmt.executeUpdate("create table t2 (id int, pid int, lvc long varchar, lvb long varchar for bit data)"),
			0);
		verifyCount(
			stmt.executeUpdate("create trigger tr21 after insert on t2 for each statement mode db2sql values 1"),
			0);
		verifyCount(
			stmt.executeUpdate("create table t3 (id int not null primary key, pid int, lvc long varchar, lvb long varchar for bit data, CONSTRAINT FK1 Foreign Key(pid) REFERENCES T3 (id))"),
			0);
		verifyCount(
			stmt.executeUpdate("create table t4 (id int, longcol long varchar)"),
			0);
		verifyCount(
			stmt.executeUpdate("create table t5 (id int, longcol long varchar)"),
			0);
	}

	static void teardown() throws SQLException {
		Statement stmt = conn.createStatement();

		verifyCount(
		    stmt.executeUpdate("drop table t1"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop trigger tr21"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop table t2"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop table t3"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop table t4"),
			0);

		verifyCount(
		    stmt.executeUpdate("drop table t5"),
			0);

		stmt.close();

		System.out.println("teardown complete");
	}

	static void verifyCount(int count, int expect) throws SQLException {
		if (count!=expect) {
			System.out.println("FAIL: Expected "+expect+" got "+count+" rows");
			throw new SQLException("Wrong number of rows returned");
		}
		else
			System.out.println("PASS: expected and got "+count+
							   (count == 1? " row":" rows"));
	}

	private static void doWork() throws Exception {

		Statement s = 		conn.createStatement();

		System.out.println("Start testing");

		PreparedStatement ps = conn.prepareStatement("insert into  t1 values(?, ?, ?,?)");
		int level1 = insertLongString(ps, 8, true);
		System.out.println("materialized insert: got reader stack level");

		ps = conn.prepareStatement("insert into  t2 values(?, ?, ?,?)");
		int level2 = insertLongString(ps, 8, true);
		System.out.println("materialized insert (for trigger): got reader stack level");

		if (level1 != level2)
			System.out.println("FAILED!! level difference not expected since streams are materialized.");
		else
			System.out.println("SUCCEED!! stack level as expected.");
		
		ps = conn.prepareStatement("insert into  t3 values(?, ?, ?,?)");
		int level3 = insertLongString(ps, 8, true);
		System.out.println("self ref foreign key insert(should not materialize): got reader stack level");
		if (level3 == level1)
			System.out.println("SUCCEED!! levels expected.");
		else
			System.out.println("FAILED!! should not materialize in this case.");

		conn.rollback();

		s.executeUpdate("insert into t3 values (1,1,'a',null), (2,2,'b',null), (3,3,'c',null)");
		ps = conn.prepareStatement("update t3 set id = ?, lvc = ? where pid = 2");
		level1 = insertLongString(ps, 8, false);
		System.out.println("materialized for multiple row update: got reader stack level");

		ps = conn.prepareStatement("update t3 set id = ?, lvc = ? where pid = 2 and id = 2");
		level2 = insertLongString(ps, 8, false);
		System.out.println("single row update: got reader stack level");

		if (level1 != level2)
			System.out.println("FAILED!! level difference not expected because streams are materialized with fix for bug 5592.");
		else
			System.out.println("SUCCEED!! single row update materialized stream.");

		s.executeUpdate("insert into t4 values (1, 'ccccc')");
		ps = conn.prepareStatement("insert into t4 values(?, ?)");
		insertLongString(ps, 6, false);
		s.executeUpdate("insert into t4 values (3, 'aaaaabbbbbb')");
		s.executeUpdate("insert into t4 values (4, 'bbbbbb')");
		insertLongString(ps, 5, false);
		ResultSet rs = s.executeQuery("select id, cast(longcol as varchar(8192)) lcol from t4 order by lcol");
		if (rs.next())			// 3, aaaaabbbbbb
			System.out.println("id = "+ rs.getInt(1) + " longcol = " + rs.getString(2));
		if (rs.next())			// 4, bbbbbb
			System.out.println("id = "+ rs.getInt(1) + " longcol = " + rs.getString(2));
		for (int i = 0; i < 2; i++)
		{
			if (rs.next())
			{
				String longcol = rs.getString(2);
				int collen = longcol.length();
				System.out.print("id = "+ rs.getInt(1) + " longcol length = " + collen);
				System.out.println(" longcol = " + longcol.substring(0, 5) + "..." +
									longcol.substring(collen - 5, collen));
			}
		}
		if (rs.next())			// 1, 'ccccc'
			System.out.println("id = "+ rs.getInt(1) + " longcol = " + rs.getString(2));
		if (rs.next())
			System.out.println("FAILED, more rows left");
		else
			System.out.println("number of rows ok");

		s.executeUpdate("insert into t5 values (1, 'bbbbbb')");
		ps = conn.prepareStatement("insert into t5 values(?, ?)");
		insertLongString(ps, 5, false);
		insertLongString(ps, 7, false);
		s.executeUpdate("insert into t5 values (3, 'aaaaabbbbbba')");
		s.executeUpdate("insert into t5 values (4, 'bbbbbbbbb')");
		rs = s.executeQuery("select t4.id, t4.longcol, t5.id, cast(t5.longcol as varchar(8192)) lcol from t4, t5 where cast(t4.longcol as varchar(8192)) = cast(t5.longcol as varchar(8192)) order by lcol");
		while (rs.next())
		{
			System.out.println("t4 id = " + rs.getInt(1) + " t4 longcol length = " + rs.getString(2).length()
					+ " t5 id = " + rs.getInt(3) + " t5 longcol length = " + rs.getString(4).length());
		}

		System.out.println("Start testing long var binary");
		conn.rollback();

		ps = conn.prepareStatement("insert into  t1 values(?, ?, ?,?)");
		level1 = insertLongBinary(ps, 8);
		System.out.println("non materialized insert: got reader stack level");

		ps = conn.prepareStatement("insert into  t2 values(?, ?, ?,?)");
		level2 = insertLongBinary(ps, 8);
		System.out.println("materialized insert (for trigger): got reader stack level");

		if (level1 > level2 + 5)
			System.out.println("SUCCEED, level difference expected.");
		else
			System.out.println("FAILED, check stack level change.");
		
		ps = conn.prepareStatement("insert into  t3 values(?, ?, ?,?)");
		level3 = insertLongBinary(ps, 8);
		System.out.println("self ref foreign key insert(should not materialize): got reader stack level");
		if (level3 == level1)
			System.out.println("SUCCEED!! levels expected.");
		else
			System.out.println("FAILED!! should not materialize stream in this case.");

		conn.rollback();
	}

	private static int insertLongString(PreparedStatement ps, int kchars, boolean isInsert) throws SQLException
	{
		// don't end on a clean boundary
		int chars = (kchars * 1024) + 273;

		long start = System.currentTimeMillis();

		DummyReader dr = new DummyReader(chars);
		if (isInsert)
		{
			ps.setInt(1, pkCount);
			ps.setInt(2, pkCount++);
			ps.setCharacterStream(3,  dr, chars);
			ps.setNull(4, Types.VARBINARY);
		}
		else
		{
			ps.setInt(1, 2);
			ps.setCharacterStream(2, dr, chars);
		}

		ps.executeUpdate();
		long end = System.currentTimeMillis();

		System.out.println("setCharacterStream " + chars + " chars");

		return dr.readerStackLevel;

	}

	private static int insertLongBinary(PreparedStatement ps, int kbytes) throws SQLException {

		// add a small number of bytes to ensure that we are not always ending on a clean Mb boundary
		int bytes = (kbytes * 1024) + 273;

		long start = System.currentTimeMillis();
		ps.setInt(1, pkCount);
		ps.setInt(2, pkCount++);
		ps.setNull(3, Types.LONGVARCHAR);
		DummyBinary db = new DummyBinary(bytes);
		ps.setBinaryStream(4, db, bytes);

		ps.executeUpdate();
		long end = System.currentTimeMillis();

		System.out.println("setBinaryStream " + bytes + " bytes");

		return db.readerStackLevel;
	}
}

class DummyReader extends java.io.Reader {

	private int count;
	public int readerStackLevel;

	DummyReader(int length) {
		this.count  = length;
	}

	private void whereAmI() {
		if (count == 0)
		{
			readerStackLevel = -1;
			try {throw new Throwable();} catch (Throwable e) {
				try {
					readerStackLevel = e.getStackTrace().length;
				//	System.out.println("================= stack array length is: " + readerStackLevel);
				//	e.printStackTrace();
				} catch (NoSuchMethodError nme) {
					DummyOutputStream dos = new DummyOutputStream();
					DummyPrintStream dps = new DummyPrintStream(dos);
					e.printStackTrace(dps);
					dps.flush();
				//	System.out.println("================= print to dop level num is: " + dps.lines);
					readerStackLevel = dps.lines;
				//	e.printStackTrace();
				}
			}
		}
	}

	public int read() {
		if (count == 0)
			return -1;

		count--;
		whereAmI();

		return 'b';
	}

	public int read(char[] buf, int offset, int length) {

		if (count == 0)
			return -1;

		if (length > count)
			length = count;

		count -= length;
		whereAmI();

		java.util.Arrays.fill(buf, offset, offset + length, 'b');

		return length;
	}

	public void close(){}
}

class DummyBinary extends java.io.InputStream {

	public int readerStackLevel;
	int count;
	byte content = 42;
	DummyBinary(int length) {
		this.count  = length;
	}

	private void whereAmI() {
		if (count == 0)
		{
			readerStackLevel = -1;
			try {throw new Throwable();} catch (Throwable e) {
				try {
					readerStackLevel = e.getStackTrace().length;
				//	System.out.println("================= stack array length is: " + readerStackLevel);
				//	e.printStackTrace();
				} catch (NoSuchMethodError nme) {
					DummyOutputStream dos = new DummyOutputStream();
					DummyPrintStream dps = new DummyPrintStream(dos);
					e.printStackTrace(dps);
					dps.flush();
				//	System.out.println("================= print to dop level num is: " + dps.lines);
					readerStackLevel = dps.lines;
				//	e.printStackTrace();
				}
			}
		}
	}

	public int read() {
		if (count == 0)
			return -1;

		count--;
		whereAmI();
		return content++;
	}

	public int read(byte[] buf, int offset, int length) {

		if (count == 0)
			return -1;

		if (length > count)
			length = count;

		count -= length;
		whereAmI();

		for (int i = 0; i < length; i++)
			buf[offset + i] = content++;

		return length;
	}

	public void close(){}
}


class DummyOutputStream extends java.io.OutputStream {
	public void close() {}
	public void flush() {}
	public void write(byte[] b) {}
	public void write(byte[] b, int off, int len) {}
	public void write(int b) {}
}

class DummyPrintStream extends java.io.PrintStream {
	public int lines;
	public DummyPrintStream(DummyOutputStream dos) {super(dos);}
	public void println() { lines++; }
	public void println(String x) { lines++; }
	public void println(Object x) { lines++; }
	public void println(char[] x) { lines++; }
	public void println(double x) { lines++; }
	public void println(float x) { lines++; }
	public void println(long x) { lines++; }
	public void println(int x) { lines++; }
	public void println(char x) { lines++; }
	public void println(boolean x) { lines++; }
}
