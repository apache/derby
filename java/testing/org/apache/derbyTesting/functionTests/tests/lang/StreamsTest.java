/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.streams

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;

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
  
  Converted to Junit based StreamsTest from the old streams.java test.
 */

public class StreamsTest extends BaseJDBCTestCase {
    
    public StreamsTest(String name) {
        super(name);
     }
    
    /**
     * Only runs embedded as it is checking stack depths
     * of when certain operations happen to streams.
     * Stack depths from the network server would be different
     * and test is not designed work with a network client.
     */
    public static Test suite() {
        
        Test test = new TestSuite(StreamsTest.class, "StreamsTest");
        
        test = DatabasePropertyTestSetup.singleProperty(test,
                "derby.storage.pageSize", "2048");
        
        return new CleanDatabaseTestSetup(test) {
            
            protected void decorateSQL(Statement s) throws SQLException
            {
                s.executeUpdate(
                        "create table t1 (id int, pid int, lvc long varchar, " +
                        "lvb long varchar for bit data)");
                s.executeUpdate("create table t2 (id int, pid int, " +
                        "lvc long varchar, lvb long varchar for bit data)");
                s.executeUpdate("create trigger tr21 after insert on t2 " +
                        "for each statement values 1");
                s.executeUpdate("create table t3 (id int not null primary key, " +
                        "pid int, lvc long varchar, lvb long varchar for bit data, " +
                        "CONSTRAINT FK1 Foreign Key(pid) REFERENCES T3 (id))");
                s.executeUpdate("create table t4 (id int, longcol long varchar)");
                s.executeUpdate("create table t5 (id int, longcol long varchar)");                
            }
            
        };
        
    }

    /**
     * Unique values for primary keys.
     */
    private int pkCount;
    
    public void testStreams() throws Exception {
        
        getConnection().setAutoCommit(false);
        
        Statement s = createStatement();
        
        PreparedStatement ps = prepareStatement(
                "insert into  t1 values(?, ?, ?,?)");
        int level1 = insertLongString(ps, 8, true);
        // materialized insert: got reader stack level
        ps.close();
        
        ps = prepareStatement("insert into  t2 values(?, ?, ?,?)");
        int level2 = insertLongString(ps, 8, true);
        // materialized insert (for trigger): got reader stack level
        assertEquals("FAILED!! level difference not expected since streams are materialized.",
                level1, level2);
        ps.close();
        
        ps = prepareStatement("insert into  t3 values(?, ?, ?,?)");
        int level3 = insertLongString(ps, 8, true);
        ps.close();
        
        // self ref foreign key insert(should not materialize):
        // got reader stack level");
        assertEquals("FAILED!! should not materialize in this case.",
                level3, level1);
        
        rollback();
        
        s.executeUpdate(
                "insert into t3 values (1,1,'a',null)," +
                "(2,2,'b',null), (3,3,'c',null)");
        ps = prepareStatement("update t3 set id = ?, lvc = ? where pid = 2");
        level1 = insertLongString(ps, 8, false);
        ps.close();
        // materialized for multiple row update: got reader stack level
        
        ps = prepareStatement("update t3 set id = ?, lvc = ? where pid = 2 " +
                "and id = 2");
        level2 = insertLongString(ps, 8, false);
        ps.close();
        // single row update: got reader stack level
        assertEquals("FAILED!! level difference not expected because streams are materialized with fix for bug 5592.",
                level1, level2);
        
        s.executeUpdate("insert into t4 values (1, 'ccccc')");
        ps = prepareStatement("insert into t4 values(?, ?)");
        insertLongString(ps, 6, false);
        s.executeUpdate("insert into t4 values (3, 'aaaaabbbbbb')");
        s.executeUpdate("insert into t4 values (4, 'bbbbbb')");
        insertLongString(ps, 5, false);
        ps.close();
        ResultSet rs = s
        .executeQuery("select id, cast(longcol as varchar(8192)) lcol from t4 order by lcol");
        
        assertTrue(rs.next()); // 3, aaaaabbbbbb
        assertEquals(3, rs.getInt(1));
        assertEquals("aaaaabbbbbb", rs.getString(2));
        
        assertTrue(rs.next()); // 4, bbbbbb
        assertEquals(4, rs.getInt(1));
        assertEquals("bbbbbb", rs.getString(2));
        
        assertTrue(rs.next()); // 2, bbbbbb... (length 5393)
        assertEquals(2, rs.getInt(1));
        String col2 = rs.getString(2);
        assertNotNull(col2);
        assertEquals(5393, col2.length());
        for (int i = 0; i < col2.length(); i++)
            assertEquals('b', col2.charAt(i));
        
        assertTrue(rs.next()); // 2, bbbbbb... (length 6417)
        assertEquals(2, rs.getInt(1));
        col2 = rs.getString(2);
        assertNotNull(col2);
        assertEquals(6417, col2.length());
        for (int i = 0; i < col2.length(); i++)
            assertEquals('b', col2.charAt(i));   
        
        assertTrue(rs.next()); // 1, 'ccccc'
        assertEquals(1, rs.getInt(1));
        assertEquals("ccccc", rs.getString(2));
        
        assertFalse(rs.next());
        rs.close();
        
        s.executeUpdate("insert into t5 values (1, 'bbbbbb')");
        ps = prepareStatement("insert into t5 values(?, ?)");
        insertLongString(ps, 5, false);
        insertLongString(ps, 7, false);
        ps.close();
        s.executeUpdate("insert into t5 values (3, 'aaaaabbbbbba')");
        s.executeUpdate("insert into t5 values (4, 'bbbbbbbbb')");
        rs = s
        .executeQuery("select t4.id, t4.longcol, t5.id, cast(t5.longcol as varchar(8192)) lcol from t4, t5 where cast(t4.longcol as varchar(8192)) = cast(t5.longcol as varchar(8192)) order by lcol");
        
        assertTrue(rs.next());
        assertEquals(4, rs.getInt(1));
        assertEquals(6, rs.getString(2).length());
        assertEquals(1, rs.getInt(3));
        assertEquals(6, rs.getString(4).length());
        
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        assertEquals(5393, rs.getString(2).length());
        assertEquals(2, rs.getInt(3));
        assertEquals(5393, rs.getString(4).length());
        
        assertFalse(rs.next());
        rs.close();
        
        // Start testing long var binary
        rollback();
        
        ps = prepareStatement("insert into  t1 values(?, ?, ?,?)");
        level1 = insertLongBinary(ps, 8);
        ps.close();
        // non materialized insert: got reader stack level");
        
        ps = prepareStatement("insert into  t2 values(?, ?, ?,?)");
        level2 = insertLongBinary(ps, 8);
        ps.close();
        // materialized insert (for trigger): got reader stack level");
        assertTrue("FAILED, check stack level change.",
                level1 > level2 + 5);
        
        
        ps = prepareStatement("insert into  t3 values(?, ?, ?,?)");
        level3 = insertLongBinary(ps, 8);
        ps.close();
        // self ref foreign key insert(should not materialize):
        // got reader stack level");
        assertEquals("FAILED!! should not materialize stream in this case.",
                level3, level1);

        
        s.close();
        rollback();
    }
    
    private int insertLongString(PreparedStatement ps, int kchars,
            boolean isInsert) throws SQLException {
        // don't end on a clean boundary
        int chars = (kchars * 1024) + 273;
                
        DummyReader dr = new DummyReader(chars);
        if (isInsert) {
            ps.setInt(1, pkCount);
            ps.setInt(2, pkCount++);
            ps.setCharacterStream(3, dr, chars);
            ps.setNull(4, Types.VARBINARY);
        } else {
            ps.setInt(1, 2);
            ps.setCharacterStream(2, dr, chars);
        }
        
        ps.executeUpdate();
        
        return dr.readerStackLevel;
        
    }
    
    private  int insertLongBinary(PreparedStatement ps, int kbytes)
    throws SQLException {
        
        // add a small number of bytes to ensure that we are not always ending
        // on a clean Mb boundary
        int bytes = (kbytes * 1024) + 273;
        
        ps.setInt(1, pkCount);
        ps.setInt(2, pkCount++);
        ps.setNull(3, Types.LONGVARCHAR);
        DummyBinary db = new DummyBinary(bytes);
        ps.setBinaryStream(4, db, bytes);
        
        ps.executeUpdate();
        
        return db.readerStackLevel;
    }
}

class DummyReader extends java.io.Reader {
    
    private int count;
    
    public int readerStackLevel;
    
    DummyReader(int length) {
        this.count = length;
    }
    
    private void whereAmI() {
        if (count == 0) {
            readerStackLevel = -1;
            try {
                throw new Throwable();
            } catch (Throwable e) {
                try {
                    readerStackLevel = e.getStackTrace().length;
                    // System.out.println("================= stack array length
                    // is: " + readerStackLevel);
                    // e.printStackTrace();
                } catch (NoSuchMethodError nme) {
                    DummyOutputStream dos = new DummyOutputStream();
                    DummyPrintStream dps = new DummyPrintStream(dos);
                    e.printStackTrace(dps);
                    dps.flush();
                    // System.out.println("================= print to dop level
                    // num is: " + dps.lines);
                    readerStackLevel = dps.lines;
                    // e.printStackTrace();
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
    
    public void close() {
    }
}

class DummyBinary extends java.io.InputStream {
    
    public int readerStackLevel;
    
    int count;
    
    byte content = 42;
    
    DummyBinary(int length) {
        this.count = length;
    }
    
    private void whereAmI() {
        if (count == 0) {
            readerStackLevel = -1;
            try {
                throw new Throwable();
            } catch (Throwable e) {
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
    
    public void close() {
    }
}

class DummyOutputStream extends java.io.OutputStream {
    public void close() {
    }
    
    public void flush() {
    }
    
    public void write(byte[] b) {
    }
    
    public void write(byte[] b, int off, int len) {
    }
    
    public void write(int b) {
    }
}

class DummyPrintStream extends java.io.PrintStream {
    int lines;
    
    public DummyPrintStream(DummyOutputStream dos) {
        super(dos);
    }
    
    public void println() {
        lines++;
    }
    
    public void println(String x) {
        lines++;
    }
    
    public void println(Object x) {
        lines++;
    }
    
    public void println(char[] x) {
        lines++;
    }
    
    public void println(double x) {
        lines++;
    }
    
    public void println(float x) {
        lines++;
    }
    
    public void println(long x) {
        lines++;
    }
    
    public void println(int x) {
        lines++;
    }
    
    public void println(char x) {
        lines++;
    }
    
    public void println(boolean x) {
        lines++;
    }
}
