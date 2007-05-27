/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ClobUpdateableReaderTest
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

import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test class to test <code>UpdateableReader</code> for <code>Clob</code> in
 * embedded driver.
 */
public class ClobUpdateableReaderTest extends BaseJDBCTestCase {
    
    private final String dummy = "This is a new String";
        
    public ClobUpdateableReaderTest (String name) {
        super (name);
    }
    
    /**
     * Tests updates on reader.
     */
    public void testUpdateableReader () throws Exception {
        Connection con = getConnection();
        try {
            con.setAutoCommit (false);
            PreparedStatement ps = con.prepareStatement ("insert into updateClob " +
                    "(id , data) values (? ,?)");
            ps.setInt (1, 1);
            StringBuffer sb = new StringBuffer ();
            String base = "SampleSampleSample";
            for (int i = 0; i < 100; i++) {
                sb.append (base);
            }
            ps.setCharacterStream (2, new StringReader (sb.toString()), 
                                                sb.length());
            ps.execute();
            ps.close();
            Statement stmt = con.createStatement ();
            ResultSet rs = stmt.executeQuery("select data from " +
                    "updateClob where id = 1");
            rs.next();
            Clob clob = rs.getClob (1);
            rs.close();
            stmt.close();
            assertEquals (sb.length(), clob.length());
            Reader r = clob.getCharacterStream();
            char [] clobData = new char [sb.length()];
            r.read (clobData);
            assertEquals ("mismatch from inserted string", 
                                String.valueOf (clobData), sb.toString());
            r.close();
            //update before gettting the reader
            clob.setString (50, dummy);        
            r = clob.getCharacterStream();
            r.skip (49);
            char [] newChars = new char [dummy.length()];
            r.read (newChars);
            assertEquals ("update not reflected", dummy,
                                        String.valueOf (newChars));
            //update again and see if stream is refreshed
            clob.setString (75, dummy);
            r.skip (75 - 50 - dummy.length());
            char [] testChars = new char [dummy.length()];
            r.read (testChars);
            assertEquals ("update not reflected", dummy,
                                        String.valueOf (newChars));
            r.close();
            //try inserting some unicode string
            String unicodeStr = getUnicodeString();
            clob.setString (50, unicodeStr);
            char [] utf16Chars = new char [unicodeStr.length()];
            r = clob.getCharacterStream();
            r.skip(49);
            r.read(utf16Chars);
            assertEquals ("update not reflected",  unicodeStr,
                                        String.valueOf (utf16Chars));
            r.close();
            Writer w = clob.setCharacterStream (1);
            //write enough data to switch the data to file
            r = clob.getCharacterStream ();
            for (int i = 0; i < 10000; i++) {
                w.write (dummy);
            }
            w.close();            
            clob.setString (500, unicodeStr);
            r.skip (499);
            char [] unicodeChars = new char [unicodeStr.length()];
            r.read (unicodeChars);
            assertEquals ("update not reflected",  unicodeStr,
                                        String.valueOf (unicodeChars));            
        }
        finally {
            if (con != null) {
                con.commit ();
                con.close();
            }
        }
    }   
    
    /**
     * Generates a (static) string containing various Unicode characters.
     *
     * @return a string with ASCII and non-ASCII characters
     */
    private String getUnicodeString () {
        char[] fill = new char[4];
        fill[0] = 'd';          // 1 byte UTF8 character (ASCII)
        fill[1] = '\u03a9';     // 2 byte UTF8 character (Greek)
        fill[2] = '\u0e14';     // 3 byte UTF8 character (Thai)
        fill[3] = 'j';          // 1 byte UTF8 character (ASCII)
        StringBuffer sb = new StringBuffer ();
        for (int i = 0; i < 4; i++) {
            sb.append (fill);
        }
        return sb.toString();        
    }
    
    /**
     * Setup the test.
     *
     * @throws SQLException if database access fails
     */
    public void setUp() throws Exception {
        Connection con = getConnection ();
        Statement stmt = con.createStatement ();
        stmt.execute ("create table updateClob " +
                "(id integer primary key, data clob)");
        stmt.close();
        con.commit();
        con.close();
    }
    
    public static Test suite() {
        TestSuite ts = new TestSuite ("ClobUpdateableReaderTest");
        ts.addTest(TestConfiguration.embeddedSuite(
                    ClobUpdateableReaderTest.class));
        TestSuite encSuite = new TestSuite ("ClobUpdateableReaderTest:encrypted");
        encSuite.addTestSuite (ClobUpdateableReaderTest.class);
        ts.addTest(Decorator.encryptedDatabase (encSuite));
        return ts;
    }        

    /**
     * Cleans up the database.
     */
    protected void tearDown() throws java.lang.Exception {
        Connection con = getConnection ();
        Statement stmt = con.createStatement ();
        stmt.execute ("drop table updateClob");
        stmt.close();
        con.close();
    }
}
