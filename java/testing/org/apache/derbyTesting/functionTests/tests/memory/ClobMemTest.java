/**
 * Derby - Class org.apache.derbyTesting.functionTests.tests.memory.ClobMemTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.derbyTesting.functionTests.tests.memory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Properties;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import junit.framework.Test;

import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

public class ClobMemTest extends BaseJDBCTestCase {

    private static final int LONG_CLOB_LENGTH = 18000000;
    private static final String LONG_CLOB_LENGTH_STRING= "18000000";
    private static final char[] SHORT_CLOB_CHARS = new char[] {'\uc911','\uc5d0','a', '\uc608', '\uae30',
            '\uce58'};

    public ClobMemTest(String name) {
        super(name);
    }

    /**
     * Insert a clob and test length.
     *
     * @param lengthless  if true use the lengthless setCharacterStream api
     *
     * @throws SQLException
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    private void testClobLength(boolean lengthless) throws SQLException, IOException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        setAutoCommit(false);
        Statement s = createStatement();
        s.executeUpdate("CREATE TABLE CLOBTABLE (K INT CONSTRAINT PK PRIMARY KEY, C CLOB(" + LONG_CLOB_LENGTH + "))");

        PreparedStatement ps = prepareStatement("INSERT INTO CLOBTABLE VALUES(?,?)");
        // We allocate 16MB for the test so use something bigger than that.
        ps.setInt(1,1);
        LoopingAlphabetReader reader = new LoopingAlphabetReader(LONG_CLOB_LENGTH);
        if (lengthless) {
            Method m = null;
            try {
                Class c = ps.getClass();
                m = c.getMethod("setCharacterStream",new Class[] {Integer.TYPE,
                            InputStream.class});
            } catch (NoSuchMethodException e) {
                // ignore method not found as method may not be present for
                // jdk's lower than 1.6.
                println("Skipping lengthless insert because method is not available");
                return;
            }
            m.invoke(ps, new Object[] {new Integer(2), reader});
        }
        else
            ps.setCharacterStream(2, reader, LONG_CLOB_LENGTH);
        ps.executeUpdate();
        // insert a zero length clob.
        ps.setInt(1, 2);
        ps.setString(2, "");
        ps.executeUpdate();
        // insert a null clob.
        ps.setInt(1, 3);
        ps.setString(2,null);
        ps.executeUpdate();
        // insert a short clob
        ps.setInt(1, 4);
        ps.setString(2, new String(SHORT_CLOB_CHARS));
        ps.executeUpdate();
        // Currently need to use optimizer override to force use of the index.
        // Derby should use sort avoidance and do it automatically, but there
        // appears to be a bug.
        ResultSet rs = s.executeQuery("SELECT K, LENGTH(C), C FROM CLOBTABLE" +
                "-- DERBY-PROPERTIES constraint=pk\n ORDER BY K");
        rs.next();
        assertEquals(LONG_CLOB_LENGTH_STRING,rs.getString(2));
        // make sure we can still access the clob after getting length.
        // It should be ok because we reset the stream
        Reader rsReader = rs.getCharacterStream(3);
        int len= 0;
        char[] buf = new char[32672];
        for (;;)  {
                int size = rsReader.read(buf);
                if (size == -1)
                        break;
                len += size;
                int expectedValue = ((len -1) % 26) + 'a';
                if (size != 0)
                    assertEquals(expectedValue,buf[size -1]);
        }

        assertEquals(LONG_CLOB_LENGTH,len);
        // empty clob
        rs.next();
        assertEquals("0",rs.getString(2));
        String chars = rs.getString(3);
        assertEquals(0, chars.length());
        // null clob
        rs.next();
        assertEquals(null, rs.getString(2));
        chars = rs.getString(3);
        assertEquals(null, chars);
        // short clob
        rs.next();
        assertEquals("" + SHORT_CLOB_CHARS.length , rs.getString(2));
        chars = rs.getString(3);
        assertTrue(Arrays.equals(chars.toCharArray(), SHORT_CLOB_CHARS));
        rs.close();

        // Select just length without selecting the clob.
        rs = s.executeQuery("SELECT K, LENGTH(C)  FROM CLOBTABLE " +
                "ORDER BY K");
        JDBC.assertFullResultSet(rs, new String [][] {{"1",LONG_CLOB_LENGTH_STRING},{"2","0"},
                {"3",null},{"4","6"}});
    }

    /**
     * Test the length after inserting with the setCharacterStream api
     * that takes length.  In this case the length will be encoded at the
     * begining of the stream and the call should be fairly low overhead.
     *
     * @throws SQLException
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void testClobLength() throws SQLException, IOException, IllegalArgumentException, IllegalAccessException,
            InvocationTargetException {
        testClobLength(false);
    }

    /**
     * Test the length after inserting the clob value with the lengthless
     * setCharacterStream api. In this case we will have to read the whole
     * stream to get the length.
     *
     * @throws SQLException
     * @throws IOException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    public void testClobLengthWithLengthlessInsert() throws SQLException, IOException, IllegalArgumentException,
            IllegalAccessException, InvocationTargetException {
        testClobLength(true);
    }

    public static Test suite() {
        Test suite =  TestConfiguration.defaultSuite(ClobMemTest.class);
        Properties p = new Properties();
        // use small pageCacheSize so we don't run out of memory on the insert.
        p.setProperty("derby.storage.pageCacheSize", "100");
        return new SystemPropertyTestSetup(suite,p);
    }



}
