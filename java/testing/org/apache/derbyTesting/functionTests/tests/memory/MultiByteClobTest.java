/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.memory.MultiByteClobTest
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

 */

package org.apache.derbyTesting.functionTests.tests.memory;

import org.apache.derbyTesting.junit.*;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.Statement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.io.Reader;
import java.io.IOException;

/**
 * Test for small and larg clobs with multibyte characters.
 */
public class MultiByteClobTest extends BaseJDBCTestCase {

    private static final int LONG_CLOB_LENGTH = 9000000;
    private static final int SHORT_CLOB_LENGTH = 100;
    private static final String LONG_CLOB_LENGTH_STRING = "9000000";
    private static final String SHORT_CLOB_LENGTH_STRING = "100";

    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public MultiByteClobTest(String name) {
        super(name);
    }

    public void testSmallMultiByteCharLob() throws SQLException, IOException {
        setAutoCommit(false);
        Statement s = createStatement();
        
        PreparedStatement ps = prepareStatement("INSERT INTO MB_CLOBTABLE VALUES(?,?)");
        // We allocate 16MB for the test so use something bigger than that.
        ps.setInt(1,1);
        LoopingAlphabetReader reader = new LoopingAlphabetReader(SHORT_CLOB_LENGTH, CharAlphabet.cjkSubset());

        ps.setCharacterStream(2, reader, SHORT_CLOB_LENGTH);
        ps.executeUpdate();

        ResultSet rs = s.executeQuery("SELECT K, LENGTH(C), C FROM MB_CLOBTABLE" +
                "-- DERBY-PROPERTIES constraint=pk\n ORDER BY K");
        rs.next();
        assertEquals(SHORT_CLOB_LENGTH_STRING, rs.getString(2));
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
                int expectedValue = ((len -1) % 12) + '\u4E00';
                if (size != 0)
                    assertEquals(expectedValue,buf[size -1]);
        }
        assertEquals(SHORT_CLOB_LENGTH, len);
        rs.close();
        // Select just length without selecting the clob.
        rs = s.executeQuery("SELECT K, LENGTH(C)  FROM MB_CLOBTABLE " +
                "ORDER BY K");
        JDBC.assertFullResultSet(rs, new String [][] {{"1",SHORT_CLOB_LENGTH_STRING}});
    }

    public void testLargeMultiByteCharLob() throws SQLException, IOException {
        setAutoCommit(false);
        Statement s = createStatement();

        PreparedStatement ps = prepareStatement("INSERT INTO MB_CLOBTABLE VALUES(?,?)");
        // We allocate 16MB for the test so use something bigger than that.
        ps.setInt(1,1);
        LoopingAlphabetReader reader = new LoopingAlphabetReader(LONG_CLOB_LENGTH, CharAlphabet.cjkSubset());

        ps.setCharacterStream(2, reader, LONG_CLOB_LENGTH);
        ps.executeUpdate();

        ResultSet rs = s.executeQuery("SELECT K, LENGTH(C), C FROM MB_CLOBTABLE" +
                "-- DERBY-PROPERTIES constraint=pk\n ORDER BY K");
        rs.next();
        assertEquals(LONG_CLOB_LENGTH_STRING, rs.getString(2));
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
                int expectedValue = ((len -1) % 12) + '\u4E00';
                if (size != 0)
                    assertEquals(expectedValue,buf[size -1]);
        }
        assertEquals(LONG_CLOB_LENGTH, len);
        rs.close();
        // Select just length without selecting the clob.
        rs = s.executeQuery("SELECT K, LENGTH(C)  FROM MB_CLOBTABLE " +
                "ORDER BY K");
        JDBC.assertFullResultSet(rs, new String [][] {{"1",LONG_CLOB_LENGTH_STRING}});
    }

    /**
	 * Runs the test fixtures in embedded and client.
	 *
	 * @return test suite
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("MultiByteClobTest");
		suite.addTest(baseSuite("MultiByteClobTest:embedded"));
                // Disable for client for now. Client clob is inordinately slow.
		//suite.addTest(TestConfiguration
		//		.clientServerDecorator(baseSuite("MultiByteClobTest:client")));
                Properties p = new Properties();
                // use small pageCacheSize so we don't run out of memory on the insert.
                p.setProperty("derby.storage.pageCacheSize", "100");
                return new SystemPropertyTestSetup(suite,p);	
	}

	/**
	 * Base suite of tests that will run in both embedded and client.
	 *
	 * @param name
	 *            Name for the suite.
	 */
	private static Test baseSuite(String name) {
		TestSuite suite = new TestSuite(name);
		suite.addTestSuite(MultiByteClobTest.class);
		return new CleanDatabaseTestSetup(DatabasePropertyTestSetup
				.setLockTimeouts(suite, 2, 4)) {

			/**
			 * Creates the tables used in the test cases.
			 *
			 * @exception java.sql.SQLException
			 *                if a database error occurs
			 */
			protected void decorateSQL(Statement stmt) throws SQLException {
				stmt.execute("CREATE TABLE MB_CLOBTABLE (K INT CONSTRAINT PK PRIMARY KEY, C CLOB(" + LONG_CLOB_LENGTH + "))");
			}
		};
	}
}
