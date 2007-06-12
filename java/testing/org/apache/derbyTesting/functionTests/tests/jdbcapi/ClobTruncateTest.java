/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.ClobTruncateTest

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.functionTests.util.streams.CharAlphabet;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for Clob.truncate
 */
public class ClobTruncateTest extends BaseJDBCTestCase {

    public ClobTruncateTest (String name) {
        super (name);
    }

    private void insertClobs () throws SQLException, IOException {
        PreparedStatement ps = prepareStatement (
                "insert into truncateclob" +
                " values (?,?,?)");
        //insert a small clob
        StringBuffer sb = new StringBuffer ();
        for (int i = 0; i < 100; i++)
            sb.append ("small clob");
        int length = sb.length();
        ps.setInt (1, length);
        ps.setCharacterStream (2, new StringReader (sb.toString()), length);
        ps.setInt (3, length/2);
        ps.execute();

        //insert a large clob
        LoopingAlphabetReader reader = new LoopingAlphabetReader (1024 * 1024);
        ps.setInt (1, 1024 * 1024);
        ps.setCharacterStream (2, reader, 1024 * 1024);
        ps.setInt (3, 1024 * 1024 / 2);
        ps.execute();

        //insert a non ascii clob
        LoopingAlphabetReader uReader =
                new LoopingAlphabetReader (300000, CharAlphabet.tamil());
        ps.setInt (1, 300000);
        ps.setCharacterStream (2, uReader, 300000);
        ps.setInt (3, 150000);
        ps.execute();
    }

    private void checkTruncate (int size, Clob clob, int newSize)
            throws SQLException {
        assertEquals ("unexpected clob size", size, clob.length());
        clob.truncate (newSize);
        assertEquals ("truncate failed ", newSize, clob.length());
        //try once more
        clob.truncate (newSize/2);
        assertEquals ("truncate failed ", newSize/2, clob.length());
    }

    public void testTruncateOnClob () throws SQLException, IOException {
        insertClobs();
        getConnection().setAutoCommit (false);
        ResultSet rs = createStatement().executeQuery("select size, data," +
                " newsize from truncateclob");
        try {
            while (rs.next()) {
                checkTruncate (rs.getInt (1), rs.getClob(2), rs.getInt(3));
            }
        }
        finally {
            rs.close();
            getConnection().commit();
        }
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate ("drop table truncateclob");
        super.tearDown();
    }

    protected void setUp() throws Exception {
        super.setUp();
        Statement stmt = createStatement();
        stmt.executeUpdate ("create table truncateclob " +
                "(size integer, data clob, newSize integer)");
    }

    public static Test suite() {
        //client code is caching clob length so this test will fail
        return TestConfiguration.embeddedSuite(ClobTruncateTest.class);
    }
}
