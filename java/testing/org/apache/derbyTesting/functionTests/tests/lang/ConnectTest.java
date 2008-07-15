/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.ConnectTest

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
       under the License
*/
package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test case for connect.sql. 
 */
public class ConnectTest extends BaseJDBCTestCase{
    
    public ConnectTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        if ( JDBC.vmSupportsJSR169())
            // Test uses DriverManager which
            // is not supported with JSR169
            return 
            new TestSuite("empty ConnectTest:DriverManager not supported");
        else
            return TestConfiguration.defaultSuite(ConnectTest.class);
    }

    /**
     *  Test whether we can reconnect.
     */
    public void testConnectRepeatedly() throws SQLException {
        String url = "jdbc:derby:wombat;create=true";
        Connection con = DriverManager.getConnection(url);

        Statement st = con.createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("values 1"), "1");
        st.close();
        con.close();

        // Can we reconnect?
        con = DriverManager.getConnection(url);
        st = con.createStatement();
        JDBC.assertSingleValueResultSet(st.executeQuery("values 1"), "1");
        st.close();
        con.close();
    }

    /**
     * Test on kinds of database names.
     */
    public void testDBName() throws SQLException {
        // Do we get a non-internal error when we try to create
        // over an existing directory? (T#674)
        String url = "jdbc:derby:wombat/seg0;create=true";
        try {
            DriverManager.getConnection(url);
            fail("Error XBM0J is expected");
        } catch (SQLException e) {
            assertEquals("XJ041", e.getSQLState());
        }

        // -- check to ensure an empty database name is taken
        // -- as the name, over any connection attribute.
        // -- this should fail.
        url = "jdbc:derby: ;databaseName=wombat";
        try {
            DriverManager.getConnection(url);
            fail("Error XJ004 is expected");
        } catch (SQLException e) {
            assertEquals("XJ004", e.getSQLState());
        }

        // and this should succeed (no database name in URL)
        url = "jdbc:derby:;databaseName=wombat";
        Connection con = DriverManager.getConnection(url);
        con.close();
    }

    /**
     * Doing some simple grant/revoke negative tests in legacy database.
     * All should fail with errors.
     */
    public void testGrantAndRevoke() throws SQLException {
        String url = "jdbc:derby:wombat";
        Connection con = DriverManager.getConnection(url);

        String sql = "create table mytab(i int)";
        Statement st = con.createStatement();
        st.execute(sql);

        sql = "grant select on mytab to satheesh";
        try {
            st.executeUpdate(sql);
            fail("Error 42Z60 is expected");
        } catch (SQLException e) {
            assertEquals("42Z60", e.getSQLState());
        }

        sql = "revoke select on mytab to satheesh";
        try {
            st.executeUpdate(sql);
            fail("Error 42Z60 is expected");
        } catch (SQLException e) {
            assertEquals("42Z60", e.getSQLState());
        }

        sql = "drop table mytab";
        st.execute(sql);

        st.close();
        con.close();
    }
}
