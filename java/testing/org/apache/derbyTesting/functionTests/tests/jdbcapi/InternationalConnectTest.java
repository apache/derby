/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.InternationalConnectTest
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

public class InternationalConnectTest extends BaseJDBCTestCase {

    /**
     * Test connecting with multibyte characters in:
     * - Database name 
     * - User
     * - Password
     * 
     * Currently just throws an exception for client.
     * Works ok for embedded.
     * 
     * This test tests DriverManager, XADataSource and ConnectionPoolDataSource
     * and is not run with J2ME.  Simple DataSource is tested with 
     * InternationalConnectSimpleDSTest
     * 
     */
   
    /**
     * @param name
     */
    public InternationalConnectTest(String name) {
        super(name);
    
    }

    /**
     * Test Chinese character in database name, user and password, using 
     * DriverManager methods.
     * 
     * @throws SQLException
     */
    public void testDriverManagerConnect() throws SQLException {
        //get a connection to load the driver
        getConnection();
        Connection conn = null;
        String url = null;
        try {
            //Test Chinese database name
            url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;create=true");
            
            conn = DriverManager.getConnection(url);
            conn.close();
        } catch (SQLException se) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }            
        try {
            // Test Chinese user name
            url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=\u4e10");
            conn = DriverManager.getConnection(url);
            conn.close();
        } catch (SQLException se ){
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }
        try {
            // Test Chinese user name in parameter to getConnection
            url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
            conn = DriverManager.getConnection(url,"\u4e10","pass");
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }
        try {
            // Test Chinese password in url
            url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=user;password=\u4e10");
            conn = DriverManager.getConnection(url);
            conn.close();
        } catch (SQLException se ){
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }
        try {
            // Test Chinese password in parameter to getConnection()
            url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
            conn = DriverManager.getConnection(url,"\u4e10","\u4e10");
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }
       
    }
    
    
    /**
     * Test XA Connection for chinese database name, user and password.
     * @throws SQLException
     */
    public void testXADSConnect() throws SQLException {
        // Test chinese database name.
        XADataSource ds = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        
        try {
            XAConnection xaconn = ds.getXAConnection();
            Connection conn = xaconn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }   
        // Chinese user
        try {
            J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
            XAConnection xaconn = ds.getXAConnection();
            Connection conn = xaconn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
        // Chinese password
        try {
            J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
            XAConnection xaconn = ds.getXAConnection();
            Connection conn = xaconn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
    }
    
    
    /**
     * Test pooled connetion for chinese database name, user and password.
     * @throws SQLException
     */
    public void testCPDSConnect() throws SQLException {
        // Test chinese database name.
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        
        try {
            PooledConnection poolConn = ds.getPooledConnection();
            Connection conn = poolConn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        }   
        // Chinese user
        try {
            J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
            PooledConnection poolConn = ds.getPooledConnection();
            Connection conn = poolConn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
        // Chinese password
        try {
            J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
            PooledConnection poolConn= ds.getPooledConnection();
            Connection conn = poolConn.getConnection();
            conn.close();
        } catch (SQLException se ) {
            if (usingEmbedded())
                throw se;
            else
                assertSQLState("22005",se);
        } 
    }
    
    public void tearDown() throws SQLException {
        String shutdownUrl = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;shutdown=true");
        try {
            DriverManager.getConnection(shutdownUrl);
        } catch (SQLException se) {
            // ignore shutdown exception
        }
        removeDirectory(getSystemProperty("derby.system.home") +  File.separator + 
                "\u4e10");
    }
    
    public static Test suite() {
        return TestConfiguration.defaultSuite(InternationalConnectTest.class);
    }
   
}
