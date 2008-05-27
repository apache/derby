package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/*
Class org.apache.derbyTesting.functionTests.tests.jdbc4.Derby3650Test

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
/** 
 * These are tests to test the cases for DERBY-3650.
 * The tests won't pass until that bug is fixed. 
 */

public class Derby3650Test extends BaseJDBCTestCase {

    public Derby3650Test(String name) {
        super(name);
     
    }
    
    public void setup() throws SQLException{
        
        getConnection().setAutoCommit(false);
    }

   
    /**
     * If join returns clob in more than one row, test that the 
     * stream can be retrieved if free is not called.
     * @param freelob  true if we should free the lob after it has been retrieved and verified.
     * @param commitAfterLobVerify true if we should commit after the lob has been retrieved and verified
     * @throws SQLException
     * @throws IOException
     */
    public void test1ToManyJoinClob(boolean freelob, boolean commitAfterLobVerify) throws SQLException, IOException     
    {           
        PreparedStatement ps = prepareStatement(
        "select c from testClob join jointab on jointab.id = testClob.id");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Clob clob = rs.getClob(1);
            verify40KClob(clob.getCharacterStream());
            if (freelob)
                clob.free();
            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        rs = ps.executeQuery();
        while (rs.next()) {
            verify40KClob(rs.getCharacterStream(1));            
        }
    }
    
    /**
     * If join returns clob in more than one row, test that the 
     * stream can be retrieved.
     * 
     * @param freelob  true if we should free the lob after it has been retrieved and verified.
     * @param commitAfterLobVerify true if we should commit after the lob has been retrieved and verified
     * @throws SQLException
     * @throws IOException
     */
    
    public void test1ToManyJoinBlob(boolean freelob, boolean commitAfterLobVerify) throws SQLException, IOException     
    {     
        PreparedStatement ps = prepareStatement(
        "select c from testBlob join jointab on jointab.id = testBlob.id");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Blob blob = rs.getBlob(1);
            verify40KBlob(blob.getBinaryStream());
            if (freelob)
                blob.free();
            if (commitAfterLobVerify)
                commit();
        }
        rs.close();
        rollback();
        rs = ps.executeQuery();
        while (rs.next()) {
            verify40KBlob(rs.getBinaryStream(1));            
        }

        
    }
    
    public void test1ToManyJoinBlobNoFree() throws SQLException, IOException {
        test1ToManyJoinBlob(false,false);
        
    }
    
    public void test1ToManyJoinBlobWithFree() throws SQLException, IOException {
        test1ToManyJoinBlob(false,true); 
    }
    
    public void test1ToManyJoinBlobWithCommit() throws SQLException, IOException {
        test1ToManyJoinBlob(true,false); 
    }
    
    
    public void test1ToManyJoinClobNoFree() throws SQLException, IOException {
        test1ToManyJoinClob(false,false);
        
    }
    
    public void test1ToManyJoinClobWithFree() throws SQLException, IOException {
        test1ToManyJoinClob(false,true); 
    }
    
    public void test1ToManyJoinClobWithCommit() throws SQLException, IOException {
        test1ToManyJoinClob(true,false); 
    }
    
    
    
    private void verify40KClob(Reader r) throws SQLException, IOException {
        
        int c;
        int charcount = 0;
        do {
            c = r.read();
            if (c != -1) {
                charcount++;
                if ((char) c != 'a') {
                    fail("Unexpected Character " + (char)c);
                }
            }
        }
        while (c != -1);
        if (charcount != 40000)
           fail("Unexcpected character count " + charcount);
     
    }
    
    private void verify40KBlob(InputStream is ) throws SQLException, IOException {
        int b;
        int bytecount = 0;
        do {
            b = is.read();
            if (b != -1) {
                bytecount++;
                if ((byte) b != (byte) 'a') {
                    fail("Unexpected byte value " + (byte) b);                    
                }
            }
        }
        while (b != -1);
        if (bytecount != 40000)
            fail("Unexpected byte count");
     
    }
    
    
    protected static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(Derby3650Test.class);
        return new CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 2, 4)) 
        {
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                stmt.executeUpdate("CREATE TABLE testClob (id int, c CLOB(2M))");
                Connection conn = stmt.getConnection();
                PreparedStatement ps = conn.prepareStatement("INSERT INTO TestClob VALUES(?,?)");
                ps.setInt(1,1);
                char[] myval = new char[40000];
                Arrays.fill(myval,'a');
                ps.setString(2,new String(myval));
                ps.executeUpdate();
                ps.close();
                stmt.executeUpdate("CREATE TABLE testBlob (id int, c BLOB(2M))");
                ps = conn.prepareStatement("INSERT INTO TestBlob VALUES(?,?)");
                ps.setInt(1,1);
                byte[] mybytes = new byte[40000];
                Arrays.fill(mybytes, (byte) 'a');
                ps.setBytes(2,mybytes);
                ps.executeUpdate();
                ps.close();
                stmt.executeUpdate("CREATE TABLE jointab (id int)");
                stmt.executeUpdate("INSERT INTO jointab values(1)");
                stmt.executeUpdate("INSERT INTO jointab values(1)");
           
           
                
            }
        };
    }
    public static Test suite() {
        TestSuite suite = new TestSuite("Derby3650Test");
        suite.addTest(baseSuite("Derby3650Test:embedded"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("Derby3650Test:client")));
        return suite;

    }
}
    
