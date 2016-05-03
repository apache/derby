package org.apache.derbyTesting.functionTests.tests.largedata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import org.apache.derbyTesting.functionTests.tests.tools.ImportExportBaseTest;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetReader;
import org.apache.derbyTesting.functionTests.util.streams.LoopingAlphabetStream;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.RuntimeStatisticsParser;
import org.apache.derbyTesting.junit.SQLUtilities;


/*
Class org.apache.derbyTesting.functionTests.tests.largedata.Derby6884Test


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


public class Derby6884Test extends ImportExportBaseTest
{
    String fileName6884;
    String lobFile6884;

    public Derby6884Test(String name) 
    {
        super(name);
        fileName6884 = (SupportFilesSetup.getReadWrite("table-data")).getPath();
        lobFile6884 = (SupportFilesSetup.getReadWrite("lob-data")).getPath();
    }

    public static Test suite() 
    {
        BaseTestSuite suite = new BaseTestSuite("Derby6884Test");
        suite.addTest(baseSuite("Derby6884Test:embedded"));
        return suite;
    }
    
    /*
     * DERBY-6884: external file behaviors when the file contains more than
     * Integer.MAX_VALUE bytes of data.
     */
    public void testDerby6884ImportLargeExtfileClob()
        throws SQLException
    {
        Statement s  = createStatement();
        PreparedStatement ps = prepareStatement(
                     "insert into DERBY_6884_TESTCLOB values(? , ?)" );
        int id = 1;
        long byteCount = 0;
        int clobSize = 0;
        while ( byteCount < Integer.MAX_VALUE ) {
            ps.setInt(1 , id++);
            clobSize = ( 10000 * 1024 ) + ( 1024 * id );
            byteCount += clobSize;
            Reader reader = new LoopingAlphabetReader(clobSize);
            ps.setCharacterStream(2, reader, clobSize);
            ps.executeUpdate();
        }
        ps.setInt(1 , id++);
        Reader reader = new LoopingAlphabetReader(clobSize);
        ps.setCharacterStream(2, reader, clobSize);
        ps.executeUpdate();

        commit();
        doExportTableLobsToExtFile("APP", "DERBY_6884_TESTCLOB", fileName6884, 
                                   null, null , null, lobFile6884);
        s.execute("TRUNCATE TABLE DERBY_6884_TESTCLOB");
        doImportTableLobsFromExtFile("APP", "DERBY_6884_TESTCLOB",
                                   fileName6884, null, null, null, 0);
        SupportFilesSetup.deleteFile(fileName6884);
        SupportFilesSetup.deleteFile(lobFile6884);
    }

    /*
     * Same as the prior test, but with BLOB column, not CLOB column.
     */
    public void testDerby6884ImportLargeExtfileBlob()
        throws SQLException
    {
        Statement s  = createStatement();
        PreparedStatement ps = prepareStatement(
                     "insert into DERBY_6884_TESTBLOB values(? , ?)" );
        int id = 1;
        long byteCount = 0;
        int blobSize = 0;
        while ( byteCount < Integer.MAX_VALUE ) {
            ps.setInt(1 , id++);
            blobSize = ( 50000 * 1024 ) + ( 1024 * id );
            byteCount += blobSize;
            InputStream stream = new LoopingAlphabetStream(blobSize);
            ps.setBinaryStream(2, stream, blobSize);
            ps.executeUpdate();
        }
        ps.setInt(1 , id++);
        InputStream stream = new LoopingAlphabetStream(blobSize);
        ps.setBinaryStream(2, stream, blobSize);
        ps.executeUpdate();

        commit();
        doExportTableLobsToExtFile("APP", "DERBY_6884_TESTBLOB", fileName6884, 
                                   null, null , null, lobFile6884);
        s.execute("TRUNCATE TABLE DERBY_6884_TESTBLOB");
        doImportTableLobsFromExtFile("APP", "DERBY_6884_TESTBLOB",
                                   fileName6884, null, null, null, 0);
        SupportFilesSetup.deleteFile(fileName6884);
        SupportFilesSetup.deleteFile(lobFile6884);
    }


    protected static Test baseSuite(String name) 
    {
        BaseTestSuite suite = new BaseTestSuite(name);
        suite.addTestSuite(Derby6884Test.class);
        Test test = suite;
        test = new SupportFilesSetup(test);
        return new CleanDatabaseTestSetup(test)
        {
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                Connection conn = stmt.getConnection();

                stmt.execute("CREATE TABLE DERBY_6884_TESTBLOB" +
                              "(id BIGINT, content BLOB)");
                stmt.execute("CREATE TABLE DERBY_6884_TESTCLOB" +
                              "(id BIGINT, content CLOB)");

                conn.commit();
            }
        };
    }
}
