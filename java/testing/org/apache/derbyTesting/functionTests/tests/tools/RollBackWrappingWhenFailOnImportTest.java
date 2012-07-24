/*
 *
 * Derby - Class RollBackWrappingWhenFailOnImportTest
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
package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.CallableStatement;


import java.sql.Connection;

import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.jdbc.Driver30;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * This test case comes from DERBY-4443. It's for show changes related to 
 * wrap rollback in exception handlers in try-catch.
 * In this test case, a MockInternalDriver is used to create a MockConnectionFailWhenRollBack
 * which will fail when rollback() is called.
 * 
 */
public class RollBackWrappingWhenFailOnImportTest extends BaseJDBCTestCase {
    class MockInternalDriver extends Driver30 {

        public class MockConnectionFailWhenRollBack extends EmbedConnection {

            public MockConnectionFailWhenRollBack(Connection connection) {
                super((EmbedConnection)connection);
            }

            public void rollback() throws SQLException {
                throw new SQLException("error in roll back", "XJ058");
            }
        }

        public Connection connect(String url, Properties info) {
            Connection conn = null;
            try {
                conn = super.connect(url, info);
            } catch (Exception e) {
                //this exception is ignored for mocking
            }
            return new MockConnectionFailWhenRollBack(conn);
        }
    }

    private String nonexistentFileName = "test/test.dat";

    public RollBackWrappingWhenFailOnImportTest(String name) {
        super(name);        
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("RollBackWrappingWhenFailOnImportTest");
        
        if (!JDBC.vmSupportsJDBC3()) {
            return suite;
        }       
        
        Test test = new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(
                        RollBackWrappingWhenFailOnImportTest.class));
                        
        suite.addTest(test);
        
        return suite;
    }

    protected void setUp() throws Exception {
        openDefaultConnection();
        
        MockInternalDriver dvr = new MockInternalDriver();
        dvr.boot(false, null);
        
        SupportFilesSetup.deleteFile(nonexistentFileName);
    }
    
    protected void tearDown() throws Exception {        
        try {           
            getTestConfiguration().shutdownEngine();            
        } catch (Exception e) {
            //Ignore exception for shut down mock driver            
        }        
        
        super.tearDown();
    }

    public void testRollBackWhenFailOnImportTable() throws SQLException { 
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                "null, 'IMP_EMP', '"  + nonexistentFileName + "test/test.dat" + 
                "' , null, null, null, 0) ";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    public void testRollBackWhenFailOnImportTableLobsFromEXTFile() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(" +
                "null, 'IET1' , '" + nonexistentFileName + "', null, null, null, 0)";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    public void testRollBackWhenFailOnImportData() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_EMP', " +
                "null, null, '" + nonexistentFileName +  "', null, null, null, 1) ";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);        
    }  
    
    public void testRollBackWhenFailOnImportDataLobsFromExtFile() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(" +
                "null, 'IET1', null, null, '" + nonexistentFileName +
                "', null, null, null, 1)";
        
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    /**
     * Call passed importSentence and process the error.
     * @param importSentence a call sentence to to import data from a nonexistent file.
     */
    private void realTestRollBackWhenImportOnNonexistentFile(
            String importSentence) throws SQLException {
      //import a non-existing file will certainly fail
        CallableStatement cSt = prepareCall(importSentence);
        
        try {
            cSt.executeUpdate();
            fail("a SQLException should be thrown " +
                    "as we import data from a nonexistent file");
        } catch (SQLException e) {            
            assertSQLState("XIE0M", e);
            assertSQLState("XJ058", e.getNextException());            
        } finally {
            cSt.close();
        }
    }
}
