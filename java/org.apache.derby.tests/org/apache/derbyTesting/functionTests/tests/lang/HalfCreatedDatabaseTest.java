/*

 Derby - Class org.apache.derbyTesting.functionTests.tests.lang.HalfCreatedDatabaseTest

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

import java.io.File;
import java.sql.SQLException;
import javax.sql.DataSource;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * This test confirms Derby's behavior when Derby dies in the middle of creating
 * a database. See DERBY-4589.
 * </p>
 */

public class HalfCreatedDatabaseTest extends BaseJDBCTestCase
{
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    /////////////////////////////////////////////////////////////////////

    private static  final   String  DB_NAME = "hcdt_db";
    private static  final   String  DB_DIRECTORY = DEFAULT_DB_DIR + File.separator + DB_NAME;
    private static  final   String  SERVICE_PROPERTIES_FILE_NAME = DB_DIRECTORY + File.separator + "service.properties";
    private static  final   String  RENAMED_FILE_NAME = DB_DIRECTORY + File.separator + "renamed.properties";
    
    /////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    /////////////////////////////////////////////////////////////////////
    
    /////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////////////////
    
    public HalfCreatedDatabaseTest(String name) { super(name); }
    
    /////////////////////////////////////////////////////////////////////
    //
    //  JUnit BEHAVIOR
    //
    /////////////////////////////////////////////////////////////////////
    
    public static Test suite()
    {
        BaseTestSuite suite = new BaseTestSuite("HalfCreatedDatabaseTest");
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        suite.addTest( decorateTest() );
        
        return suite;
    }
    
    private static Test decorateTest()
    {
        Test test = new BaseTestSuite( HalfCreatedDatabaseTest.class );
//IC see: https://issues.apache.org/jira/browse/DERBY-6590

        test = TestConfiguration.singleUseDatabaseDecorator( test, DB_NAME );

        test = SecurityManagerSetup.noSecurityManager( test );

        return test;
    }

    /////////////////////////////////////////////////////////////////////
    //
    //  TESTS
    //
    /////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that you get the expected error if you try to connect to a database
     * which is missing service.properties, the tell-tale sign of an aborted
     * database creation. See DERBY-4589.
     * </p>
     */
    public  void    test_4589() throws Exception
    {
        // make sure the database is created
        getConnection();

        // now shutdown the database
        getTestConfiguration().shutdownDatabase();

        //
        // Now move service.properties aside. This will make it look as
        // though Derby crashed in the middle of database creation.
        //
        File    serviceProperties = new File( SERVICE_PROPERTIES_FILE_NAME );
        File    renamedProperties = new File( RENAMED_FILE_NAME );
        serviceProperties.renameTo( renamedProperties );

        // getting a connection should fail
        try {
            getConnection();
            fail( "Should not be able to get a connection." );
        }
        catch (SQLException se)
        {
            boolean sawCorrectError = false;
            
            while ( se != null )
            {
                String  sqlstate = se.getSQLState();
                if ( "XBM0A".equals( sqlstate ) )
                {
                    sawCorrectError = true;
                    break;
                }

                se = se.getNextException();
            }

            assertTrue( sawCorrectError );
        }

        // move service.properties back so that tearDown() won't explode
        renamedProperties.renameTo( serviceProperties );
    }
    
    /**
     * Verify that a user data dir (in this case empty) throws the old message
     * 
     */
    public void test_5526()  throws SQLException {
        String mydatadirStr = BaseTestCase.getSystemProperty("derby.system.home") +
                File.separator + "mydatadir";
        File mydatadir = new File(mydatadirStr);
        assertTrue(mydatadir.mkdir());
        DataSource ds = JDBCDataSource.getDataSource(mydatadirStr);
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        try {
            ds.getConnection();
            fail("Should not be able to create database on existing directory " + mydatadirStr);
        } catch (SQLException se) {
            // should be nested exception XJ041 -> XBM0J (Directory exists)
            assertSQLState("XJ041",se);
            se = se.getNextException();
            assertSQLState("XBM0J",se);
        } finally {
            BaseTestCase.removeDirectory(mydatadir);
        }
        
    }
    
}


