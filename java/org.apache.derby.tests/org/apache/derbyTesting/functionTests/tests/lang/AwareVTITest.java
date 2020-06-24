/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.AwareVTITest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.SQLException;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * <p>
 * Test AwareVTIs. See DERBY-6117.
 * </p>
 */
public class AwareVTITest  extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  CANNOT_CHANGE_COLUMNS = "X0Y92";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Create a new instance.
     */

    public AwareVTITest(String name)
    {
        super(name);
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6590
        BaseTestSuite suite = (BaseTestSuite)TestConfiguration.embeddedSuite(
            AwareVTITest.class);
        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Basic test of AwareVTIs.
     * </p>
     */
    public void test_01_basic() throws Exception
    {
        Connection conn = getConnection();

        vetBasic( conn, "s1", "f1" );
        vetBasic( conn, "s2", "f2" );
    }
    private void    vetBasic( Connection conn, String schema, String function ) throws Exception
    {
        goodStatement( conn, "create schema " + schema );
        goodStatement
            (
             conn,
             "create function " + schema + "." + function + "() returns table\n" +
             "(\n" +
             "    schema_name varchar( 20 ),\n" +
             "    vti_name varchar( 20 ),\n" +
             "    statement_text varchar( 100 )\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set no sql\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.AwareVTITest.dummyAwareVTI'\n"
             );

        String  query = "select * from table( " + schema + "." + function + "() ) s";
        assertResults
            (
             conn,
             query,
             new String[][]
             {
                 { schema.toUpperCase(), function.toUpperCase(), query },
             },
             false
             );
    }

    /**
     * <p>
     * Test that column names can be set in a StringColumnVTI, but only
     * if they haven't already been set.
     * </p>
     */
    public void test_02_StringColumnVTI() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6117

        String[][]  rows = new String[][]
            {
                { "foo", "bar" },
                { "wibble", "baz" }
            };
        UnnamedColumnsVTI   ucv = new UnnamedColumnsVTI( rows );

        // you can set the column names once...
        ucv.setColumnNames( new String [] { "A", "B" } );

        // ...but only once
        try {
            ucv.setColumnNames( new String [] { "C", "D" } );
            fail( "Attempt to reset column names should have failed." );
        }
        catch (SQLException se)
        {
            assertEquals( CANNOT_CHANGE_COLUMNS, se.getSQLState() );
        }

        assertResults( ucv, rows, false );
    }
    
    /**
     * <p>
     * Test the ArchiveVTI table function. This table function may be an example
     * in the Derby user docs. If you break this table function, then you need to
     * adjust the user docs accordingly. That documentation should be linked from
     * DERBY-6117.
     * </p>
     */
    public void test_03_ArchiveVTI() throws Exception
    {
        Connection conn = getConnection();
//IC see: https://issues.apache.org/jira/browse/DERBY-6117

        goodStatement
            (
             conn,
             "create table t1\n" +
             "(\n" +
             "    keyCol int,\n" +
             "    aCol int,\n" +
             "    bCol int\n" +
             ")\n"
             );
        goodStatement( conn, "create table t1_archive_001 as select * from t1 with no data" );
        goodStatement( conn, "create table t1_archive_002 as select * from t1 with no data" );
        goodStatement( conn, "insert into t1_archive_002 values ( 1, 100, 1000 ), ( 2, 200, 2000 ), ( 3, 300, 3000 )" );
        goodStatement( conn, "insert into t1_archive_001 values ( 4, 400, 4000 ), ( 5, 500, 5000 ), ( 6, 600, 6000 )" );
        goodStatement( conn, "insert into t1 values ( 7, 700, 7000 ), ( 8, 800, 8000 ), ( 9, 900, 9000 )" );
        goodStatement
            (
             conn,
             "create function t1( archiveSuffix varchar( 32672 ) ) returns table\n" +
             "(\n" +
             "    keyCol int,\n" +
             "    aCol int,\n" +
             "    bCol int\n" +
             ")\n" +
             "language java parameter style derby_jdbc_result_set reads sql data\n" +
             "external name 'org.apache.derbyTesting.functionTests.tests.lang.ArchiveVTI.archiveVTI'\n"
             );
        
        assertResults
            (
             conn,
             "select keyCol, bCol from table( t1( '_ARCHIVE_' ) ) s\n" +
             "where keyCol between 3 and 7\n" +
             "order by keyCol\n",
             new String[][]
             {
                 { "3", "3000" },
                 { "4", "4000" },
                 { "5", "5000" },
                 { "6", "6000" },
                 { "7", "7000" },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ROUTINES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  DummyAwareVTI   dummyAwareVTI()
    {
        return new DummyAwareVTI();
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  class   UnnamedColumnsVTI    extends StringArrayVTI
    {
        public  UnnamedColumnsVTI( String[][] rows )
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-6117
            super( null, rows );
        }
    }

}

