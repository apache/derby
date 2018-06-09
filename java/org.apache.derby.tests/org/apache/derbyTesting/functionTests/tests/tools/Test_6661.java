/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.tools.Test_6661

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

package org.apache.derbyTesting.functionTests.tests.tools;

import java.sql.Connection;
import java.sql.SQLException;
import junit.framework.Test;

import org.apache.derby.tools.dblook;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that deferrable constraints are recreated correctly by dblook.
 */
public class Test_6661 extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String[][]  EXPECTED_CONSTRAINT_STATE = new String[][]
    {
        { "CHECK6661_INIT_DEFERRED", "e" },
        { "CHECK6661_INIT_IMMEDIATE", "i" },
        { "CHECK6661_VANILLA", "E" },
        { "TPRIM6661_INIT_DEFERRED", "e" },
        { "TPRIM6661_INIT_IMMEDIATE", "i" },
        { "TPRIM6661_VANILLA", "E" },
        { "TREF6661_INIT_DEFERRED", "e" },
        { "TREF6661_INIT_IMMEDIATE","i" },
        { "TREF6661_VANILLA", "E" },
        { "TUNIQUE6661_INIT_DEFERRED", "e" },
        { "TUNIQUE6661_INIT_IMMEDIATE", "i" },
        { "TUNIQUE6661_VANILLA", "E" },
    };

    private static  final   String  DBLOOK_OUTPUT = SupportFilesSetup.getReadWrite( "dblookOutput.sql" ).getPath();

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

	public Test_6661(String name)
    {
		super( name );
	}
	
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public static Test suite()
    {
        BaseTestSuite baseTest = new BaseTestSuite( Test_6661.class, "Test_6661" );
        Test        singleUseWrapper = TestConfiguration.singleUseDatabaseDecorator( baseTest );
        Test        cleanDatabaseWrapper = new CleanDatabaseTestSetup( singleUseWrapper );
        Test        supportFileWrapper = new SupportFilesSetup( cleanDatabaseWrapper );
        Test        noSecurityWrapper = SecurityManagerSetup.noSecurityManager( supportFileWrapper );

        return noSecurityWrapper;
	}
	
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

	public void test_01() throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create and verify a number of constraints.
        //
        goodStatement
            (
             conn,
             "create table tprim6661_vanilla\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tprim6661_vanilla primary key( keyCol )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tprim6661_init_deferred\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tprim6661_init_deferred primary key( keyCol ) deferrable initially deferred\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tprim6661_init_immediate\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tprim6661_init_immediate primary key( keyCol ) deferrable\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tunique6661_vanilla\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tunique6661_vanilla primary key( keyCol )\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tunique6661_init_deferred\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tunique6661_init_deferred primary key( keyCol ) deferrable initially deferred\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tunique6661_init_immediate\n" +
             "(\n" +
             "    keyCol  int not null,\n" +
             "    constraint tunique6661_init_immediate primary key( keyCol ) deferrable\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tref6661\n" +
             "(\n" +
             "    tref6661_vanilla int,\n" +
             "    tref6661_init_deferred int,\n" +
             "    tref6661_init_immediate int,\n" +
             "\n" +
             "    constraint tref6661_vanilla foreign key( tref6661_vanilla ) references tprim6661_vanilla( keyCol ),\n" +
"    constraint tref6661_init_deferred foreign key( tref6661_init_deferred ) references tprim6661_vanilla( keyCol ) deferrable initially deferred,\n" +
             "    constraint tref6661_init_immediate foreign key( tref6661_init_immediate ) references tprim6661_vanilla( keyCol ) deferrable\n" +
             ")\n"
             );
        goodStatement
            (
             conn,
             "create table tcheck6661\n" +
             "(\n" +
             "    a int,\n" +
             "\n" +
             "    constraint check6661_vanilla check( a > 0 ),\n" +
             "    constraint check6661_init_deferred check( a > 10 ) deferrable initially deferred,\n" +
             "    constraint check6661_init_immediate check( a > 100 ) deferrable\n" +
             ")\n" 
             );

        String  query = "select constraintname, state from sys.sysconstraints order by constraintname";
        assertResults( conn, query, EXPECTED_CONSTRAINT_STATE, true );

        //
        // Create a dblook script.
        //
        TestConfiguration   config = getTestConfiguration();
        String      dbName = config.getPhysicalDatabaseName( config.getDefaultDatabaseName() );
        new dblook
            (
             new String[]
             {
                 "-d",
                 "jdbc:derby:" + dbName,
                 "-o",
                 DBLOOK_OUTPUT
             }
             );

        //
        // Drop the schema objects.
        //
        goodStatement( conn, "drop table tcheck6661" );
        goodStatement( conn, "drop table tref6661" );
        goodStatement( conn, "drop table tunique6661_init_immediate" );
        goodStatement( conn, "drop table tunique6661_init_deferred" );
        goodStatement( conn, "drop table tunique6661_vanilla" );
        goodStatement( conn, "drop table tprim6661_init_immediate" );
        goodStatement( conn, "drop table tprim6661_init_deferred" );
        goodStatement( conn, "drop table tprim6661_vanilla" );
        assertResults( conn, query, new String[][] {}, true );

        //
        // Now run the script created by dblook and verify that the
        // deferred constraints were re-created correctly.
        //
        dblook_test.runDDL( conn, DBLOOK_OUTPUT );
        assertResults( conn, query, EXPECTED_CONSTRAINT_STATE, true );
	}
    
}
