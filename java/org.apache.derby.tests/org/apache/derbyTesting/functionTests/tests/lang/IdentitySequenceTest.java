/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.IdentitySequenceTest

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test for identity columns backed by sequence generators.
 */
public class IdentitySequenceTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String  BAD_NEXT_VALUE = "42XAR";
    private static  final   String  TABLE_DOESNT_HAVE_IDENTITY = "X0X81";

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

    public IdentitySequenceTest( String name )
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
        BaseTestSuite suite = new BaseTestSuite();

        Test    cleanTest = new CleanDatabaseTestSetup
            (
             TestConfiguration.embeddedSuite( IdentitySequenceTest.class )
             );

        suite.addTest( cleanTest );
        
        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test catalog changes.
     * </p>
     */
    public  void    test_001_catalog()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement
            (
             conn,
             "create table T1_01_IST\n" +
             "(\n" +
             "    a int generated always as identity ( start with 10, increment by 20 ),\n" +
             "    b int\n" +
             ")\n"
             );
        String  sequenceName = getIdentitySequenceName( conn, "T1_01_IST" );

        // sequence should be in SYS, its name should be based on the table id,
        // and its start/stop/max/min/cycle values should be correct.

        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "10", "10", "-2147483648", "2147483647", "20", "N" },
             },
             false
             );

        assertResults
            (
             conn,
             "values syscs_util.syscs_peek_at_identity( 'APP', 'T1_01_IST' )",
             new String[][]
             {
                 { "10" },
             },
             false
             );

        // should not be able to issue a NEXT VALUE on the sequence generator which backs the identity column
        expectCompilationError
            ( conn, BAD_NEXT_VALUE, "values ( next value for sys.\"" + sequenceName + "\" )" );

        // alter the identity column and observe that the sequence generator changes
        goodStatement( conn, "alter table T1_01_IST alter column a set increment by 15" );
        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "10", "10", "-2147483648", "2147483647", "15", "N" },
             },
             false
             );
        goodStatement( conn, "alter table T1_01_IST alter column a restart with 500" );
        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "500", "500", "-2147483648", "2147483647", "15", "N" },
             },
             false
             );
        
        // system sequence should disappear when the table is dropped
        goodStatement( conn, "drop table T1_01_IST" );
        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][] {},
             false
             );

        // can add an identity column to a table (DERBY-3888)
        goodStatement( conn, "create table T2_01_IST( b int )" );
        goodStatement( conn,
              "alter table T2_01_IST add column a int generated always as identity ( start with 10, increment by 20 )" );
        goodStatement( conn, "drop table T2_01_IST" );

        // dropping an identity column should drop the sequence generator too
        goodStatement
            (
             conn,
             "create table T3_03_IST\n" +
             "(\n" +
             "    a int generated always as identity ( start with 10, increment by 20 ),\n" +
             "    b int\n" +
             ")\n"
             );
        sequenceName = getIdentitySequenceName( conn, "T3_03_IST" );
        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "10", "10", "-2147483648", "2147483647", "20", "N" },
             },
             false
             );
        assertResults
            (
             conn,
             "values syscs_util.syscs_peek_at_identity( 'APP', 'T3_03_IST' )",
             new String[][]
             {
                 { "10" },
             },
             false
             );
        goodStatement( conn, "alter table T3_03_IST drop column a" );
        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][] {},
             false
             );
        expectExecutionError
            ( conn, TABLE_DOESNT_HAVE_IDENTITY,
             "values syscs_util.syscs_peek_at_identity( 'APP', 'T3_03_IST' )"
              );
    }
    
    /**
     * <p>
     * Test ALTER TABLE behavior.
     * </p>
     */
    public  void    test_002_alterTable()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Test that changing the increment value for an identity
        // column does not affect its current value. See DERBY-6579.
        //
        goodStatement( conn, "create table t1_002( a int, b int generated always as identity )" );
        goodStatement( conn, "insert into t1_002( a ) values ( 100 ), ( 200 )" );
        goodStatement( conn, "alter table t1_002 alter b set increment by 10" );
        goodStatement( conn, "insert into t1_002( a ) values ( 300 ), ( 400 )" );
        assertResults
            (
             conn,
             "select * from t1_002 order by a",
             new String[][]
             {
                 { "100", "1" },
                 { "200", "2" },
                 { "300", "3" },
                 { "400", "13" },
             },
             false
             );

        goodStatement( conn, "drop table t1_002" );
        goodStatement( conn, "create table t1_002( a int, b int generated always as identity )" );
        goodStatement( conn, "insert into t1_002( a ) values ( 100 ), ( 200 )" );
        goodStatement( conn, "delete from t1_002 where a = 200" );
        goodStatement( conn, "alter table t1_002 alter b set increment by 10" );
        goodStatement( conn, "insert into t1_002( a ) values ( 300 ), ( 400 )" );
        assertResults
            (
             conn,
             "select * from t1_002 order by a",
             new String[][]
             {
                 { "100", "1" },
                 { "300", "3" },
                 { "400", "13" },
             },
             false
             );

        // now restart the identity column at a later number
        goodStatement( conn, "alter table t1_002 alter b restart with 1000" );
        goodStatement( conn, "insert into t1_002( a ) values ( 500 ), ( 600 )" );
        assertResults
            (
             conn,
             "select * from t1_002 order by a",
             new String[][]
             {
                 { "100", "1" },
                 { "300", "3" },
                 { "400", "13" },
                 { "500", "1000" },
                 { "600", "1010" },
             },
             false
             );
    }

    /**
     * <p>
     * Test that too much contention on an identity column raises a LOCK_TIMEOUT.
     * </p>
     */
    public  void    test_003_identityTimeout()
        throws Exception
    {
        Connection  conn = getConnection();

        goodStatement( conn, "call syscs_util.syscs_set_database_property( 'derby.locks.waitTimeout', '1' )" );
        goodStatement( conn, "create table t_timeout( a int generated always as identity, b int )" );
        conn.setAutoCommit( false );

        try {
            PreparedStatement   ps = chattyPrepare( conn, "select count(*) from sys.syssequences with rs\n" );
            getScalarInteger( ps );
            expectExecutionError( conn, LOCK_TIMEOUT, "insert into t_timeout( b ) values ( 1 )" );
        }
        finally
        {
            conn.setAutoCommit( true );
            goodStatement( conn, "call syscs_util.syscs_set_database_property( 'derby.locks.waitTimeout', '60' )" );
        }
    }

	    /**
     * <p>
     * Tests for newly added cycle option in DERBY-6852.
     * </p>
     */
    public  void    testcycleOption()
        throws Exception
    {
        Connection  conn = getConnection();

        
        // Test the restart value and syntax of identity column without COMMA. 
        goodStatement( conn, "create table t( a int generated always as identity(START WITH 2147483647 INCREMENT BY 2 CYCLE) , b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
                 { "-2147483646", "3" },
             },
             false
             );
        goodStatement( conn, "drop table t" );
		// Test the restart value and syntax of identity column with COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 2147483647, INCREMENT BY 2, CYCLE) , b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
                 { "-2147483646", "3" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Only Start with. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 47), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "47", "1" },
                 { "48", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Start with and increment by without COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 47 INCREMENT BY 3), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "47", "1" },
                 { "50", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Start with and increment by with COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 47, INCREMENT BY 3), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "47", "1" },
                 { "50", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Start with and increment by with COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 47, INCREMENT BY 3), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "47", "1" },
                 { "50", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Start with and cycle with COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 2147483647, CYCLE), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
				 { "-2147483647", "3" },
             },
             false
             );

        goodStatement( conn, "drop table t" );
		//Start with and cycle without COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(START WITH 2147483647 CYCLE), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
				 { "-2147483647", "3" },
             },
             false
             );

 		goodStatement( conn, "drop table t" );
		//increment by. 
		goodStatement( conn, "create table t( a int generated always as identity(INCREMENT BY 3), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "1", "1" },
                 { "4", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//increment by with COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(INCREMENT BY 3, CYCLE), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "1", "1" },
                 { "4", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//increment by without COMMA. 
		goodStatement( conn, "create table t( a int generated always as identity(INCREMENT BY 3, CYCLE), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "1", "1" },
                 { "4", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Cycle. 
		goodStatement( conn, "create table t( a int generated always as identity(CYCLE), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "1", "1" },
                 { "2", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		//Changing the order of the identity column options. 
		goodStatement( conn, "create table t( a int generated always as identity(increment by 4 start with 4), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "4", "1" },
                 { "8", "2" },
             },
             false
             );
		goodStatement( conn, "drop table t" );
		// Changing the order of identity column options. 
        goodStatement( conn, "create table t( a int generated always as identity(CYCLE START WITH 2147483647 INCREMENT BY 2 ) , b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
                 { "-2147483646", "3" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		// With comma and without comma. 
        goodStatement( conn, "create table t( a int generated always as identity(start with 2147483647, increment by 7 cycle) , b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 ), ( 3 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "2147483647", "1" },
                 { "-2147483648", "2" },
                 { "-2147483641", "3" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		// Changing the order of identity column options. 
        goodStatement( conn, "create table t( a int generated always as identity(cycle , increment by 4), b int)" );
        goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
        assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "1", "1" },
                 { "5", "2" },
             },
             false
             );

		goodStatement( conn, "drop table t" );
		// () should be an error 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated by default as identity ())" );

		// Missing "with". 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated by default as identity (start 47))" );
	
		// Missing "by". 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated by default as identity (increment 4))" );

		// Redundant cycle
        expectCompilationError( conn,"42XAJ" ,"create table t(a int generated by default as identity (cycle cycle))" );
		
		// Redundant start with  
        expectCompilationError( conn,"42XAJ" ,"create table t( a int generated by default as identity(start with 4 start with 8))" );

		// Syntax error. 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated by default as identity(start with 4 , , , cycle))" );

		// try to exceed the maximum value without cycle option. 
        goodStatement( conn, "create table t( a int generated always as identity(start with 2147483647, increment by 4), b int)" );
        expectExecutionError( conn,"2200H" ,"insert into t( b ) values ( 1 ), ( 2 )" );

	goodStatement( conn, "drop table t" );
		// Syntax error. 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated always as identity(START WITH, 47))" );
		
		// Syntax error. 
        expectCompilationError( conn,"42X01" ,"create table t( a int generated always as identity(START WITH 47 ,))" );

		// Support cycle option after altering the table.
        goodStatement( conn, "create table t( a int generated always as identity(start with 2147483647 cycle), b int)" );
	goodStatement( conn, "alter table t alter column a set increment by 4" );
	goodStatement( conn ,"insert into t( b ) values ( 1 ), ( 2 )" );
        

	goodStatement( conn, "drop table t" );
		// Alter table works fine without cycling with cycle option in the syntax.
	goodStatement( conn, "create table t( a int generated always as identity(start with 7 increment by 2 cycle), b int)" );
	goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
	goodStatement( conn, "alter table t alter column a set increment by 4" );
        goodStatement( conn, "insert into t( b ) values ( 3 ), ( 4 )" );
	assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "7", "1" },
                 { "9", "2" },
		 { "11", "3" },
                 { "15", "4" },
             },
             false
             );

	goodStatement( conn, "drop table t" );
		// supports cycle option after altering the table.
        goodStatement( conn, "create table t( a int generated always as identity(increment by 2 cycle), b int)" );
	goodStatement( conn, "alter table t alter column a restart with 2147483647" );
    goodStatement( conn ,"insert into t( b ) values ( 1 ), ( 2 )" );

	goodStatement( conn, "drop table t" );
		// Alter table works fine without cycling with cycle option in the syntax.
	goodStatement( conn, "create table t( a int generated always as identity(start with 7 increment by 2 cycle), b int)" );
	goodStatement( conn, "insert into t( b ) values ( 1 ), ( 2 )" );
	goodStatement( conn, "alter table t alter column a restart with 50" );
        goodStatement( conn, "insert into t( b ) values ( 3 ), ( 4 )" );
	assertResults
            (
             conn,
             "select * from t",
             new String[][]
             {
                 { "7", "1" },
                 { "9", "2" },
		 { "50", "3" },
                 { "52", "4" },
             },
             false
             );

	goodStatement( conn, "drop table t" );
	goodStatement( conn, "create table t( A_6852 int generated always as identity(start with 7 increment by 2 cycle), b_6852 int)" );
		//SELECT from sys.syscolumns with cycle option
        // ADDING AUTOINCREMENTCYCLE Derby-6905

	assertResults
            (
             conn,
             "select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC, AUTOINCREMENTCYCLE from sys.syscolumns where COLUMNNAME ='A_6852'",
             new String[][]
             {
                 { "7", "7", "2", "true" },
             },
             false
             );

	assertResults
            (
             conn,
             "select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC, AUTOINCREMENTCYCLE from sys.syscolumns where COLUMNNAME ='B_6852'",
             new String[][]
             {
                 { null, null, null, "false" },
             },
             false
             );

	goodStatement( conn, "drop table t" );
		//SELECT from sys.syscolumns without cycle option
        // ADDING AUTOINCREMENTCYCLE Derby-6905

        goodStatement( conn, "create table T_6852( A_6852 int generated always as identity(start with 7 increment by 2))" );

	assertResults
            (
             conn,
             "select AUTOINCREMENTVALUE, AUTOINCREMENTSTART, AUTOINCREMENTINC,  AUTOINCREMENTCYCLE  from sys.syscolumns where COLUMNNAME ='A_6852'",
             new String[][]
             {
                 { "7", "7", "2", "false" },
             },
             false
             );

		//SELECT from sys.syssequences without cycle option
        String  sequenceName = getIdentitySequenceName( conn, "T_6852" );

        // sequence should be in SYS, its name should be based on the table id,
        // and its start/stop/max/min/cycle values should be correct.

        assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "7", "7", "-2147483648", "2147483647", "2", "N" },
             },
             false
             );

	goodStatement( conn, "drop table T_6852" );
		//SELECT from sys.syssequences with cycle option
	goodStatement( conn, "create table T_6852( A_6852 int generated always as identity(start with 7 increment by 2 cycle))" );

        sequenceName = getIdentitySequenceName( conn, "T_6852" );
	assertResults
            (
             conn,
            "select\n" +
            "    c.schemaName, s.sequenceName, s.currentValue, s.startValue,\n" +
            "    s.minimumValue, s.maximumValue, s.increment, s.cycleoption\n" +
            "from sys.syssequences s, sys.sysschemas c\n" +
            "where s.schemaID = c.schemaID\n" +
             "and s.sequenceName = '" + sequenceName + "'",
             new String[][]
             {
                 { "SYS", sequenceName, "7", "7", "-2147483648", "2147483647", "2", "Y" },
             },
             false
             );
	
	goodStatement( conn, "drop table T_6852" );
  
    }

    /** Get a scalar integer result from a query */
    private int getScalarInteger( PreparedStatement ps ) throws Exception
    {
        ResultSet rs = ps.executeQuery();
        rs.next();
        int retval = rs.getInt( 1 );

        rs.close();
        ps.close();

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static String  getIdentitySequenceName( Connection conn, String tableName )
        throws Exception
    {
        PreparedStatement   ps = conn.prepareStatement( "select tableID from sys.systables where tablename = ?" );
        ps.setString( 1, tableName.toUpperCase() );
        ResultSet   rs = ps.executeQuery();
        rs.next();
        String  uuidString = rs.getString( 1 );
        rs.close();
        ps.close();

        return uuidToSequenceName( uuidString );
    }

    public  static  String  uuidToSequenceName( String uuidString )
    {
        return "U" + uuidString.replace( "-", "X" );
    }

    
}
