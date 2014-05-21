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
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.Decorator;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.SupportFilesSetup;

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
        TestSuite suite = new TestSuite();

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

        // can't add an identity column to a table
        goodStatement( conn, "create table T2_01_IST( b int )" );
        expectCompilationError
            ( conn, CANT_ADD_IDENTITY,
              "alter table T2_01_IST add column a int generated always as identity ( start with 10, increment by 20 )" );

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
