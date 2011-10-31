/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_9

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
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import org.apache.derbyTesting.junit.SupportFilesSetup;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.JDBC;


/**
 * Upgrade test cases for 10.9.
 */
public class Changes10_9 extends UpgradeChange
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public Changes10_9(String name)
    {
        super(name);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.7.
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        TestSuite suite = new TestSuite("Upgrade test for 10.9");

        suite.addTestSuite(Changes10_9.class);
        return new SupportFilesSetup((Test) suite);
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Make sure that generator-based identity columns don't break upgrade/downgrade.
     */
    public void testIdentity() throws Exception
    {
        Statement s = createStatement();

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            s.execute( "create table t_identity1( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity1( a ) values ( 100 )" );
            vetIdentityValues( s, "t_identity1", 1 );
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            s.execute( "insert into t_identity1( a ) values ( 200 )" );
            vetIdentityValues( s, "t_identity1", 2 );

            s.execute( "create table t_identity2( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity2( a ) values ( 100 )" );
            vetIdentityValues( s, "t_identity2", 1 );

            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            s.execute( "insert into t_identity1( a ) values ( 300 )" );
            vetIdentityValues( s, "t_identity1", 3 );

            s.execute( "insert into t_identity2( a ) values ( 200 )" );
            vetIdentityValues( s, "t_identity2", 2 );

            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            s.execute( "insert into t_identity1( a ) values ( 400 )" );
            vetIdentityValues( s, "t_identity1", 4 );

            s.execute( "insert into t_identity2( a ) values ( 300 )" );
            vetIdentityValues( s, "t_identity2", 3 );

            break;
        }
        
        s.close();
    }
    private void    vetIdentityValues( Statement s, String tableName, int expectedRowCount ) throws Exception
    {
        int     actualRowCount = 0;
        int     lastValue = 0;

        ResultSet   rs = s.executeQuery( "select * from " + tableName + " order by a" );

        while( rs.next() )
        {
            actualRowCount++;
            
            int currentValue = rs.getInt( 2 );
            if ( actualRowCount > 1 )
            {
                assertTrue( currentValue > lastValue );
            }
            lastValue = currentValue;
        }

        assertEquals( expectedRowCount, actualRowCount );
    }

    /**
     * Make sure that generator-based identity columns don't break upgrade/downgrade.
     * See DERBY-4437. Originally, this behavior was backported to 10.8.2. However, the behavior
     * was backed out of 10.8.2.
     */
    public void testIdentity10_8_2() throws Exception
    {
        Statement s = createStatement();

        boolean supportsSequences = oldAtLeast( 10, 6 );

        switch ( getPhase() )
        {
        case PH_CREATE: // create with old version
            s.execute( "create table t_identity1_4437( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity1_4437( a ) values ( 100 )" );
            vetIdentityValues_4437( s, "t_identity1_4437", 1, 2 );

            if ( supportsSequences )
            {
                s.execute( "create sequence seq1_4437" );
                vetSequenceValue( s, "seq1_4437", -2147483648, -2147483643 );
            }
            
            break;
            
        case PH_SOFT_UPGRADE: // boot with new version and soft-upgrade
            s.execute( "insert into t_identity1_4437( a ) values ( 200 )" );
            vetIdentityValues_4437( s, "t_identity1_4437", 2, 22 );

            s.execute( "create table t_identity2_4437( a int, b int generated always as identity )" );
            s.execute( "insert into t_identity2_4437( a ) values ( 100 )" );
            vetIdentityValues_4437( s, "t_identity2_4437", 1, 21 );

            if ( supportsSequences )
            {
                vetSequenceValue( s, "seq1_4437", -2147483643, -2147483623 );
            
                s.execute( "create sequence seq2_4437" );
                vetSequenceValue( s, "seq2_4437", -2147483648, -2147483628 );
            }

            break;
            
        case PH_POST_SOFT_UPGRADE: // soft-downgrade: boot with old version after soft-upgrade
            s.execute( "insert into t_identity1_4437( a ) values ( 300 )" );
            vetIdentityValues_4437( s, "t_identity1_4437", 3, 4 );

            s.execute( "insert into t_identity2_4437( a ) values ( 200 )" );
            vetIdentityValues_4437( s, "t_identity2_4437", 2, 3 );

            if ( supportsSequences )
            {
                vetSequenceValue( s, "seq1_4437", -2147483642, -2147483637 );
                vetSequenceValue( s, "seq2_4437", -2147483647, -2147483642 );
            }
            
            break;

        case PH_HARD_UPGRADE: // boot with new version and hard-upgrade
            s.execute( "insert into t_identity1_4437( a ) values ( 400 )" );
            vetIdentityValues_4437( s, "t_identity1_4437", 4, 24 );

            s.execute( "insert into t_identity2_4437( a ) values ( 300 )" );
            vetIdentityValues_4437( s, "t_identity2_4437", 3, 23 );

            if ( supportsSequences )
            {
                vetSequenceValue( s, "seq1_4437", -2147483637, -2147483617 );
                vetSequenceValue( s, "seq2_4437", -2147483642, -2147483622 );
            }
            
            break;
        }
        
        s.close();
    }
    private void    vetIdentityValues_4437( Statement s, String tableName, int expectedRowCount, int expectedSyscolumnsValue ) throws Exception
    {
        vetTable( s, tableName, expectedRowCount );

        ResultSet rs = s.executeQuery
            (
             "select c.autoincrementvalue\n" +
             "from sys.syscolumns c, sys.systables t\n" +
             "where t.tablename = '" + tableName.toUpperCase() + "'\n" +
             "and t.tableid = c.referenceid\n" +
             "and c.columnname = 'B'"
             );
        rs.next();
        int    actualSyscolumnsValue = rs.getInt( 1 );
        vetValues( expectedSyscolumnsValue, actualSyscolumnsValue );
        rs.close();
    }
    private void    vetSequenceValue( Statement s, String sequenceName, int expectedSequenceValue, int expectedSyssequencesValue ) throws Exception
    {
        ResultSet   rs = s.executeQuery( "values ( next value for " + sequenceName + " )" );
        rs.next();
        int actualSequenceValue = rs.getInt( 1 );
        vetValues( expectedSequenceValue, actualSequenceValue );
        rs.close();

        rs = s.executeQuery
            (
             "select currentvalue\n" +
             "from sys.syssequences\n" +
             "where sequencename = '" + sequenceName.toUpperCase() + "'\n"
             );
        rs.next();
        int    actualSyssequencesValue = rs.getInt( 1 );
        vetValues( expectedSyssequencesValue, actualSyssequencesValue );
        rs.close();
    }
    private void    vetTable( Statement s, String tableName, int expectedRowCount ) throws Exception
    {
        int     actualRowCount = 0;
        int     lastValue = 0;

        ResultSet   rs = s.executeQuery( "select * from " + tableName + " order by a" );

        while( rs.next() )
        {
            actualRowCount++;
            
            int currentValue = rs.getInt( 2 );
            if ( actualRowCount > 1 )
            {
                assertTrue( currentValue > lastValue );
            }
            lastValue = currentValue;
        }
        rs.close();

        vetValues( expectedRowCount, actualRowCount );
    }
    private void    vetValues( int expected, int actual )   throws Exception
    {
        assertEquals
            (
             getOldVersionString(),
             expected,
             actual
             );
    }

}
