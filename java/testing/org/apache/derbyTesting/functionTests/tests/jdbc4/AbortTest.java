/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.AbortTest

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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.security.AccessControlException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SupportFilesSetup;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.JDBCDataSource;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests for the new JDBC 4.1 Connection.abort(Executor) method. This
 * class tests the affect of SecurityManagers on the method. A related
 * test case can be found in ConnectionMethodsTest.
 */
public class AbortTest extends Wrapper41Test
{
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private boolean _hasSecurityManager;

    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////

    public AbortTest(String name, boolean hasSecurityManager)
    {
        super(name);
        
        _hasSecurityManager = hasSecurityManager;
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public static Test suite()
    {
        TestSuite suite = new TestSuite( "AbortTest" );

        suite.addTest( baseSuite( true ) );
        suite.addTest( baseSuite( false ) );

        suite.addTest
            (
             TestConfiguration.clientServerDecorator
             ( baseSuite( true ) )
             );
        suite.addTest
            (
             TestConfiguration.clientServerDecorator
             ( baseSuite( false ) )
             );
        
        return suite;
    }

    public static Test baseSuite( boolean hasSecurityManager )
    {
        AbortTest   abortTest = new AbortTest( "test_basic", hasSecurityManager );
        
        Test test = new CleanDatabaseTestSetup( abortTest )
            {
                protected void decorateSQL( Statement s ) throws SQLException
                {
                    s.execute("create table abort_table( a int )");
                }
            };

        if ( hasSecurityManager )
        {
            return new SecurityManagerSetup( test, "org/apache/derbyTesting/functionTests/tests/jdbc4/noAbortPermission.policy" );
        }
        else
        {
            return SecurityManagerSetup.noSecurityManager( test );
        }
    }
    
    
    ///////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test Connection.abort(Executor) with and without a security manager.
     * </p>
     */
    public  void    test_basic() throws Exception
    {
        //
        // Only run if we can grant permissions to the jar files.
        //
        if ( !TestConfiguration.loadingFromJars() ) { return; }

        println( "AbortTest( " + _hasSecurityManager + " )" );
        assertEquals( _hasSecurityManager, (System.getSecurityManager() != null) );

        physical();
        pooled();
        xa();
    }

    private void    physical()  throws Exception
    {
        Connection conn0 = openUserConnection( "user0");
        Connection conn1 = openUserConnection( "user1");
        Connection conn2 = openUserConnection( "user2");

        vet( conn0, conn1, conn2 );
    }

    private void    pooled()    throws Exception
    {
        ConnectionPoolDataSource cpDs =
                J2EEDataSource.getConnectionPoolDataSource();
        
        PooledConnection conn0 = getPooledConnection( cpDs, "user3");
        PooledConnection conn1 = getPooledConnection( cpDs, "user4");
        PooledConnection conn2 = getPooledConnection( cpDs, "user5");

        vet( conn0.getConnection(), conn1.getConnection(), conn2.getConnection() );
    }
    private PooledConnection    getPooledConnection
        ( ConnectionPoolDataSource cpDs, String userName ) throws Exception
    {
        return cpDs.getPooledConnection( userName, getTestConfiguration().getPassword( userName ) );
    }
    
    private void    xa()        throws Exception
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        
        XAConnection conn0 = getXAConnection( xads, "user6");
        XAConnection conn1 = getXAConnection( xads, "user7");
        XAConnection conn2 = getXAConnection( xads, "user8");

        vet( conn0.getConnection(), conn1.getConnection(), conn2.getConnection() );
    }
    private XAConnection    getXAConnection
        ( XADataSource xads, String userName ) throws Exception
    {
        return xads.getXAConnection( userName, getTestConfiguration().getPassword( userName ) );
    }
    
    /**
     * <p>
     * Test Connection.abort(Executor) with and without a security manager.
     * </p>
     */
    public  void    vet( Connection conn0, Connection conn1, Connection conn2 ) throws Exception
    {
        assertNotNull( conn0 );
        assertNotNull( conn1 );
        assertNotNull( conn2 );
        
        // NOP if called on a closed connection
        conn0.close();
        Wrapper41Conn   wrapper0 = new Wrapper41Conn( conn0 );
        wrapper0.abort( new ConnectionMethodsTest.DirectExecutor() );

        conn1.setAutoCommit( false );
        final   Wrapper41Conn   wrapper1 = new Wrapper41Conn( conn1 );

        // the Executor may not be null
        try {
            wrapper1.abort( null );
        }
        catch (SQLException se)
        {
            assertSQLState( "XCZ02", se );
        }

        if ( _hasSecurityManager ) { missingPermission( wrapper1 ); }
        else { noSecurityManager( wrapper1, conn2 ); }
    }

    // Run if we have a security manager. This tests that abort() fails
    // if the caller has not been granted the correct permission.
    private void    missingPermission( final Wrapper41Conn wrapper1 ) throws Exception
    {
        // should not be able to abort the connection because this code
        // lacks the permission
        try {
            //
            // This doPrivileged block absolves outer code blocks (like JUnit)
            // of the need to be granted SQLPermission( "callAbort" ). However,
            // derbyTesting.jar still needs that permission.
            //
            AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<Object>()
                 {
                     public Object    run() throws Exception
                     {
                         ConnectionMethodsTest.DirectExecutor  executor = new ConnectionMethodsTest.DirectExecutor();
                         wrapper1.abort( executor );
                         return null;
                     }
                 }
                 );
            fail( "The call to Connection.abort(Executor) should have failed." );
        }
        catch (Exception e)
        {
            assertTrue( e instanceof AccessControlException );
        }
    }

    // Run if we don't have a security manager. Verifies that abort() is uncontrolled
    // in that situation.
    private void    noSecurityManager(  final Wrapper41Conn wrapper1, Connection conn2  ) throws Exception
    {
        PreparedStatement   ps = prepareStatement
            ( wrapper1.getWrappedObject(), "insert into app.abort_table( a ) values ( 1 )" );
        ps.execute();
        ps.close();

        // abort the connection
        ConnectionMethodsTest.DirectExecutor  executor = new ConnectionMethodsTest.DirectExecutor();
        wrapper1.abort( executor );

        // verify that the connection is closed
        try {
            prepareStatement( wrapper1.getWrappedObject(), "select * from sys.systables" );
            fail( "Connection should be dead!" );
        }
        catch (SQLException se)
        {
            assertSQLState( "08003", se );
        }

        // verify that the changes were rolled back
        ps = prepareStatement( conn2, "select * from app.abort_table" );
        ResultSet   rs = ps.executeQuery();
        assertFalse( rs.next() );
        rs.close();
        ps.close();
        conn2.close();
    }

}
