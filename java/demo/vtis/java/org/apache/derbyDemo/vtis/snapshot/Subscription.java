/*

Derby - Class org.apache.derbyDemo.vtis.snapshot.Subscription

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

package org.apache.derbyDemo.vtis.snapshot;

import java.io.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;

import org.apache.derbyDemo.vtis.core.*;

/**
 * <p>
 * This is the superclass of parameterized subscriptions to foreign data. This
 * provides the machinery to drop/create a subscription and to refresh it with
 * the latest foreign data filtered according to the subscription parameters.
 * </p>
 *
  */
public    abstract  class Subscription    extends QueryVTIHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This is the state variable used by the logic which creates and refreshes
     * a Subscription. This state is shared across all of the queries against
     * the foreign database. 
     * </p>
     *
     */
    public  static  final   class   SubscriptionContext
    {
        private SubscriptionSignature   _signature;
        private HashMap<String, String> _parameterValues;
        private String                  _connectionURL;

        public  SubscriptionContext( SubscriptionSignature signature, HashMap<String, String> parameterValues, String connectionURL )
        {
            _signature = signature;
            _parameterValues = parameterValues;
            _connectionURL = connectionURL;
        }

        public  SubscriptionSignature   getSubscriptionSignature() { return _signature; }
        public  HashMap<String, String> getParameterValues() { return _parameterValues; }
        public  String                  getConnectionURL() { return _connectionURL; }

        public  String    toString()
        {
            StringBuffer    buffer = new StringBuffer();

            buffer.append( "SubscriptionContext( " );
            buffer.append( " signature = " + _signature );
            buffer.append( ", parameterValues = " + _parameterValues );
            buffer.append( ", connectionURL = " + _connectionURL );
            buffer.append( " )" );

            return buffer.toString();
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  HashMap<String, SubscriptionContext> _contexts = new HashMap<String, SubscriptionContext>();
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC PROCEDURES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create an empty subscription. You must refresh it later on to actually
     * populate it with data. This is registered with Derby as the
     * "createSubscription" procedure.
     * </p>
     *
     */
    public  static  void  createSubscription( String subscriptionClassName, String connectionURL )
        throws Exception
    {
        Class<?>                   subscriptionClass = Class.forName( subscriptionClassName );
        SubscriptionSignature   subscriptionSignature = (SubscriptionSignature) subscriptionClass.getAnnotation( SubscriptionSignature.class );
        String                  jdbcDriverName = subscriptionSignature.jdbcDriverName();
        String[]                subscriptionParameters = subscriptionSignature.parameters();
        Method[]                methods = subscriptionClass.getMethods();
        int                     methodCount = methods.length;
        Method                  candidate = null;
        int                     paramCount = subscriptionParameters.length;
        HashSet<String>         parameterMap = new HashSet<String>();

        for ( int i = 0; i < paramCount; i++ )
        {
            parameterMap.add( subscriptionParameters[ i ] );
        }

        createContext( subscriptionClassName, null, connectionURL );

        try {
            for ( int i = 0; i < methodCount; i++ )
            {
                candidate = methods[ i ];

                if ( isSnapshotQuery( candidate ) )
                {
                    createVTIAndEmptyTable( subscriptionClassName, candidate, jdbcDriverName, connectionURL, parameterMap );
                }            
            }

            registerRefreshProcedure( subscriptionSignature );
        }
        finally
        {
            dropContext( subscriptionClassName );
        }
    }
    
    /**
     * <p>
     * Drop a subscription. This is registered with Derby as the
     * "dropSubscription" procedure.
     * </p>
     *
     */
    public  static  void  dropSubscription( String subscriptionClassName )
        throws Exception
    {
        Class<?>                   subscriptionClass = Class.forName( subscriptionClassName );
        SubscriptionSignature   subscriptionSignature = (SubscriptionSignature) subscriptionClass.getAnnotation( SubscriptionSignature.class );
        Method[]                methods = subscriptionClass.getMethods();
        int                     methodCount = methods.length;
        Method                  candidate = null;
        SnapshotQuery           snapshotQueryAnnotation = null;

        createContext( subscriptionClassName, null, null );

        try {
            for ( int i = 0; i < methodCount; i++ )
            {
                candidate = methods[ i ];

                if ( isSnapshotQuery( candidate ) ) { dropVTIAndTable( candidate ); }            
            }

            unregisterRefreshProcedure( subscriptionSignature );
        }
        finally
        {
            dropContext( subscriptionClassName );
        }
    }

    /**
     * <p>
     * Refresh a subscription. This is called by the
     * refresh procedure whose name is the refreshProcedureName from the
     * subscription's SubscriptionSignature. The trailing varargs are the
     * parameter values.
     * </p>
     *
     */
    public  static  void  refreshSubscription( String subscriptionClassName, String connectionURL, String... parameterValues )
        throws Exception
    {
        Class<?>                   subscriptionClass = Class.forName( subscriptionClassName );
        SubscriptionSignature   subscriptionSignature = (SubscriptionSignature) subscriptionClass.getAnnotation( SubscriptionSignature.class );
        String                  jdbcDriverName = subscriptionSignature.jdbcDriverName();
        String[]                parameterNames = subscriptionSignature.parameters();
        Method[]                methods = subscriptionClass.getMethods();
        int                     methodCount = methods.length;
        Method                  candidate = null;
        ArrayList<Method>       snapshotQueries = new ArrayList<Method>();
        Connection              foreignConnection = getConnection( jdbcDriverName, connectionURL );
        Connection              localConnection = VTIHelper.getLocalConnection();
        boolean                 oldLocalAutoCommitState = localConnection.getAutoCommit();

        createContext( subscriptionClassName, parameterValues, connectionURL );

        try {
            // turn off autocommit so that the whole batch occurs in one transaction
            foreignConnection.setAutoCommit( false );
            localConnection.setAutoCommit( false );
            
            // find all the snapshot queries
            for ( int i = 0; i < methodCount; i++ )
            {
                candidate = methods[ i ];

                if ( isSnapshotQuery( candidate ) ) { snapshotQueries.add( candidate ); }            
            }

            truncateTables( snapshotQueries );
            fillTables( snapshotQueries );

            // commit foreign and local transactions
            foreignConnection.commit();
            localConnection.commit();

            // return autocommit to its previous state
            localConnection.setAutoCommit( oldLocalAutoCommitState );

            // now release the foreign connection
            closeConnection( connectionURL );
        }
        finally
        {
            dropContext( subscriptionClassName );
        }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PROTECTED BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a VTI ResultSet. It is assumed that our caller is a
     * SnapshotQuery-annotated method with no arguments.
     * </p>
     *
     */
    protected   static  ResultSet  instantiateSnapshotQueryVTI()
        throws SQLException
    {        
        String                  subscriptionClassName = null;
        SnapshotQuery           annotation = null;

        try {
            // look up the method on top of us
            StackTraceElement[]     stack = (new Throwable()).getStackTrace();
            StackTraceElement       caller = stack[ 1 ];
            Class                   callerClass = Class.forName( caller.getClassName() );
            String                  methodName = caller.getMethodName();
            Method                  method = callerClass.getMethod
                ( methodName, new Class[] {} );

            subscriptionClassName = callerClass.getName();
            annotation = method.getAnnotation( SnapshotQuery.class );
        } catch (Throwable t) { throw new SQLException( t.getMessage() ); }

        SubscriptionContext     context = getContext( subscriptionClassName, true );

        String                  jdbcDriverName = context.getSubscriptionSignature().jdbcDriverName();
        String                  connectionURL = context.getConnectionURL();
        String                  query = annotation.query();
        String[]                queryParameterNames = annotation.parameters();
        int                     count = queryParameterNames.length;
        String[]                params = new String[ count ];
        HashMap<String, String> parameterValues = context.getParameterValues();

        if ( parameterValues != null )
        {
            for ( int i = 0; i < count; i++ )
            {
                params[ i ] = parameterValues.get( queryParameterNames[ i ] );
            }
                        
        }

        return instantiateVTI( jdbcDriverName, connectionURL, query, params );
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Returns true if the method is a snapshot query.
     * </p>
     *
     */
    private static  boolean isSnapshotQuery( Method candidate )
    {
        int         modifiers = candidate.getModifiers();

        return
            (
             Modifier.isPublic( modifiers ) &&
             Modifier.isStatic( modifiers ) &&
             candidate.getReturnType() == ResultSet.class &&
             ( candidate.getAnnotation( SnapshotQuery.class ) != null )
             );
    }
    
 
    /**
     * <p>
     * Create a VTI to grab data from a foreign data source. Also create an
     * empty Derby table to hold its results.
     * </p>
     *
     */
    private  static  void  createVTIAndEmptyTable
        ( String subscriptionClassName, Method method, String jdbcDriverName, String connectionURL, HashSet<String> parameterMap )
        throws Exception
    {
        SnapshotQuery   details = method.getAnnotation( SnapshotQuery.class );
        String          query = details.query();
        String[]        queryParameters = details.parameters();
        int             paramCount = queryParameters.length;
        // placeholders just so that we can determine the query's shape
        String[]        argValues = new String[ paramCount ];
        String          functionName = getFunctionName( method );
        String          tableName = getTableName( method );

        for ( int i = 0; i < paramCount; i++ )
        {
            String      paramName = queryParameters[ i ];

            if ( !parameterMap.contains( paramName ) )
            {
                throw new SQLException( paramName + " is not a parameter defined for subscription " + subscriptionClassName );
            }
        }
        
        // first create the table function to read from the foreign database
        registerVTI( method, jdbcDriverName, connectionURL, query, argValues );

        // now create a table based on the shape of the query
        createEmptyTable( tableName, functionName );
    }
    
    /**
     * <p>
     * Drop a snapshot VTI and the table where its results are dumped.
     * </p>
     *
     */
    private  static  void  dropVTIAndTable
        ( Method method )
        throws Exception
    {
        String          functionName = getFunctionName( method );
        String          tableName = getTableName( method );

        VTIHelper.dropObject( "function", functionName, false );
        VTIHelper.dropObject( "table", tableName, false );
    }
    
    /**
     * <p>
     * Create an empty table based on the shape of a table function.
     * </p>
     *
     */
    private  static  void  createEmptyTable
        ( String tableName, String functionName )
        throws SQLException
    {
        StringBuilder       buffer = new StringBuilder();

        buffer.append( "create table " ); buffer.append( tableName ); buffer.append( "\n" );
        buffer.append( "as select s.*  from table( " + functionName + "( ) ) s\n" );
        buffer.append( "with no data\n" );

        VTIHelper.executeDDL( buffer.toString() );
    }
    
    /**
     * <p>
     * Declare the refresh procedure for the subscription.
     * </p>
     *
     */
    private  static  void  registerRefreshProcedure( SubscriptionSignature subscriptionSignature )
        throws Exception
    {
        String                  refreshProcedureName = subscriptionSignature.refreshProcedureName();
        String[]                subscriptionParameters = subscriptionSignature.parameters();
        int                     parameterCount = subscriptionParameters.length;
        StringBuffer            buffer = new StringBuffer();

        buffer.append( "create procedure " + refreshProcedureName + "\n" );
        buffer.append( "(\n" );
        buffer.append( "\tsubscriptionClassName varchar( 32672 ),\n" );
        buffer.append( "\tconnectionURL varchar( 32672 )\n" );
        for ( int i = 0; i < parameterCount; i++ )
        {
            buffer.append( ", arg" + i + " varchar( 32672 )\n" );
        }
        buffer.append( ")\n" );
        buffer.append( "language java\n" );
        buffer.append( "parameter style java\n" );
        buffer.append( "modifies sql data\n" );
        buffer.append( "external name 'org.apache.derbyDemo.vtis.snapshot.Subscription.refreshSubscription'\n" );

        VTIHelper.executeDDL( buffer.toString() );        
    }
    
    /**
     * <p>
     * Drop the refresh procedure for a subscription.
     * </p>
     *
     */
    private  static  void  unregisterRefreshProcedure( SubscriptionSignature signature )
        throws Exception
    {
        VTIHelper.dropObject( "procedure", signature.refreshProcedureName(), false );        
    }
    
    /**
     * <p>
     * Empty all of the subscribed tables.
     * </p>
     *
     */
    private  static  void  truncateTables( ArrayList<Method> snapshotQueries )
        throws Exception
    {
        Connection          conn = VTIHelper.getLocalConnection();
        int                 count = snapshotQueries.size();

        for ( int i = count - 1; i > -1; i-- )
        {
            Method              method = snapshotQueries.get( i );
            String              tableName = getTableName( method );
            String              sql = "delete from " + tableName;

            VTIHelper.print( sql );
            
            PreparedStatement   ps = conn.prepareStatement( sql );

            ps.execute();
            ps.close();
        }
    }

    /**
     * <p>
     * Fill all of the subscribed tables.
     * </p>
     *
     */
    private  static  void  fillTables( ArrayList<Method> snapshotQueries )
        throws Exception
    {
        Connection          conn = VTIHelper.getLocalConnection();
        int                 count = snapshotQueries.size();

        for ( int i = 0; i < count; i++ )
        {
            Method              method = snapshotQueries.get( i );
            String              tableName = getTableName( method );
            String              functionName = getFunctionName( method );
            String              alias = "xxx";
            String              sql =
                ( "insert into " + tableName + " select " + alias + ".* from table( " + functionName + "() ) " + alias );

            VTIHelper.print( sql );
            
            PreparedStatement   ps = conn.prepareStatement( sql );

            ps.execute();
            ps.close();
        }
    }
    
    /**
     * <p>
     * Create a table name from a method name.
     * </p>
     *
     */
    private  static  String getTableName( Method method )
    {
        return VTIHelper.doubleQuote( method.getName() );
    }

    /**
     * <p>
     * Create a function name from a method name.
     * </p>
     *
     */
    private  static  String getFunctionName( Method method )
    {
        return VTIHelper.doubleQuote( method.getName() );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MANAGING THE SUBSCRIPTION CONTEXTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Create a new subscription context for creating or refreshing a subscription.
     * </p>
     *
     */
    private  static  void  createContext
        ( String subscriptionClassName, String[] parameterValues, String connectionURL )
        throws Exception
    {
        Class<?>                       subscriptionClass = Class.forName( subscriptionClassName );
        SubscriptionSignature       subscriptionSignature = (SubscriptionSignature) subscriptionClass.getAnnotation( SubscriptionSignature.class );
        HashMap<String, String>     parameterMap = null;
        String[]                    parameterNames = subscriptionSignature.parameters();

        if ( parameterValues != null )
        {        
            parameterMap = new HashMap<String, String>();
            int                         count = parameterNames.length;
            int                         actual = parameterValues.length;

            if ( count != actual )
            {
                throw new SQLException( "Expected " + count + " parameters, but saw " + actual );
            }

            for ( int i = 0; i < count; i++ )
            {
                parameterMap.put( parameterNames[ i ], parameterValues[ i ] );
            }
        }

        SubscriptionContext            newContext = new SubscriptionContext( subscriptionSignature, parameterMap, connectionURL );
        SubscriptionContext            oldContext = getContext( subscriptionClassName, false );

        if ( oldContext != null )
        {
            throw new SQLException( subscriptionClassName + " already in use. Try again later." );
        }
        
        _contexts.put( subscriptionClassName, newContext );
    }

    /**
     * <p>
     * Drop a subscription context.
     * </p>
     *
     */
    private  static  void  dropContext
        ( String subscriptionClassName )
    {
        _contexts.remove( subscriptionClassName );
    }

     /**
     * <p>
     * Get a subscription context.
     * </p>
     *
     */
    private  static  SubscriptionContext  getContext
        ( String subscriptionClassName, boolean shouldExist )
        throws SQLException
    {
        SubscriptionContext context = _contexts.get( subscriptionClassName );

        if ( shouldExist && (context == null) )
        {
            throw new SQLException
                ( "Could not find execution context for " + subscriptionClassName + ". Maybe you are trying to invoke a snapshot table function outside of the refresh procedure?" );
        }
        
        return context;
    }

    
}
