/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.Wrapper41Conn
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.client.am.LogicalConnection;
import org.apache.derby.client.net.NetConnection;

/**
 * A wrapper around the abort(Executor) method added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41Conn
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbedConnection    _embedded;
    private NetConnection      _netclient;
    private BrokeredConnection _brokeredConnection;
    private LogicalConnection _logicalConnection;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41Conn( Object wrapped ) throws Exception
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
        if ( wrapped instanceof EmbedConnection ) { _embedded = (EmbedConnection) wrapped; }
        else if ( wrapped instanceof BrokeredConnection ) { _brokeredConnection = (BrokeredConnection) wrapped; }
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        else if ( wrapped instanceof NetConnection) { _netclient = (NetConnection) wrapped; }
        else if ( wrapped instanceof LogicalConnection ) { _logicalConnection = (LogicalConnection) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  void    abort( Executor executor ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.abort( executor ); }
        else if ( _netclient != null ) { _netclient.abort( executor ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.abort( executor ); }
        else if ( _logicalConnection != null ) { _logicalConnection.abort( executor ); }
        else { throw nothingWrapped(); }
    }

    public  String    getSchema() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        if ( _embedded != null ) { return _embedded.getSchema(); }
        else if ( _netclient != null ) { return _netclient.getSchema(); }
        else if ( _brokeredConnection != null ) { return _brokeredConnection.getSchema(); }
        else if ( _logicalConnection != null ) { return _logicalConnection.getSchema(); }
        else { throw nothingWrapped(); }
    }

    public  void    setSchema( String schemaName ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.setSchema( schemaName ); }
        else if ( _netclient != null ) { _netclient.setSchema( schemaName ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.setSchema( schemaName ); }
        else if ( _logicalConnection != null ) { _logicalConnection.setSchema( schemaName ); }
        else { throw nothingWrapped(); }
    }

    public  int    getNetworkTimeout() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        if ( _embedded != null ) { return _embedded.getNetworkTimeout(); }
        else if ( _netclient != null ) { return _netclient.getNetworkTimeout(); }
        else if ( _brokeredConnection != null ) { return _brokeredConnection.getNetworkTimeout(); }
        else if ( _logicalConnection != null ) { return _logicalConnection.getNetworkTimeout(); }
        else { throw nothingWrapped(); }
    }

    public  void    setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.setNetworkTimeout( executor, milliseconds ); }
        else if ( _netclient != null ) { _netclient.setNetworkTimeout( executor, milliseconds ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.setNetworkTimeout( executor, milliseconds ); }
        else if ( _logicalConnection != null ) { _logicalConnection.setNetworkTimeout( executor, milliseconds ); }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public Connection   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
//IC see: https://issues.apache.org/jira/browse/DERBY-4869
        else if ( _brokeredConnection != null ) { return _brokeredConnection; }
        else if ( _logicalConnection != null ) { return _logicalConnection; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

