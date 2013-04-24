/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Wrapper41DBMD
 
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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.SQLException;

import org.apache.derby.impl.jdbc.EmbedDatabaseMetaData;
import org.apache.derby.client.am.ClientDatabaseMetaData;

/**
 * A wrapper around the new DatabaseMetaData methods added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41DBMD
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    protected EmbedDatabaseMetaData    _embedded;
    protected ClientDatabaseMetaData      _netclient;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41DBMD( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EmbedDatabaseMetaData ) { _embedded = (EmbedDatabaseMetaData) wrapped; }
        else if ( wrapped instanceof ClientDatabaseMetaData ) { _netclient = (ClientDatabaseMetaData) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  boolean    generatedKeyAlwaysReturned() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.generatedKeyAlwaysReturned(); }
        else if ( _netclient != null ) { return _netclient.generatedKeyAlwaysReturned(); }
        else { throw nothingWrapped(); }
    }

    public  java.sql.ResultSet    getPseudoColumns
        ( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern )
        throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getPseudoColumns( catalog, schemaPattern, tableNamePattern, columnNamePattern ); }
        else if ( _netclient != null ) { return _netclient.getPseudoColumns( catalog, schemaPattern, tableNamePattern, columnNamePattern ); }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public java.sql.DatabaseMetaData   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    protected SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

