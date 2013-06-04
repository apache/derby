/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.Wrapper41DataSource
 
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

import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.CommonDataSource;
import org.apache.derby.jdbc.BasicClientDataSource40;
import org.apache.derby.jdbc.BasicEmbeddedDataSource40;
import org.apache.derby.jdbc.ClientConnectionPoolDataSource;
import org.apache.derby.jdbc.ClientDataSource;
import org.apache.derby.jdbc.ClientXADataSource;
import org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;
import org.apache.derbyTesting.junit.JDBC;

/**
 * A wrapper around the methods added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41DataSource
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbeddedDataSource               _embedded;
    private EmbeddedConnectionPoolDataSource _ecpds;
    private EmbeddedXADataSource             _exads;
    private BasicEmbeddedDataSource40    _basicembedded;

    private ClientDataSource               _netclient;
    private ClientConnectionPoolDataSource _ccpds;
    private ClientXADataSource             _cxads;
    private BasicClientDataSource40      _basicnetclient;


    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41DataSource( Object wrapped ) throws Exception
    {
        if (JDBC.vmSupportsJNDI()) {
            if ( wrapped instanceof EmbeddedDataSource  ) {
                _embedded = (EmbeddedDataSource) wrapped; }
            else if ( wrapped instanceof EmbeddedConnectionPoolDataSource ) {
                _ecpds = (EmbeddedConnectionPoolDataSource) wrapped; }
            else if ( wrapped instanceof EmbeddedXADataSource ) {
                _exads = (EmbeddedXADataSource) wrapped; }
            else if ( wrapped instanceof ClientDataSource ) {
                _netclient = (ClientDataSource) wrapped; }
            else if ( wrapped instanceof ClientConnectionPoolDataSource ) {
                _ccpds = (ClientConnectionPoolDataSource) wrapped; }
            else if ( wrapped instanceof ClientXADataSource ) {
                _cxads = (ClientXADataSource) wrapped; }
            else { throw nothingWrapped(); }
        } else {
            if ( wrapped instanceof BasicEmbeddedDataSource40  ) {
                _basicembedded = (BasicEmbeddedDataSource40 ) wrapped; }
            else if ( wrapped instanceof BasicClientDataSource40 ) {
                _basicnetclient = (BasicClientDataSource40) wrapped; }
            else { throw nothingWrapped(); }
        }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  Logger    getParentLogger() throws SQLException
    {
        if (JDBC.vmSupportsJNDI()) {
            if ( _embedded != null ) {return _embedded.getParentLogger(); }
            else if (_netclient != null) {return _netclient.getParentLogger();}
            else if ( _ecpds != null ) {return _ecpds.getParentLogger(); }
            else if ( _exads != null ) {return _exads.getParentLogger(); }
            else if ( _ccpds != null ) {return _ccpds.getParentLogger(); }
            else if ( _cxads != null ) {return _cxads.getParentLogger(); }
            else { throw nothingWrapped(); }
        } else {
            if ( _basicembedded != null ) {
                return _basicembedded.getParentLogger(); }
            else if ( _basicnetclient != null) {
                return _basicnetclient.getParentLogger(); }
            else { throw nothingWrapped(); }
        }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public CommonDataSource   getWrappedObject() throws SQLException
    {
        if (JDBC.vmSupportsJNDI()) {
            if ( _embedded != null ) { return _embedded; }
            else if ( _netclient != null ) { return _netclient; }
            else if ( _ecpds != null ) { return _ecpds; }
            else if ( _exads != null ) { return _exads; }
            else if ( _ccpds != null ) { return _ccpds; }
            else if ( _cxads != null ) { return _cxads; }
            else { throw nothingWrapped(); }
        } else {
            if ( _basicembedded != null ) { return _basicembedded; }
            else if ( _basicnetclient != null ) { return _basicnetclient; }
            else { throw nothingWrapped(); }
        }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

