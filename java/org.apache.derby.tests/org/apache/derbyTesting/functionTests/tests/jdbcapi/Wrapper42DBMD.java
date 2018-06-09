/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Wrapper42DBMD
 
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
 * A wrapper around the new DatabaseMetaData methods added by JDBC 4.2.
 */
public  class   Wrapper42DBMD   extends Wrapper41DBMD
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper42DBMD( Object wrapped ) throws Exception { super( wrapped ); }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.2 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  long getMaxLogicalLobSize() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getMaxLogicalLobSize(); }
        else if ( _netclient != null ) { return _netclient.getMaxLogicalLobSize(); }
        else { throw nothingWrapped(); }
    }

    public  boolean supportsRefCursors() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.supportsRefCursors(); }
        else if ( _netclient != null ) { return _netclient.supportsRefCursors(); }
        else { throw nothingWrapped(); }
    }

}

