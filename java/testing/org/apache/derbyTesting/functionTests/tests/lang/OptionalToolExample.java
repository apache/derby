/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.OptionalToolExample

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

import org.apache.derby.iapi.sql.dictionary.OptionalTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * <p>
 * Simple OptionalTool.
 * </p>
 */
public class OptionalToolExample implements OptionalTool
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

    /** 0-arg constructor required by the OptionalTool contract */
    public  OptionalToolExample() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  void    loadTool( String... configurationParameters )
        throws SQLException
    { loadToolMinion( "toString" ); }

    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    { unloadToolMinion( "toString" ); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected   void    loadToolMinion( String functionName )
        throws SQLException
    {
        getDerbyConnection().prepareStatement
            (
             "create function " + functionName + "( intVal int ) returns varchar( 32672 )\n" +
             "language java parameter style java no sql\n" +
             "external name 'java.lang.Integer.toString'"
             ).execute();

    }
    
    protected   void    unloadToolMinion( String functionName )
        throws SQLException
    {
        getDerbyConnection().prepareStatement( "drop function " + functionName ).execute();
    }

    private Connection  getDerbyConnection() throws SQLException
    {
        return DriverManager.getConnection( "jdbc:default:connection" );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    //  NESTED CLASS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   VariableName    extends OptionalToolExample
    {
        public  void    loadTool( String... configurationParameters )
            throws SQLException
        { loadToolMinion( configurationParameters[ 0 ] ); }

        public  void    unloadTool( String... configurationParameters )
            throws SQLException
        { unloadToolMinion( configurationParameters[ 0 ]  ); }
    }

}
