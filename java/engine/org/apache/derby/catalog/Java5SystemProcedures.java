/*

   Derby - Class org.apache.derby.catalog.Java5SystemProcedures

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

package org.apache.derby.catalog;

import java.sql.SQLException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * <p>
 * System procedures which run only on Java 5 or higher.
 * </p>
 */
public  class   Java5SystemProcedures
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Offsets into arrays in OPTIONAL_TOOLS */
    private static  final   int TOOL_NAME = 0;
    private static  final   int TOOL_CLASS_NAME = TOOL_NAME + 1;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Mapping of tool names to their implementing classes for use by SYSCS_REGISTER_TOOL */
    private static  final   String[][]  OPTIONAL_TOOLS = new String[][]
    {
        { "dbmd", "org.apache.derby.impl.tools.optional.DBMDWrapper" },
        { "fdbv", "org.apache.derby.impl.tools.optional.ForeignDBViews" },
    };

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Load or unload an optional tool package.
     * </p>
     *
     * @param toolName  Name of the tool package.
     * @param register  True if the package should be loaded, false otherwise.
     * @param optionalArgs  Tool-specific configuration parameters.
     */
    public  static  void    SYSCS_REGISTER_TOOL
        (
         String toolName,
         boolean    register,
         String...  optionalArgs
         )
        throws SQLException
    {
        try {
			CompilerContext cc = (CompilerContext) ContextService.getContext( CompilerContext.CONTEXT_ID );
            ClassFactory    classFactory = cc.getClassFactory();

            String              toolClassName = findToolClassName( toolName );            
            OptionalTool    tool = null;

            try {
                tool = (OptionalTool) classFactory.loadApplicationClass( toolClassName ).newInstance();
            }
            catch (ClassNotFoundException cnfe) { throw wrap( cnfe ); }
            catch (InstantiationException ie) { throw wrap( ie ); }
            catch (IllegalAccessException iae) { throw wrap( iae ); }

            if ( register ) { tool.loadTool( optionalArgs ); }
            else { tool.unloadTool( optionalArgs ); }
        }
        catch (StandardException se) { throw PublicAPI.wrapStandardException( se ); }
    }
    /** Lookup the class name corresponding to the name of an optional tool */
    private static  String  findToolClassName( String toolName ) throws StandardException
    {
        for ( String[] descriptor : OPTIONAL_TOOLS )
        {
            if ( descriptor[ TOOL_NAME ].equals( toolName ) ) { return descriptor[ TOOL_CLASS_NAME ]; }
        }

        throw StandardException.newException( SQLState.LANG_UNKNOWN_TOOL_NAME,  toolName );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  StandardException wrap( Throwable t )   { return StandardException.plainWrapException( t ); }
}

