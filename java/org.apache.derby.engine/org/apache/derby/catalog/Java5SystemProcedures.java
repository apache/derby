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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;

import org.apache.derby.iapi.sql.dictionary.OptionalTool;
import org.apache.derby.shared.common.error.PublicAPI;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassFactoryContext;

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

    /** Generic name for all user-supplied tools: the first optional arg is the tool class name */
    private static  final   String  CUSTOM_TOOL_CLASS_NAME = "customTool";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Mapping of tool names to their implementing classes for use by SYSCS_REGISTER_TOOL */
    private static  final   String[][]  OPTIONAL_TOOLS = new String[][]
    {
        { "databaseMetaData", "org.apache.derby.impl.tools.optional.DBMDWrapper" },
        { "foreignViews", "org.apache.derby.impl.tools.optional.ForeignDBViews" },
        { "luceneSupport", "org.apache.derby.optional.lucene.LuceneSupport" },
        { "optimizerTracing", "org.apache.derby.impl.sql.compile.OptimizerTracer" },
        { "optimizerTracingViews", "org.apache.derby.impl.sql.compile.OptTraceViewer" },
        { "rawDBReader", "org.apache.derby.optional.dump.RawDBReader" },
        { "simpleJson", "org.apache.derby.optional.json.SimpleJsonTool" },
    };

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Load or unload an optional tool package. If the tool name is the special
     * CUSTOM_TOOL_CLASS_NAME tool, then the first optionalArg is the name
     * of a user-supplied class which implements OptionalTool.
     * </p>
     *
     * @param toolName  Name of the tool package.
     * @param register  True if the package should be loaded, false otherwise.
     * @param optionalArgs  Tool-specific configuration parameters.
     *
     * @throws SQLException on error
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
			ClassFactoryContext cfc = (ClassFactoryContext) getContext( ClassFactoryContext.CONTEXT_ID );
            ClassFactory    classFactory = cfc.getClassFactory();

            String              toolClassName = findToolClassName( toolName, optionalArgs );            
            OptionalTool    tool = null;

            Class<?> toolClass;
            try {
                toolClass = classFactory.loadApplicationClass( toolClassName );
            }
            catch (ClassNotFoundException cnfe) { throw wrap( cnfe ); }

            if (!OptionalTool.class.isAssignableFrom(toolClass)) {
                throw badCustomTool(toolClassName);
            }

            try {
                tool = (OptionalTool) toolClass.getConstructor().newInstance();
            }
            catch (InstantiationException ie) { throw wrap( ie ); }
            catch (IllegalAccessException iae) { throw wrap( iae ); }
            catch (NoSuchMethodException ie) { throw wrap( ie ); }
            catch (java.lang.reflect.InvocationTargetException iae) { throw wrap( iae ); }

            // Strip the custom tool class name from the optional args as necessary
            if ( CUSTOM_TOOL_CLASS_NAME.equals( toolName ) )
            {
                optionalArgs = stripCustomClassName( optionalArgs );
            }

            if ( register ) { tool.loadTool( optionalArgs ); }
            else { tool.unloadTool( optionalArgs ); }
        }
        catch (StandardException se) { throw PublicAPI.wrapStandardException( se ); }
    }
    /** Lookup the class name corresponding to the name of an optional tool */
    private static  String  findToolClassName( String toolName, String... optionalArgs ) throws StandardException
    {
        //
        // For a user-supplied tool, the first optional arg is the name of a user-supplied class
        // which implements OptionalTool
        //
        if ( CUSTOM_TOOL_CLASS_NAME.equals( toolName ) )
        {
            if ( (optionalArgs == null) || (optionalArgs.length == 0) )
            {
                throw badTool( "null" );
            }
            else { return optionalArgs[ 0 ]; }
        }
        
        for ( String[] descriptor : OPTIONAL_TOOLS )
        {
            if ( descriptor[ TOOL_NAME ].equals( toolName ) ) { return descriptor[ TOOL_CLASS_NAME ]; }
        }

        throw badTool( toolName );
    }
    private static  StandardException   badTool( String toolName )
    {
        return StandardException.newException( SQLState.LANG_UNKNOWN_TOOL_NAME,  toolName );
    }

    private static StandardException badCustomTool(String className) {
        return StandardException.newException(
                SQLState.LANG_UNKNOWN_CUSTOM_TOOL_NAME, className);
    }

    /**
     * <p>
     * For a custom tool, we strip the first arg from the list of optional args. By
     * the time we get to this method, it has already been determined that there is
     * at least one arg and it is the name of a class which implements OptionalTool.
     * </p>
     */
    private static  String[]    stripCustomClassName( String... optionalArgs )
    {
        int     count = optionalArgs.length - 1;
        String[]    retval = new String[ count ];

        for ( int i = 0; i < count; i++ ) { retval[ i ] = optionalArgs[ i + 1 ]; }

        return retval;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  StandardException wrap( Throwable t )   { return StandardException.plainWrapException( t ); }
    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContext( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContext( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContext( contextID );
                     }
                 }
                 );
        }
    }

}

