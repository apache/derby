/*

   Derby - Class org.apache.derby.impl.sql.compile.OptimizerTracer

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

package org.apache.derby.impl.sql.compile;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.sql.SQLException;
import org.apache.derby.iapi.db.OptimizerTrace;
import org.apache.derby.shared.common.error.PublicAPI;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassFactoryContext;
import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;

/**
 * <p>
 * OptionalTool for tracing the Optimizer.
 * </p>
 */
public	class   OptimizerTracer  implements OptionalTool
{
    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTANTS
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	STATE
    //
    ////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////
    //
    //	CONSTRUCTOR
    //
    ////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor required by the OptionalTool contract */
    public  OptimizerTracer() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OptionalTool BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Turns on optimizer tracing. May take optional parameters:
     * </p>
     *
     * <ul>
     * <li>xml - If the first arg is the "xml" literal, then trace output will be
     * formatted as xml.</li>
     * <li>custom, $class - If the first arg is the "custom" literal, then the next arg must be
     * the name of a class which implements org.apache.derby.iapi.sql.compile.OptTrace
     * and which has a 0-arg constructor. The 0-arg constructor is called and the resulting
     * OptTrace object is plugged in to trace the optimizer.</li>
     * </ul>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        OptTrace    tracer;

        if ( (configurationParameters == null) || (configurationParameters.length == 0) )
        {
            tracer = new DefaultOptTrace();
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6211
        else if ( "xml".equals( configurationParameters[ 0 ] ) )
        {
            try {
                tracer = new XMLOptTrace();
            }
            catch (Throwable t) { throw wrap( t ); }
        }
        else if ( "custom".equals( configurationParameters[ 0 ] ) )
        {
            if ( configurationParameters.length != 2 )
            { throw wrap( MessageService.getTextMessage( SQLState.LANG_BAD_OPTIONAL_TOOL_ARGS ) ); }

            String  customOptTraceName = configurationParameters[ 1 ];

            try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
                ClassFactoryContext cfc = (ClassFactoryContext) getContext( ClassFactoryContext.CONTEXT_ID );
                ClassFactory    classFactory = cfc.getClassFactory();

//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                Class<?> clazz = classFactory.loadApplicationClass( customOptTraceName );
                tracer = (OptTrace) clazz.getConstructor().newInstance();
            }
            catch (InstantiationException cnfe) { throw cantInstantiate( customOptTraceName ); }
            catch (ClassNotFoundException cnfe) { throw cantInstantiate( customOptTraceName ); }
            catch (IllegalAccessException cnfe) { throw cantInstantiate( customOptTraceName ); }
            catch (NoSuchMethodException cnfe) { throw cantInstantiate( customOptTraceName ); }
            catch (java.lang.reflect.InvocationTargetException cnfe) { throw cantInstantiate( customOptTraceName ); }
            catch (Throwable t) { throw wrap( t ); }
        }
        else { throw wrap( MessageService.getTextMessage( SQLState.LANG_BAD_OPTIONAL_TOOL_ARGS ) ); }
                     
        OptimizerTrace.setOptimizerTracer( tracer );
    }
    private SQLException    cantInstantiate( String className )
    {
        return wrap( MessageService.getTextMessage( SQLState.LANG_CANT_INSTANTIATE_CLASS, className ) );
    }

    /**
     * <p>
     * Print the optimizer trace and turn off tracing. Takes optional parameters:
     * </p>
     *
     * <ul>
     * <li><b>fileName</b> - Where to write the optimizer trace. If omitted, the trace is written to System.out.</li>
     * </ul>
     */
    public  void    unloadTool( final String... configurationParameters )
        throws SQLException
    {
        try {
            final   OptTrace    tracer = OptimizerTrace.getOptimizerTracer();

            boolean     needsClosing = false;
//IC see: https://issues.apache.org/jira/browse/DERBY-6256
            PrintWriter pw;
            
            if (
                (configurationParameters != null) &&
                (configurationParameters.length > 0)
                )
            {
                pw = AccessController.doPrivileged
                    (
//IC see: https://issues.apache.org/jira/browse/DERBY-6635
                     new PrivilegedExceptionAction<PrintWriter>()
                     {
                         public PrintWriter run() throws SQLException
                         {
                             try {
                                 String fileName = configurationParameters[ 0 ];
                                 File   outputFile = new File( fileName );

                                 if ( outputFile.exists() )
                                 {
                                     throw PublicAPI.wrapStandardException
                                         (
                                          StandardException.newException( SQLState.DATA_FILE_EXISTS, fileName )
                                          );
                                 }
                                 
                                 return new PrintWriter( outputFile );
                             } catch (IOException ioe) { throw new IllegalArgumentException( ioe.getMessage(), ioe ); }
                         }  
                     }
                     );
                needsClosing = true;
            }
            else { pw = new PrintWriter( System.out ); }
        
            if ( tracer != null )
            {
                tracer.printToWriter( pw );
                pw.flush();
            }

            if ( needsClosing ) { pw.close(); }
            
        }
//IC see: https://issues.apache.org/jira/browse/DERBY-6635
        catch (Exception e)
        {
            if ( e.getMessage() == null )
            {
                Throwable   cause = e.getCause();
                if ( (cause != null) && (cause instanceof SQLException) )
                { throw (SQLException) cause; }
            }
            
            throw wrap( e );
        }
        finally
        {
            OptimizerTrace.setOptimizerTracer( null );
        }
    }

    ////////////////////////////////////////////////////////////////////////
    //
    //	MINIONS
    //
    ////////////////////////////////////////////////////////////////////////

    /** Wrap an exception in a SQLException */
    private SQLException    wrap( Throwable t )
    {
        return new SQLException( t.getMessage(), t );
    }
    
    private SQLException    wrap( String errorMessage )
    {
        String  sqlState = org.apache.derby.shared.common.reference.SQLState.JAVA_EXCEPTION.substring( 0, 5 );

        return new SQLException( errorMessage, sqlState );
    }
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContext( final String contextID )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
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

