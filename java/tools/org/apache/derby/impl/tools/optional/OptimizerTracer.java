/*

   Derby - Class org.apache.derby.impl.tools.optional.OptimizerTracer

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

package org.apache.derby.impl.tools.optional;

import java.sql.SQLException;

import org.apache.derby.iapi.db.OptimizerTrace;
import org.apache.derby.iapi.sql.dictionary.OptionalTool;

/**
 * <p>
 * OptionalTool to create wrapper functions and views for all of the user tables
 * in a foreign database.
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
     * Turns on optimizer tracing.
     * </p>
     */
    public  void    loadTool( String... configurationParameters )
        throws SQLException
    {
        OptimizerTrace.setOptimizerTrace( true );
    }

    /**
     * <p>
     * Dump the optimizer trace and turn off tracing.
     * </p>
     */
    public  void    unloadTool( String... configurationParameters )
        throws SQLException
    {
        System.out.println( OptimizerTrace.getOptimizerTraceOutput() );

        OptimizerTrace.setOptimizerTrace( false );
    }

}

