/*

   Derby - Class org.apache.derby.iapi.sql.compile.ScopeFilter

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

package org.apache.derby.iapi.sql.compile;

import java.util.List;

import org.apache.derby.shared.common.error.StandardException;

/**
 * Filter which passes Visitables only if the compiler is inside
 * a named scope.
 *
 */
public class ScopeFilter implements VisitableFilter
{
    ///////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    ///////////////////////////////////////////////////////////////////////////

    private CompilerContext _compilerContext;
    private String  _scopeName;
    private int     _minDepth;
    
    ///////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////

    /** Construct a filter for the given scope and minimal expected depth. */
    public  ScopeFilter
        (
         CompilerContext    compilerContext,
         String                 scopeName,
         int                    minDepth
         )
    {
        _compilerContext = compilerContext;
        _scopeName = scopeName;
        _minDepth = minDepth;
    }
    
    ///////////////////////////////////////////////////////////////////////////
    //
    //  VisitableFilter BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////
    
	public  boolean accept( Visitable visitable ) 
		throws StandardException
    {
        return (_compilerContext.scopeDepth( _scopeName ) >= _minDepth);
    }
    
}
