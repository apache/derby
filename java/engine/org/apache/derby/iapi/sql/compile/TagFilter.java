/*

   Derby - Class org.apache.derby.iapi.sql.compile.TagFilter

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
 * Filter which passes Visitables which have been marked with a given tag.
 *
 */
public class TagFilter implements VisitableFilter
{
    ///////////////////////////////////////////////////////////////////////////
    //
    //  CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////

    /** Tag placed on QueryTreeNodes which need privilege checks for UPDATE statements */
    public  static  final   String      NEED_PRIVS_FOR_UPDATE_STMT = "updatePrivs";

    /** Tag placed on the original ColumnReferences in an UPDATE, before unreferenced columns are added */
    public  static  final   String      ORIG_UPDATE_COL = "origUpdateCol";

    ///////////////////////////////////////////////////////////////////////////
    //
    //  STATE
    //
    ///////////////////////////////////////////////////////////////////////////
    
    private String  _tag;
    
    ///////////////////////////////////////////////////////////////////////////
    //
    //  CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////

    /** Construct a filter for the given tag. */
    public  TagFilter( String tag ) { _tag = tag; }
    
    ///////////////////////////////////////////////////////////////////////////
    //
    //  VisitableFilter BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////
    
	public  boolean accept( Visitable visitable ) 
		throws StandardException
    {
        return visitable.taggedWith( _tag );
    }
    
}
