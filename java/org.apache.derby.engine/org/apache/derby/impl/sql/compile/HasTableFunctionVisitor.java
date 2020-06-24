/*

   Derby - Class org.apache.derby.impl.sql.compile.HasTableFunctionVisitor

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.Visitable; 

/**
 * Find out if we have a user-defined table function anywhere in the
 * tree.  Stop traversal as soon as we find one.
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class HasTableFunctionVisitor extends HasNodeVisitor
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public HasTableFunctionVisitor()
    {
        super( FromVTI.class );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    @Override
	public Visitable visit(Visitable node)
	{
		if ( node instanceof FromVTI )
		{
            FromVTI vti = (FromVTI) node;

            if ( vti.isDerbyStyleTableFunction() ) { hasNode = true; }
		}
		return node;
	}
}

