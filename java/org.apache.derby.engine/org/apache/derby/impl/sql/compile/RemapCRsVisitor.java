/*

   Derby - Class org.apache.derby.impl.sql.compile.RemapCRsVisitor

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Remap/unremap the CRs to the underlying
 * expression.
 *
 */
class RemapCRsVisitor implements Visitor
{
	private boolean remap;

    RemapCRsVisitor(boolean remap)
	{
		this.remap = remap;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Don't do anything unless we have a ColumnReference
	 * node.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on error
	 */
	public Visitable visit(Visitable node)
		throws StandardException
	{
		/*
		 * Remap all of the ColumnReferences in this expression tree
		 * to point to the ResultColumn that is 1 level under their
		 * current source ResultColumn.
		 * This is useful for pushing down single table predicates.
		 */
		if (node instanceof ColumnReference)
		{
			ColumnReference cr = (ColumnReference) node;
			if (remap)
			{
				cr.remapColumnReferences();
			}
			else
			{
				cr.unRemapColumnReferences();
			}
		}

		return node;
	}

	/**
	 * No need to go below a SubqueryNode.
	 *
	 * @return Whether or not to go below the node.
	 */
	public boolean skipChildren(Visitable node)
	{
		return (node instanceof SubqueryNode);
	}

	public boolean visitChildrenFirst(Visitable node)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		return false;
	}

	public boolean stopTraversal()
	{
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////

}	
