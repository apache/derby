/*

   Derby - Class org.apache.derby.impl.sql.compile.HasCorrelatedCRsVisitor

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;


import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.error.StandardException;

/**
 * Find out if we have an correlated column reference
 * anywhere below us.  Stop traversal as soon as we find one.
 *
 * @author jamie
 */
public class HasCorrelatedCRsVisitor implements Visitor
{
	private boolean hasCorrelatedCRs;

	/**
	 * Construct a visitor
	 */
	public HasCorrelatedCRsVisitor()
	{
	}



	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * If we have found the target node, we are done.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 */
	public Visitable visit(Visitable node)
	{
		if (node instanceof ColumnReference)
		{
			if (((ColumnReference)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof VirtualColumnNode)
		{
			if (((VirtualColumnNode)node).getCorrelated())
			{
				hasCorrelatedCRs = true;
			}
		}
		else if (node instanceof MethodCallNode)
		{
			/* trigger action references are correlated
			 */
			if (((MethodCallNode)node).getMethodName().equals("getTriggerExecutionContext") ||
				((MethodCallNode)node).getMethodName().equals("TriggerOldTransitionRows") ||
				((MethodCallNode)node).getMethodName().equals("TriggerNewTransitionRows")
			   )
			{
				hasCorrelatedCRs = true;
			}
		}
		return node;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasCorrelatedCRs;
	}

	public boolean skipChildren(Visitable v)
	{
		return false;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
	public boolean hasCorrelatedCRs()
	{
		return hasCorrelatedCRs;
	}

	/**
	 * Shortcut to set if hasCorrelatedCRs
	 *
	 *	@param	true/false
	 *	@return	nothing
	 */
	public void setHasCorrelatedCRs(boolean value)
	{
		hasCorrelatedCRs = value;
	}
}
