/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
