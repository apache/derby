/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */


package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.error.StandardException;

/**
 * Remap/unremap the CRs to the underlying
 * expression.
 *
 * @author jerry
 */
public class RemapCRsVisitor implements Visitor
{
	private boolean remap;

	public RemapCRsVisitor(boolean remap)
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
