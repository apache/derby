/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */


package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.error.StandardException;

/**
 * Replace all aggregates with result columns.
 *
 * @author jamie
 */
public class ReplaceAggregatesWithCRVisitor implements Visitor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	private ResultColumnList rcl;
	private Class skipOverClass;
	private int tableNumber;

	/**
	 * Replace all aggregates with column references.  Add
	 * the reference to the RCL.  Delegates most work to
	 * AggregateNode.replaceAggregatesWithColumnReferences(rcl, tableNumber).
	 *
	 * @param rcl the result column list
	 * @param tableNumber	The tableNumber for the new CRs
	 * @param nodeToSkip don't examine anything below nodeToSkip
	 */
	public ReplaceAggregatesWithCRVisitor(ResultColumnList rcl, int tableNumber)
	{
		this.rcl = rcl;
		this.tableNumber = tableNumber;
	}

	/**
	 * Replace all aggregates with column references.  Add
	 * the reference to the RCL.  Delegates most work to
	 * AggregateNode.replaceAggregatesWithColumnReferences(rcl).
	 * Doesn't traverse below the passed in class.
	 *
	 * @param rcl the result column list
	 * @param nodeToSkip don't examine anything below nodeToSkip
	 */
	public ReplaceAggregatesWithCRVisitor(ResultColumnList rcl, Class nodeToSkip)
	{
		this.rcl = rcl;
		this.skipOverClass = nodeToSkip;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Don't do anything unless we have an aggregate
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
		if (node instanceof AggregateNode)
		{
			/*
			** Let aggregateNode replace itself.
			*/
			node = ((AggregateNode)node).replaceAggregatesWithColumnReferences(rcl, tableNumber);
		}

		return node;
	}

	/**
	 * Don't visit childen under the skipOverClass
	 * node, if it isn't null.
	 *
	 * @return true/false
	 */
	public boolean skipChildren(Visitable node)
	{
		return (skipOverClass == null) ?
				false:
				skipOverClass.isInstance(node);
	}
	
	public boolean stopTraversal()
	{
		return false;
	}
}
