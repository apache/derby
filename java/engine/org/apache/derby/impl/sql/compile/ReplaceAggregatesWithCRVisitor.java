/*

   Derby - Class org.apache.derby.impl.sql.compile.ReplaceAggregatesWithCRVisitor

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
