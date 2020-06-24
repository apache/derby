/*

   Derby - Class org.apache.derby.impl.sql.compile.ReplaceAggregatesWithCRVisitor

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
 * Replace all aggregates with result columns.
 *
 */
class ReplaceAggregatesWithCRVisitor implements Visitor
{
	private ResultColumnList rcl;
    private Class<?> skipOverClass;
	private int tableNumber;

	/**
	 * Replace all aggregates with column references.  Add
	 * the reference to the RCL.  Delegates most work to
	 * AggregateNode.replaceAggregatesWithColumnReferences(rcl, tableNumber).
	 *
	 * @param rcl the result column list
	 * @param tableNumber	The tableNumber for the new CRs
	 */
    ReplaceAggregatesWithCRVisitor(ResultColumnList rcl, int tableNumber)
	{
		this(rcl, tableNumber, null);
	}

    ReplaceAggregatesWithCRVisitor(
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        ResultColumnList rcl, int tableNumber, Class<?> skipOverClass)
	{
		this.rcl = rcl;
		this.tableNumber = tableNumber;
		this.skipOverClass = skipOverClass;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-673
    ReplaceAggregatesWithCRVisitor(ResultColumnList rcl, Class<?> nodeToSkip)
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
     * Don't visit children under the skipOverClass
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
	
	public boolean visitChildrenFirst(Visitable node)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		return false;
	}

	public boolean stopTraversal()
	{
		return false;
	}
}
