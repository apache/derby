/*
   Derby - Class org.apache.derby.impl.sql.compile.ReplaceWindowFuncCallsWithCRVisitor

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

/**
 * Replace all window function calls with result columns.
 *
 */
class ReplaceWindowFuncCallsWithCRVisitor implements Visitor
{
	private ResultColumnList rcl;
    private Class<?> skipOverClass;
	private int tableNumber;

	/**
	 * Replace all window function calls with column references.  Add
	 * the reference to the RCL.  Delegates most work to
	 * WindowFunctionNode.replaceCallsWithColumnReferences(rcl, tableNumber).
	 *
	 * @param rcl the result column list
	 * @param tableNumber	The tableNumber for the new CRs
	 * @param skipOverClass Don't go past this
	 */
    ReplaceWindowFuncCallsWithCRVisitor(ResultColumnList rcl,
											int tableNumber,
                                            Class<?> skipOverClass)
	{
		this.rcl = rcl;
		this.tableNumber = tableNumber;
		this.skipOverClass = skipOverClass;
	}

	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Don't do anything unless we have a window function node
     * node. Visitor override.
	 * @see Visitor#visit
	 *
	 */
	public Visitable visit(Visitable node)
		throws StandardException
	{
		if (node instanceof WindowFunctionNode)
		{
			/*
			** Let windowFunctionNode replace itself.
			*/
			node = ((WindowFunctionNode)node).
				replaceCallsWithColumnReferences(rcl, tableNumber);
		}

		return node;
	}

	/**
     * Don't visit children under the skipOverClass
     * node, if it isn't null. Visitor override.
	 * @see Visitor#skipChildren
	 */
	public boolean skipChildren(Visitable node)
	{
		return (skipOverClass == null) ?
				false:
				skipOverClass.isInstance(node);
	}


	/**
     * Visitor override.
	 * @return false
	 * @see Visitor#visitChildrenFirst
	 */
	public boolean visitChildrenFirst(Visitable node)
	{
		return false;
	}

	/**
     * Visitor override.
	 * @return false
	 * @see Visitor#skipChildren
	 */
	public boolean stopTraversal()
	{
		return false;
	}
}
