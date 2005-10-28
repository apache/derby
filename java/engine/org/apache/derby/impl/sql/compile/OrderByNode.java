/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderByNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;

import java.util.Properties;

/**
 * An OrderByNode represents a result set for a sort operation
 * for an order by list.  It is expected to only be generated at 
 * the end of optimization, once we have determined that a sort
 * is required.
 *
 * @author jerry
 */
public class OrderByNode extends SingleChildResultSetNode
{

	OrderByList		orderByList;

	/**
	 * Initializer for a OrderByNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param orderByList	The order by list.
 	 * @param tableProperties	Properties list associated with the table
    *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
						Object childResult,
						Object orderByList,
						Object tableProperties)
		throws StandardException
	{
		ResultSetNode child = (ResultSetNode) childResult;

		super.init(childResult, tableProperties);

		this.orderByList = (OrderByList) orderByList;

		ResultColumnList prRCList;

		/*
			We want our own resultColumns, which are virtual columns
			pointing to the child result's columns.

			We have to have the original object in the distinct node,
			and give the underlying project the copy.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = child.getResultColumns().copyListAndObjects();
		resultColumns = child.getResultColumns();
		child.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		resultColumns.genVirtualColumnNodes(this, prRCList);
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return childResult.toString() + "\n" + 
				"orderByList: " + 
				(orderByList != null ? orderByList.toString() : "null") + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	ResultColumnDescriptor[] makeResultDescriptors(ExecutionContext ec)
	{
	    return childResult.makeResultDescriptors(ec);
	}

    /**
     * generate the distinct result set operating over the source
	 * resultset.
     *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		// Get the cost estimate for the child
		if (costEstimate == null)
		{
			costEstimate = childResult.getFinalCostEstimate();
		}

	    orderByList.generate(acb, mb, childResult);
	}
}
