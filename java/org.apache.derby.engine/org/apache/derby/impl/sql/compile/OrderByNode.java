/*

   Derby - Class org.apache.derby.impl.sql.compile.OrderByNode

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

import java.util.Properties;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;

/**
 * An OrderByNode represents a result set for a sort operation
 * for an order by list.  It is expected to only be generated at 
 * the end of optimization, once we have determined that a sort
 * is required.
 *
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class OrderByNode extends SingleChildResultSetNode
{

	OrderByList		orderByList;

	/**
     * Constructor for a OrderByNode.
	 *
     * @param childRes      The child ResultSetNode
	 * @param orderByList	The order by list.
 	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
     *
	 * @exception StandardException		Thrown on error
	 */
    OrderByNode(ResultSetNode childRes,
                OrderByList orderByList,
                Properties tableProperties,
                ContextManager cm) throws StandardException {
        super(childRes, tableProperties, cm);

        this.orderByList = orderByList;

        // We want our own resultColumns, which are virtual columns pointing to
        // the child result's columns.
        //
        // We have to have the original object in the distinct node, and give
        // the underlying project the copy.
        //
        // We get a shallow copy of the ResultColumnList and its ResultColumns.
        // (Copy maintains ResultColumn.expression for now.)
        final ResultColumnList prRCList =
            childRes.getResultColumns().copyListAndObjects();
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        setResultColumns( childRes.getResultColumns() );
        childRes.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
        getResultColumns().genVirtualColumnNodes(this, prRCList);
	}


	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);
//IC see: https://issues.apache.org/jira/browse/DERBY-4087

			if (orderByList != null)
			{
				printLabel(depth, "orderByList: ");
				orderByList.treePrint(depth + 1);
			}
		}
	}

    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-2661
	ResultColumnDescriptor[] makeResultDescriptors()
	{
	    return childResult.makeResultDescriptors();
	}

    /**
     * generate the distinct result set operating over the source
     * result set.
     *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		// Get the cost estimate for the child
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		if (getCostEstimate() == null)
		{
			setCostEstimate( childResult.getFinalCostEstimate() );
		}

	    orderByList.generate(acb, mb, childResult);

		// We need to take note of result set number if ORDER BY is used in a
		// subquery for the case where a PRN is inserted in top of the select's
		// PRN to project away a sort column that is not part of the select
		// list, e.g.
		//
		//     select * from (select i from t order by j desc) s
		//
		// If the resultSetNumber is not correctly set in our resultColumns,
		// code generation for the PRN above us will fail when calling
		// resultColumns.generateCore -> VCN.generateExpression, cf. the Sanity
		// assert in VCN.generateExpression on sourceResultSetNumber >= 0.
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		setResultSetNumber( orderByList.getResultSetNumber() );
		getResultColumns().setResultSetNumber(getResultSetNumber());
	}
}
