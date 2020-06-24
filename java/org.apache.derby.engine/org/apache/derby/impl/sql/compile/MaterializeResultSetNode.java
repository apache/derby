/*

   Derby - Class org.apache.derby.impl.sql.compile.MaterializeResultSetNode

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
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * A MaterializeResultSetNode represents a materialization result set for any 
 * child result set that needs one.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class MaterializeResultSetNode extends SingleChildResultSetNode
{
	/**
     * Constructor for a MaterializeResultSetNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param rcl			The RCL for the node
	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
	 */

    MaterializeResultSetNode(ResultSetNode childResult,
                             ResultColumnList rcl,
                             Properties tableProperties,
                             ContextManager cm) {
        super(childResult, tableProperties, cm);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        setResultColumns( rcl );
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
		}
	}

    /**
     *
	 *
	 * @exception StandardException		Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		if (SanityManager.DEBUG)
            SanityManager.ASSERT(getResultColumns() != null, "Tree structure bad");
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// Get the cost estimate from the child if we don't have one yet
		setCostEstimate( childResult.getFinalCostEstimate() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

		// build up the tree.

		// Generate the child ResultSet
		acb.pushGetResultSetFactoryExpression(mb);

		childResult.generate(acb, mb);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		mb.push(getResultSetNumber());
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getMaterializedResultSet",
//IC see: https://issues.apache.org/jira/browse/DERBY-1700
						ClassName.NoPutResultSet, 4);
	}
}
