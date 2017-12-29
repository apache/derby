/*

   Derby - Class org.apache.derby.impl.sql.compile.ScrollInsensitiveResultSetNode

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
 * A ScrollInsensitiveResultSetNode represents the insensitive scrolling cursor
 * functionality for any 
 * child result set that needs one.
 *
 */

class ScrollInsensitiveResultSetNode  extends SingleChildResultSetNode
{
	/**
     * Constructor for a ScrollInsensitiveResultSetNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param rcl			The RCL for the node
	 * @param tableProperties	Properties list associated with the table
     * @param cm            The context manager
	 */
    ScrollInsensitiveResultSetNode(
                            ResultSetNode childResult,
                            ResultColumnList rcl,
                            Properties tableProperties,
                            ContextManager cm) {
        super(childResult, tableProperties, cm);
        setResultColumns( rcl );
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

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		// Generate the child ResultSet

		// Get the cost estimate for the child
		setCostEstimate( childResult.getFinalCostEstimate() );

		int erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);

		childResult.generate(acb, mb);
		acb.pushThisAsActivation(mb);
		mb.push(getResultSetNumber());
		mb.push(getResultColumns().size());

		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "getScrollable",
						"boolean", 0);

		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getScrollInsensitiveResultSet",
						ClassName.NoPutResultSet, 7);
	}
}
