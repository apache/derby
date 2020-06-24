/*

   Derby - Class org.apache.derby.impl.sql.compile.UntypedNullConstantNode

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

import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * An UntypedNullConstantNode represents a SQL NULL before it has
 * been bound.  The bind() operation will replace the UntypedNullConstantNodes
 * with typed ConstantNodes.
 */

public final class UntypedNullConstantNode extends ConstantNode
{
	/**
	 * Constructor for an UntypedNullConstantNode.  Untyped constants
	 * contain no state (not too surprising).
	 */

   UntypedNullConstantNode(ContextManager cm)
	{
       super(cm);
	}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 */

	//public int	getLength()
	//{
	//	if (SanityManager.DEBUG)
	//	SanityManager.ASSERT(false,
	//	  "Unimplemented method - should not be called on UntypedNullConstantNode");
	//	return 0;
	//}

	/**
	 * Should never be called for UntypedNullConstantNode because
	 * we shouldn't make it to generate
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("generateConstant() not expected to be called for UntypedNullConstantNode because we have implemented our own generateExpression().");
		}
	}

	/**
	 * Translate a Default node into a default value, given a type descriptor.
	 *
	 * @param typeDescriptor	A description of the required data type.
	 *
	 */
   @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DataValueDescriptor convertDefaultNode(DataTypeDescriptor typeDescriptor)
//IC see: https://issues.apache.org/jira/browse/DERBY-2335
	throws StandardException
	{
		/*
		** The default value is null, so set nullability to TRUE
		*/
		return typeDescriptor.getNull();
	}
	
	/** @see ValueNode#bindExpression(FromList, SubqueryList, List)
	 * @see ResultColumnList#bindUntypedNullsToResultColumns
	 * This does nothing-- the node is actually bound when
	 * bindUntypedNullsToResultColumns is called.
	 */
   @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates)
	{
		return this;
	}
}
