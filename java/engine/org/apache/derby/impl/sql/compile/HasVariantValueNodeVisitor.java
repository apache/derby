/*

   Derby - Class org.apache.derby.impl.sql.compile.HasVariantValueNodeVisitor

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;


import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.error.StandardException;

/**
 * Find out if we have a value node with variant type less than what the
 * caller desires, anywhere below us.  Stop traversal as soon as we find one.
 * This is used in two places: one to check the values clause of an insert
 * statement; i.e 
 * <pre>
 * insert into <table> values (?, 1, foobar());
 * </pre>
 * If all the expressions in the values clause are QUERY_INVARIANT (and an
 * exception is made for parameters) then we can cache the results in the
 * RowResultNode. This is useful when we have a prepared insert statement which
 * is repeatedly executed.
 * <p>
 * The second place where this is used is to check if a subquery can be
 * materialized or not. 
 * @see org.apache.derby.iapi.store.access.Qualifier 
 *
 * @author jamie
 */
public class HasVariantValueNodeVisitor implements Visitor
{
	private boolean hasVariant;
	private int variantType;
	private boolean ignoreParameters;


	/**
	 * Construct a visitor
	 */
	public HasVariantValueNodeVisitor()
	{
		this.variantType = Qualifier.VARIANT;
		this.ignoreParameters = false;
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(Qualifier.VARIANT < Qualifier.SCAN_INVARIANT, "qualifier constants not ordered as expected");
			SanityManager.ASSERT(Qualifier.SCAN_INVARIANT < Qualifier.QUERY_INVARIANT, "qualifier constants not ordered as expected");
		}		
	}

	
	/**
	 * Construct a visitor.  Pass in the variant
	 * type.  We look for nodes that are less
	 * than or equal to this variant type.  E.g.,
	 * if the variantType is Qualifier.SCAN_VARIANT,
	 * then any node that is either VARIANT or
	 * SCAN_VARIANT will cause the visitor to 
	 * consider it variant.
	 *
	 * @param variantType the type of variance we consider
	 *		variant
	 * @param skipParameter should I ignore parameter nodes?
 	 */
	public HasVariantValueNodeVisitor(int variantType, boolean ignoreParameters)
	{
		this.variantType = variantType;
		this.ignoreParameters = ignoreParameters;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(variantType >= Qualifier.VARIANT, "bad variantType");
			// note: there is no point in (variantType == Qualifier.CONSTANT) so throw an
			// exception for that case too
			SanityManager.ASSERT(variantType <= Qualifier.QUERY_INVARIANT, "bad variantType");
		}		
	}
	
	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * If we have found the target node, we are done.
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on error
	 */
	public Visitable visit(Visitable node) throws StandardException
	{
		if (node instanceof ValueNode)
		{
			if (ignoreParameters && ((ValueNode)node).isParameterNode())
				return node;
				
			if (((ValueNode)node).getOrderableVariantType() <= variantType)
			{
				hasVariant = true;
			}
		}
		return node;
	}

	public boolean skipChildren(Visitable node)
	{
		return false;
	}

	/**
	 * Stop traversal if we found the target node
	 *
	 * @return true/false
	 */
	public boolean stopTraversal()
	{
		return hasVariant;
	}

	////////////////////////////////////////////////
	//
	// CLASS INTERFACE
	//
	////////////////////////////////////////////////
	/**
	 * Indicate whether we found the node in
	 * question
	 *
	 * @return true/false
	 */
	public boolean hasVariant()
	{
		return hasVariant;
	}
}
