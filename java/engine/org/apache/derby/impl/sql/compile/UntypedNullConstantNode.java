/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.util.Vector;
/**
 * An UntypedNullConstantNode represents a SQL NULL before it has
 * been bound.  The bind() operation will replace the UntypedNullConstantNodes
 * with typed ConstantNodes.
 */

public final class UntypedNullConstantNode extends ConstantNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	 * Constructor for an UntypedNullConstantNode.  Untyped constants
	 * contain no state (not too surprising).
	 */

	public UntypedNullConstantNode()
	{
		super();
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
	public DataValueDescriptor convertDefaultNode(DataTypeDescriptor typeDescriptor)
	{
		/*
		** The default value is null, so set nullability to TRUE
		*/
		return typeDescriptor.getNull();
	}
	
	/** @see ValueNode#bindExpression(FromList, SubqueryList, Vector)
	 * @see ResultColumnList#bindUntypedNullsToResultColumns
	 * This does nothing-- the node is actually bound when
	 * bindUntypedNullsToResultColumns is called.
	 */
	public ValueNode bindExpression(FromList fromList, SubqueryList	subqueryList, Vector aggregateVector)
	{
		return this;
	}
}
