/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.util.Vector;

public abstract class UnaryLogicalOperatorNode extends UnaryOperatorNode
{
	/**
	 * Initializer for a UnaryLogicalOperatorNode
	 *
	 * @param operand	The operand of the operator
	 * @param methodName	The name of the method to call in the generated
	 *						class.  In this case, it's actually an operator
	 *						name.
	 */

	public void init(
				Object	operand,
				Object		methodName)
	{
		/* For logical operators, the operator and method names are the same */
		super.init(operand, methodName, methodName);
	}

	/**
	 * Bind this logical operator.  All that has to be done for binding
	 * a logical operator is to bind the operand, check that the operand
	 * is SQLBoolean, and set the result type to SQLBoolean.
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector)
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList,
							 aggregateVector);

		/*
		** Logical operators work only on booleans.  If the operand 
		** is not boolean, throw an exception.
		**
		** For now, this exception will never happen, because the grammar
		** does not allow arbitrary expressions with NOT.  But when
		** we start allowing generalized boolean expressions, we will modify
		** the grammar, so this test will become useful.
		*/

		if ( ! operand.getTypeServices().getTypeId().equals(TypeId.BOOLEAN_ID))
		{
operand.treePrint();
			throw StandardException.newException(SQLState.LANG_UNARY_LOGICAL_NON_BOOLEAN);
		}

		/* Set the type info */
		setFullTypeInfo();

		return this;
	}
		
	/**
	 * Set all of the type info (nullability and DataTypeServices) for
	 * this node.  Extracts out tasks that must be done by both bind()
	 * and post-bind() AndNode generation.
	 *
	 * @return	None.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected void setFullTypeInfo()
		throws StandardException
	{
		boolean nullableResult;

		/*
		** Set the result type of this comparison operator based on the
		** operands.  The result type is always SQLBoolean - the only question
		** is whether it is nullable or not.  If either of the operands is
		** nullable, the result of the comparison must be nullable, too, so
		** we can represent the unknown truth value.
		*/
		nullableResult = operand.getTypeServices().isNullable();
		setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));
	}
}
