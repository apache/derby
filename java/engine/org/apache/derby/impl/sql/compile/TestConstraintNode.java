/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Vector;

/**
 * A TestConstraintNode is used to determine when a constraint
 * has been violated.
 *
 * @author jeff
 */

public class TestConstraintNode extends UnaryLogicalOperatorNode
{
	private String sqlState;
	private String tableName;
	private String constraintName;

	/**
	 * Initializer for a TestConstraintNode
	 *
	 * @param operand	The operand of the constraint test
	 * @param sqlState	The SQLState of the exception to throw if the
	 *					constraint has failed
	 * @param tableName	The name of the table that the constraint is on
	 * @param constraintName	The name of the constraint being checked
	 */

	public void init(Object booleanValue,
					 Object sqlState,
					 Object tableName,
					 Object constraintName)
	{
		super.init(booleanValue, "throwExceptionIfFalse");
		this.sqlState = (String) sqlState;
		this.tableName = (String) tableName;
		this.constraintName = (String) constraintName;
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
		bindUnaryOperator(fromList, subqueryList, aggregateVector);

		/*
		** If the operand is not boolean, cast it.
		*/

		if ( ! operand.getTypeServices().getTypeId().getSQLTypeName().equals(
														TypeId.BOOLEAN_NAME))
		{
			operand = (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					operand,
					new DataTypeDescriptor(TypeId.BOOLEAN_ID, true),
					getContextManager());
			((CastNode) operand).bindCastNodeOnly();
		}

		/* Set the type info */
		setFullTypeInfo();

		return this;
	}

	/**
	 * Do code generation for the TestConstraint operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{

		/*
		** This generates the following code:
		**
		** operand.testConstraint(sqlState, tableName, constraintName)
		*/

		operand.generateExpression(acb, mb);

		mb.push(sqlState);
		mb.push(tableName);
		mb.push(constraintName);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.BooleanDataValue,
				"throwExceptionIfFalse", ClassName.BooleanDataValue, 3);

	}
}
