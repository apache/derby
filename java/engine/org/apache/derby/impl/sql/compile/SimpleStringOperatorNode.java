/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.types.StringDataValue;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a unary upper or lower operator
 *
 * @author Jerry
 */

public class SimpleStringOperatorNode extends UnaryOperatorNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	 * Initializer for a SimpleOperatorNode
	 *
	 * @param operand		The operand
	 * @param methodName	The method name
	 */

	public void init(Object operand, Object methodName)
	{
		super.init(operand, methodName, methodName);
	}

	/**
	 * Bind this operator
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
		FromList	fromList, SubqueryList subqueryList,
		Vector	aggregateVector)
			throws StandardException
	{
		TypeId	operandType;

		super.bindExpression(fromList, subqueryList, 
				aggregateVector);

		/*
		** Check the type of the operand - this function is allowed only on
		** string value (char and bit) types.
		*/
		operandType = operand.getTypeId();

		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
					break;
				case org.apache.derby.iapi.reference.JDBC20Translation.SQL_TYPES_JAVA_OBJECT:
				case Types.OTHER:	
				{
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
										methodName,
										operandType.getSQLTypeName());
				}

				default:
					operand =  (ValueNode)
						getNodeFactory().getNode(
							C_NodeTypes.CAST_NODE,
							operand,
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 
										  operand.getTypeCompiler().
											getCastToCharWidth(
												operand.getTypeServices())),
							getContextManager());
					((CastNode) operand).bindCastNodeOnly();
					operandType = operand.getTypeId();
		}

		/*
		** The result type of upper()/lower() is the type of the operand.
		*/

		setType(new DataTypeDescriptor(operandType,
				operand.getTypeServices().isNullable(),
				operand.getTypeCompiler().
					getCastToCharWidth(operand.getTypeServices())
						)
				);

		return this;
	}

	/**
	 * Bind a ? parameter operand of the upper/lower function.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if bit_length has a ? operand,
		** its type is bit varying with the implementation-defined maximum length
		** for a bit.
		*/

		((ParameterNode) operand).setDescriptor(
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.StringDataValue;
	}
}
