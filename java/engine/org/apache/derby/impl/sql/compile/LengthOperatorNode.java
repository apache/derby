/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.JDBC20Translation;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a unary XXX_length operator
 *
 * @author Jeff Lichtman
 */

public final class LengthOperatorNode extends UnaryOperatorNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	private int parameterType;
	private int parameterWidth;

	public void setNodeType(int nodeType)
	{
		String operator = null;
		String methodName = null;

		if (nodeType == C_NodeTypes.CHAR_LENGTH_OPERATOR_NODE)
		{
				operator = "char_length";
				methodName = "charLength";
				parameterType = Types.VARCHAR;
				parameterWidth = TypeId.VARCHAR_MAXWIDTH;
		}
		else
		{
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT(
						"Unexpected nodeType = " + nodeType);
				}
		}
		setOperator(operator);
		setMethodName(methodName);
		super.setNodeType(nodeType);
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
		** string value types.  
		*/
		operandType = operand.getTypeId();
		switch (operandType.getJDBCTypeId())
		{
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.BINARY:
				case Types.VARBINARY:
				case Types.LONGVARBINARY:
				case Types.LONGVARCHAR:
                case JDBC20Translation.SQL_TYPES_BLOB:
                case JDBC20Translation.SQL_TYPES_CLOB:
					break;
			
				default:
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
											getOperatorString(),
											operandType.getSQLTypeName());
		}

		/*
		** The result type of XXX_length is int.
		*/
		setType(new DataTypeDescriptor(
							TypeId.INTEGER_ID,
							operand.getTypeServices().isNullable()
						)
				);
		return this;
	}

	/**
	 * Bind a ? parameter operand of the XXX_length function.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if XXX_length has a ? operand,
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		*/

		((ParameterNode) operand).setDescriptor(
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(parameterType, true, 
												parameterWidth));
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
	public String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
	}
}
