/*

   Derby - Class org.apache.derby.impl.sql.compile.ConcatenationOperatorNode

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.types.BitDataValue;

import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import java.sql.Types;

import java.util.Vector;


/**
 * This node represents a concatenation comparison operator
 *
 * @author Jerry Brenner -- modified by jamie for bit and bit
 *							varying.
 */

public class ConcatenationOperatorNode extends BinaryOperatorNode
{
	/**
	 * Initializer for a ConcatenationOperatorNode
	 *
	 * @param leftOperand	The left operand of the concatenation
	 * @param rightOperand	The right operand of the concatenation
	 */
	public void init(Object leftOperand, Object rightOperand)
	{
		super.init(leftOperand, rightOperand, "||", "concatenate",
				ClassName.ConcatableDataValue, ClassName.ConcatableDataValue);
	}

	/**
	 * overrides BindOperatorNode.bindExpression because concatenation has special
	 * requirements for parameter binding.
	 *
	 * @exception StandardException thrown on failure
	 */
	public ValueNode bindExpression(
		FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector)
			throws StandardException
	{
		// deal with binding operands
		leftOperand = leftOperand.bindExpression(fromList, subqueryList,
			aggregateVector);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList,
			aggregateVector);

		// deal with operand parameters
		/*
			Is there a ? parameter on the left?
			If so, it's type is the type of the other parameter, with
			maximum length for that type.
		*/

		if (leftOperand.isParameterNode())
		{
			if (rightOperand.isParameterNode())
			{
				throw StandardException.newException(SQLState.LANG_BINARY_OPERANDS_BOTH_PARMS, 
																	operator);
			}

			TypeId 	leftType;

			/*
			** A ? on the left gets its type from the right.  There are six
			** legal types for the concatenation operator: CHAR, VARCHAR,
			** LONG VARCHAR, BIT, BIT VARYING, and LONG BIT VARYING.  If the
			** right type is one of the bit types, set the parameter type to
			** BIT VARYING with maximum length.
			**
			** If the right type is anything else, set it to VARCHAR with
			** maximum length.  We count on the resolveConcatOperation method to
			** catch an illegal type.
			**
			** NOTE: When I added the long types, I could have changed the
			** resulting parameter types to LONG VARCHAR and LONG BIT VARYING,
			** but they were already VARCHAR and BIT VARYING, and it wasn't
			** clear to me what effect it would have to change it.
			**
			**				-	Jeff
			*/
			if (rightOperand.getTypeId().isBitTypeId())
			{
				leftType = TypeId.getBuiltInTypeId(Types.VARBINARY);
			}
			else
			{
				leftType = TypeId.getBuiltInTypeId(Types.VARCHAR);
			}
		
		((ParameterNode) leftOperand).setDescriptor(new DataTypeDescriptor(leftType, true));
		}

		/*
			Is there a ? parameter on the right?
		*/
		if (rightOperand.isParameterNode())
		{
			TypeId 	rightType;

			/*
			** A ? on the right gets its type from the left.  There are six
			** legal types for the concatenation operator: CHAR, VARCHAR,
			** LONG VARCHAR, BIT, BIT VARYING, and LONG BIT VARYING.  If the
			** left type is one of the bit types, set the parameter type to
			** BIT VARYING with maximum length.  If the left type is anything
			** else, set it to VARCHAR with maximum length.  We count on the
			** resolveConcatOperation method to catch an illegal type.
			**
			** NOTE: When I added the long types, I could have changed the
			** resulting parameter types to LONG VARCHAR and LONG BIT VARYING,
			** but they were already VARCHAR and BIT VARYING, and it wasn't
			** clear to me what effect it would have to change it.
			**
			**				-	Jeff
			*/
			if (leftOperand.getTypeId().isBitTypeId())
			{
				rightType = TypeId.getBuiltInTypeId(Types.VARBINARY);
			}
			else
			{
				rightType = TypeId.getBuiltInTypeId(Types.VARCHAR);
			}
		
		((ParameterNode) rightOperand).setDescriptor(
							new DataTypeDescriptor(
										rightType,
										true));
		}

		/* If the left operand is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (! leftOperand.getTypeId().systemBuiltIn())
		{
			leftOperand = leftOperand.genSQLJavaSQLTree();
		}

		/* If the right operand is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (! rightOperand.getTypeId().systemBuiltIn())
		{
			rightOperand = rightOperand.genSQLJavaSQLTree();
		}

		/* If either the left or right operands are non-string, non-bit types,
		 * then we generate an implicit cast to VARCHAR.
		 */
		TypeCompiler tc = leftOperand.getTypeCompiler();
		if (! (leftOperand.getTypeId().isStringTypeId() || leftOperand.getTypeId().isBitTypeId()))
		{
			leftOperand =  (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						leftOperand, 
						DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 
										    tc.getCastToCharWidth(
												leftOperand.getTypeServices())),
						getContextManager());
			((CastNode) leftOperand).bindCastNodeOnly();
		}
		tc = rightOperand.getTypeCompiler();
		if (! (rightOperand.getTypeId().isStringTypeId() || rightOperand.getTypeId().isBitTypeId()))
		{
			rightOperand =  (ValueNode)
					getNodeFactory().getNode(
						C_NodeTypes.CAST_NODE,
						rightOperand, 
						DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true,
										    tc.getCastToCharWidth(
												rightOperand.getTypeServices())),
						getContextManager());
			((CastNode) rightOperand).bindCastNodeOnly();
		}


		/*
		** Set the result type of this operator based on the operands.
		** By convention, the left operand gets to decide the result type
		** of a binary operator.
		*/
		tc = leftOperand.getTypeCompiler();
		setType(resolveConcatOperation(
						leftOperand.getTypeServices(),
						rightOperand.getTypeServices()));

		/*
		** Make sure the maximum width set for the result doesn't exceed the result type's maximum width
		*/
		if (SanityManager.DEBUG)
		{
			if (getTypeServices().getMaximumWidth() > getTypeId().getMaximumMaximumWidth())
			{
				SanityManager.THROWASSERT("The maximum length " + getTypeServices().getMaximumWidth() +
						" for the result type " + getTypeId().getSQLTypeName() +
						" can't be greater than it's maximum width of result's typeid" + getTypeId().getMaximumMaximumWidth());
			}
		}

		/*
		** Now that we know the target interface type, set it.  This assumes
		** that both operands have the same interface type, which is a safe
		** assumption for the concatenation operator.
		*/
		this.setLeftRightInterfaceType(tc.interfaceName());

		return this;
	}

	/**
	 * Resolve a concatenation operator
	 *
	 * @param leftType	The DataTypeDescriptor of the left operand
	 * @param rightType	The DataTypeDescriptor of the right operand
	 *
	 * @return	A DataTypeDescriptor telling the result type of the 
	 *			concatenate operation
	 *
	 * @exception StandardException BinaryOperatorNotSupported
	 *						Thrown when a BinaryOperator is not supported
	 *						on the operand types.
	 */
	private DataTypeDescriptor resolveConcatOperation(
								DataTypeDescriptor leftType,
								DataTypeDescriptor rightType
							) throws StandardException
	{
		TypeId	leftTypeId;
		TypeId	rightTypeId;
		String	higherType;
		int					resultLength;
		boolean				nullable;

		leftTypeId = leftType.getTypeId();
		rightTypeId = rightType.getTypeId();

		/*
		** Check the right type to be sure it's a concatable.  By convention,
		** we call this method off the TypeId of the left operand, so if
		** we get here, we know the left operand is a concatable.
		*/
		/*
		** Make sure we haven't been given a char and a
		** bit to concatenate.
		*/

		if (!leftTypeId.isConcatableTypeId()
			|| !rightTypeId.isConcatableTypeId()
			|| (rightTypeId.isBitTypeId() && leftTypeId.isStringTypeId())
			|| (leftTypeId.isBitTypeId() && rightTypeId.isStringTypeId()))
				throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "||", "FUNCTION");

		/*
		** The types aren't the same.  The result of the operation is the
		** type of higher precedence.
		*/
    
		higherType = (leftTypeId.typePrecedence() >=
									rightTypeId.typePrecedence()) ?
				leftType.getTypeName() : rightType.getTypeName();

		/* Get the length of the result */
		resultLength = leftType.getMaximumWidth() +
					   rightType.getMaximumWidth();

		/*
		** Use following chart to handle overflow
		** operands CHAR(A) CHAR(B) and A+B<255 then result is CHAR(A+B)
		** operands CHAR FOR BIT DATA(A) CHAR FOR BIT DATA(B) and A+B<255 then result is CHAR FOR BIT DATA(A+B)
		**
		** operands CHAR(A) CHAR(B) and A+B>254 then result is VARCHAR(A+B)
		** operands CHAR FOR BIT DATA(A) CHAR FOR BIT DATA(B) and A+B>254 then result is VARCHAR FOR BIT DATA(A+B)
		**
		** operands CHAR(A) VARCHAR(B) and A+B<4001 then result is VARCHAR(A+B)
		** operands CHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B<4001 then result is VARCHAR FOR BIT DATA(A+B)
		**
		** operands CHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
		** operands CHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
		**
		** operands CHAR(A) LONG VARCHAR then result is LONG VARCHAR
		** operands CHAR FOR BIT DATA(A) LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
		**
		** operands VARCHAR(A) VARCHAR(B) and A+B<4001 then result is VARCHAR(A+B)
		** operands VARCHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B<4001 then result is VARCHAR FOR BIT DATA(A+B)
		**
		** operands VARCHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
		** operands VARCHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
		**
		** operands VARCHAR(A) LONG VARCHAR then result is LONG VARCHAR
		** operands VARCHAR FOR BIT DATA(A) LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
		**
		** operands LONG VARCHAR, LONG VARCHAR then result is LONG VARCHAR
		** operands LONG VARCHAR FOR BIT DATA, LONG VARCHAR FOR BIT DATA then result is LONG VARCHAR FOR BIT DATA
		**
		** operands CLOB(A), CHAR(B) then result is CLOB(MIN(A+B,2G))
		** operands CLOB(A), VARCHAR(B) then result is CLOB(MIN(A+B,2G))
		** operands CLOB(A), LONG VARCHAR then result is CLOB(MIN(A+32K,2G))
		** operands CLOB(A), CLOB(B) then result is CLOB(MIN(A+B,2G))
		**
		** operands BLOB(A), CHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
		** operands BLOB(A), VARCHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
		** operands BLOB(A), LONG VARCHAR FOR BIT DATA then result is BLOB(MIN(A+32K,2G))
		** operands BLOB(A), BLOB(B) then result is BLOB(MIN(A+B,2G))
		**
		** operands CHAR(A)/VARCHAR(A)/LONGVARCHAR, LONGVARCHAR and "concatenated string length">32700 does not cause automatic escalation
		** to LOB for compatibility with previous releases. Any such cases would result in an error at runtime
		**
		*/
		//in the following code, I can assume that left and right operands both will be either char kind
		//of datatypes or they will be both binary kind of datatypes. That's because operand datatypes
		//mismatch has already been handled earlier
		if (leftTypeId.getJDBCTypeId() == Types.CHAR || leftTypeId.getJDBCTypeId() == Types.BINARY)
		{
			switch (rightTypeId.getJDBCTypeId())
			{
				case Types.CHAR:
				case Types.BINARY:
					if (resultLength > DB2Limit.DB2_CHAR_MAXWIDTH) {
						if (rightTypeId.getJDBCTypeId() == Types.CHAR)
								//operands CHAR(A) CHAR(B) and A+B>254 then result is VARCHAR(A+B)
								higherType = TypeId.VARCHAR_NAME;
						else
								//operands CHAR FOR BIT DATA(A) CHAR FOR BIT DATA(B) and A+B>254 then result is VARCHAR FOR BIT DATA(A+B)
								higherType = TypeId.VARBIT_NAME;
					}
					break;

				case Types.VARCHAR:
				case Types.VARBINARY:
					if (resultLength > DB2Limit.DB2_CONCAT_VARCHAR_LENGTH) {
						if (rightTypeId.getJDBCTypeId() == Types.VARCHAR)
								//operands CHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
								higherType = TypeId.LONGVARCHAR_NAME;
						else
								//operands CHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
								higherType = TypeId.LONGVARBIT_NAME;
					}
					break;

				case Types.CLOB:
				case Types.BLOB:
					//operands CHAR(A), CLOB(B) then result is CLOB(MIN(A+B,2G))
					//operands CHAR FOR BIT DATA(A), BLOB(B) then result is BLOB(MIN(A+B,2G))
					resultLength = clobBlobHandling(rightType, leftType);
					break;
			}
		} else if (leftTypeId.getJDBCTypeId() == Types.VARCHAR) {
			switch (rightTypeId.getJDBCTypeId())
			{
				case Types.CHAR: //operands CHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
				case Types.VARCHAR: //operands VARCHAR(A) VARCHAR(B) and A+B>4000 then result is LONG VARCHAR
					if (resultLength > DB2Limit.DB2_CONCAT_VARCHAR_LENGTH)
						higherType = TypeId.LONGVARCHAR_NAME;
					break;

				case Types.CLOB:
					//operands VARCHAR(A), CLOB(B) then result is CLOB(MIN(A+B,2G))
					resultLength = clobBlobHandling(rightType, leftType);
					break;
			}
		} else if (leftTypeId.getJDBCTypeId() == Types.VARBINARY) {
			switch (rightTypeId.getJDBCTypeId())
			{
				case Types.BINARY: //operands CHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
				case Types.VARBINARY://operands VARCHAR FOR BIT DATA(A) VARCHAR FOR BIT DATA(B) and A+B>4000 then result is LONG VARCHAR FOR BIT DATA
					if (resultLength > DB2Limit.DB2_CONCAT_VARCHAR_LENGTH)
						higherType = TypeId.LONGVARBIT_NAME;
					break;

				case Types.BLOB:
					//operands VARCHAR FOR BIT DATA(A), BLOB(B) then result is BLOB(MIN(A+B,2G))
					resultLength = clobBlobHandling(rightType, leftType);
					break;
			}
		} else if (leftTypeId.getJDBCTypeId() == Types.CLOB || leftTypeId.getJDBCTypeId() == Types.BLOB) {
			//operands CLOB(A), CHAR(B) then result is CLOB(MIN(A+B,2G))
			//operands CLOB(A), VARCHAR(B) then result is CLOB(MIN(A+B,2G))
			//operands CLOB(A), LONG VARCHAR then result is CLOB(MIN(A+32K,2G))
			//operands CLOB(A), CLOB(B) then result is CLOB(MIN(A+B,2G))
			//operands BLOB(A), CHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
			//operands BLOB(A), VARCHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
			//operands BLOB(A), LONG VARCHAR FOR BIT DATA then result is BLOB(MIN(A+32K,2G))
			//operands BLOB(A), BLOB(B) then result is BLOB(MIN(A+B,2G))
			resultLength = clobBlobHandling(leftType, rightType);
		} else if (rightTypeId.getJDBCTypeId() == Types.CLOB || rightTypeId.getJDBCTypeId() == Types.BLOB) {
			//operands LONG VARCHAR, CLOB(A) then result is CLOB(MIN(A+32K,2G))
			//operands LONG VARCHAR FOR BIT DATA, BLOB(A) then result is BLOB(MIN(A+32K,2G))
			resultLength = clobBlobHandling(rightType, leftType);
		}

		//bug - 5837. long varchar and long binary can't hold more data than their specific limits. If this length is violated by resulting
		//concatenated string, an exception will be thrown at execute time.
		if (higherType.equals(TypeId.LONGVARCHAR_NAME))
			resultLength = TypeId.LONGVARCHAR_MAXWIDTH;
		else if (higherType.equals(TypeId.LONGVARBIT_NAME))
			resultLength = TypeId.LONGVARBIT_MAXWIDTH;


		/*
		** Result Length can't be negative
		*/
		if (SanityManager.DEBUG)
		{
			if (resultLength < 0)
			{
				SanityManager.THROWASSERT("There should not be an overflow of maximum length for any result type at this point. Overflow for BLOB/CLOB has already been handled earlier");
			}
		}

		/* The result is nullable if either side is nullable */
		nullable = leftType.isNullable() || rightType.isNullable();

		/*
		** Create a new DataTypeDescriptor that has the correct
		** type and nullability.
		**
		** It's OK to call the implementation of the DataTypeDescriptorFactory
		** here, because we're in the same package.
		*/
		return new DataTypeDescriptor(
					TypeId.getBuiltInTypeId(higherType),
					nullable,
					resultLength
				);
	}

	/*
	 *for conatenation operator, we generate code as follows
	 *field = method(p1, p2, field);
	 *what we are ensuring here is if field is null then initialize it to NULL SQLxxx type.
	 *Because of the following, at execution time, SQLxxx concatenate method do not have to
	 *worry about field coming in as null
	*/
	protected void initializeResultField(ExpressionClassBuilder acb, MethodBuilder mb, LocalField resultField)
	{
		mb.conditionalIfNull();//get the field on the stack and if it is null
			acb.generateNull(mb, getTypeCompiler());// yes, it is, hence create a NULL SQLxxx type object and put that on stack
		mb.startElseCode(); //no, it is not null
			mb.getField(resultField); //so put it back on the stack
		mb.completeConditional(); //complete if else block
	}

	private static int clobBlobHandling(
								DataTypeDescriptor clobBlobType,
								DataTypeDescriptor otherType
							) throws StandardException
	{
		int resultLength;

		if (otherType.getTypeId().getJDBCTypeId() == Types.LONGVARCHAR ||
			otherType.getTypeId().getJDBCTypeId() == Types.LONGVARBINARY) {
			//operands CLOB(A), LONG VARCHAR then result is CLOB(MIN(A+32K,2G))
			//operands BLOB(A), LONG VARCHAR FOR BIT DATA then result is BLOB(MIN(A+32K,2G))
			resultLength = clobBlobType.getMaximumWidth() + 32768;
		} else {
			//operands CLOB(A), CHAR(B) then result is CLOB(MIN(A+B,2G))
			//operands CLOB(A), VARCHAR(B) then result is CLOB(MIN(A+B,2G))
			//operands CLOB(A), CLOB(B) then result is CLOB(MIN(A+B,2G))
			//operands BLOB(A), CHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
			//operands BLOB(A), VARCHAR FOR BIT DATA(B) then result is BLOB(MIN(A+B,2G))
			//operands BLOB(A), BLOB(B) then result is BLOB(MIN(A+B,2G))
			resultLength = clobBlobType.getMaximumWidth() + otherType.getMaximumWidth();
		}

		if (resultLength < 1) //this mean A+B or A+32K is bigger than 2G
			return(Integer.MAX_VALUE); 
		else
			return(resultLength);

	}
}
