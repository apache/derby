/*

   Derby - Class org.apache.derby.impl.sql.compile.ExtractOperatorNode

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.sql.Types;

import java.util.Vector;

/**
 * This node represents a unary extract operator, used to extract
 * a field from a date/time. The field value is returned as an integer.
 *
 * @author ames
 */
public class ExtractOperatorNode extends UnaryOperatorNode {

	static private final String fieldName[] = {
		"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"
	};
	static private final String fieldMethod[] = {
		"getYear","getMonth","getDate","getHours","getMinutes","getSeconds"
	};

	private int extractField;

	/**
	 * Initializer for a ExtractOperatorNode
	 *
	 * @param field		The field to extract
	 * @param operand	The operand
	 */
	public void init(Object field, Object operand) {
		extractField = ((Integer) field).intValue();
		super.init( operand,
					"EXTRACT "+fieldName[extractField],
					fieldMethod[extractField] );
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
		FromList		fromList, 
		SubqueryList	subqueryList,
		Vector	aggregateVector)
			throws StandardException 
	{
		int	operandType;
		TypeId opTypeId;

		super.bindExpression(fromList, subqueryList,
				aggregateVector);

		opTypeId = operand.getTypeId();
		operandType = opTypeId.getJDBCTypeId();
		TypeCompiler tc = operand.getTypeCompiler();

		/*
		** Cast the operand, if necessary, - this function is allowed only on
		** date/time types.  By default, we cast to DATE if extracting
		** YEAR, MONTH or DAY and to TIME if extracting HOUR, MINUTE or
		** SECOND.
		*/
		if (opTypeId.isStringTypeId())
		{
			int castType = (extractField < 3) ? Types.DATE : Types.TIME;
			operand =  (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					operand, 
					DataTypeDescriptor.getBuiltInDataTypeDescriptor(castType, true, 
										tc.getCastToCharWidth(
												operand.getTypeServices())),
					getContextManager());
			((CastNode) operand).bindCastNodeOnly();

			opTypeId = operand.getTypeId();
			operandType = opTypeId.getJDBCTypeId();
		}

		if ( ! ( ( operandType == Types.DATE )
			   || ( operandType == Types.TIME ) 
			   || ( operandType == Types.TIMESTAMP ) 
			)	) {
			throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"EXTRACT "+fieldName[extractField],
						opTypeId.getSQLTypeName());
		}

		/*
			If the type is DATE, ensure the field is okay.
		 */
		if ( (operandType == Types.DATE) 
			 && (extractField > DateTimeDataValue.DAY_FIELD) ) {
			throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"EXTRACT "+fieldName[extractField],
						opTypeId.getSQLTypeName());
		}

		/*
			If the type is TIME, ensure the field is okay.
		 */
		if ( (operandType == Types.TIME) 
			 && (extractField < DateTimeDataValue.HOUR_FIELD) ) {
			throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"EXTRACT "+fieldName[extractField],
						opTypeId.getSQLTypeName());
		}

		/*
		** The result type of extract is int,
		** unless it is TIMESTAMP and SECOND, in which case
		** for now it is DOUBLE but eventually it will need to
		** be DECIMAL(11,9).
		*/
		if ( (operandType == Types.TIMESTAMP)
			 && (extractField == DateTimeDataValue.SECOND_FIELD) ) {
			setType(new DataTypeDescriptor(
							TypeId.getBuiltInTypeId(Types.DOUBLE),
							operand.getTypeServices().isNullable()
						)
				);
		} else {
			setType(new DataTypeDescriptor(
							TypeId.INTEGER_ID,
							operand.getTypeServices().isNullable()
						)
				);
		}

		return this;
	}

	public String toString() {
		if (SanityManager.DEBUG)
		{
			return super.toString() + "field is "+fieldName[extractField]+"\n";
		}
		else
		{
			return "";
		}
	}
}
