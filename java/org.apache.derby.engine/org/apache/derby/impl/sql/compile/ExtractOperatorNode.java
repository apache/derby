/*

   Derby - Class org.apache.derby.impl.sql.compile.ExtractOperatorNode

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

import java.sql.Types;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a unary extract operator, used to extract
 * a field from a date/time. The field value is returned as an integer.
 *
 */
class ExtractOperatorNode extends UnaryOperatorNode {

static private final String fieldName[] = {
		"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"
	};
	static private final String fieldMethod[] = {
		"getYear","getMonth","getDate","getHours","getMinutes","getSeconds"
	};

	private int extractField;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ExtractOperatorNode(int field, ValueNode operand, ContextManager cm)
            throws StandardException {
        super(operand,
                "EXTRACT " + fieldName[field],
                fieldMethod[field],
                cm);
        this.extractField = field;
    }

	/**
	 * Bind this operator
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression(
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException 
	{
		int	operandType;
		TypeId opTypeId;

        bindOperand(fromList, subqueryList, aggregates);

		opTypeId = operand.getTypeId();
		operandType = opTypeId.getJDBCTypeId();

		/*
		** Cast the operand, if necessary, - this function is allowed only on
		** date/time types.  By default, we cast to DATE if extracting
		** YEAR, MONTH or DAY and to TIME if extracting HOUR, MINUTE or
		** SECOND.
		*/
		if (opTypeId.isStringTypeId())
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-5017
            TypeCompiler tc = operand.getTypeCompiler();
			int castType = (extractField < 3) ? Types.DATE : Types.TIME;
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
            operand = new CastNode(
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

    @Override
	public String toString() {
		if (SanityManager.DEBUG)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-4087
			return "fieldName: " + fieldName[extractField] + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}
}
