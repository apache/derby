/*

   Derby - Class org.apache.derby.impl.sql.compile.SimpleStringOperatorNode

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
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a unary upper or lower operator
 *
 */

class SimpleStringOperatorNode extends UnaryOperatorNode
{
    SimpleStringOperatorNode(
            ValueNode operand,
            String methodName,
            ContextManager cm) throws StandardException {
        super(operand, methodName, methodName, cm);
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
		TypeId	operandType;

        bindOperand(fromList, subqueryList, aggregates);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
				case Types.JAVA_OBJECT:
				case Types.OTHER:	
				{
					throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
										methodName,
										operandType.getSQLTypeName());
				}

				default:
					DataTypeDescriptor dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true, 
							  operand.getTypeCompiler().
								getCastToCharWidth(
									operand.getTypeServices()));
			
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
                    operand = new CastNode(operand, dtd, getContextManager());
					
				// DERBY-2910 - Match current schema collation for implicit cast as we do for
				// explicit casts per SQL Spec 6.12 (10)					
			    operand.setCollationUsingCompilationSchema();
			    
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
		//Result of upper()/lower() will have the same collation as the   
		//argument to upper()/lower(). 
        setCollationInfo(operand.getTypeServices());

		return this;
	}

	/**
	 * Bind a ? parameter operand of the upper/lower function.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	void bindParameter()
			throws StandardException
	{
		/*
		** According to the SQL standard, if bit_length has a ? operand,
		** its type is bit varying with the implementation-defined maximum length
		** for a bit.
		*/

//IC see: https://issues.apache.org/jira/browse/DERBY-582
		operand.setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
		//collation of ? operand should be same as the compilation schema
		operand.setCollationUsingCompilationSchema();
	}

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getReceiverInterfaceName() {
	    return ClassName.StringDataValue;
	}
}
