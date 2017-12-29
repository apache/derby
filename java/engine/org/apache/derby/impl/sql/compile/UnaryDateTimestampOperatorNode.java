/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryDateTimestampOperatorNode

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
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DateTimeDataValue;

/**
 * This class implements the timestamp( x) and date(x) functions.
 *
 * These two functions implement a few special cases of string conversions beyond the normal string to
 * date/timestamp casts.
 */
class UnaryDateTimestampOperatorNode extends UnaryOperatorNode
{
    private static final String TIMESTAMP_METHOD_NAME = "getTimestamp";
    private static final String DATE_METHOD_NAME = "getDate";
    
    // Allowed kinds
    final static int K_DATE = 0;
    final static int K_TIMESTAMP = 1;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

    /**
     * @param operand The operand of the function
     * @param kind The kind of the result, Date or Timestamp.
     * @param cm context manager
     * @throws StandardException
     */
    UnaryDateTimestampOperatorNode(
            ValueNode operand,
            int kind,
            ContextManager cm) throws StandardException {
        super(operand,
                kind == K_DATE ? "date" : "timestamp",
                kind == K_DATE ? DATE_METHOD_NAME : TIMESTAMP_METHOD_NAME,
                cm);
        this.kind = kind;
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(
                kind == K_DATE ? Types.DATE : Types.TIMESTAMP));
    }

    /**
     * Called by UnaryOperatorNode.bindExpression.
     *
     * If the operand is a constant then evaluate the function at compile time. Otherwise,
     * if the operand input type is the same as the output type then discard this node altogether.
     * If the function is "date" and the input is a timestamp then change this node to a cast.
     *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression (
                    FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
				throws StandardException
	{
        boolean isIdentity = false; // Is this function the identity operator?
        boolean operandIsNumber = false;
        
        bindOperand( fromList, subqueryList, aggregates);
        DataTypeDescriptor operandType = operand.getTypeServices();
        switch( operandType.getJDBCTypeId())
        {
        case Types.BIGINT:
        case Types.INTEGER:
        case Types.SMALLINT:
        case Types.TINYINT:
        case Types.DECIMAL:
        case Types.NUMERIC:
        case Types.DOUBLE:
        case Types.FLOAT:
            if( TIMESTAMP_METHOD_NAME.equals( methodName))
                invalidOperandType();
            operandIsNumber = true;
            break;
            
        case Types.CHAR:
        case Types.VARCHAR:
            break;

        case Types.DATE:
            if( TIMESTAMP_METHOD_NAME.equals( methodName))
                invalidOperandType();
            isIdentity = true;
            break;
            
        case Types.NULL:
            break;
           
        case Types.TIMESTAMP:
            if( TIMESTAMP_METHOD_NAME.equals( methodName))
                isIdentity = true;
            break;

        default:
            invalidOperandType();
        }
       
        if( operand instanceof ConstantNode)
        {
            DataValueFactory dvf = getLanguageConnectionContext().getDataValueFactory();
            DataValueDescriptor sourceValue = ((ConstantNode) operand).getValue();
            DataValueDescriptor destValue;
            if( sourceValue.isNull())
            {
                destValue = (TIMESTAMP_METHOD_NAME.equals( methodName))
                ? dvf.getNullTimestamp( (DateTimeDataValue) null)
                : dvf.getNullDate( (DateTimeDataValue) null);
            }
            else
            {
                destValue = (TIMESTAMP_METHOD_NAME.equals( methodName))
                  ? dvf.getTimestamp( sourceValue) : dvf.getDate( sourceValue);
            }

            return new UserTypeConstantNode(destValue, getContextManager());
        }

        if( isIdentity)
            return operand;
        return this;
    } // end of bindUnaryOperator

    private void invalidOperandType() throws StandardException
    {
        throw StandardException.newException( SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                                              getOperatorString(), getOperand().getTypeServices().getSQLstring());
    }

	/**
	 * Do code generation for this unary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression( ExpressionClassBuilder acb, MethodBuilder mb)
        throws StandardException
	{
        acb.pushDataValueFactory( mb);
        operand.generateExpression( acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
        mb.callMethod( VMOpcode.INVOKEINTERFACE, (String) null, methodName, getTypeCompiler().interfaceName(), 1);
    } // end of generateExpression

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) &&
                ((UnaryDateTimestampOperatorNode)o).kind == kind;
    }
}
