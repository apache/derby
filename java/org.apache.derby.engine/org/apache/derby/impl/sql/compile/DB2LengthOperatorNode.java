/*

   Derby - Class org.apache.derby.impl.sql.compile.DB2LengthOperatorNode

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

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a unary DB2 compatible length operator
 *
 */

public final class DB2LengthOperatorNode extends UnaryOperatorNode
{
    /**
     * @param operand The operand of the node
     * @param cm context manager
     * @throws StandardException
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    DB2LengthOperatorNode(ValueNode operand, ContextManager cm)
            throws StandardException {
        super(operand, "length", "getDB2Length", cm);
    }

    /**
	 * Bind this operator
	 *
	 * @param fromList			The query's FROM list
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates    The aggregate list being built as we find AggregateNodes
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
        bindOperand( fromList, subqueryList, aggregates);

        // This operator is not allowed on XML types.
        TypeId operandType = operand.getTypeId();
        if (operandType.isXMLTypeId()) {
            throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE,
                                    getOperatorString(),
                                    operandType.getSQLTypeName());
        }

        setType( new DataTypeDescriptor( TypeId.getBuiltInTypeId( Types.INTEGER),
                                         operand.getTypeServices().isNullable()));
        return this;
    }

	/**
	 * This is a length operator node.  Overrides this method
	 * in UnaryOperatorNode for code generation purposes.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String getReceiverInterfaceName() {
	    return ClassName.ConcatableDataValue;
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
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		if (operand == null)
			return;

        int constantLength = getConstantLength();
        // -1 if the length of a non-null operand depends on the data
            
		String resultTypeName = getTypeCompiler().interfaceName();

        mb.pushThis();
		operand.generateExpression(acb, mb);
        mb.upCast( ClassName.DataValueDescriptor);
        mb.push( constantLength);

        /* Allocate an object for re-use to hold the result of the operator */
        LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);
        mb.getField(field);
        mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, methodName, resultTypeName, 3);

        /*
        ** Store the result of the method call in the field, so we can re-use
        ** the object.
        */
        mb.putField(field);
    } // end of generateExpression

    private int getConstantLength( ) throws StandardException
    {
        DataTypeDescriptor typeDescriptor = operand.getTypeServices();
        
        switch( typeDescriptor.getJDBCTypeId())
        {
        case Types.BIGINT:
            return 8;
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
		case Types.BOOLEAN:
        case Types.BIT:
            return 1;
        case Types.BINARY:
        case Types.CHAR:
            return typeDescriptor.getMaximumWidth();
        case Types.DATE:
            return 4;
        case Types.DECIMAL:
        case Types.NUMERIC:
            return typeDescriptor.getPrecision()/2 + 1;
        case Types.DOUBLE:
            return 8;
        case Types.FLOAT:
        case Types.REAL:
        case Types.INTEGER:
            return 4;
        case Types.SMALLINT:
            return 2;
        case Types.TIME:
            return 3;
        case Types.TIMESTAMP:
            return 10;
        case Types.TINYINT:
            return 1;
        case Types.LONGVARCHAR:
        case Types.VARCHAR:
        case Types.LONGVARBINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            return getConstantNodeLength();
        default:
			return -1;
        }
    } // end of getConstantLength

    private int getConstantNodeLength() throws StandardException
    {
        if( operand instanceof ConstantNode)
            return ((ConstantNode) operand).getValue().getLength();
        return -1;
    }        
}
