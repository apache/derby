/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.sql.Types;

import java.util.Vector;

/**
 * The TimestampOperatorNode class implements the timestamp( date, time) function.
 */

public class TimestampOperatorNode extends BinaryOperatorNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2004;

    /**
     * Initailizer for a TimestampOperatorNode.
     *
     * @param date The date
     * @param time The time
     */

    public void init( Object date,
                      Object time)
    {
        leftOperand = (ValueNode) date;
        rightOperand = (ValueNode) time;
        operator = "timestamp";
        methodName = "getTimestamp";
    }


	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
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
		leftOperand = leftOperand.bindExpression(fromList, subqueryList, 
			aggregateVector);
		rightOperand = rightOperand.bindExpression(fromList, subqueryList, 
			aggregateVector);

        TypeId leftTypeId = leftOperand.getTypeId();
        TypeId rightTypeId = rightOperand.getTypeId();
        if( !(leftTypeId.isStringTypeId() || leftTypeId.getJDBCTypeId() == Types.DATE || leftOperand.isParameterNode()))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        if( !(rightTypeId.isStringTypeId() || rightTypeId.getJDBCTypeId() == Types.TIME || rightOperand.isParameterNode()))
            throw StandardException.newException(SQLState.LANG_BINARY_OPERATOR_NOT_SUPPORTED, 
                                                 operator, leftTypeId.getSQLTypeName(), rightTypeId.getSQLTypeName());
        setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor( Types.TIMESTAMP));
		return genSQLJavaSQLTree();
	} // end of bindExpression

    /**
	 * Do code generation for this binary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
        acb.pushDataValueFactory(mb);
		leftOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
		rightOperand.generateExpression(acb, mb);
        mb.cast( ClassName.DataValueDescriptor);
        mb.callMethod( VMOpcode.INVOKEINTERFACE, null, methodName, ClassName.DateTimeDataValue, 2);
    } // end of generateExpression
}
