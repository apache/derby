/*

   Derby - Class org.apache.derby.impl.sql.compile.UnaryArithmeticOperatorNode

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
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents a unary arithmetic operator
 *
 */

class UnaryArithmeticOperatorNode extends UnaryOperatorNode
{
	private final static String[] UNARY_OPERATORS = {"+","-","SQRT", "ABS/ABSVAL"};
	private final static String[] UNARY_METHODS = {"plus","minus","sqrt", "absolute"};

    // Allowed kinds
    final static int K_PLUS = 0;
    final static int K_MINUS = 1;
    final static int K_SQRT = 2;
    final static int K_ABS = 3;
    
    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

    /**
     * @param operand The operand of the node
     * @param kind unary operator identity
     * @param cm context manager
     * @throws StandardException
     */
    UnaryArithmeticOperatorNode(
            ValueNode operand,
            int kind,
            ContextManager cm) throws StandardException {
        super(operand,
              UNARY_OPERATORS[kind],
              UNARY_METHODS[kind],
              cm);
        this.kind = kind;
    }

    /**
     * Unary + and - require their type to be set if
     * they wrap another node (e.g. a parameter) that
     * requires type from its context.
     * @see ValueNode#requiresTypeFromContext
     */
    @Override
    public boolean requiresTypeFromContext()
    {
        if (kind == K_PLUS ||
            kind == K_MINUS) {
            return operand.requiresTypeFromContext(); 
        }
        return false;
    }
    
    /**
     * A +? or a -? is considered a parameter.
     */
    @Override
    public boolean isParameterNode()
    {
        if (kind == K_PLUS ||
            kind == K_MINUS) {
            return operand.isParameterNode(); 
        }
        return false;
    }

	/**
     * For SQRT and ABS the parameter becomes a DOUBLE.
     * For unary + and - no change is made to the
     * underlying node. Once this node's type is set
     * using setType, then the underlying node will have
     * its type set.
	 *
	 * @exception StandardException		Thrown if ?  parameter doesn't
	 *									have a type bound to it yet.
	 *									? parameter where it isn't allowed.
	 */
    @Override
	void bindParameter() throws StandardException
	{
       if (kind == K_SQRT ||
            kind == K_ABS)
		{
			operand.setType(
				new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), true));
            return;
		}
        
		//Derby-582 add support for dynamic parameter for unary plus and minus
       if (kind == K_MINUS ||
            kind == K_PLUS)
			return;
        
        // Not expected to get here since only the above types are supported
        // but the super-class method will throw an exception
        super.bindParameter();
        
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
        FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
			throws StandardException
	{
		//Return with no binding, if the type of unary minus/plus parameter is not set yet.
       if (operand.requiresTypeFromContext() &&
                ((kind == K_PLUS ||
                  kind == K_MINUS))
				&& operand.getTypeServices() == null)
				return this;

        bindOperand(fromList, subqueryList, aggregates);

       if (kind == K_SQRT ||
            kind == K_ABS)
		{
			bindSQRTABS();
		}
       else if (kind == K_PLUS ||
                 kind == K_MINUS)
		{
            checkOperandIsNumeric(operand.getTypeId());
		}
		/*
		** The result type of a +, -, SQRT, ABS is the same as its operand.
		*/
		super.setType(operand.getTypeServices());
		return this;
	}
    
    /**
     * Only called for Unary +/-.
     *
     */
	private void checkOperandIsNumeric(TypeId operandType) throws StandardException
	{
	    if (!operandType.isNumericTypeId())
	    {
	        throw StandardException.newException(
                    SQLState.LANG_UNARY_ARITHMETIC_BAD_TYPE, 
                   (kind == K_PLUS) ? "+" : "-",
	                        operandType.getSQLTypeName());
	    }
	    
	}

	/**
	 * Do code generation for this unary plus operator
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		/* Unary + doesn't do anything.  Just return the operand */
       if (kind == K_PLUS)
			operand.generateExpression(acb, mb);
		else
			super.generateExpression(acb, mb);
	}
	/**
	 * Bind SQRT or ABS
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void bindSQRTABS()
			throws StandardException
	{
		TypeId	operandType;
		int 	jdbcType;

		/*
		** Check the type of the operand 
		*/
		operandType = operand.getTypeId();

		/*
	 	 * If the operand is not a build-in type, generate a bound conversion
		 * tree to build-in types.
		 */
		if (operandType.userType() )
		{
			operand = operand.genSQLJavaSQLTree();
		}
		/* DB2 doesn't cast string types to numeric types for numeric functions  */

		jdbcType = operandType.getJDBCTypeId();

		/* Both SQRT and ABS are only allowed on numeric types */
		if (!operandType.isNumericTypeId())
			throw StandardException.newException(
						SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						getOperatorString(), operandType.getSQLTypeName());

		/* For SQRT, if operand is not a DOUBLE, convert it to DOUBLE */
       if (kind == K_SQRT &&
            jdbcType != Types.DOUBLE)
		{
            operand = new CastNode(
					operand,
					new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.DOUBLE), true),
					getContextManager());
			((CastNode) operand).bindCastNodeOnly();
		}
	}

	/** We are overwriting this method here because for -?/+?, we now know
	the type of these dynamic parameters and hence we can do the parameter
	binding. The setType method will call the binding code after setting
	the type of the parameter*/
    @Override
    void setType(DataTypeDescriptor descriptor) throws StandardException
	{
        if (operand.requiresTypeFromContext() && operand.getTypeServices() == null)
        {
            checkOperandIsNumeric(descriptor.getTypeId());
		    operand.setType(descriptor);
        }
		super.setType(descriptor);
	}

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) &&
                ((UnaryArithmeticOperatorNode)o).kind == kind;
    }
}
