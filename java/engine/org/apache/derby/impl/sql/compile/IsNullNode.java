/*

   Derby - Class org.apache.derby.impl.sql.compile.IsNullNode

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.TypeId;

/**
 * This node represents either a unary 
 * IS NULL or IS NOT NULL comparison operator
 *
 */

public final class IsNullNode extends UnaryComparisonOperatorNode
						implements RelationalOperator
{
	private DataValueDescriptor nullValue;

    /**
     * If {@code true}, this node represents a NOT NULL node rather than a
     * NULL node. Note that this state is mutable, cf {@link #getNegation}.
     */
    private boolean notNull;

    IsNullNode(ValueNode operand, boolean notNull, ContextManager cm)
            throws StandardException {
        super(operand, cm);
        this.notNull = notNull;
        updateOperatorDetails();
    }

    private void updateOperatorDetails()
	{
        setOperator(notNull ? "is not null" : "is null");
        setMethodName(notNull ? "isNotNull" : "isNullOp");
	}

	/**
	 * Negate the comparison.
	 *
	 * @param operand	The operand of the operator
	 *
	 * @return UnaryOperatorNode	The negated expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	UnaryOperatorNode getNegation(ValueNode operand)
				throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(getTypeServices() != null,
						"dataTypeServices is expected to be non-null");
		}

        notNull = !notNull;
        updateOperatorDetails();
		return this;
	}

	/**
	 * Bind a ? parameter operand of the IS [NOT] NULL predicate.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
	void bindParameter()
			throws StandardException
	{
		/*
		** If IS [NOT] NULL has a ? operand, we assume
		** its type is varchar with the implementation-defined maximum length
		** for a varchar.
		** Also, for IS [NOT] NULL, it doesn't matter what is VARCHAR's 
		** collation (since for NULL check, no collation sensitive processing
		** is required) and hence we will not worry about the collation setting
		*/

		operand.setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
	}

	/* RelationalOperator interface */

	/** @see RelationalOperator#usefulStartKey */
	public boolean usefulStartKey(Optimizable optTable)
	{
		// IS NULL is start/stop key, IS NOT NULL is not
		return (isNullNode());
	}

	/** @see RelationalOperator#usefulStopKey */
	public boolean usefulStopKey(Optimizable optTable)
	{
		// IS NULL is start/stop key, IS NOT NULL is not
		return (isNullNode());
	}

	/** @see RelationalOperator#getStartOperator */
    @Override
	public int getStartOperator(Optimizable optTable)
	{
        if (SanityManager.DEBUG) {
            if (notNull) {
                SanityManager.THROWASSERT("NOT NULL not expected here");
			}
		}

		return ScanController.GE;
	}

	/** @see RelationalOperator#getStopOperator */
    @Override
	public int getStopOperator(Optimizable optTable)
	{
        if (SanityManager.DEBUG) {
            if (notNull) {
                SanityManager.THROWASSERT("NOT NULL not expected here");
			}
		}

		return ScanController.GT;
	}

	/** @see RelationalOperator#generateOperator */
	public void generateOperator(MethodBuilder mb,
										Optimizable optTable)
	{
		mb.push(Orderable.ORDER_OP_EQUALS);
	}

	/** @see RelationalOperator#generateNegate */
	public void generateNegate(MethodBuilder mb,
										Optimizable optTable)
	{
        mb.push(notNull);
	}

	/** @see RelationalOperator#getOperator */
	public int getOperator()
	{
        return notNull ? IS_NOT_NULL_RELOP : IS_NULL_RELOP;
	}

	/** @see RelationalOperator#compareWithKnownConstant */
	public boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters)
	{
		return true;
	}

	/**
	 * @see RelationalOperator#getCompareValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getCompareValue(Optimizable optTable)
					throws StandardException
	{
		if (nullValue == null)
		{
			nullValue = operand.getTypeServices().getNull();
		}

		return nullValue;
	}

	/** @see RelationalOperator#equalsComparisonWithConstantExpression */
	public boolean equalsComparisonWithConstantExpression(Optimizable optTable)
	{
		// Always return false for NOT NULL
        if (notNull)
		{
			return false;
		}

        boolean retval = false;

		/*
		** Is the operand a column in the given table?
		*/
		if (operand instanceof ColumnReference)
		{
			int tabNum = ((ColumnReference) operand).getTableNumber();
			if (optTable.hasTableNumber() &&
				(optTable.getTableNumber() == tabNum))
			{
				retval = true;
			}
		}

		return retval;
	}

	/** 
	 * @see RelationalOperator#getTransitiveSearchClause 
	 *
	 * @exception StandardException	thrown on error
	 */
	public RelationalOperator getTransitiveSearchClause(ColumnReference otherCR)
		throws StandardException
	{
        return new IsNullNode(otherCR, notNull, getContextManager());
	}

	/**
	 * null operators are defined on DataValueDescriptor.
	 * Overrides method in UnaryOperatorNode for code generation purposes.
	 */
    @Override
    String getReceiverInterfaceName() {
	    return ClassName.DataValueDescriptor;
	}

    @Override
	public double selectivity(Optimizable optTable) 
	{
        if (notNull) {
            /* IS NOT NULL is like <>, so should have same selectivity */
			return 0.9d;
        } else {
            /** IS NULL is like =, so should have the same selectivity */
            return 0.1d;
        }
	}

    boolean isNullNode()
	{
        return !notNull;
	}

	/** @see ValueNode#isRelationalOperator */
    @Override
    boolean isRelationalOperator()
	{
		return true;
	}

	/** @see ValueNode#optimizableEqualityNode */
    @Override
    boolean optimizableEqualityNode(Optimizable optTable,
										   int columnNumber, 
										   boolean isNullOkay)
	{
		if (!isNullNode() || !isNullOkay)
			return false;
		
		ColumnReference cr = getColumnOperand(optTable,
											  columnNumber);
		if (cr == null)
		{
			return false;
		}

		return true;
	}

}
