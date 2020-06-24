/*

   Derby - Class org.apache.derby.impl.sql.compile.BooleanConstantNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.types.SQLBoolean;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.services.context.ContextManager;

public final class BooleanConstantNode extends ConstantNode
{
	/* Cache actual value to save overhead and
	 * throws clauses.
	 */
	boolean booleanValue;
	boolean unknownValue;

    /**
     * @param cm context manager
     * @throws StandardException
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    BooleanConstantNode(ContextManager cm) throws StandardException {
		/*
		** RESOLVE: The length is fixed at 1, even for nulls.
		** Is that OK?
		*/
        // Fill in the type information in the parent ValueNode
        super(TypeId.BOOLEAN_ID, true, 1, cm);
        setValue( null );
    }

    BooleanConstantNode(boolean value, ContextManager cm)
            throws StandardException {
        super(TypeId.BOOLEAN_ID, false, 1, cm);
        super.setValue(new SQLBoolean(value));
        this.booleanValue = value;
    }

    BooleanConstantNode(TypeId t, ContextManager cm)
            throws StandardException {
        super(t, true, 0, cm);
        this.unknownValue = true;
    }

	/**
	 * Return the value from this BooleanConstantNode
	 *
	 * @return	The value of this BooleanConstantNode.
	 *
	 */

	//public boolean	getBoolean()
	//{
	//	return booleanValue;
	//}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */

	//public int	getLength() throws StandardException
	//{
	//	return value.getLength();
	//}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 */
    @Override
	Object getConstantValueAsObject()
	{
		return booleanValue ? Boolean.TRUE : Boolean.FALSE;
	}

	/**
	 * Return the value as a string.
	 *
	 * @return The value as a string.
	 *
	 */
	String getValueAsString()
	{
		if (booleanValue)
		{
			return "true";
		}
		else
		{
			return "false";
		}
	}

	/**
	 * Does this represent a true constant.
	 *
	 * @return Whether or not this node represents a true constant.
	 */
    @Override
	boolean isBooleanTrue()
	{
		return (booleanValue && !unknownValue);
	}

	/**
	 * Does this represent a false constant.
	 *
	 * @return Whether or not this node represents a false constant.
	 */
    @Override
	boolean isBooleanFalse()
	{
		return (!booleanValue && !unknownValue);
	}

	/**
	 * The default selectivity for value nodes is 50%.  This is overridden
	 * in specific cases, such as the RelationalOperators.
	 */
    @Override
	public double selectivity(Optimizable optTable)
	{
		if (isBooleanTrue())
		{
			return 1.0;
		}
		else
		{
			return 0.0;
		}
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 */
    @Override
	ValueNode eliminateNots(boolean underNotNode) 
	{
		if (! underNotNode)
		{
			return this;
		}

		booleanValue = !booleanValue;
		super.setValue(new SQLBoolean(booleanValue));
//IC see: https://issues.apache.org/jira/browse/DERBY-4062
//IC see: https://issues.apache.org/jira/browse/DERBY-4062

		return this;
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
	{
		mb.push(booleanValue);
	}

	/**
	 * Set the value in this ConstantNode.
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    void setValue(DataValueDescriptor value)
	{
		super.setValue( value);
        unknownValue = true;
        try
        {
            if( value != null && value.isNotNull().getBoolean())
            {
                booleanValue = value.getBoolean();
                unknownValue = false;
            }
        }
        catch( StandardException se){}
	} // end of setValue
}
