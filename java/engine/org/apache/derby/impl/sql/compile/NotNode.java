/*

   Derby - Class org.apache.derby.impl.sql.compile.NotNode

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.reference.ClassName;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;

/**
 * A NotNode represents a NOT operator. Preprocessing will eliminate the 
 * NotNodes which exist above comparison operators so that the optimizer
 * will see a query tree in CNF.
 *
 * @author Jerry Brenner
 */

public final class NotNode extends UnaryLogicalOperatorNode
{
	/**
	 * Initializer for a NotNode
	 *
	 * @param operand	The operand of the NOT
	 */

	public void init(Object operand)
	{
		super.init(operand, "not");
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		return operand.eliminateNots(! underNotNode);
	}

	/**
	 * Do code generation for the NOT operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		/*
		** This generates the following code:
		**
		** <boolean field> = <operand>.equals(<operand>,
		**					 					<false truth value>);
		*/

		/*
		** Generate the code for a Boolean false constant value.
		*/
		String interfaceName = getTypeCompiler().interfaceName();
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, interfaceName);
		/*
		** Generate the call to the equals method.
		** equals is only on Orderable, not any subinterfaces.
		*/

		/* Generate the code for operand */
		operand.generateExpression(acb, mb);
		mb.upCast(ClassName.DataValueDescriptor);

		mb.dup(); // arg 1 is instance

		// arg 2
		mb.push(false);
		acb.generateDataValue(mb, getTypeCompiler(), field);
		mb.upCast(ClassName.DataValueDescriptor);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "equals", interfaceName, 2);
	}
}
