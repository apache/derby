/*

   Derby - Class org.apache.derby.impl.sql.compile.InListOperatorNode

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.lang.reflect.Modifier;

/**
 * An InListOperatorNode represents an IN list.
 *
 * @author Jerry Brenner
 */

public final class InListOperatorNode extends BinaryListOperatorNode
{
	private boolean isOrdered;

	/**
	 * Initializer for a InListOperatorNode
	 *
	 * @param leftOperand		The left operand of the node
	 * @param rightOperandList	The right operand list of the node
	 */

	public void init(Object leftOperand, Object rightOperandList)
	{
		init(leftOperand, rightOperandList, "IN", "in");
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return "isOrdered: " + isOrdered + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Preprocess an expression tree.  We do a number of transformations
	 * here (including subqueries, IN lists, LIKE and BETWEEN) plus
	 * subquery flattening.
	 * NOTE: This is done before the outer ResultSetNode is preprocessed.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode preprocess(int numTables,
								FromList outerFromList,
								SubqueryList outerSubqueryList,
								PredicateList outerPredicateList) 
					throws StandardException
	{
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList,
						 outerPredicateList);

		/* Check for the degenerate case of a single element in the IN list.
		 * If found, then convert to "=".
		 */
		if (rightOperandList.size() == 1)
		{
			BinaryComparisonOperatorNode equal = 
				(BinaryComparisonOperatorNode) getNodeFactory().getNode(
						C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
						leftOperand, 
						(ValueNode) rightOperandList.elementAt(0),
						getContextManager());
			/* Set type info for the operator node */
			equal.bindComparisonOperator();
			return equal;
		}
		else if ((leftOperand instanceof ColumnReference) &&
				 rightOperandList.containsAllConstantNodes())
		{
			/* When sorting or choosing min/max in the list, if types are not an exact
			 * match, we use the left operand's type as the "judge", assuming that they
			 * are compatible, as also the case with DB2.
			 */
			TypeId judgeTypeId = leftOperand.getTypeServices().getTypeId();
			DataValueDescriptor judgeODV = null;  //no judge, no argument
			if (! rightOperandList.allSamePrecendence(judgeTypeId.typePrecedence()))
				judgeODV = (DataValueDescriptor) judgeTypeId.getNull();

			//Sort the list in ascending order
			rightOperandList.sortInAscendingOrder(judgeODV);
			isOrdered = true;

			/* If the leftOperand is a ColumnReference
			 * and the IN list is all constants, then we generate
			 * an additional BETWEEN clause of the form:
			 *	CRClone BETWEEN minValue and maxValue
			 */
			ValueNode leftClone = leftOperand.getClone();
			ValueNode minValue = (ValueNode) rightOperandList.elementAt(0);  //already sorted
			ValueNode maxValue = (ValueNode) rightOperandList.elementAt(rightOperandList.size() - 1);

			/* Handle the degenerate case where 
			 * the min and the max are the same value.
			 */
			DataValueDescriptor minODV =
				 ((ConstantNode) minValue).getValue();
			DataValueDescriptor maxODV =
				 ((ConstantNode) maxValue).getValue();
			if ((judgeODV == null && minODV.compare(maxODV) == 0) ||
				(judgeODV != null && judgeODV.equals(minODV, maxODV).equals(true)))
			{
				BinaryComparisonOperatorNode equal = 
					(BinaryComparisonOperatorNode) getNodeFactory().getNode(
						C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
						leftOperand, 
						minValue,
						getContextManager());
				/* Set type info for the operator node */
				equal.bindComparisonOperator();
				return equal;
			}

			// Build the Between
			ValueNodeList vnl = (ValueNodeList) getNodeFactory().getNode(
													C_NodeTypes.VALUE_NODE_LIST,
													getContextManager());
			vnl.addValueNode(minValue);
			vnl.addValueNode(maxValue);

			BetweenOperatorNode bon = 
				(BetweenOperatorNode) getNodeFactory().getNode(
									C_NodeTypes.BETWEEN_OPERATOR_NODE,
									leftClone,
									vnl,
									getContextManager());

			/* The transformed tree has to be normalized:
			 *				AND
			 *			   /   \
			 *		IN LIST    AND
			 *				   /   \
			 *				  >=	AND
			 *						/   \
			 *					   <=	TRUE
			 */

			/* Create the AND */
			AndNode newAnd;

			newAnd = (AndNode) getNodeFactory().getNode(
									C_NodeTypes.AND_NODE,
									this,
									bon.preprocess(numTables,
												   outerFromList,
												   outerSubqueryList,
												   outerPredicateList),
									getContextManager());
			newAnd.postBindFixup();

			/* Mark this node as transformed so that we don't get
			 * calculated into the selectivity mulitple times.
			 */
			setTransformed();

			// Return new AndNode
			return newAnd;
		}
		else
		{
			return this;
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
	 * @exception StandardException		Thrown on error
	 */
	ValueNode eliminateNots(boolean underNotNode) 
					throws StandardException
	{
		AndNode						 newAnd = null;
		BinaryComparisonOperatorNode leftBCO;
		BinaryComparisonOperatorNode rightBCO;
		int							 listSize = rightOperandList.size();
		ValueNode					 leftSide;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(listSize > 0,
			"rightOperandList.size() is expected to be > 0");

		if (! underNotNode)
		{
			return this;
		}

		/* we want to convert the IN List into = OR = ... as
		 * described below.  
		 */

		/* Convert:
		 *		leftO IN rightOList.elementAt(0) , rightOList.elementAt(1) ...
		 * to:
		 *		leftO <> rightOList.elementAt(0) AND leftO <> rightOList.elementAt(1) ...
		 * NOTE - We do the conversion here since the single table clauses
		 * can be pushed down and the optimizer may eventually have a filter factor
		 * for <>.
		 */

		/* leftO <> rightOList.at(0) */
		/* If leftOperand is a ColumnReference, it may be remapped during optimization, and that
		 * requires each <> node to have a separate object.
		 */
		ValueNode leftClone = (leftOperand instanceof ColumnReference) ? leftOperand.getClone() : leftOperand;
		leftBCO = (BinaryComparisonOperatorNode) 
					getNodeFactory().getNode(
						C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
						leftClone,
						(ValueNode) rightOperandList.elementAt(0),
						getContextManager());
		/* Set type info for the operator node */
		leftBCO.bindComparisonOperator();

		leftSide = leftBCO;

		for (int elemsDone = 1; elemsDone < listSize; elemsDone++)
		{

			/* leftO <> rightOList.elementAt(elemsDone) */
			leftClone = (leftOperand instanceof ColumnReference) ? leftOperand.getClone() : leftOperand;
			rightBCO = (BinaryComparisonOperatorNode) 
						getNodeFactory().getNode(
							C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
							leftClone,
							(ValueNode) rightOperandList.elementAt(elemsDone),
							getContextManager());
			/* Set type info for the operator node */
			rightBCO.bindComparisonOperator();

			/* Create the AND */
			newAnd = (AndNode) getNodeFactory().getNode(
												C_NodeTypes.AND_NODE,
												leftSide,
												rightBCO,
												getContextManager());
			newAnd.postBindFixup();

			leftSide = newAnd;
		}

		return leftSide;
	}

	/**
	 * See if this IN list operator is referencing the same table.
	 *
	 * @param cr	The column reference.
	 *
	 * @return	true if in list references the same table as in cr.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean selfReference(ColumnReference cr)
		throws StandardException
	{
		int size = rightOperandList.size();
		for (int i = 0; i < size; i++)
		{
			ValueNode vn = (ValueNode) rightOperandList.elementAt(i);
			if (vn.getTablesReferenced().get(cr.getTableNumber()))
				return true;
		}
		return false;
	}

	/**
	 * The selectivity for an "IN" predicate is generally very small.
	 * This is an estimate applicable when in list are not all constants.
	 */
	public double selectivity(Optimizable optTable)
	{
		return 0.3d;
	}
 
	/**
	 * Do code generation for this IN list operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb The MethodBuilder the expression will go into
	 *
	 * @return	An Expression to evaluate this operator
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		int			listSize = rightOperandList.size();
		String		resultTypeName;
		String		receiverType = ClassName.DataValueDescriptor;
	
		String		leftInterfaceType = ClassName.DataValueDescriptor;
		String		rightInterfaceType = ClassName.DataValueDescriptor + "[]";

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(listSize > 0,
				"listSize is expected to be > 0");
		}

		/*
		** There are 2 parts to the code generation for an IN list -
		** the code in the constructor and the code for the expression evaluation.
		** The code that gets generated for the constructor is:
		**		DataValueDescriptor[] field = new DataValueDescriptor[size];
		**	For each element in the IN list that is a constant, we also generate:
		**		field[i] = rightOperandList[i];
		**	
		** If the IN list is composed entirely of constants, then we generate the
		** the following:
		**		leftOperand.in(rightOperandList, leftOperand, isNullable(), ordered, result);
		**
		** Otherwise, we create a new method.  This method contains the 
		** assignment of the non-constant elements into the array and the call to the in()
		** method, which is in the new method's return statement.  We then return a call
		** to the new method.
		*/

		receiver = leftOperand;

		/* Figure out the result type name */
		resultTypeName = getTypeCompiler().interfaceName();

		// Generate the code to build the array
		LocalField arrayField =
			acb.newFieldDeclaration(Modifier.PRIVATE, rightInterfaceType);

		/* The array gets created in the constructor.
		 * All constant elements in the array are initialized
		 * in the constructor.  All non-constant elements, if any,
		 * are initialized each time the IN list is evaluated.
		 */
		/* Assign the initializer to the DataValueDescriptor[] field */
		MethodBuilder cb = acb.getConstructor();
		cb.pushNewArray(ClassName.DataValueDescriptor, listSize);
		cb.putField(arrayField);
		cb.endStatement();

		/* Set the array elements that are constant */
		int numConstants = 0;
		MethodBuilder nonConstantMethod = null;
		MethodBuilder currentConstMethod = cb;
		for (int index = 0; index < listSize; index++)
		{
			MethodBuilder setArrayMethod;
	
			if (rightOperandList.elementAt(index) instanceof ConstantNode)
			{
				numConstants++;
		
				/*if too many statements are added  to a  method, 
				*size of method can hit  65k limit, which will
				*lead to the class format errors at load time.
				*To avoid this problem, when number of statements added 
				*to a method is > 2048, remaing statements are added to  a new function
				*and called from the function which created the function.
				*See Beetle 5135 or 4293 for further details on this type of problem.
				*/
				if(currentConstMethod.statementNumHitLimit(1))
				{
					MethodBuilder genConstantMethod = acb.newGeneratedFun("void", Modifier.PRIVATE);
					currentConstMethod.pushThis();
					currentConstMethod.callMethod(VMOpcode.INVOKEVIRTUAL,
												  (String) null, 
												  genConstantMethod.getName(),
												  "void", 0);
					//if it is a generate function, close the metod.
					if(currentConstMethod != cb){
						currentConstMethod.methodReturn();
						currentConstMethod.complete();
					}
					currentConstMethod = genConstantMethod;
				}
				setArrayMethod = currentConstMethod;
			} else {
				if (nonConstantMethod == null)
					nonConstantMethod = acb.newGeneratedFun("void", Modifier.PROTECTED);
				setArrayMethod = nonConstantMethod;

			}

			setArrayMethod.getField(arrayField); // first arg
			((ValueNode) rightOperandList.elementAt(index)).generateExpression(acb, setArrayMethod);
			setArrayMethod.upCast(receiverType); // second arg
			setArrayMethod.setArrayElement(index);
		}

		//if a generated function was created to reduce the size of the methods close the functions.
		if(currentConstMethod != cb){
			currentConstMethod.methodReturn();
			currentConstMethod.complete();
		}

		if (nonConstantMethod != null) {
			nonConstantMethod.methodReturn();
			nonConstantMethod.complete();
			mb.pushThis();
			mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, nonConstantMethod.getName(), "void", 0);
		}

		/*
		** Call the method for this operator.
		*/
		/*
		** Generate (field = <left expression>).  This assignment is
		** used as the receiver of the method call for this operator,
		** and the field is used as the left operand:
		**
		**	(field = <left expression>).method(field, <right expression>...)
		*/

		//LocalField receiverField =
		//	acb.newFieldDeclaration(Modifier.PRIVATE, receiverType);

		leftOperand.generateExpression(acb, mb);
		mb.dup();
		//mb.putField(receiverField); // instance for method call
		/*mb.getField(receiverField);*/ mb.upCast(leftInterfaceType); // first arg
		mb.getField(arrayField); // second arg
		mb.push(isOrdered); // third arg
		mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultTypeName, 3);


	}


	/**
	 * Generate start/stop key for this IN list operator.  Bug 3858.
	 *
	 * @param isAsc		is the index ascending on the column in question
	 * @param isStartKey	are we generating start key or not
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb The MethodBuilder the expression will go into
	 *
	 * @return	nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateStartStopKey(boolean isAsc, boolean isStartKey,
									 ExpressionClassBuilder acb,
									 MethodBuilder mb)
											   throws StandardException
	{
		/* left side of the "in" operator is our "judge" when we try to get
		 * the min/max value of the operands on the right side.  Judge's type
		 * is important for us, and is input parameter to min/maxValue.
		 */
		int leftTypeFormatId = leftOperand.getTypeId().getTypeFormatId();
		int leftJDBCTypeId = leftOperand.getTypeId().isUserDefinedTypeId() ?
								leftOperand.getTypeId().getJDBCTypeId() : -1;

		int listSize = rightOperandList.size();
		int numLoops, numValInLastLoop, currentOpnd = 0;

		/* We first calculate how many times (loops) we generate a call to
		 * min/maxValue function accumulatively, since each time it at most
		 * takes 4 input values.  An example of the calls generated will be:
		 * minVal(minVal(...minVal(minVal(v1,v2,v3,v4,judge), v5,v6,v7,judge),
		 *        ...), vn-1, vn, NULL, judge)
		 * Unused value parameters in the last call are filled with NULLs.
		 */
		if (listSize < 5)
		{
			numLoops = 1;
			numValInLastLoop = (listSize - 1) % 4 + 1;
		}
		else
		{
			numLoops = (listSize - 5) / 3 + 2;
			numValInLastLoop = (listSize - 5) % 3 + 1;
		}

		for (int i = 0; i < numLoops; i++)
		{
			/* generate value parameters of min/maxValue
			 */
			int numVals = (i == numLoops - 1) ? numValInLastLoop :
							  ((i == 0) ? 4 : 3);
			for (int j = 0; j < numVals; j++)
			{
				ValueNode vn = (ValueNode) rightOperandList.elementAt(currentOpnd++);
				vn.generateExpression(acb, mb);
				mb.upCast(ClassName.DataValueDescriptor);
			}

			/* since we have fixed number of input values (4), unused ones
			 * in the last loop are filled with NULLs
			 */
			int numNulls = (i < numLoops - 1) ? 0 :
							((i == 0) ? 4 - numValInLastLoop : 3 - numValInLastLoop);
			for (int j = 0; j < numNulls; j++)
				mb.pushNull(ClassName.DataValueDescriptor);

			/* have to put judge's types in the end
			 */
			mb.push(leftTypeFormatId);
			mb.push(leftJDBCTypeId);

			/* decide to get min or max value
			 */
			String methodName;
			if ((isAsc && isStartKey) || (! isAsc && ! isStartKey))
				methodName = "minValue";
			else
				methodName = "maxValue";
		
			mb.callMethod(VMOpcode.INVOKESTATIC, ClassName.BaseExpressionActivation, methodName, ClassName.DataValueDescriptor, 6);

		}
	}
}
