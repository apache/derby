/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryOperatorNode

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

import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import java.lang.reflect.Modifier;
import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Vector;

/**
 * A BinaryOperatorNode represents a built-in binary operator as defined by
 * the ANSI/ISO SQL standard.  This covers operators like +, -, *, /, =, <, etc.
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 * @author Jeff Lichtman
 */

public class BinaryOperatorNode extends ValueNode
{
	String	operator;
	String	methodName;
	ValueNode	receiver; // used in generation

	/*
	** These identifiers are used in the grammar.
	*/
	public final static int PLUS	= 1;
	public final static int MINUS	= 2;
	public final static int TIMES	= 3;
	public final static int DIVIDE	= 4;
	public final static int CONCATENATE	= 5;
	public final static int EQ	= 6;
	public final static int NE	= 7;
	public final static int GT	= 8;
	public final static int GE	= 9;
	public final static int LT	= 10;
	public final static int LE	= 11;
	public final static int AND	= 12;
	public final static int OR	= 13;
	public final static int LIKE	= 14;

	ValueNode	leftOperand;
	ValueNode	rightOperand;

	String		leftInterfaceType;
	String		rightInterfaceType;

	/**
	 * Initializer for a BinaryOperatorNode
	 *
	 * @param leftOperand	The left operand of the node
	 * @param rightOperand	The right operand of the node
	 * @param operator		The name of the operator
	 * @param methodName	The name of the method to call for this operator
	 * @param leftInterfaceType	The name of the interface for the left operand
	 * @param rightInterfaceType	The name of the interface for the right
	 *								operand
	 */

	public void init(
			Object leftOperand,
			Object rightOperand,
			Object operator,
			Object methodName,
			Object leftInterfaceType,
			Object rightInterfaceType)
	{
		this.leftOperand = (ValueNode) leftOperand;
		this.rightOperand = (ValueNode) rightOperand;
		this.operator = (String) operator;
		this.methodName = (String) methodName;
		this.leftInterfaceType = (String) leftInterfaceType;
		this.rightInterfaceType = (String) rightInterfaceType;
	}

	public void init(
			Object leftOperand,
			Object rightOperand,
			Object leftInterfaceType,
			Object rightInterfaceType)
	{
		this.leftOperand = (ValueNode) leftOperand;
		this.rightOperand = (ValueNode) rightOperand;
		this.leftInterfaceType = (String) leftInterfaceType;
		this.rightInterfaceType = (String) rightInterfaceType;
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
			return "operator: " + operator + "\n" +
				"methodName: " + methodName + "\n" + 
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Set the operator.
	 *
	 * @param operator	The operator.
	 *
	 * @return Nothing.
	 */
	void setOperator(String operator)
	{
		this.operator = operator;
	}

	/**
	 * Set the methodName.
	 *
	 * @param methodName	The methodName.
	 *
	 * @return Nothing.
	 */
	void setMethodName(String methodName)
	{
		this.methodName = methodName;
	}

	/**
	 * Set the interface type for the left and right arguments.
	 * Used when we don't know the interface type until
	 * later in binding.
	 *
	 * @return void
	 */
	public void setLeftRightInterfaceType(String iType)
	{
		leftInterfaceType = iType;
		rightInterfaceType = iType;
	}

	/**
	 * Set the clause that this node appears in.
	 *
	 * @param clause	The clause that this node appears in.
	 *
	 * @return Nothing.
	 */
	public void setClause(int clause)
	{
		super.setClause(clause);
		leftOperand.setClause(clause);
		rightOperand.setClause(clause);
	}

	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 *
	 * @return	Nothing
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (leftOperand != null)
			{
				printLabel(depth, "leftOperand: ");
				leftOperand.treePrint(depth + 1);
			}

			if (rightOperand != null)
			{
				printLabel(depth, "rightOperand: ");
				rightOperand.treePrint(depth + 1);
			}
		}
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


		/* Is there a ? parameter on the left? */
		if (leftOperand.isParameterNode())
		{
			/*
			** It's an error if both operands are ? parameters.
			*/
			if (rightOperand.isParameterNode())
			{
				throw StandardException.newException(SQLState.LANG_BINARY_OPERANDS_BOTH_PARMS, 
																	operator);
			}

			/* Set the left operand to the type of right parameter. */
			((ParameterNode) leftOperand).setDescriptor(rightOperand.getTypeServices());
		}

		/* Is there a ? parameter on the right? */
		if (rightOperand.isParameterNode())
		{
			/* Set the right operand to the type of the left parameter. */
			((ParameterNode) rightOperand).setDescriptor(leftOperand.getTypeServices());
		}

		return genSQLJavaSQLTree();
	}

	/** generate a SQL->Java->SQL conversion tree above the left and right
	 * operand of this Binary Operator Node if needed. Subclasses can override
	 * the default behavior.
	 */
	public ValueNode genSQLJavaSQLTree() throws StandardException
	{
		TypeId leftTypeId = leftOperand.getTypeId();
		
		if (!(leftTypeId.systemBuiltIn()))
			leftOperand = leftOperand.genSQLJavaSQLTree();

		TypeId rightTypeId = rightOperand.getTypeId();
		if (!(rightTypeId.systemBuiltIn()))
			rightOperand = rightOperand.genSQLJavaSQLTree();

		return this;
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
		leftOperand = leftOperand.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);
		rightOperand = rightOperand.preprocess(numTables,
											   outerFromList, outerSubqueryList,
											   outerPredicateList);
		return this;
	}

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
		String		resultTypeName;
		LocalField	receiverField;
		String		receiverType;

/*
** if i have a operator.getOrderableType() == constant, then just cache 
** it in a field.  if i have QUERY_INVARIANT, then it would be good to
** cache it in something that is initialized each execution,
** but how?
*/


		/*
		** The receiver is the operand with the higher type precedence.
		** Like always makes the left the receiver.
		**
		** Allocate an object for re-use to hold the receiver.  This is because
		** the receiver is passed to the method as one of the parameters,
		** and we don't want to evaluate it twice.
		*/
		if (leftOperand.getTypeId().typePrecedence() >
			rightOperand.getTypeId().typePrecedence())
		{
			receiver = leftOperand;
			/*
			** let the receiver type be determined by an
			** overridable method so that if methods are
			** not implemented on the lowest interface of
			** a class, they can note that in the implementation
			** of the node that uses the method.
			*/
		    receiverType = getReceiverInterfaceName();

			/*
			** Generate (field = <left expression>).  This assignment is
			** used as the receiver of the method call for this operator,
			** and the field is used as the left operand:
			**
			**	(field = <left expression>).method(field, <right expression>...)
			*/
			receiverField =
				acb.newFieldDeclaration(Modifier.PRIVATE, receiverType);

			leftOperand.generateExpression(acb, mb);
			mb.putField(receiverField); // method instance
			mb.cast(receiverType); // cast the method instance
			mb.getField(receiverField); mb.cast(leftInterfaceType); // first arg with cast
			rightOperand.generateExpression(acb, mb); mb.cast(rightInterfaceType); // second arg with cast
		}
		else
		{
			receiver = rightOperand;
			/*
			** let the receiver type be determined by an
			** overridable method so that if methods are
			** not implemented on the lowest interface of
			** a class, they can note that in the implementation
			** of the node that uses the method.
			*/
		    receiverType = getReceiverInterfaceName();

			/*
			** Generate (field = <right expression>).  This assignment is
			** used as the receiver of the method call for this operator,
			** and the field is used as the right operand:
			**
			**	(field = <right expression>).method(<left expression>, field...)
			*/
			receiverField =
				acb.newFieldDeclaration(Modifier.PRIVATE, rightInterfaceType);

			rightOperand.generateExpression(acb, mb);
			mb.putField(receiverField); // method instance
			mb.cast(receiverType); // cast the method instance
			leftOperand.generateExpression(acb, mb); mb.cast(leftInterfaceType); // second arg with cast
			mb.getField(receiverField); mb.cast(rightInterfaceType); // first arg with cast
		}

		/* Figure out the result type name */
		resultTypeName = getTypeCompiler().interfaceName();

		// Boolean return types don't need a result field
		boolean needField = !getTypeId().isBooleanTypeId();

		if (needField) {

			/* Allocate an object for re-use to hold the result of the operator */
			LocalField resultField =
				acb.newFieldDeclaration(Modifier.PRIVATE, resultTypeName);

			/*
			** Call the method for this operator.
			*/
			mb.getField(resultField); // third arg
			//following method is special code for concatenation where if field is null, we want it to be initialized to NULL SQLxxx type object
			//before generating code "field = method(p1, p2, field);"
			initializeResultField(acb, mb, resultField);

			/* pass statically calculated scale to decimal divide method to make
			 * result set scale consistent, beetle 3901
			 */
			int jdbcType;
			if ((dataTypeServices != null) &&
				((jdbcType = dataTypeServices.getJDBCTypeId()) == java.sql.Types.DECIMAL ||
				 jdbcType == java.sql.Types.NUMERIC) &&
				operator.equals("/"))
			{
				mb.push(dataTypeServices.getScale());		// 4th arg
				mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultTypeName, 4);
			}
			else
				mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultTypeName, 3);

			//the need for following if was realized while fixing bug 5704 where decimal*decimal was resulting an overflow value but we were not detecting it
			if (getTypeId().variableLength())//since result type is numeric variable length, generate setWidth code.
			{
				if (getTypeId().isNumericTypeId())
				{
					mb.push(getTypeServices().getPrecision());
					mb.push(getTypeServices().getScale());
					mb.push(true);
					mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.VariableSizeDataValue, "setWidth", ClassName.DataValueDescriptor, 3);
				}
			}


			/*
			** Store the result of the method call in the field, so we can re-use
			** the object.
			*/

			mb.putField(resultField);
		} else {
			mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultTypeName, 2);
		}
	}

	//following method is no-op here but in concatenation node, this method is used to check if resultField is null,
	//and if yes, then we want it to be initialized to NULL SQLxxx type object
	protected void initializeResultField(ExpressionClassBuilder acb, MethodBuilder mb, LocalField resultField)
	{
	}

	/**
	 * Set the leftOperand to the specified ValueNode
	 *
	 * @param newLeftOperand	The new leftOperand
	 *
	 * @return None.
	 */
	public void setLeftOperand(ValueNode newLeftOperand)
	{
		leftOperand = newLeftOperand;
	}

	/**
	 * Get the leftOperand
	 *
	 * @return The current leftOperand.
	 */
	public ValueNode getLeftOperand()
	{
		return leftOperand;
	}

	/**
	 * Set the rightOperand to the specified ValueNode
	 *
	 * @param newRightOperand	The new rightOperand
	 *
	 * @return None.
	 */
	public void setRightOperand(ValueNode newRightOperand)
	{
		rightOperand = newRightOperand;
	}

	/**
	 * Get the rightOperand
	 *
	 * @return The current rightOperand.
	 */
	public ValueNode getRightOperand()
	{
		return rightOperand;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 * If the source of this ColumnReference (at the next underlying level) 
	 * is not a ColumnReference or a VirtualColumnNode then this predicate
	 * will not be pushed down.
	 *
	 * For example, in:
	 *		select * from (select 1 from s) a (x) where x = 1
	 * we will not push down x = 1.
	 * NOTE: It would be easy to handle the case of a constant, but if the
	 * inner SELECT returns an arbitrary expression, then we would have to copy
	 * that tree into the pushed predicate, and that tree could contain
	 * subqueries and method calls.
	 * RESOLVE - revisit this issue once we have views.
	 *
	 * @param referencedTabs	JBitSet with bit map of referenced FromTables
	 * @param simplePredsOnly	Whether or not to consider method
	 *							calls, field references and conditional nodes
	 *							when building bit map
	 *
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 * @exception StandardException			Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		boolean pushable;
		pushable = leftOperand.categorize(referencedTabs, simplePredsOnly);
		pushable = (rightOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
		return pushable;
	}

	/**
	 * Remap all ColumnReferences in this tree to be clones of the
	 * underlying expression.
	 *
	 * @return ValueNode			The remapped expression tree.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public ValueNode remapColumnReferencesToExpressions()
		throws StandardException
	{
		leftOperand = leftOperand.remapColumnReferencesToExpressions();
		rightOperand = rightOperand.remapColumnReferencesToExpressions();
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return (leftOperand.isConstantExpression() &&
				rightOperand.isConstantExpression());
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return (leftOperand.constantExpression(whereClause) &&
				rightOperand.constantExpression(whereClause));
	}

	/**
	 * Determine the type the binary method is called on.
	 * By default, based on the receiver.
	 *
	 * Override in nodes that use methods on super-interfaces of
	 * the receiver's interface, such as comparisons.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public String getReceiverInterfaceName() throws StandardException {
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(receiver!=null,"can't get receiver interface name until receiver is set");
		}

		return receiver.getTypeCompiler().interfaceName();
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *		CONSTANT			- immutable
	 *
	 * @return	The variant type for the underlying expression.
	 * @exception StandardException	thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		int leftType = leftOperand.getOrderableVariantType();
		int rightType = rightOperand.getOrderableVariantType();

		return Math.min(leftType, rightType);
	}

	/**
	 * Swap the left and right sides.
	 *
	 * @return Nothing.
	 */
	void swapOperands()
	{
		String	  tmpInterfaceType = leftInterfaceType;
		ValueNode tmpVN = leftOperand;

		leftOperand = rightOperand;
		rightOperand = tmpVN;
		leftInterfaceType = rightInterfaceType;
		rightInterfaceType = tmpInterfaceType;
	}

	/**
	 * Accept a visitor, and call v.visit()
	 * on child nodes as necessary.  
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
	public Visitable accept(Visitor v) 
		throws StandardException
	{
		Visitable returnNode = v.visit(this);
	
		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (leftOperand != null && !v.stopTraversal())
		{
			leftOperand = (ValueNode)leftOperand.accept(v);
		}

		if (rightOperand != null && !v.stopTraversal())
		{
			rightOperand = (ValueNode)rightOperand.accept(v);
		}
		
		return returnNode;
	}
}
