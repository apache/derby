/*

   Derby - Class org.apache.derby.impl.sql.compile.TernaryOperatorNode

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.util.JBitSet;

import java.lang.reflect.Modifier;

import java.sql.Types;
import java.util.Vector;
/**
 * A TernaryOperatorNode represents a built-in ternary operators.
 * This covers  built-in functions like substr().
 * Java operators are not represented here: the JSQL language allows Java
 * methods to be called from expressions, but not Java operators.
 *
 * @author Jerry Brenner
 */

public class TernaryOperatorNode extends ValueNode
{
	String		operator;
	String		methodName;
	int			operatorType;
	ValueNode	receiver; 

	ValueNode	leftOperand;
	ValueNode	rightOperand;

	String		resultInterfaceType;
	String		receiverInterfaceType;
	String		leftInterfaceType;
	String		rightInterfaceType;
	int			trimType;

	public static final int TRIM = 0;
	public static final int LOCATE = 1;
	public static final int SUBSTRING = 2;
	public static final int LIKE = 3;
	static final String[] TernaryOperators = {"trim", "LOCATE", "substring", "like"};
	static final String[] TernaryMethodNames = {"trim", "locate", "substring", "like"};
	static final String[] TernaryResultType = {ClassName.StringDataValue, 
			ClassName.NumberDataValue,
			ClassName.ConcatableDataValue,
			ClassName.BooleanDataValue};
	static final String[][] TernaryArgType = {
	{ClassName.StringDataValue, ClassName.StringDataValue, "java.lang.Integer"},
	{ClassName.StringDataValue, ClassName.StringDataValue, ClassName.NumberDataValue},
	{ClassName.ConcatableDataValue, ClassName.NumberDataValue, ClassName.NumberDataValue},
	{ClassName.DataValueDescriptor, ClassName.DataValueDescriptor, ClassName.DataValueDescriptor}
	};

	/**
	 * Initializer for a TernaryOperatorNode
	 *
	 * @param receiver		The receiver (eg, string being operated on in substr())
	 * @param leftOperand	The left operand of the node
	 * @param rightOperand	The right operand of the node
	 * @param operatorType	The type of the operand
	 */

	public void init(
					Object receiver,
					Object leftOperand,
					Object rightOperand,
					Object operatorType,
					Object trimType)
	{
		this.receiver = (ValueNode) receiver;
		this.leftOperand = (ValueNode) leftOperand;
		this.rightOperand = (ValueNode) rightOperand;
		this.operatorType = ((Integer) operatorType).intValue();
		this.operator = (String) TernaryOperators[this.operatorType];
		this.methodName = (String) TernaryMethodNames[this.operatorType];
		this.resultInterfaceType = (String) TernaryResultType[this.operatorType];
		this.receiverInterfaceType = (String) TernaryArgType[this.operatorType][0];
		this.leftInterfaceType = (String) TernaryArgType[this.operatorType][1];
		this.rightInterfaceType = (String) TernaryArgType[this.operatorType][2];
		if (trimType != null)
				this.trimType = ((Integer) trimType).intValue();
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
				"resultInterfaceType: " + resultInterfaceType + "\n" + 
				"receiverInterfaceType: " + receiverInterfaceType + "\n" + 
				"leftInterfaceType: " + leftInterfaceType + "\n" + 
				"rightInterfaceType: " + rightInterfaceType + "\n" + 
				super.toString();
		}
		else
		{
			return "";
		}
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
		receiver.setClause(clause);
		leftOperand.setClause(clause);
		if (rightOperand != null)
		{
			rightOperand.setClause(clause);
		}
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

			if (receiver != null)
			{
				printLabel(depth, "receiver: ");
				receiver.treePrint(depth + 1);
			}

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

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		receiver = receiver.bindExpression(fromList, subqueryList, 
			aggregateVector);
		leftOperand = leftOperand.bindExpression(fromList, subqueryList, 
			aggregateVector);

		if (rightOperand != null)
		{
			rightOperand = rightOperand.bindExpression(fromList, subqueryList, 
				aggregateVector);
		}
		if (operatorType == TRIM)
			trimBind();
		else if (operatorType == LOCATE)
			locateBind();
		else if (operatorType == SUBSTRING)
			substrBind();

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
		receiver = receiver.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);

		leftOperand = leftOperand.preprocess(numTables,
											 outerFromList, outerSubqueryList,
											 outerPredicateList);
		if (rightOperand != null)
		{
			rightOperand = rightOperand.preprocess(numTables,
												   outerFromList, outerSubqueryList,
												   outerPredicateList);
		}
		return this;
	}
	/**
	 * Do code generation for this ternary operator.
	 *
	 * @param acb	The ExpressionClassBuilder for the class we're generating
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
		throws StandardException
	{
		int nargs = 0;
		String receiverType = null;

		/* Allocate an object for re-use to hold the result of the operator */
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, resultInterfaceType);

		receiver.generateExpression(acb, mb);
		if (operatorType == TRIM)
		{
			mb.push(trimType);
			mb.getField(field);
			nargs = 2;
			receiverType = receiverInterfaceType;
		}
		else if (operatorType == LOCATE)
		{
			leftOperand.generateExpression(acb, mb); 
			mb.upCast(leftInterfaceType);
			rightOperand.generateExpression(acb, mb);
			mb.upCast(rightInterfaceType);
			mb.getField(field);
			nargs = 3;
		
		}
		else if (operatorType == SUBSTRING)
		{
			leftOperand.generateExpression(acb, mb); 
			mb.upCast(leftInterfaceType);
			if (rightOperand != null)
			{
				rightOperand.generateExpression(acb, mb);
				mb.upCast(rightInterfaceType);
			}
			else
			{
				mb.pushNull(rightInterfaceType);
			}

			mb.getField(field); // third arg
			mb.push(receiver.getTypeServices().getMaximumWidth());
			nargs = 4;
			receiverType = receiverInterfaceType;
		}
		mb.callMethod(VMOpcode.INVOKEINTERFACE, receiverType, methodName, resultInterfaceType, nargs);

		/*
		** Store the result of the method call in the field, so we can re-use
		** the object.
		*/
		mb.putField(field);
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
		pushable = receiver.categorize(referencedTabs, simplePredsOnly);
		pushable = (leftOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
		if (rightOperand != null)
		{
			pushable = (rightOperand.categorize(referencedTabs, simplePredsOnly) && pushable);
		}
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
		receiver = receiver.remapColumnReferencesToExpressions();
		leftOperand = leftOperand.remapColumnReferencesToExpressions();
		if (rightOperand != null)
		{
			rightOperand = rightOperand.remapColumnReferencesToExpressions();
		}
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return (receiver.isConstantExpression() &&
				leftOperand.isConstantExpression() &&
				(rightOperand == null || rightOperand.isConstantExpression()));
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return (receiver.constantExpression(whereClause) &&
				leftOperand.constantExpression(whereClause) &&
				(rightOperand == null ||
					rightOperand.constantExpression(whereClause)));
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

		if (receiver != null && !v.stopTraversal())
		{
			receiver = (ValueNode)receiver.accept(v);
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
	/**
	 * Bind trim expression. 
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	private ValueNode trimBind() 
			throws StandardException
	{
		TypeId	receiverType;
		TypeId	resultType = TypeId.getBuiltInTypeId(Types.VARCHAR);

		// handle parameters here

		/* Is there a ? parameter for the receiver? */
		if (receiver.isParameterNode())
		{
			/*
			** According to the SQL standard, if trim has a ? receiver,
			** its type is varchar with the implementation-defined maximum length
			** for a varchar.
			*/
	
			((ParameterNode) receiver).setDescriptor(getVarcharDescriptor());
		}

		/* Is there a ? parameter on the left? */
		if (leftOperand.isParameterNode())
		{
			/* Set the left operand type to varchar. */
			((ParameterNode) leftOperand).setDescriptor(getVarcharDescriptor());
		}

		bindToBuiltIn();

		/*
		** Check the type of the receiver - this function is allowed only on
		** string value types.  
		*/
		receiverType = receiver.getTypeId();
		if (receiverType.userType())
			throwBadType("trim", receiverType.getSQLTypeName());

		receiver = castArgToString(receiver);

		if ((receiverType.getTypeFormatId() == StoredFormatIds.CLOB_TYPE_ID) ||
		   (receiverType.getTypeFormatId() == StoredFormatIds.NCLOB_TYPE_ID)) {
		// special case for CLOBs: if we start with a CLOB, we have to get
		// a CLOB as a result (as opposed to a VARCHAR), because we can have a 
		// CLOB that is beyond the max length of VARCHAR (ex. "clob(100k)").
		// This is okay because CLOBs, like VARCHARs, allow variable-length
		// values (which is a must for the trim to actually work).
			resultType = receiverType;
		}

		/*
		** Check the type of the leftOperand (trimSet).
		** The leftOperand should be a string value type.  
		*/
		TypeId	leftCTI;
		leftCTI = leftOperand.getTypeId();
		if (leftCTI.userType())
			throwBadType("trim", leftCTI.getSQLTypeName());

		leftOperand = castArgToString(leftOperand);

		/*
		** The result type of trim is varchar.
		*/
		setResultType(resultType);

		return this;
	}
	/*
	** set result type for operator
	*/
	private void setResultType(TypeId resultType) throws StandardException
	{
		setType(new DataTypeDescriptor(
						resultType,
						true,
						receiver.getTypeServices().getMaximumWidth()
					)
				);
	}
	/**
	 * Bind locate operator
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode locateBind() throws StandardException
	{
		TypeId	firstOperandType, secondOperandType, offsetType;

		/*
		 * Is there a ? parameter for the first arg.  Copy the 
		 * left/firstOperand's.  If the left/firstOperand are both parameters,
		 * both will be max length.
		 */
		if( receiver.isParameterNode())
		{
			if( leftOperand.isParameterNode())
			{
				((ParameterNode) receiver).setDescriptor(getVarcharDescriptor());
			}
			else
			{
				if( leftOperand.getTypeId().isStringTypeId() )
				{
					((ParameterNode) receiver).setDescriptor(
							         leftOperand.getTypeServices());
				}
			}
		}
							                            
		/*
		 * Is there a ? parameter for the second arg.  Copy the receiver's.
		 * If the receiver are both parameters, both will be max length.
		 */
		if(leftOperand.isParameterNode())
		{
			if(receiver.isParameterNode())
			{
				((ParameterNode) leftOperand).setDescriptor(getVarcharDescriptor());
			}
			else
			{
				if( receiver.getTypeId().isStringTypeId() )
				{
					((ParameterNode) leftOperand).setDescriptor(
							         receiver.getTypeServices());
				}
			}
		}

		/*
		 * Is there a ? paramter for the third arg.  It will be an int.
		 */
		if( rightOperand.isParameterNode())
		{
			((ParameterNode) rightOperand).setDescriptor(
				new DataTypeDescriptor(TypeId.INTEGER_ID, true)); 
		}

		bindToBuiltIn();

		/*
		** Check the type of the operand - this function is allowed only
		** for: receiver = CHAR
		**      firstOperand = CHAR
		**      secondOperand = INT
		*/
		secondOperandType = leftOperand.getTypeId();
		offsetType = rightOperand.getTypeId();
		firstOperandType = receiver.getTypeId();

		if (!firstOperandType.isStringTypeId() ||
			!secondOperandType.isStringTypeId() || 
			offsetType.getJDBCTypeId() != Types.INTEGER)
			throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
					"LOCATE", "FUNCTION");

		/*
		** The result type of a LocateFunctionNode is an integer.
		*/
		setType(new DataTypeDescriptor(TypeId.INTEGER_ID, 
				receiver.getTypeServices().isNullable())); 

		return this;
	}

	/* cast arg to a varchar */
	protected ValueNode castArgToString(ValueNode vn) throws StandardException
	{
		TypeCompiler vnTC = vn.getTypeCompiler();
		if (! vn.getTypeId().isStringTypeId())
		{
			ValueNode newNode = (ValueNode)
						getNodeFactory().getNode(
							C_NodeTypes.CAST_NODE,
							vn,
							DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, true,
							                vnTC.getCastToCharWidth(
							                    vn.getTypeServices())),
							getContextManager());
			((CastNode) newNode).bindCastNodeOnly();
			return newNode;
		}
		return vn;
	}

	/**
	 * Bind substr expression.  
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

 	public ValueNode substrBind() 
			throws StandardException
	{
		TypeId	receiverType;
		TypeId	resultType;

		// handle parameters here

		/* Is there a ? parameter for the receiver? */
		if (receiver.isParameterNode())
		{
			/*
			** According to the SQL standard, if substr has a ? receiver,
			** its type is varchar with the implementation-defined maximum length
			** for a varchar.
			*/
	
			((ParameterNode) receiver).setDescriptor(getVarcharDescriptor());
		}

		/* Is there a ? parameter on the left? */
		if (leftOperand.isParameterNode())
		{
			/* Set the left operand type to int. */
			((ParameterNode) leftOperand).setDescriptor(							
				new DataTypeDescriptor(TypeId.INTEGER_ID, true)); 
		}

		/* Is there a ? parameter on the right? */
		if ((rightOperand != null) && rightOperand.isParameterNode())
		{
			/* Set the right operand type to int. */
			((ParameterNode) rightOperand).setDescriptor(							
				new DataTypeDescriptor(TypeId.INTEGER_ID, true)); 
		}

		bindToBuiltIn();

		if (!leftOperand.getTypeId().isNumericTypeId() ||
			(rightOperand != null && !rightOperand.getTypeId().isNumericTypeId()))
			throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "SUBSTR", "FUNCTION");

		/*
		** Check the type of the receiver - this function is allowed only on
		** string value types.  
		*/
		resultType = receiverType = receiver.getTypeId();
		switch (receiverType.getJDBCTypeId())
		{
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
			case Types.CLOB:
				break;
			default:
			{
				throwBadType("SUBSTR", receiverType.getSQLTypeName());
			}
		}

		// Determine the maximum length of the result
		int resultLen = receiver.getTypeServices().getMaximumWidth();

		if (rightOperand != null && rightOperand instanceof ConstantNode)
		{
			if (((ConstantNode)rightOperand).getValue().getInt() < resultLen)
				resultLen = ((ConstantNode)rightOperand).getValue().getInt();
		}

		/*
		** The result type of substr is a string type
		*/
		setType(new DataTypeDescriptor(
						resultType,
						true,
						resultLen
					));

		return this;
	}

	public ValueNode getReceiver()
	{
		return receiver;
	}

	/* throw bad type message */
	private void throwBadType(String funcName, String type) 
		throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
										funcName,
										type);
	}

	/* bind arguments to built in types */
	protected void bindToBuiltIn() 
		throws StandardException
	{
		/* If the receiver is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (! receiver.getTypeId().systemBuiltIn())
		{
			receiver = receiver.genSQLJavaSQLTree();
		}

		/* If the left operand is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (! leftOperand.getTypeId().systemBuiltIn())
		{
			leftOperand = leftOperand.genSQLJavaSQLTree();
		}

		/* If the right operand is not a built-in type, then generate a bound conversion
		 * tree to a built-in type.
		 */
		if (rightOperand != null)
		{
			if (! rightOperand.getTypeId().systemBuiltIn())
			{
				rightOperand = rightOperand.genSQLJavaSQLTree();
			}
		}
	}

	private DataTypeDescriptor getVarcharDescriptor() {
		return new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true);
	}
}
