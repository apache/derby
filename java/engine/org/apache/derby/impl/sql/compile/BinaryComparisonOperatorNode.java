/*

   Derby - Class org.apache.derby.impl.sql.compile.BinaryComparisonOperatorNode

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
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.sql.compile.TypeCompiler;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import java.util.Vector;
import java.sql.Types;

/**
 * This node is the superclass  for all binary comparison operators, such as =,
 * <>, <, etc.
 *
 * @author Jeff Lichtman
 */

public abstract class BinaryComparisonOperatorNode extends BinaryOperatorNode
{
	// Use between selectivity?
	private boolean forQueryRewrite;
	private boolean betweenSelectivity;

	/**
	 * Initializer for a BinaryComparisonOperatorNode
	 *
	 * @param leftOperand	The left operand of the comparison
	 * @param rightOperand	The right operand of the comparison
	 * @param operator		The name of the operator
	 * @param methodName	The name of the method to call in the generated
	 *						class
	 */

	public void init(
				Object	leftOperand,
				Object	rightOperand,
				Object		operator,
				Object		methodName)
	{
		super.init(leftOperand, rightOperand, operator, methodName,
				ClassName.DataValueDescriptor, ClassName.DataValueDescriptor);
	}

	/**
	 * This node was generated as part of a query rewrite. Bypass the
	 * normal comparability checks.
	 * @param val  true if this was for a query rewrite
	 *
	 * @return Nothing
	 */
	public void setForQueryRewrite(boolean val)
	{
		forQueryRewrite=val;
	}

	/**
	 * Was this node generated in a query rewrite?
	 *
	 * @return  true if it was generated in a query rewrite.
	 */
	public boolean getForQueryRewrite()
	{
		return forQueryRewrite;
	}

	/**
	 * Use between selectivity when calculating the selectivity.
	 *
	 * @return Nothing.
	 */
	void setBetweenSelectivity()
	{
		betweenSelectivity = true;
	}

	/**
	 * Return whether or not to use the between selectivity for this node.
	 *
	 * @return Whether or not to use the between selectivity for this node.
	 */
	boolean getBetweenSelectivity()
	{
		return betweenSelectivity;
	}


	/**
	 * Bind this comparison operator.  All that has to be done for binding
	 * a comparison operator is to bind the operands, check the compatibility
	 * of the types, and set the result type to SQLBoolean.
	 *
	 * @param fromList			The query's FROM list
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
		super.bindExpression(fromList, subqueryList, aggregateVector);

//RESOLVELOCALIZE - convert constants to national constants
		TypeCompiler leftTC = leftOperand.getTypeCompiler();
		TypeCompiler rightTC = rightOperand.getTypeCompiler();
		TypeId leftTypeId = leftOperand.getTypeId();
		TypeId rightTypeId = rightOperand.getTypeId();

		/*
		 * If we are comparing a non-string with a string type, then we
		 * must prevent the non-string value from being used to probe into
		 * an index on a string column. This is because the string types
		 * are all of low precedence, so the comparison rules of the non-string
		 * value are used, so it may not find values in a string index because
		 * it will be in the wrong order. So, cast the string value to its
		 * own type. This is easier than casting it to the non-string type,
		 * because we would have to figure out the right length to cast it to.
		 */
		if (! leftTypeId.isStringTypeId() && rightTypeId.isStringTypeId())
		{
			DataTypeDescriptor rightTypeServices = rightOperand.getTypeServices();

			rightOperand =  (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					rightOperand, 
					new DataTypeDescriptor(
							rightTypeId,
							true, 
							rightTypeServices.getMaximumWidth()),
					getContextManager());
			((CastNode) rightOperand).bindCastNodeOnly();
		}
		else if (! rightTypeId.isStringTypeId() && leftTypeId.isStringTypeId())
		{
			DataTypeDescriptor leftTypeServices = leftOperand.getTypeServices();

			leftOperand =  (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					leftOperand, 
					new DataTypeDescriptor(
							leftTypeId,
							true, 
							leftTypeServices.getMaximumWidth()),
					getContextManager());
			((CastNode) leftOperand).bindCastNodeOnly();
		}
		/* If we are comparing a char with a national char then
		 * we need to generate a cast to the appropriate national
		 * char above the char operand.
		 */
		else if (! leftTypeId.isNationalStringTypeId() &&
			rightTypeId.isNationalStringTypeId())
		{
			leftOperand =  (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					leftOperand, 
					DataTypeDescriptor.getBuiltInDataTypeDescriptor(leftTC.getMatchingNationalCharTypeName(),
										leftTC.getCastToCharWidth(
											leftOperand.getTypeServices())),
					getContextManager());
			((CastNode) leftOperand).bindCastNodeOnly();
		}
		else if (! rightTypeId.isNationalStringTypeId() &&
				leftTypeId.isNationalStringTypeId())
		{
			rightOperand =  (ValueNode)
				getNodeFactory().getNode(
					C_NodeTypes.CAST_NODE,
					rightOperand, 
					DataTypeDescriptor.getBuiltInDataTypeDescriptor(rightTC.getMatchingNationalCharTypeName(), 
										rightTC.getCastToCharWidth(
											rightOperand.getTypeServices())),
					getContextManager());
			((CastNode) rightOperand).bindCastNodeOnly();
		}

		/* Test type compatability and set type info for this node */
		bindComparisonOperator();

		return this;
	}


	/**
	 * Test the type compatability of the operands and set the type info
	 * for this node.  This method is useful both during binding and
	 * when we generate nodes within the language module outside of the parser.
	 *
	 * @param fromQueryRewrite  If true this call came from a query that was
	 * rewritten by the optimizer.  The normal isComparable check will be 
	 * bypassed to allow > < and the like with longvarchar.  
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindComparisonOperator()
			throws StandardException
	{
		TypeId	leftType;
		TypeId	rightType;
		boolean				nullableResult;

		leftType = leftOperand.getTypeId();
		rightType = rightOperand.getTypeId();


		/*
		** Can the types be compared to each other?  If not, throw an
		** exception.
		*/
		boolean forEquals = operator.equals("=") || operator.equals("<>");

        boolean cmp = leftOperand.getTypeCompiler().comparable(rightType,
                                                               forEquals,
                                                               getClassFactory());
		// Bypass the comparable check if this is a rewrite from the 
		// optimizer.  We will assume Mr. Optimizer knows what he is doing.
          if (!cmp && !forQueryRewrite) {
			throw StandardException.newException(SQLState.LANG_NOT_COMPARABLE, 
					leftType.getSQLTypeName(),
					rightType.getSQLTypeName()
				);
		  }

		
		/*
		** Set the result type of this comparison operator based on the
		** operands.  The result type is always SQLBoolean - the only question
		** is whether it is nullable or not.  If either of the operands is
		** nullable, the result of the comparison must be nullable, too, so
		** we can represent the unknown truth value.
		*/
		nullableResult = leftOperand.getTypeServices().isNullable() ||
							rightOperand.getTypeServices().isNullable();
		setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));


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

		/* This is where we start to consider flattening expression subqueries based
		 * on a uniqueness condition.  If the right child is a SubqueryNode then
		 * it is a potentially flattenable expression subquery.  If we flatten the
		 * subquery then we at least need to change the right operand of this 
		 * comparison.  However, we may want to push the comparison into the subquery
		 * itself and replace this outer comparison with TRUE in the tree.  Thus we
		 * return rightOperand.preprocess() if the rightOperand is a SubqueryNode.
		 * NOTE: SubqueryNode.preprocess() is smart enough to return this node
		 * if it is not flattenable.
		 * NOTE: We only do this if the subquery has not yet been preprocessed.
		 * (A subquery can get preprocessed multiple times if it is a child node
		 * in an expression that gets transformed, like BETWEEN.  The subquery
		 * remembers whether or not it has been preprocessed and simply returns if
		 * it has already been preprocessed.  The return returns the SubqueryNode,
		 * so an invalid tree is returned if we set the parent comparison operator
		 * when the subquery has already been preprocessed.)
		 */
		if ((rightOperand instanceof SubqueryNode) &&
			!((SubqueryNode) rightOperand).getPreprocessed())
		{
			((SubqueryNode) rightOperand).setParentComparisonOperator(this);
			return rightOperand.preprocess(numTables,
										   outerFromList, outerSubqueryList,
										   outerPredicateList);
		}
		else
		{
			rightOperand = rightOperand.preprocess(numTables,
												   outerFromList, outerSubqueryList,
												   outerPredicateList);
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
		if (! underNotNode)
		{
			return this;
		}

		/* Convert the BinaryComparison operator to its negation */
		return getNegation(leftOperand, rightOperand);
	}

	/**
	 * Negate the comparison.
	 *
	 * @param leftOperand	The left operand of the comparison operator
	 * @param rightOperand	The right operand of the comparison operator
	 *
	 * @return BinaryOperatorNode	The negated expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	BinaryOperatorNode getNegation(ValueNode leftOperand,
										  ValueNode rightOperand)
				throws StandardException
	{
		/* Keep the compiler happy - this method should never be called.
		 * We should always be calling the method in a sub-class.
		 */
		if (SanityManager.DEBUG)
		SanityManager.ASSERT(false,
					"expected to call getNegation() for subclass " +
					getClass().toString());
		return this;
	}

	/**
	 * Finish putting an expression into conjunctive normal
	 * form.  An expression tree in conjunctive normal form meets
	 * the following criteria:
	 *		o  If the expression tree is not null,
	 *		   the top level will be a chain of AndNodes terminating
	 *		   in a true BooleanConstantNode.
	 *		o  The left child of an AndNode will never be an AndNode.
	 *		o  Any right-linked chain that includes an AndNode will
	 *		   be entirely composed of AndNodes terminated by a true BooleanConstantNode.
	 *		o  The left child of an OrNode will never be an OrNode.
	 *		o  Any right-linked chain that includes an OrNode will
	 *		   be entirely composed of OrNodes terminated by a false BooleanConstantNode.
	 *		o  ValueNodes other than AndNodes and OrNodes are considered
	 *		   leaf nodes for purposes of expression normalization.
	 *		   In other words, we won't do any normalization under
	 *		   those nodes.
	 *
	 * In addition, we track whether or not we are under a top level AndNode.  
	 * SubqueryNodes need to know this for subquery flattening.
	 *
	 * @param	underTopAndNode		Whether or not we are under a top level AndNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode changeToCNF(boolean underTopAndNode) 
					throws StandardException
	{
		/* If our right child is a subquery and we are under a top and node
		 * then we want to mark the subquery as under a top and node.
		 * That will allow us to consider flattening it.
		 */
		if (underTopAndNode && (rightOperand instanceof SubqueryNode))
		{
			rightOperand = rightOperand.changeToCNF(underTopAndNode);
		}

		return this;
	}
	
	/** @see BinaryOperatorNode#genSQLJavaSQLTree */
	public ValueNode genSQLJavaSQLTree() throws StandardException
	{
		TypeId leftTypeId = leftOperand.getTypeId();

		/* If I have Java types, I need only add java->sql->java if the types
		 * are not comparable 
		 */
		if (!leftTypeId.systemBuiltIn())
		{
			if (leftOperand.getTypeCompiler().comparable(leftTypeId, false,
														   getClassFactory()))
				return this;

			leftOperand = leftOperand.genSQLJavaSQLTree();
		}

		TypeId rightTypeId = rightOperand.getTypeId();

		if (!rightTypeId.systemBuiltIn())
		{
			if (rightOperand.getTypeCompiler().comparable(rightTypeId, false,
															getClassFactory()))
				return this;

			rightOperand = rightOperand.genSQLJavaSQLTree();
		}

		return this;
	}
}
