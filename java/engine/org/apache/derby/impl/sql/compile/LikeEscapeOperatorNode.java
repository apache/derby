/*

   Derby - Class org.apache.derby.impl.sql.compile.LikeEscapeOperatorNode

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.compile.TypeCompiler;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.util.ReuseFactory;

import java.lang.reflect.Modifier;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.types.Like;

import java.sql.Types;
import org.apache.derby.iapi.reference.ClassName;

import java.util.Vector;


/**
 * This node represents a like comparison operator (no escape)


	If the like pattern is a constant or a parameter then if possible
	the like is modified to include a >= and < operator. In some cases
	the like can be eliminated.


	constant or parameter LIKE pattern with prefix followed by optional wild card
	e.g. Cloudscape%

	CHAR(n), VARCHAR(n) where n < 255

		>=   prefix padded with '\u0000' to length n -- e.g. Cloudscape\u0000\u0000
		<=   prefix appended with '\uffff' -- e.g. Cloudscape\uffff
		
		[ can eliminate LIKE if constant. ]

	
	CHAR(n), VARCHAR(n), LONG VARCHAR where n >= 255

	    >= prefix backed up one characer
		<= prefix appended with '\uffff'

		no elimination of like


	parameter like pattern starts with wild card

		CHAR(n), VARCHAR(n) where n <= 256

		>= '\u0000' padded with '\u0000' to length n
		<= '\uffff'

		no elimination of like

	CHAR(n), VARCHAR(n), LONG VARCHAR where n > 256


		>= NULL

		<= '\uffff'
	
	    
	Note that the Unicode value is '\uffff' is defined as not a character value
	and can be used by a program for any purpose. We use it to set an upper
	bound on a character range with a less than predicate. We only need a single
	'\uffff' appended because the string 'Cloudscape\uffff\uffff' is not a valid
	String because '\uffff' is not a valid character.
		



	

		


 *
 * @author ames
 * 		converted to TernaryOperatorNode by been
 */

public final class LikeEscapeOperatorNode extends TernaryOperatorNode
{
	boolean addedEquals;
	String escape;

	/**
	 * Initializer for a LikeEscapeOperatorNode
	 *
	 *	receiver like pattern [ escape escapeValue ]
	 *
	 * @param receiver   		The left operand of the like, column, CharConstant or Parameter
	 * @param leftOperand		The right operand of the like, the pattern
	 * @param rightOperand		The optional escape clause, null if not present
	 */

	public void init(
			Object receiver,
			Object leftOperand,
			Object rightOperand)
	{
		/* By convention, the method name for the like operator is "like" */
		// super.init(leftOperand, rightOperand, new Integer(LIKE), null, null);
		super.init(receiver, leftOperand, rightOperand, ReuseFactory.getInteger(TernaryOperatorNode.LIKE), null); 
	}

	/**
	 * overrides BindOperatorNode.bindExpression because like has special
	 * requirements for parameter binding.
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException thrown on failure
	 */
	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
		Vector	aggregateVector) 
			throws StandardException
	{
		super.bindExpression(fromList, subqueryList, aggregateVector);

		String pattern = null;

		// pattern cannot be a column
		if (leftOperand instanceof ColumnReference)
			throw StandardException.newException(SQLState.LANG_DB2_LIKE_SYNTAX_ERROR);

		// pattern must be a string or a parameter

		if (!(leftOperand.isParameterNode()) && !(leftOperand.getTypeId().isStringTypeId()))
			throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
													 "LIKE", "FUNCTION");

		// escape cannot be a column
		if (rightOperand != null && rightOperand instanceof ColumnReference)
		{
			throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
										 ((ColumnReference) rightOperand).getColumnName());
		}

		// escape must be a string or a parameter
		if ((rightOperand != null) && 
			!(rightOperand.isParameterNode()) && 
			!(rightOperand.getTypeId().isStringTypeId()))
		{
			throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE,
										 "LIKE", "FUNCTION");
		}

		// deal with operand parameters

		/* 
		 *  Is there a ? parameter on the left?
		 *  Do left first because its length is always maximum;
		 *  a parameter on the right copies its length from
		 *  the left, since it won't match if it is any longer than it.
		 */

		if (receiver.isParameterNode())
		{
			((ParameterNode) receiver).setDescriptor(
							new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
		}

		/* 
		 *  Is there a ? parameter for the PATTERN of LIKE?
		 *  Copy from the receiver -- legal if both are parameters,
		 *  both will be max length.
		 *  REMIND: should nullability be copied, or set to true?
		 */

		if (leftOperand.isParameterNode())
		{
			/*
			 * Set the pattern to the type of the left parameter, if
			 * the left is a string, otherwise set it to be VARCHAR. 
			 */
			if (receiver.getTypeId().isStringTypeId())
			{
				((ParameterNode) leftOperand).setDescriptor(receiver.getTypeServices());
			}
			else
			{
				((ParameterNode) leftOperand).setDescriptor(
							new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
			}
		}

		/* 
		 *  Is there a ? parameter for the ESCAPE of LIKE?
		 *  Copy from the receiver -- legal if both are parameters,
		 *  both will be max length.  nullability is set to true.
		 */

		if (rightOperand != null && rightOperand.isParameterNode())
		{
			/*
			 * Set the pattern to the type of the left parameter, if
			 * the left is a string, otherwise set it to be VARCHAR. 
			 */
			if (receiver.getTypeId().isStringTypeId())
			{
				((ParameterNode) rightOperand).setDescriptor(receiver.getTypeServices());
			}
			else
			{
				((ParameterNode) rightOperand).setDescriptor(
							new DataTypeDescriptor(TypeId.getBuiltInTypeId(Types.VARCHAR), true));
			}
		}

		bindToBuiltIn();

		TypeCompiler receiverTC = receiver.getTypeCompiler();
		TypeCompiler leftTC = leftOperand.getTypeCompiler();

		/* The receiver must be a string type
		 */
		if (! receiver.getTypeId().isStringTypeId())
		{
			throw StandardException.newException(SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "LIKE", "FUNCTION");

		}

		/* If either the left or right operands are non-string types,
		 * then we generate an implicit cast to VARCHAR.
		 */
		if (! leftOperand.getTypeId().isStringTypeId())
		{
			leftOperand = castArgToString(leftOperand);
			leftTC = leftOperand.getTypeCompiler();
		}

		if (rightOperand != null)
		{
			rightOperand = castArgToString(rightOperand);
		}

		/* Remember whether or not right side is a string constant.
		 * We need to remember here so that we can transform 
		 * LIKE 'constant' into = 'constant' for national char
		 * columns.
		 */
		boolean leftConstant = (leftOperand instanceof CharConstantNode);
		if (leftConstant)
		{
			pattern = ((CharConstantNode) leftOperand).getString();
		}

		boolean rightConstant = (rightOperand instanceof CharConstantNode);

		if (rightConstant)
		{
			escape = ((CharConstantNode) rightOperand).getString();
			if (escape.length() != 1)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_ESCAPE_CHARACTER,
						escape);
			}
		}
		else if (rightOperand == null)
		{
			// No Escape clause: Let optimization continue for the = case below
			rightConstant = true;
		}

		/* If the left side of LIKE is a ColumnReference and
		 * the right side is a string constant without 
		 * a wildcard then we transform the LIKE into the 
		 * equivalent LIKE AND =.  
		 * If we have an escape clause it also must be a constant.
		 * These types of transformations are normally done 
		 * at preprocess time, but we make an exception 
		 * and do this one at bind time because we transform
		 * a NOT LIKE 'a' into (a LIKE 'a') = false prior to 
		 * preprocessing.
		 * The transformed tree will become:
		 *			AND
		 *		   /   \
		 *	     LIKE   =
		 */

		if (receiver instanceof ColumnReference &&
			leftConstant && rightConstant)
		{
			if (Like.isOptimizable(pattern))
			{
				String newPattern = null;

                /*
                 * If our pattern has no pattern chars (after stripping them out
                 * for the ESCAPE case), we are good to apply = to this match
                 */

                if (escape != null)
                {
                    /* we return a new pattern stripped of ESCAPE chars */
                    newPattern =
                            Like.stripEscapesNoPatternChars(pattern, escape.charAt(0));
                }
                else if (pattern.indexOf('_') == -1 && pattern.indexOf('%') == -1)
                {
                    newPattern = pattern;
                }

                if (newPattern != null)
				{
					AndNode	  newAnd = null;
					BinaryComparisonOperatorNode equals = null;
					ValueNode leftClone = receiver.getClone();

					// Remember that we did xform, see preprocess()
					addedEquals = true;

					equals = 
						(BinaryComparisonOperatorNode) getNodeFactory().getNode(
							C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
							leftClone, 
							(ValueNode) getNodeFactory().getNode(
												C_NodeTypes.CHAR_CONSTANT_NODE,
												newPattern,
												getContextManager()),
							getContextManager());

					// Set forQueryRewrite to bypass comparability checks
					equals.setForQueryRewrite(true);
					equals = (BinaryComparisonOperatorNode) equals.bindExpression(fromList, subqueryList, aggregateVector);
					
					newAnd = (AndNode) getNodeFactory().getNode(
														C_NodeTypes.AND_NODE,
														this,
														equals,
														getContextManager());
					finishBindExpr();
					newAnd.postBindFixup();
					return newAnd;
				}
			}
		}

		/* If we are comparing a char with a national char then
		 * we generate a cast above the reciever to force preprocess to
		 * not attempt any of the > <= optimizations since there is no
		 * way to determine the 'next' character for the <= operand.
		 */

		TypeId leftTypeId = leftOperand.getTypeId();
		TypeId receiverTypeId = receiver.getTypeId();

		if (receiverTypeId.isNationalStringTypeId() && ! leftTypeId.isNationalStringTypeId())
		{
			receiver = castArgToNationalString(receiver, receiverTC, receiverTypeId);
		}
		else if (leftTypeId.isNationalStringTypeId() && ! receiverTypeId.isNationalStringTypeId())
		{
			leftOperand = castArgToNationalString(leftOperand, leftTC, leftTypeId);
		}

		finishBindExpr();

		return this;
	}
	private ValueNode castArgToNationalString(ValueNode vn, TypeCompiler vnTC, TypeId vnTypeId)
		throws StandardException
	{
		ValueNode newNode =  (ValueNode)
			getNodeFactory().getNode(
			C_NodeTypes.CAST_NODE,
			vn, 
			new DataTypeDescriptor(vnTypeId, 
			true,
			vnTC.getCastToCharWidth(vn.getTypeServices())), getContextManager());
		((CastNode) newNode).bindCastNodeOnly();
		return newNode;
	}
	private void finishBindExpr()
		throws StandardException
	{
		// deal with compatability of operands and result type
		bindComparisonOperator();

		/*
		** The result type of LIKE is Boolean
		*/

		boolean nullableResult =
			receiver.getTypeServices().isNullable() || leftOperand.getTypeServices().isNullable();

		if (rightOperand != null)
		{
			nullableResult |= rightOperand.getTypeServices().isNullable();
		}

		setType(new DataTypeDescriptor(
						TypeId.BOOLEAN_ID,
						nullableResult
					)
				);
	}

	/**
	 * Bind this operator
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindComparisonOperator()
			throws StandardException
	{
		TypeId	receiverType = receiver.getTypeId();
		TypeId	leftType = leftOperand.getTypeId();

		/*
		** Check the type of the operands - this function is allowed only on
		** string types.
		*/

		if ( ! receiverType.isStringTypeId())
		{
			throw StandardException.newException(SQLState.LANG_LIKE_BAD_TYPE, 
												receiverType.getSQLTypeName());
		}

		if (! leftType.isStringTypeId())
		{
			throw StandardException.newException(SQLState.LANG_LIKE_BAD_TYPE, 
												leftType.getSQLTypeName());
		}

		if (rightOperand != null && ! rightOperand.getTypeId().isStringTypeId())
		{
			throw StandardException.newException(SQLState.LANG_LIKE_BAD_TYPE, 
												rightOperand.getTypeId().getSQLTypeName());
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
		boolean eliminateLikeComparison = false;
		String	greaterEqualString = null;
		String  lessThanString = null;
		String  pattern;

		/* We must 1st preprocess the component parts */
		super.preprocess(numTables,
						 outerFromList, outerSubqueryList,
						 outerPredicateList);

        /* Don't try to optimize for (C)LOB type since it
         * doesn't allow comparison.
         */
        if (receiver.getTypeId().getSQLTypeName().equals("CLOB")) {
            return this;
        }
		/* No need to consider transformation if we
		 * already did transformation that added =
		 * at bind time.
		 */
		if (addedEquals)
		{
			return this;
		}

		/* This is where we do the transformation for LIKE to make it optimizable.
		 * c1 LIKE 'asdf%' -> c1 LIKE 'asdf%' AND c1 >= 'asdf' AND c1 < 'asdg'
		 * c1 LIKE ? -> c1 LIKE ? and c1 >= ?', where ?' gets calculated at the
		 * beginning of execution.
		 */
		if (!(leftOperand instanceof CharConstantNode) &&
			!(leftOperand.isParameterNode()))
		{
			return this;
		}

		/* This transformation is only worth doing if it is pushable, ie, if
		 * the receiver is a ColumnReference.
		 */
		if (!(receiver instanceof ColumnReference))
		{
			//
			// We also do an early return here if in bindExpression we found we had
			// a National Char and put a CAST above the receiver.
			//
			return this;
		}


		// Build String constants if right side is a constant
		if (leftOperand instanceof CharConstantNode)
		{
			pattern = ((CharConstantNode) leftOperand).getString();

			if (! Like.isOptimizable(pattern))
			{
				return this;
			}

			greaterEqualString = Like.greaterEqualString(pattern, escape);



			/* We do not generate the < and we cannot drop the LIKE
			 * when doing LIKE on a national char column.
			 */
            if ( ! receiver.getTypeId().isNationalStringTypeId() )
			{
				lessThanString = Like.lessThanString(greaterEqualString);
				eliminateLikeComparison = ! Like.isLikeComparisonNeeded(pattern);
			}
		}

		//System.out.println(receiver.getTypeServices());
		//System.out.println("MAX WIDTH " + receiver.getTypeServices().getMaximumWidth());

		/* For some unknown reason we need to clone the receiver if it is
		 * a ColumnReference because reusing them in Qualifiers for a scan
		 * does not work.  
		 */

		/* The transformed tree has to be normalized.  Either:
		 *			AND									AND
		 *		   /   \							   /   \
		 *	     LIKE   AND				OR:			 LIKE	AND
		 *			   /   \							   /   \
		 *			  >=    AND							  >=    TRUE
		 *				   /   \
		 *				  <     TRUE
		 * unless the like string is of the form CONSTANT%, in which
		 * case we can do away with the LIKE altogether:
		 *			AND									AND
		 *		   /   \							   /   \
		 *	     >=     AND				OR:			 >=  	TRUE
		 *			   /   \
		 *			  <    TRUE
		 */

		AndNode	  newAnd = null;
        ValueNode trueNode = (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());

		/* Create the AND <, if lessThanString is non-null  or 
		 * leftOperand is a parameter.
		   Currently for a national string we do not add a < than operator
		   since we don't know (?) how to calculate such a string.
		 */
		if ( lessThanString != null || ( leftOperand.isParameterNode() &&
                                         ! receiver.getTypeId().isNationalStringTypeId() ))
		{
			QueryTreeNode likeLTopt;
			if (leftOperand.isParameterNode())
			{
				likeLTopt = setupOptimizeStringFromParameter(leftOperand, rightOperand,
								"lessThanStringFromParameter");
			}
			else
			{
				likeLTopt = getNodeFactory().getNode(
												C_NodeTypes.CHAR_CONSTANT_NODE,
												lessThanString,
												getContextManager());
			}

			BinaryComparisonOperatorNode lessThan = 
					(BinaryComparisonOperatorNode) getNodeFactory().getNode(
							C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE,
							receiver.getClone(), 
							likeLTopt,
							getContextManager());

			// Disable comparability checks
			lessThan.setForQueryRewrite(true);
			/* Set type info for the operator node */
			lessThan.bindComparisonOperator();

			// Use between selectivity for the <
			lessThan.setBetweenSelectivity();

			/* Create the AND */
			newAnd = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													lessThan,
													trueNode,
													getContextManager());
			newAnd.postBindFixup();
		}

		/* Create the AND >=.  Right side could
		 * be a CharConstantNode or a ParameterNode.
		 */

		ValueNode likeGEopt;
		if (leftOperand.isParameterNode()) {

			// Create an expression off the parameter
			// new SQLChar(Like.greaterEqualString(?));

			likeGEopt = setupOptimizeStringFromParameter(leftOperand, rightOperand,
									"greaterEqualStringFromParameter");

		} else {



			likeGEopt = (ValueNode) getNodeFactory().getNode(C_NodeTypes.CHAR_CONSTANT_NODE,
												greaterEqualString,
												getContextManager());
		}

		BinaryComparisonOperatorNode greaterEqual = 
				(BinaryComparisonOperatorNode) getNodeFactory().getNode(
						C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE,
						receiver.getClone(), 
						likeGEopt,
						getContextManager());


		// Disable comparability checks
		greaterEqual.setForQueryRewrite(true);
		/* Set type info for the operator node */
		greaterEqual.bindComparisonOperator();

		// Use between selectivity for the >=
		greaterEqual.setBetweenSelectivity();

		/* Create the AND */
		if (newAnd == null)
		{
			newAnd = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													greaterEqual,
													trueNode,
													getContextManager());
		}
		else
		{
			newAnd = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													greaterEqual,
													newAnd,
													getContextManager());
		}
		newAnd.postBindFixup();

		/* Finally, we put a AND LIKE on top of the left deep tree, but
		 * only if it is still necessary.
	     */
		if (! eliminateLikeComparison)
		{
			newAnd = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													this,
													newAnd,
													getContextManager());
			newAnd.postBindFixup();
		}

		/* Mark this node as transformed so that we don't get
		 * calculated into the selectivity mulitple times.
		 */
		setTransformed();

		return newAnd;
	}

	/**
	 * Do code generation for this binary operator.
	 *
	 *		This code was copied from BinaryOperatorNode and stripped down
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

/*
** if i have a operator.getOrderableType() == constant, then just cache 
** it in a field.  if i have QUERY_INVARIANT, then it would be good to
** cache it in something that is initialized each execution,
** but how?
*/

		/*
		** let the receiver type be determined by an
		** overridable method so that if methods are
		** not implemented on the lowest interface of
		** a class, they can note that in the implementation
		** of the node that uses the method.
		*/
		// receiverType = getReceiverInterfaceName();

		/*
		** Generate LHS (field = <receiver operand>). This assignment is
		** used as the receiver of the method call for this operator.
		**
		**	(<receiver operand>).method(<left operand>,
		**                      <right operand>, [<escaperightOp>,]
		**						<result field>)
		*/

		receiver.generateExpression(acb, mb);	// first arg

		receiverInterfaceType = receiver.getTypeCompiler().interfaceName();

		mb.upCast(receiverInterfaceType); 		// cast the method instance

		leftOperand.generateExpression(acb, mb);
		mb.upCast(leftInterfaceType); 			// first arg with cast

		if (rightOperand != null)
		{
			rightOperand.generateExpression(acb, mb);
			mb.upCast(rightInterfaceType); 		// second arg with cast
		}

		/* Figure out the result type name */
		// resultTypeName = getTypeCompiler().interfaceName();

		mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
			methodName, resultInterfaceType,
			rightOperand == null ? 1 : 2);
	}

	private ValueNode setupOptimizeStringFromParameter(ValueNode parameterNode,
						ValueNode escapeNode,String methodName)
		throws StandardException {

		Vector param;

		if (escapeNode != null)
		{
			param = new Vector(2);
			methodName += "WithEsc";
		}
		else param = new Vector(1);

		StaticMethodCallNode methodCall = (StaticMethodCallNode)
						getNodeFactory().getNode(C_NodeTypes.STATIC_METHOD_CALL_NODE,
											methodName,
											"org.apache.derby.iapi.types.Like",
											getContextManager());

		// using a method call directly, thus need internal sql capability
		methodCall.internalCall = true;

		param.addElement(parameterNode);
		if (escapeNode != null)
			param.addElement(escapeNode);

		methodCall.addParms(param);


		ValueNode java2SQL = (ValueNode) getNodeFactory().getNode(
									C_NodeTypes.JAVA_TO_SQL_VALUE_NODE,
									methodCall,
									getContextManager());


		java2SQL = (ValueNode) java2SQL.bindExpression(null, null, null);

		CastNode likeOpt = (CastNode)
			getNodeFactory().getNode(
				C_NodeTypes.CAST_NODE,
				java2SQL,
				parameterNode.getTypeServices(),
				getContextManager());

		likeOpt.bindCastNodeOnly();

		return likeOpt;
	}
}
