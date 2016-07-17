/*

   Derby - Class org.apache.derby.impl.sql.compile.LikeEscapeOperatorNode

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

package org.apache.derby.impl.sql.compile;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.Like;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;

/**
    This node represents a like comparison operator (no escape)

    If the like pattern is a constant or a parameter then if possible
    the like is modified to include a &gt;= and &lt; operator. In some cases
    the like can be eliminated.  By adding =, &gt;= or &lt; operators it may
    allow indexes to be used to greatly narrow the search range of the
    query, and allow optimizer to estimate number of rows to affected.


    constant or parameter LIKE pattern with prefix followed by optional wild 
    card e.g. Derby%

    CHAR(n), VARCHAR(n) where n &lt; 255

        &gt;=   prefix padded with '\u0000' to length n -- e.g. Derby\u0000\u0000
        &lt;=   prefix appended with '\uffff' -- e.g. Derby\uffff

        [ can eliminate LIKE if constant. ]


    CHAR(n), VARCHAR(n), LONG VARCHAR where n &gt;= 255

        &gt;= prefix backed up one characer
        &lt;= prefix appended with '\uffff'

        no elimination of like


    parameter like pattern starts with wild card e.g. %Derby

    CHAR(n), VARCHAR(n) where n &lt;= 256

        &gt;= '\u0000' padded with '\u0000' to length n
        &lt;= '\uffff'

        no elimination of like

    CHAR(n), VARCHAR(n), LONG VARCHAR where n &gt; 256

        &gt;= NULL

        &lt;= '\uffff'


    Note that the Unicode value '\uffff' is defined as not a character value
    and can be used by a program for any purpose. We use it to set an upper
    bound on a character range with a less than predicate. We only need a single
    '\uffff' appended because the string 'Derby\uffff\uffff' is not a valid
    String because '\uffff' is not a valid character.

**/

public final class LikeEscapeOperatorNode extends TernaryOperatorNode
{
    /**************************************************************************
    * Fields of the class
    **************************************************************************
    */
    boolean addedEquals;
    String  escape;

    /**
     * Constructor for a LikeEscapeOperatorNode
     *
     * receiver like pattern [ escape escapeValue ]
     *
     * @param receiver      The left operand of the like: 
     *                              column, CharConstant or Parameter
     * @param leftOperand   The right operand of the like: the pattern
     * @param rightOperand  The optional escape clause, null if not present
     * @param cm            The context manager
     */
    LikeEscapeOperatorNode(
            ValueNode receiver,
            ValueNode leftOperand,
            ValueNode rightOperand,
            ContextManager cm)
    {
        /* By convention, the method name for the like operator is "like" */
        super(receiver,
              leftOperand,
              rightOperand,
              TernaryOperatorNode.K_LIKE,
              cm);
    }

    /**
     * implement binding for like expressions.
     * <p>
     * overrides BindOperatorNode.bindExpression because like has special
     * requirements for parameter binding.
     *
     * @return  The new top of the expression tree.
     *
     * @exception StandardException thrown on failure
     */
    @Override
    ValueNode bindExpression(
    FromList fromList, SubqueryList subqueryList, List<AggregateNode> aggregates)
        throws StandardException
    {
        super.bindExpression(fromList, subqueryList, aggregates);

        String pattern = null;

        // pattern must be a string or a parameter

        if (!(leftOperand.requiresTypeFromContext()) && 
             !(leftOperand.getTypeId().isStringTypeId()))
        {
            throw StandardException.newException(
                SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "LIKE", "FUNCTION");
        }

        // escape must be a string or a parameter
        if ((rightOperand != null) && 
            !(rightOperand.requiresTypeFromContext()) && 
            !(rightOperand.getTypeId().isStringTypeId()))
        {
            throw StandardException.newException(
                SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "LIKE", "FUNCTION");
        }

        // deal with operand parameters

        /* 
        *  Is there a ? parameter on the left? ie. "? like 'Derby'"
        *
        *  Do left first because its length is always maximum;
        *  a parameter on the right copies its length from
        *  the left, since it won't match if it is any longer than it.
        */
        if (receiver.requiresTypeFromContext())
        {
            receiver.setType(
                new DataTypeDescriptor(
                    TypeId.getBuiltInTypeId(Types.VARCHAR), true));
            //check if this parameter can pick up it's collation from pattern
            //or escape clauses in that order. If not, then it will take it's
            //collation from the compilation schema.
            if (!leftOperand.requiresTypeFromContext()) {
                receiver.setCollationInfo(leftOperand.getTypeServices());

            } else if (rightOperand != null && !rightOperand.requiresTypeFromContext()) {
                receiver.setCollationInfo(rightOperand.getTypeServices());          	
            } else {
    			receiver.setCollationUsingCompilationSchema();            	
            }
        }

        /* 
         *  Is there a ? parameter for the PATTERN of LIKE? ie. "column like ?"
         *  
         *  Copy from the receiver -- legal if both are parameters,
         *  both will be max length.
         *  REMIND: should nullability be copied, or set to true?
         */
        if (leftOperand.requiresTypeFromContext())
        {
            /*
            * Set the pattern to the type of the left parameter, if
            * the left is a string, otherwise set it to be VARCHAR. 
            */
            if (receiver.getTypeId().isStringTypeId())
            {
                leftOperand.setType(receiver.getTypeServices());
            }
            else
            {
                leftOperand.setType(
                    new DataTypeDescriptor(
                        TypeId.getBuiltInTypeId(Types.VARCHAR), true));
            }
			//collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for the other ? in LIKE clause
            leftOperand.setCollationInfo(receiver.getTypeServices());          	
        }

        /* 
         *  Is there a ? parameter for the ESCAPE of LIKE?
         *  Copy from the receiver -- legal if both are parameters,
         *  both will be max length.  nullability is set to true.
         */

        if (rightOperand != null && rightOperand.requiresTypeFromContext())
        {
            /*
             * Set the pattern to the type of the left parameter, if
             * the left is a string, otherwise set it to be VARCHAR. 
             */
            if (receiver.getTypeId().isStringTypeId())
            {
                rightOperand.setType(receiver.getTypeServices());
            }
            else
            {
                rightOperand.setType(
                    new DataTypeDescriptor(
                        TypeId.getBuiltInTypeId(Types.VARCHAR), true));
            }
			//collation of ? operand should be picked up from the context.
            //By the time we come here, receiver will have correct collation
            //set on it and hence we can rely on it to get correct collation
            //for the other ? in LIKE clause
            rightOperand.setCollationInfo(receiver.getTypeServices());    	
        }

        bindToBuiltIn();

        /* The receiver must be a string type
        */
        if (! receiver.getTypeId().isStringTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_DB2_FUNCTION_INCOMPATIBLE, "LIKE", "FUNCTION");
        }

        /* If either the left or right operands are non-string types,
         * then we generate an implicit cast to VARCHAR.
         */
        if (!leftOperand.getTypeId().isStringTypeId())
        {
            leftOperand = castArgToString(leftOperand);
        }

        if (rightOperand != null)
        {
            rightOperand = castArgToString(rightOperand);
        }

        /* 
         * Remember whether or not the right side (the like pattern) is a string 
         * constant.  We need to remember here so that we can transform LIKE 
         * 'constant' into = 'constant' for non unicode based collation columns.
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
                throw StandardException.newException(
                    SQLState.LANG_INVALID_ESCAPE_CHARACTER, escape);
            }
        }
        else if (rightOperand == null)
        {
            // No Escape clause: Let optimization continue for the = case below
            rightConstant = true;
        }

        /* If we are comparing a UCS_BASIC char with a terriotry based char 
         * then we generate a cast above the receiver to force preprocess to
         * not attempt any of the > <= optimizations since there is no
         * way to determine the 'next' character for the <= operand.
         *
         * TODO-COLLATE - probably need to do something about different 
         *                collation types here.
         */

        // The left and the pattern of the LIKE must be same collation type
        // and derivation.
        if (!receiver.getTypeServices().compareCollationInfo(
        		leftOperand.getTypeServices()))
        {
            // throw error.
            throw StandardException.newException(
                        SQLState.LANG_LIKE_COLLATION_MISMATCH, 
                        receiver.getTypeServices().getSQLstring(),
                        receiver.getTypeServices().getCollationName(),
                        leftOperand.getTypeServices().getSQLstring(),
                        leftOperand.getTypeServices().getCollationName());
        }

        /* If the left side of LIKE is a ColumnReference and right side is a 
         * string constant without a wildcard (eg. column LIKE 'Derby') then we 
         * transform the LIKE into the equivalent LIKE AND =.  
         * If we have an escape clause it also must be a constant 
         * (eg. column LIKE 'Derby' ESCAPE '%').
         *
         * These types of transformations are normally done at preprocess time, 
         * but we make an exception and do this one at bind time because we 
         * transform a NOT LIKE 'a' into (a LIKE 'a') = false prior to 
         * preprocessing.  
         *
         * The transformed tree will become:
         *
         *        AND
         *       /   \
         *     LIKE   =
         */

        if ((receiver instanceof ColumnReference) && 
            leftConstant                          && 
            rightConstant)
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
                        Like.stripEscapesNoPatternChars(
                            pattern, escape.charAt(0));
                }
                else if (pattern.indexOf('_') == -1 && 
                         pattern.indexOf('%') == -1)
                {
                    // no pattern characters.
                    newPattern = pattern;
                }

                if (newPattern != null)
                {
                    // met all conditions, transform LIKE into a "LIKE and ="

                    ValueNode leftClone = receiver.getClone();

                    // Remember that we did xform, see preprocess()
                    addedEquals = true;

                    // create equals node of the form (eg. column like 'Derby' :
                    //       =
                    //     /   \
                    //  column  'Derby'
                    BinaryComparisonOperatorNode equals = 
                        new BinaryRelationalOperatorNode(
                            BinaryRelationalOperatorNode.K_EQUALS,
                            leftClone, 
                            new CharConstantNode(newPattern,
                                                 getContextManager()),
                            false,
                            getContextManager());

                    // Set forQueryRewrite to bypass comparability checks
                    equals.setForQueryRewrite(true);

                    equals = (BinaryComparisonOperatorNode) 
                        equals.bindExpression(
                            fromList, subqueryList, aggregates);

                    // create new and node and hook in "equals" the new "=' node
                    //
                    //        AND
                    //       /   \
                    //     LIKE   = 
                    //           / \
                    //       column 'Derby'

                    AndNode newAnd =
                            new AndNode(this, equals, getContextManager());

                    finishBindExpr();
                    newAnd.postBindFixup();

                    return newAnd;
                }
            }
        }

        finishBindExpr();

        return this;
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
            receiver.getTypeServices().isNullable() || 
            leftOperand.getTypeServices().isNullable();

        if (rightOperand != null)
        {
            nullableResult |= rightOperand.getTypeServices().isNullable();
        }

        setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, nullableResult));
    }

    /**
    * Bind this operator
    *
    * @exception StandardException  Thrown on error
    */

    public void bindComparisonOperator()
        throws StandardException
    {
        TypeId receiverType = receiver.getTypeId();
        TypeId leftType     = leftOperand.getTypeId();

        /*
        ** Check the type of the operands - this function is allowed only on
        ** string types.
        */

        if (!receiverType.isStringTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_LIKE_BAD_TYPE, receiverType.getSQLTypeName());
        }

        if (!leftType.isStringTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_LIKE_BAD_TYPE, leftType.getSQLTypeName());
        }

        if (rightOperand != null && ! rightOperand.getTypeId().isStringTypeId())
        {
            throw StandardException.newException(
                SQLState.LANG_LIKE_BAD_TYPE, 
                rightOperand.getTypeId().getSQLTypeName());
        }
    }

    /**
    * Preprocess an expression tree.  We do a number of transformations
    * here (including subqueries, IN lists, LIKE and BETWEEN) plus
    * subquery flattening.
    * NOTE: This is done before the outer ResultSetNode is preprocessed.
    *
    * @param numTables          Number of tables in the DML Statement
    * @param outerFromList      FromList from outer query block
    * @param outerSubqueryList  SubqueryList from outer query block
    * @param outerPredicateList PredicateList from outer query block
    *
    * @return The modified expression
    *
    * @exception StandardException  Thrown on error
    */
    @Override
    ValueNode preprocess(
    int             numTables,
    FromList        outerFromList,
    SubqueryList    outerSubqueryList,
    PredicateList   outerPredicateList) 
        throws StandardException
    {
        boolean eliminateLikeComparison = false;
        String  greaterEqualString      = null;
        String  lessThanString          = null;

        /* We must 1st preprocess the component parts */
        super.preprocess(
            numTables, outerFromList, outerSubqueryList, outerPredicateList);

        /* Don't try to optimize for (C)LOB type since it doesn't allow 
         * comparison.
         * RESOLVE: should this check be for LONG VARCHAR also?
         */
        if (receiver.getTypeId().getSQLTypeName().equals("CLOB")) 
        {
            return this;
        }

        /* No need to consider transformation if we already did transformation 
         * that added = * at bind time.
         */
        if (addedEquals)
        {
            return this;
        }


        /* if like pattern is not a constant and not a parameter, 
         * then can't optimize, eg. column LIKE column
         */
        if (!(leftOperand instanceof CharConstantNode) && 
                !(leftOperand.requiresTypeFromContext()))
        {
            return this;
        }

        /* This transformation is only worth doing if it is pushable, ie, if
         * the receiver is a ColumnReference.
         */
        if (!(receiver instanceof ColumnReference))
        {
            // We also do an early return here if in bindExpression we found 
            // we had a territory based Char and put a CAST above the receiver.
            //
            return this;
        }

        /* 
         * In first implementation of non default collation don't attempt
         * any transformations for LIKE.  
         *
         * Future possibilities:
         * o is it valid to produce a >= clause for a leading constant with
         *   a wildcard that works across all possible collations?  Is 
         *   c1 like a% the same as c1 like a% and c1 >= a'\u0000''\u0000',... ?
         *
         *   This is what was done for national char's.  It seems like a 
         *   given collation could sort: ab, a'\u0000'.  Is there any guarantee
         *   about the sort of the unicode '\u0000'.
         *
         * o National char's didn't try to produce a < than, is there a way
         *   in collation?
         */
        if (receiver.getTypeServices().getCollationType() != 
                StringDataValue.COLLATION_TYPE_UCS_BASIC)
        {
            // don't do any < or >= transformations for non default collations.
            return this;
        }

        /* This is where we do the transformation for LIKE to make it 
         * optimizable.
         * c1 LIKE 'asdf%' -> c1 LIKE 'asdf%' AND c1 >= 'asdf' AND c1 < 'asdg'
         * c1 LIKE ?       -> c1 LIKE ? and c1 >= ?
         *     where ? gets calculated at the beginning of execution.
         */

        // Build String constants if right side (pattern) is a constant
        if (leftOperand instanceof CharConstantNode)
        {
            String pattern = ((CharConstantNode) leftOperand).getString();

            if (!Like.isOptimizable(pattern))
            {
                return this;
            }

            int maxWidth = receiver.getTypeServices().getMaximumWidth();

            // DERBY-6477: Skip this optimization if the receiver column has
            // a very high maximum width (typically columns in the system
            // tables, as they don't have the same restrictions as columns
            // in user tables). Since greaterEqualString and lessThanString
            // are padded to the maximum width, this optimization may cause
            // OOME if the maximum width is high.
            if (maxWidth > Limits.DB2_LONGVARCHAR_MAXWIDTH) {
                return this;
            }

            greaterEqualString = 
                Like.greaterEqualString(pattern, escape, maxWidth);

            lessThanString          = 
                Like.lessThanString(pattern, escape, maxWidth);
            eliminateLikeComparison = 
                !Like.isLikeComparisonNeeded(pattern);
        }

        /* For some unknown reason we need to clone the receiver if it is
         * a ColumnReference because reusing them in Qualifiers for a scan
         * does not work.  
         */

        /* The transformed tree has to be normalized.  Either:
         *        AND                   AND
         *       /   \                 /   \
         *     LIKE   AND     OR:   LIKE   AND
         *           /   \                /   \
         *          >=    AND           >=    TRUE
         *               /   \
         *              <     TRUE
         * unless the like string is of the form CONSTANT%, in which
         * case we can do away with the LIKE altogether:
         *        AND                   AND
         *       /   \                 /   \
         *      >=   AND      OR:     >=  TRUE
         *          /   \
         *         <    TRUE
         */

        AndNode   newAnd   = null;
        ValueNode trueNode = new BooleanConstantNode(true, getContextManager());

        /* Create the AND <, if lessThanString is non-null or 
         * leftOperand is a parameter.
         */
        if (lessThanString != null || 
            leftOperand.requiresTypeFromContext())
        {
            ValueNode likeLTopt;
            if (leftOperand.requiresTypeFromContext())
            {
                // pattern string is a parameter 

                likeLTopt = 
                    setupOptimizeStringFromParameter(
                        leftOperand, 
                        rightOperand, 
                        "lessThanStringFromParameter", 
                        receiver.getTypeServices().getMaximumWidth());
            }
            else
            {
                // pattern string is a constant
                likeLTopt = 
                    new CharConstantNode(lessThanString, getContextManager());
            }

            BinaryComparisonOperatorNode lessThan = 
                new BinaryRelationalOperatorNode(
                    BinaryRelationalOperatorNode.K_LESS_THAN,
                    receiver.getClone(), 
                    likeLTopt,
                    false,
                    getContextManager());

            // Disable comparability checks
            lessThan.setForQueryRewrite(true);
            /* Set type info for the operator node */
            lessThan.bindComparisonOperator();

            // Use between selectivity for the <
            lessThan.setBetweenSelectivity();

            /* Create the AND */
            newAnd = new AndNode(lessThan, trueNode, getContextManager());

            newAnd.postBindFixup();
        }

        /* Create the AND >=.  Right side could be a CharConstantNode or a 
         * ParameterNode.
         */

        ValueNode likeGEopt;
        if (leftOperand.requiresTypeFromContext()) 
        {
            // the pattern is a ?, eg. c1 LIKE ?

            // Create an expression off the parameter
            // new SQLChar(Like.greaterEqualString(?));

            likeGEopt    = 
                setupOptimizeStringFromParameter(
                    leftOperand, 
                    rightOperand, 
                    "greaterEqualStringFromParameter", 
                    receiver.getTypeServices().getMaximumWidth());
        } 
        else 
        {
            // the pattern is a constant, eg. c1 LIKE 'Derby'

            likeGEopt = 
                new CharConstantNode(greaterEqualString, getContextManager());
        }

        // greaterEqual from (reciever LIKE pattern):
        //       >=
        //      /   \
        //  reciever pattern
        BinaryComparisonOperatorNode greaterEqual = 
            new BinaryRelationalOperatorNode(
                BinaryRelationalOperatorNode.K_GREATER_EQUALS,
                receiver.getClone(), 
                likeGEopt,
                false,
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
            newAnd = new AndNode(greaterEqual, trueNode, getContextManager());
        }
        else
        {
            newAnd = new AndNode(greaterEqual, newAnd, getContextManager());
        }
        newAnd.postBindFixup();

        /* Finally, we put an AND LIKE on top of the left deep tree, but
         * only if it is still necessary.
         */
        if (!eliminateLikeComparison)
        {
            newAnd = new AndNode(this, newAnd, getContextManager());
            newAnd.postBindFixup();
        }

        /* Mark this node as transformed so that we don't get
        * calculated into the selectivity multiple times.
        */
        setTransformed();

        return newAnd;
    }

    /**
     * Do code generation for this binary operator.
     *
     * This code was copied from BinaryOperatorNode and stripped down
     *
     * @param acb   The ExpressionClassBuilder for the class we're generating
     * @param mb    The method the code to place the code
     *
     *
     * @exception StandardException Thrown on error
     */
    @Override
    void generateExpression(
    ExpressionClassBuilder acb, MethodBuilder mb)
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
        ** (<receiver operand>).method(
        **     <left operand>, 
        **     <right operand>, 
        **     [<escaperightOp>,] 
        **     result field>)
        */

        receiver.generateExpression(acb, mb);   // first arg

        receiverInterfaceType = receiver.getTypeCompiler().interfaceName();

        mb.upCast(receiverInterfaceType);       // cast the method instance

        leftOperand.generateExpression(acb, mb);
        mb.upCast(leftInterfaceType);           // first arg with cast

        if (rightOperand != null)
        {
            rightOperand.generateExpression(acb, mb);
            mb.upCast(rightInterfaceType);      // second arg with cast
        }

        /* Figure out the result type name */
        // resultTypeName = getTypeCompiler().interfaceName();

        mb.callMethod(
            VMOpcode.INVOKEINTERFACE, 
            null, 
            methodName, 
            resultInterfaceType, 
            rightOperand == null ? 1 : 2);
    }

    private ValueNode setupOptimizeStringFromParameter(
    ValueNode   parameterNode, 
    ValueNode   escapeNode,
    String      methodName, 
    int         maxWidth)
        throws StandardException 
    {

        if (escapeNode != null)
        {
            methodName += "WithEsc";
        }

        StaticMethodCallNode methodCall = new StaticMethodCallNode(
                methodName,
                "org.apache.derby.iapi.types.Like",
                getContextManager());

        // using a method call directly, thus need internal sql capability
        methodCall.internalCall = true;

        NumericConstantNode maxWidthNode = new NumericConstantNode(
            TypeId.getBuiltInTypeId(Types.INTEGER),
            Integer.valueOf(maxWidth),
            getContextManager());

        ValueNode[] param = (escapeNode == null) ?
            new ValueNode[] { parameterNode, maxWidthNode } :
            new ValueNode[] { parameterNode, escapeNode, maxWidthNode };

        methodCall.addParms(Arrays.asList(param));

        ValueNode java2SQL = 
                new JavaToSQLValueNode(methodCall, getContextManager());

        java2SQL = java2SQL.bindExpression(null, null, null);

        CastNode likeOpt = new CastNode(
            java2SQL,
            parameterNode.getTypeServices(),
            getContextManager());

        likeOpt.bindCastNodeOnly();

        return likeOpt;
    }
}
