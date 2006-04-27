/*

   Derby - Class org.apache.derby.impl.sql.compile.Predicate

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

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.util.JBitSet;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * A Predicate represents a top level predicate.
 *
 * @author Jerry Brenner
 */

public final class Predicate extends QueryTreeNode implements OptimizablePredicate,
														Comparable
{
	/* Top of the predicate */
	AndNode		andNode;
	boolean		pushable;
	/* Bit map of referenced tables */
	JBitSet		referencedSet;
	/* Join clauses are placed into equivalence classes when applying transitive
	 * closure for join clauses.  This is useful for eliminating redundant predicates.
	 */
	int			equivalenceClass = -1;
	int			indexPosition;
	protected boolean startKey;
	protected boolean stopKey;
	protected boolean isQualifier;

	/* Hashtable used for tracking the search clause types that have been
	 * pushed through this predicate (if an equijoin) via transitive closure.
	 */
	private Hashtable searchClauseHT;

	// Whether or not this predicate has been scoped; see the
	// getPredScopedForResultSet() method of this class for more.
	private boolean scoped;

	/**
	 * Initializer.
	 *
	 * @param andNode		The top of the predicate	 
	 * @param referencedSet	Bit map of referenced tables
	 */

	public void init(Object andNode, Object referencedSet)
	{
		this.andNode = (AndNode) andNode;
		pushable = false;
		this.referencedSet = (JBitSet) referencedSet;
		scoped = false;
	}

	/*
	 *  Optimizable interface
	 */

	/**
	 * @see org.apache.derby.iapi.sql.compile.OptimizablePredicate#getReferencedMap
	 */
	public JBitSet getReferencedMap()
	{
		return referencedSet;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.OptimizablePredicate#hasSubquery
	 */
	public boolean hasSubquery()
	{
		/* RESOLVE - Currently, we record whether or not a predicate is pushable based
		 * on whether or not it contains a subquery or method call, but we do not
		 * record the underlying info.
		 */
		return ! pushable;
	}

	/**
	 * @see org.apache.derby.iapi.sql.compile.OptimizablePredicate#hasMethodCall
	 */
	public boolean hasMethodCall()
	{
		/* RESOLVE - Currently, we record whether or not a predicate is pushable based
		 * on whether or not it contains a subquery or method call, but we do not
		 * record the underlying info.
		 */
		return ! pushable;
	}

	/** @see OptimizablePredicate#markStartKey */
	public void markStartKey()
	{
		startKey = true;
	}

	/** @see OptimizablePredicate#isStartKey */
	public boolean isStartKey()
	{
		return startKey;
	}

	/** @see OptimizablePredicate#markStopKey */
	public void markStopKey()
	{
		stopKey = true;
	}

	/** @see OptimizablePredicate#isStopKey */
	public boolean isStopKey()
	{
		return stopKey;
	}

	/** @see OptimizablePredicate#markQualifier */
	public void markQualifier()
	{
		isQualifier = true;
	}

	/** @see OptimizablePredicate#isQualifier */
	public boolean isQualifier()
	{
		return isQualifier;
	}

	/** @see OptimizablePredicate#compareWithKnownConstant */
	public boolean compareWithKnownConstant(Optimizable optTable, boolean considerParameters)
	{
		boolean retval = false;
		RelationalOperator relop = getRelop();

		/* if this is for "in" operator node's dynamic start/stop key, relop is
		 * null, and it's not comparing with constant, beetle 3858
		 */
		if (relop == null)
			return false;

		if (relop.compareWithKnownConstant(optTable, considerParameters))
			retval = true;

		return retval;
	}

	public int hasEqualOnColumnList(int[] baseColumnPositions,
										Optimizable optTable)
		throws StandardException
	{
		RelationalOperator relop = getRelop();

		if (relop == null)
			return -1;
		
		if (!(relop.getOperator() == RelationalOperator.EQUALS_RELOP))
			return -1;
			
		for (int i = 0; i < baseColumnPositions.length; i++)
		{
			ColumnReference cr = relop.getColumnOperand(optTable, 
														baseColumnPositions[i]);
		
			if (cr == null)
				continue;
			
			if (relop.selfComparison(cr))
				continue;

			// If I made it thus far in the loop, we've found
			// something.
			return i;
		}
		
		return -1;
	}

	/**
	 * @see OptimizablePredicate#getCompareValue
	 *
	 * @exception StandardException		Thrown on error
	 */
	public DataValueDescriptor getCompareValue(Optimizable optTable)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(compareWithKnownConstant(optTable, true),
				"Cannot get the compare value if not comparing with a known constant.");
		}

		RelationalOperator relop = getRelop();

		return relop.getCompareValue(optTable);
	}

	/** @see OptimizablePredicate#equalsComparisonWithConstantExpression */
	public boolean equalsComparisonWithConstantExpression(Optimizable optTable)
	{
		RelationalOperator relop = getRelop();
		boolean retval = false;

		if (relop != null)
		{
			retval = relop.equalsComparisonWithConstantExpression(optTable);
		}

		return retval;
	}

	/** @see OptimizablePredicate#selectivity */
	public double selectivity(Optimizable optTable)
	{
		return andNode.getLeftOperand().selectivity(optTable);
	}

	/** @see OptimizablePredicate#getIndexPosition */
	public int getIndexPosition()
	{
		return indexPosition;
	}


	/* Comparable interface */

	public int compareTo(Object other)
	{
		Predicate	otherPred = (Predicate) other;

		/* Not all operators are "equal". If the predicates are on the
		 * same key column, then a "=" opertor takes precedence over all
		 * other operators.  This ensures that the "=" will be both the start
		 * and stop predicates.  Otherwise, we could end up with it being one
		 * but not the other and get incorrect results.
		 *
		 * Also, we want "<>" to come after all the other operators.
		 * Other parts of the optimizer use the first predicate on an index
		 * column to determine the cost of using the index, so we want the
		 * "<>" to come last because it's not useful for limiting the scan.
		 *
		 * In other words, P1 is before() P2 if:
		 *		o  The P1.indexPosition < P2.indexPosition
		 *	or  o  P1.indexPosition == P2.indexPosition and
		 *		   P1's operator is ("=" or IS NULL) and
		 *		   P2's operator is not ("=" or IS NULL)
		 * or	o  P1.indexPosition == P2.indexPosition and
		 *		   P1's operator is not ("<>" or IS NOT NULL) and
		 *		   P2's operator is ("<>" or IS NOT NULL)
		 *
		 * (We have to impose an arbitrary, but reproducible ordering
		 * on the the "=" predicates on the same column, otherwise an
		 * ASSERTion, that after the predicates are order, Pn+1 is not
		 * before() Pn, will be violated.
		 */

		int otherIndexPosition = otherPred.getIndexPosition();

		if (indexPosition < otherIndexPosition)
			return -1;

		if (indexPosition > otherIndexPosition)
			return 1;

		// initialize these flags as if they are for "in" operator, then
		// change them if they are not
		//
		boolean thisIsEquals = false, otherIsEquals = false;
		boolean thisIsNotEquals = true, otherIsNotEquals = true;

		if (getRelop() != null)		// this is not "in"
		{
			int thisOperator = ((RelationalOperator)andNode.getLeftOperand()).getOperator();
			thisIsEquals = (thisOperator == RelationalOperator.EQUALS_RELOP ||
								thisOperator == RelationalOperator.IS_NULL_RELOP);
			thisIsNotEquals = (thisOperator == RelationalOperator.NOT_EQUALS_RELOP ||
								   thisOperator == RelationalOperator.IS_NOT_NULL_RELOP);
		}
		if (otherPred.getRelop() != null)		// other is not "in"
		{
			int	otherOperator = ((RelationalOperator)(otherPred.getAndNode().getLeftOperand())).getOperator();
			otherIsEquals = (otherOperator == RelationalOperator.EQUALS_RELOP ||
								 otherOperator == RelationalOperator.IS_NULL_RELOP);
			otherIsNotEquals = (otherOperator == RelationalOperator.NOT_EQUALS_RELOP ||
								 otherOperator == RelationalOperator.IS_NOT_NULL_RELOP);
		}

		boolean thisIsBefore = (thisIsEquals && ! otherIsEquals) || ( ! thisIsNotEquals && otherIsNotEquals);
		if (thisIsBefore)
			return -1;

		boolean otherIsBefore = (otherIsEquals && ! thisIsEquals) || ( ! otherIsNotEquals && thisIsNotEquals);
		if (otherIsBefore)
			return 1;
		return 0;
	}

	/**
	 * Return the andNode.
	 *
	 * @return AndNode	The andNode.
	 */
	public AndNode getAndNode()
	{
		return andNode;
	}

	/**
	 * Set the andNode.
	 *
	 * @param andNode	The new andNode.
	 *
	 * @return Nothing.
	 */
	public void setAndNode(AndNode andNode)
	{
		this.andNode = andNode;
	}

	/**
	 * Return the pushable.
	 *
	 * @return boolean	Whether or not the predicate is pushable.
	 */
	public boolean getPushable()
	{
		return pushable;
	}

	/**
	 * Set whether or not this predicate is pushable.  This method
	 * is intended for use when creating a copy of the predicate, ex
	 * for predicate pushdown.  We choose not to add this assignment
	 * to copyFields() because the comments for that method say that
	 * it should copy all fields _except_ the two specified at init
	 * time; "pushable" is one of the two specified at init time.
	 *
	 * @param pushable Whether or not the predicate is pushable.
	 */
	public void setPushable(boolean pushable) {
		this.pushable = pushable;
	}

	/**
	 * Return the referencedSet.
	 *
	 * @return JBitSet	The referencedSet.
	 */
	public JBitSet getReferencedSet()
	{
		return referencedSet;
	}

	/**
	 * Set the equivalence class, if any, for this predicate.
	 *
	 * @param equivalenceClass	The equivalence class for this predicate.
	 *
	 * @return	Nothing.
	 */
	void setEquivalenceClass(int equivalenceClass)
	{
		this.equivalenceClass = equivalenceClass;
	}

	/**
	 * Get the equivalenceClass for this predicate.
	 *
	 * @return The equivalenceClass for this predicate.
	 */
	int getEquivalenceClass()
	{
		return equivalenceClass;
	}

	/**
	 * Categorize this predicate.  Initially, this means
	 * building a bit map of the referenced tables for each predicate.
	 *
	 * @return None.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void categorize() throws StandardException
	{
		pushable = andNode.categorize(referencedSet, false);
	}

	/**
	 * Get the RelationalOperator on the left side of the AND node, if
	 * there is one.  If the left side is not a RelationalOperator, return
	 * null.
	 *
	 * @return	The RelationalOperator on the left side of the AND node,
	 *			if any.
	 */
	public RelationalOperator getRelop()
	{
		
		if (andNode.getLeftOperand() instanceof RelationalOperator)
		{
			return (RelationalOperator) andNode.getLeftOperand();
		}
		else
		{
			return null;
		}
	}

	public final boolean isOrList()
    {
        return(andNode.getLeftOperand() instanceof OrNode);
    }

    /**
     * Is this predicate a possible Qualifier for store?
     * <p>
     * Current 2 types of predicates can be pushed to store: 
     *   1) RelationalOperator - 
     *      represented with by left operand as instance of RelationalOperator.
     *   2) A single And'd term of a list of OR terms
     *      represented by left operand as instance of OrNode.
     *
     * More checking specific operator's terms to see if they are finally
     * pushable to store.  In the final push at execution each term of the AND 
     * or OR must be a Relational operator with a column reference on one side 
     * and a constant on the other.
     *
     *
	 * @return true if term is wither a AND of a RelationalOperator, or an
     *              OR of one or more Relational Operators.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public final boolean isStoreQualifier()
    {
		if ((andNode.getLeftOperand() instanceof RelationalOperator) ||
		    (andNode.getLeftOperand() instanceof OrNode))
		{
            return(true);
		}
		else
		{
            return(false);
		}
    }

    /**
     * Is this predicate an pushable OR list?
     * <p>
     * Does the predicate represent a AND'd list of OR term's, all of which
     * are pushable.  To be pushable each of OR terms must be a legal 
     * qualifier, which is a column reference on one side of a Relational
     * operator and a constant on the other.
     *
	 * @return true if the predicate is a pushable set of OR clauses.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public final boolean isPushableOrClause(Optimizable optTable)
        throws StandardException
	{
        boolean ret_val = true;

        if (andNode.getLeftOperand() instanceof OrNode)
        {
            QueryTreeNode node = andNode.getLeftOperand();

            while (node instanceof OrNode)
            {
                OrNode or_node = (OrNode) node;

                if (or_node.getLeftOperand() instanceof RelationalOperator)
                {
                    // if any term of the OR clause is not a qualifier, then
                    // reject the entire OR clause.
                    if (!((RelationalOperator) or_node.getLeftOperand()).isQualifier(optTable))
                    {
                        // one of the terms is not a pushable Qualifier.
                        return(false);
                    }

                    node = or_node.getRightOperand();
                }
                else
                {
                    // one of the terms is not a RelationalOperator

                    return(false);
                }
            }

            return(true);
        }
        else
        {
            // Not an OR list
            return(false);
        }
	}

	/**
	 * Return whether or not this predicate has been used
	 * to add a new search clause of the specified type via transitive closure.
	 * NOTE: This can only be true if this is an equijoin
	 * between 2 column references.
	 *
	 * @param ro	The search clause that we are currently considering
	 *				as the source for transitive closure
	 *
	 * @return	Whether or not this predicate has been used
	 *			to add a new search clause of the specified type via transitive 
     *			closure.
	 */
	boolean transitiveSearchClauseAdded(RelationalOperator ro)
	{
		if (searchClauseHT == null || 
			searchClauseHT.get(new Integer(ro.getOperator())) == null)
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	/**
	 * Mark this predicate as having been used to add a new predicate
	 * of the specified type via transitive closure on search clauses.
	 *
	 * @param ro	The search clause that we are currently considering
	 *				as the source for transitive closure
	 *
	 * @return Nothing.
	 */
	void setTransitiveSearchClauseAdded(RelationalOperator ro)
	{
		if (searchClauseHT == null)
		{
			searchClauseHT = new Hashtable();
		}
		/* I have to remember that this ro has been added to this predicate as a
		 * transitive search clause.
		 */
		Integer i = new Integer(ro.getOperator());
		searchClauseHT.put(i, i);
	}

	/**
	 * Get the start operator for this predicate for a scan.
	 *
	 * @param optTable	The optimizable table, so we can tell which side of
	 *					the operator the search column is on.
	 *
	 * @return	The start operator for a start key on this column.
	 */
	int getStartOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(startKey, "Getting a start operator from a Predicate that's not a start key.");
		}

		/* if it's for "in" operator's dynamic start key, operator is GE,
		 * beetle 3858
		 */
		if (andNode.getLeftOperand() instanceof InListOperatorNode)
			return ScanController.GE;

		return getRelop().getStartOperator(optTable);
	}

	int getStopOperator(Optimizable optTable)
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(stopKey, "Getting a stop operator from a Predicate that's not a stop key.");
		}

		 /* if it's for "in" operator's dynamic stop key, operator is GT,
		  * beetle 3858
		  */
		if (andNode.getLeftOperand() instanceof InListOperatorNode)
			return ScanController.GT;

		return getRelop().getStopOperator(optTable);
	}

	/**
	 * Set the position of the index column that this predicate restricts
	 *
	 * @param indexPosition	The position of the index column that this
	 *						predicate restricts.
	 */
	void setIndexPosition(int indexPosition)
	{
		this.indexPosition = indexPosition;
	}

	/**
	 * Clear the start/stop position and qualifier flags
	 *
	 * @return Nothing.
	 */
	void clearScanFlags()
	{
		startKey = false;
		stopKey = false;
		isQualifier = false;
	}

	/**
	 * Clear the qualifier flag.
	 *
	 * @return Nothing.
	 */
	void clearQualifierFlag()
	{
		isQualifier = false;
	}

	void generateExpressionOperand(Optimizable optTable,
										int columnPosition,
										ExpressionClassBuilder acb,
										MethodBuilder mb)
				throws StandardException
	{
		getRelop().generateExpressionOperand(optTable,
													columnPosition,
													acb,
													mb);
	}

	void generateAbsoluteColumnId(MethodBuilder mb,
										Optimizable optTable)
	{
		getRelop().generateAbsoluteColumnId(mb, optTable);
	}

	void generateRelativeColumnId(MethodBuilder mb,
										Optimizable optTable)
	{
		getRelop().generateRelativeColumnId(mb, optTable);
	}

	void generateOperator(MethodBuilder mb,
								Optimizable optTable)
	{
		getRelop().generateOperator(mb, optTable);
	}

	void generateQualMethod(ExpressionClassBuilder acb,
								MethodBuilder mb,
								Optimizable optTable)
					throws StandardException
	{
		getRelop().generateQualMethod(acb, mb, optTable);
	}

	void generateOrderedNulls(MethodBuilder mb)
	{
		getRelop().generateOrderedNulls(mb);
	}

	void generateNegate(MethodBuilder mb,
								Optimizable optTable)
	{
		getRelop().generateNegate(mb, optTable);
	}

	void generateOrderableVariantType(MethodBuilder mb,
								Optimizable optTable)
					throws StandardException
	{
		int variantType = getRelop().getOrderableVariantType(optTable);
		mb.push(variantType);

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
			return binaryRelOpColRefsToString() + "\nreferencedSet: " +
				referencedSet  + "\n" + "pushable: " + pushable + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Get a string version of the column references for this predicate
	 * IF it's a binary relational operator.  We only print out the
	 * names of the operands if they are column references; otherwise
	 * we just print a dummy value.  This is for debugging purposes
	 * only--it's a convenient way to see what columns the predicate
	 * is referencing, especially when tracing through code and printing
	 * assert failure.
	 */
	public String binaryRelOpColRefsToString()
	{
		// We only consider binary relational operators here.
		if (!(getAndNode().getLeftOperand()
			instanceof BinaryRelationalOperatorNode))
		{
			return "";
		}

		final String DUMMY_VAL = "<expr>";
		java.lang.StringBuffer sBuf = new java.lang.StringBuffer();
		BinaryRelationalOperatorNode opNode =
			(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

		// Get left operand's name.
		if (opNode.getLeftOperand() instanceof ColumnReference)
		{
			sBuf.append(
				((ColumnReference)opNode.getLeftOperand()).getTableName() +
				"." +
				((ColumnReference)opNode.getLeftOperand()).getColumnName()
			);
		}
		else
			sBuf.append(DUMMY_VAL);

		// Get the operator type.
		sBuf.append(" " + opNode.operator + " ");

		// Get right operand's name.
		if (opNode.getRightOperand() instanceof ColumnReference) {
			sBuf.append(
				((ColumnReference)opNode.getRightOperand()).getTableName() +
				"." +
				((ColumnReference)opNode.getRightOperand()).getColumnName()
			);
		}
		else
			sBuf.append(DUMMY_VAL);

		return sBuf.toString();
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
			printLabel(depth, "andNode: ");
			andNode.treePrint(depth + 1);
			super.printSubNodes(depth);
		}
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
		if (v.skipChildren(this))
		{
			return v.visit(this);
		}

		Visitable returnNode = super.accept(v);

		if (andNode != null && !v.stopTraversal())
		{
			andNode = (AndNode)andNode.accept(v);
		}

		return returnNode;
	}

	/**
	 * Copy all fields of this Predicate (except the two that
	 * are set from 'init').
	 *
	 */

	public void copyFields(Predicate otherPred) {

		this.equivalenceClass = otherPred.getEquivalenceClass();
		this.indexPosition = otherPred.getIndexPosition();
		this.startKey = otherPred.isStartKey();
		this.stopKey = otherPred.isStopKey();
		this.isQualifier = otherPred.isQualifier();
		this.searchClauseHT = otherPred.getSearchClauseHT();

	}

	/**
	 * Get the search clause Hash Table.
	 */

	public Hashtable getSearchClauseHT() {
		return searchClauseHT;
	}

	/**
	 * Determine whether or not this predicate is eligible for
	 * push-down into subqueries.  Right now the only predicates
	 * we consider to be eligible are those which 1) are Binary
	 * Relational operator nodes, 2) have a column reference
	 * on BOTH sides, and 3) have column references such that
	 * each column reference has a reference to a base table
	 * somewhere beneath it.
	 *
	 * @return Whether or not this predicate is eligible to be
	 *  pushed into subqueries.
	 */
	protected boolean pushableToSubqueries()
		throws StandardException
	{
		// If the predicate isn't a binary relational operator,
		// then we don't push it.
		if (!(getAndNode().getLeftOperand()
			instanceof BinaryRelationalOperatorNode))
		{
			return false;
		}

		BinaryRelationalOperatorNode opNode =
			(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

		// If either side is not a column reference, we don't push.
		if (!((opNode.getLeftOperand() instanceof ColumnReference) &&
			(opNode.getRightOperand() instanceof ColumnReference)))
		{
			return false;
		}

		// Make sure both column references ultimately point to base
		// tables.  If, for example, either column reference points to a
		// a literal or an aggregate, then we do not push the predicate.
		// This is because pushing involves remapping the references--
		// but if the reference doesn't have a base table beneath it,
		// the notion of "remapping" it doesn't (seem to) apply.  RESOLVE:
		// it might be okay to make the "remap" operation a no-op for
		// such column references, but it's not clear whether that's
		// always a safe option; further investigation required.

		JBitSet tNums = new JBitSet(getReferencedSet().size());
		BaseTableNumbersVisitor btnVis = new BaseTableNumbersVisitor(tNums);
		opNode.getLeftOperand().accept(btnVis);
		if (tNums.getFirstSetBit() == -1)
			return false;

		tNums.clearAll();
		opNode.getRightOperand().accept(btnVis);
		if (tNums.getFirstSetBit() == -1)
			return false;

		return true;
	}

	/**
	 * If this predicate's operator is a BinaryRelationalOperatorNode,
	 * then look at the operands and return a new, equivalent predicate
	 * that is "scoped" to the received ResultSetNode.  By "scoped" we
	 * mean that the operands, which shold be column references, have been
	 * mapped to the appropriate result columns in the received RSN.
	 * This is useful for pushing predicates from outer queries down
	 * into inner queries, in which case the column references need
	 * to be remapped.
	 *
	 * For example, let V1 represent
	 *
	 *    select i,j from t1 UNION select i,j from t2
	 * 
	 * and V2 represent
	 *
	 *    select a,b from t3 UNION select a,b from t4
	 * 
	 * Then assume we have the following query:
	 *
	 *    select * from V1, V2 where V1.j = V2.b
	 *
	 * Let's further assume that this Predicate object represents the
	 * "V1.j = V2.b" operator and that the childRSN we received
	 * as a parameter represents one of the subqueries to which we
	 * want to push the predicate; let's say it's:
	 *
	 *    select i,j from t1
	 *
	 * Then this method will return a new predicate whose binary
	 * operator represents the expression "T1.j = V2.b" (that is, V1.j
	 * will be mapped to the corresponding column in T1).  For more on
	 * how that mapping is made, see the "getScopedOperand()" method
	 * in BinaryRelationalOperatorNode.java.
	 *
	 * ASSUMPTION: We should only get to this method if we know that
	 * at least one operand in this predicate can and should be mapped
	 * to the received childRSN.  For an example of where that check is
	 * made, see the pushOptPredicate() method in SetOperatorNode.java.
	 *
	 * @param parentRSNsTables Set of all table numbers referenced by
	 *  the ResultSetNode that is _parent_ to the received childRSN.
	 *  We need this to make sure we don't scope the operands to a
	 *  ResultSetNode to which they don't apply.
	 * @param childRSN The result set node for which we want to create
	 *  a scoped predicate.
	 * @return A new predicate whose operands have been scoped to the
	 *  received childRSN.
	 */
	protected Predicate getPredScopedForResultSet(
		JBitSet parentRSNsTables, ResultSetNode childRSN)
		throws StandardException
	{
		// We only deal with binary relational operators here.
		if (!(getAndNode().getLeftOperand()
			instanceof BinaryRelationalOperatorNode))
		{
			return this;
		}

		// The predicate must have an AndNode in CNF, so we
		// need to create an AndNode representing:
		//    <scoped_bin_rel_op> AND TRUE
		// First create the boolean constant for TRUE.
		ValueNode trueNode = (ValueNode) getNodeFactory().getNode(
			C_NodeTypes.BOOLEAN_CONSTANT_NODE,
			Boolean.TRUE,
			getContextManager());

		BinaryRelationalOperatorNode opNode =
			(BinaryRelationalOperatorNode)getAndNode().getLeftOperand();

		// Create a new op node with left and right operands that point
		// to the received result set's columns as appropriate.
		BinaryRelationalOperatorNode newOpNode = 
			(BinaryRelationalOperatorNode) getNodeFactory().getNode(
				opNode.getNodeType(),
				opNode.getScopedOperand(
					BinaryRelationalOperatorNode.LEFT,
					parentRSNsTables,
					childRSN),
				opNode.getScopedOperand(
					BinaryRelationalOperatorNode.RIGHT,
					parentRSNsTables,
					childRSN),
				getContextManager());

		// Bind the new op node.
		newOpNode.bindComparisonOperator();

		// Create and bind a new AND node in CNF form,
		// i.e. "<newOpNode> AND TRUE".
		AndNode newAnd = (AndNode) getNodeFactory().getNode(
			C_NodeTypes.AND_NODE,
			newOpNode,
			trueNode,
			getContextManager());
		newAnd.postBindFixup();

		// Categorize the new AND node; among other things, this
		// call sets up the new operators's referenced table map,
		// which is important for correct pushing of the new
		// predicate.
		JBitSet tableMap = new JBitSet(
			childRSN.getReferencedTableMap().size());
		newAnd.categorize(tableMap, false);

		// Now put the pieces together to get a new predicate.
		Predicate newPred = (Predicate) getNodeFactory().getNode(
			C_NodeTypes.PREDICATE,
			newAnd,
			tableMap,
			getContextManager());

		// Copy all of this predicates other fields into the new predicate.
		newPred.clearScanFlags();
		newPred.copyFields(this);
		newPred.setPushable(getPushable());

		// Take note of the fact that the new predicate is scoped for
		// the sake of pushing; we need this information during optimization
		// to figure out what we should and should not "pull" back up.
		newPred.markAsScopedForPush();
		return newPred;
	}

	/**
	 * Indicate that this predicate is a scoped copy of some other
	 * predicate (i.e. it was created as the result of a call to
	 * getPredScopedForResultSet() on some other predicate).
	 */
	protected void markAsScopedForPush() {
		this.scoped = true;
	}

	/**
	 * Return whether or not this predicate is a scoped copy of
	 * another predicate.
	 */
	protected boolean isScopedForPush() {
		return scoped;
	}

}
