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
			return "referencedSet: " + referencedSet  + "\n" +
			   "pushable: " + pushable + "\n" +
				super.toString();
		}
		else
		{
			return "";
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

}
