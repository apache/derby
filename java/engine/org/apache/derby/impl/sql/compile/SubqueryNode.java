/*

   Derby - Class org.apache.derby.impl.sql.compile.SubqueryNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.sql.Row;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import java.lang.reflect.Modifier;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.OnceResultSet;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;
import java.util.Vector;

/**
 * A SubqueryNode represents a subquery.  Subqueries return values to their
 * outer queries. An quantified subquery is one that appears under a quantified
 * operator (like IN or EXISTS) - quantified subqueries can return more than
 * one value per invocation. An expression subquery is one that is not directly
 * under a quantified operator - expression subqueries are allowed to return
 * at most one value per invocation (returning no value is considered to be
 * equivalent to returning NULL).
 *
 * There are a large number of subquery types.  Because of the large number of
 * types, and the large amount of shared code, we have decided to have 1 SubqueryNode
 * without any subclasses.  The subquery type (and operator) is encoded in the
 * subqueryType field.
 *
 * The query optimizer is responsible for optimizing subqueries, and also for
 * transforming them so that code can be generated for them. The optimizer may
 * eliminate some subqueries by transforming them into joins, or it may
 * change the internal form of a subquery (for example, transforming
 * 'where x in (select y from z where ...)' into
 * 'where (select true from z where x = y and ...)').
 *
 * Note that aggregates present some additional issues.  A transformation
 * such as:
 *	<UL> where x in (SELECT <I>expression</I> FROM z) </UL>
 * has to be treated specially if <I>expression</I> has an aggregate.
 * We change it to:
 *	<UL> where x = (SELECT true FROM (SELECT MAX(x) FROM z) WHERE SQLCOL1 = y) </UL>
 *
 * @author Jeff Lichtman
 */

public class SubqueryNode extends ValueNode
{
	/*
	** This must be a single-column result set.  If the subquery is
	** not quantified, it must also be a single-row result set - that is,
	** expression subqueries are allowed to return only a single value
	** per invocation.
	** NOTE: SubqueryNodes are used as an intermediate step within the parser
	** for building a derived table.  Derived tables can be multi-column and 
	** multi-table.
	*/
	ResultSetNode	resultSet;

	/* Type of this subquery */
	int				subqueryType;

	/* Whether or not this subquery is immediately under a top level AndNode.
	 * (Important for subquery flattening.)
	 */
	boolean			underTopAndNode;

	/* Whether or not we've been preprocessed. (Only do the work once.) */
	boolean			preprocessed;

	/* Whether or not this subquery began life as a distinct expression subquery */
	boolean			distinctExpression;

	/* Since we do not have separate subquery operator nodes, the
	 * type of the subquery is stored in the subqueryType field.  Most subquery
	 * types take a left operand (except for expression and exists).  We could
	 * either add a leftOperand field here or subclass SubqueryNode for those
	 * types that take a left operand.  We have decided to add the left operand
	 * here for now.
	 */
	ValueNode		leftOperand;
	boolean			pushedNewPredicate;

	/* Expression subqueries on the right side of a BinaryComparisonOperatorNode
	 * will get passed a pointer to that node prior to preprocess().  This
	 * allows us to replace the entire comparison, if we want to, when
	 * flattening.
	 */
	BinaryComparisonOperatorNode parentComparisonOperator;

	/* Private fields (all references via private methods) - 
	 * We reuse true BooleanConstantNodes within
	 * this class, creating them on the first reference.
	 */
	private BooleanConstantNode trueNode;
	/* Reuse generated code where possible */
	//private Expression genResult;

	/* Subquery # for this subquery */
	private int subqueryNumber = -1;

	/* ResultSet # for the point of attachment for this subquery */
	private int pointOfAttachment = -1;

	/* 
	** Indicate whether we found a correlation or not.
	** And track whether we have checked yet.
	*/
	private boolean foundCorrelation;
	private boolean doneCorrelationCheck;

	/* 
	** Indicate whether we found an invariant node
	** below us or not. And track whether we have 
	** checked yet.
	*/
	private boolean foundVariant;
	private boolean doneInvariantCheck;

	/* Subquery types.
	 * NOTE: FROM_SUBQUERY only exists for a brief second in the parser.  It
	 * should never appear in a query tree.
	 * NOTE: NOT EXISTS and NOT IN subquery types do not exist prior to NOT 
	 * elimination during preprocessing.  Prior to that, there is a separate
	 * NotNode above the SubqueryNode in the tree.
	 *
	 */
	public final static int NOTIMPLEMENTED_SUBQUERY		= -1;
	public final static int FROM_SUBQUERY	    = 0;
	public final static int IN_SUBQUERY			= 1;
	public final static int NOT_IN_SUBQUERY		= 2;
	public final static int EQ_ANY_SUBQUERY		= 3;
	public final static int EQ_ALL_SUBQUERY		= 4;
	public final static int NE_ANY_SUBQUERY		= 5;
	public final static int NE_ALL_SUBQUERY		= 6;
	public final static int GT_ANY_SUBQUERY		= 7;
	public final static int GT_ALL_SUBQUERY		= 8;
	public final static int GE_ANY_SUBQUERY		= 9;
	public final static int GE_ALL_SUBQUERY		= 10;
	public final static int LT_ANY_SUBQUERY		= 11;
	public final static int LT_ALL_SUBQUERY		= 12;
	public final static int LE_ANY_SUBQUERY		= 13;
	public final static int LE_ALL_SUBQUERY		= 14;
	public final static int EXISTS_SUBQUERY		= 15;
	public final static int NOT_EXISTS_SUBQUERY	= 16;
	public final static int EXPRESSION_SUBQUERY = 17;


	/**
	 * Initializer.
	 *
	 * @param resultSet		The ResultSetNode for the subquery
	 * @param subqueryType	The type of the subquery
	 * @param leftOperand	The left operand, if any, of the subquery
	 */

	public void init(
							Object resultSet,
							Object subqueryType,
							Object leftOperand)
	{
		this.resultSet = (ResultSetNode) resultSet;
		this.subqueryType = ((Integer) subqueryType).intValue();

		/* Subqueries are presumed not to be under a top level AndNode by
		 * default.  This is because expression normalization only recurses
		 * under Ands and Ors, not under comparison operators, method calls,
		 * built-in functions, etc.
		 */
		underTopAndNode = false;
		this.leftOperand = (ValueNode) leftOperand;
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
			return "subqueryType: " + subqueryType + "\n" +
			   "underTopAndNode: " + underTopAndNode + "\n" +
			   "subqueryNumber: " + subqueryNumber + "\n" +
			   "pointOfAttachment: " + pointOfAttachment + "\n" +
			   "preprocessed: " + preprocessed + "\n" +
			   "distinctExpression: " + distinctExpression + "\n" +
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
			super.printSubNodes(depth);

			if (resultSet != null)
			{
				printLabel(depth, "resultSet: ");
				resultSet.treePrint(depth + 1);
			}

			if (leftOperand != null)
			{
				printLabel(depth, "leftOperand: ");
				leftOperand.treePrint(depth + 1);
			}
		}
	}

	/**
	 * Return the resultSet for this SubqueryNode.
	 *
	 * @return ResultSetNode underlying this SubqueryNode.
	 */
	public ResultSetNode getResultSet()
	{
		return resultSet;
	}

	/**
	 * Return the type of this subquery.
	 *
	 * @return int	Type of this subquery.
	 */
	public int getSubqueryType()
	{
		return subqueryType;
	}

	/**
	 * Set the type of this subquery.
	 *
	 * @param Type of this subquery.
	 *
	 * @return None.
	 */
	public void setSubqueryType(int subqueryType)
	{
		this.subqueryType = subqueryType;
	}

	/**
	 * Set the point of attachment of this subquery.
	 *
	 * @param pointOfAttachment	The point of attachment of this subquery.
	 *
	 * @return None.
	 *
	 * @exception StandardException			Thrown on error
	 */
	public void setPointOfAttachment(int pointOfAttachment)
		throws StandardException
	{
		/* Materialized subqueries always keep their point of
		 * attachment as -1.
		 */
		if (! isMaterializable())
		{
			this.pointOfAttachment = pointOfAttachment;
		}
	}

	/**
	 * Return whether or not this subquery is immediately under a top level
	 * AndNode.
	 *
	 * @return boolean	Whether or not this subquery is immediately under a
	 *					top level AndNode.
	 */
	public boolean getUnderTopAndNode()
	{
		return underTopAndNode;
	}

	/**
	 * Get the ResultSet # for the point of attachment for this SubqueryNode.
	 *
	 * @return int		The ResultSet # for the point of attachment
	 */
	public int getPointOfAttachment()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(pointOfAttachment >= 0,
				"pointOfAttachment expected to be >= 0");
		}
		return pointOfAttachment;
	}

	/**
	 * Get whether or not this SubqueryNode has already been
	 * preprocessed.
	 * 
	 * @return	Whether or not this SubqueryNode has already been
	 *			preprocessed.
	 */
	boolean getPreprocessed()
	{
		return preprocessed;
	}

	/**
	 * Set the parent BCON.  Useful when considering flattening
	 * expression subqueries.
	 *
	 * @param parent	The parent BCON.
	 *
	 * @return Nothing.
	 */
  	void setParentComparisonOperator(BinaryComparisonOperatorNode parent)
  	{
  		parentComparisonOperator = parent;
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
		/* We need to remap both the SELECT and Predicate lists 
		 * since there may be correlated columns in either of them.
		 */
		if (resultSet instanceof SelectNode)
		{
			ResultColumnList selectRCL = resultSet.getResultColumns();
			SelectNode		 select = (SelectNode) resultSet;
			PredicateList	 selectPL = select.getWherePredicates();

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(selectPL != null,
					"selectPL expected to be non-null");
			}
			selectRCL.remapColumnReferencesToExpressions();
			selectPL.remapColumnReferencesToExpressions();
		}
		return this;
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 *
	 * @param fromList			The FROM list for the query this
	 *							expression is in, for binding columns.
	 *							NOTE: fromList will be null if the subquery appears
	 *							in a VALUES clause.
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
		ResultColumnList	resultColumns;

		//check if subquery is allowed in expression tree
		checkReliability( CompilerContext.SUBQUERY_ILLEGAL, SQLState.LANG_SUBQUERY );

		resultColumns = resultSet.getResultColumns();

		/* The parser does not enforce the fact that a subquery can only return
		 * a single column, so we must check here.
		 */
		if (resultColumns.size() != 1)
		{
			throw StandardException.newException(SQLState.LANG_NON_SINGLE_COLUMN_SUBQUERY);
		}

		/* Verify the usage of "*" in the select list:
		 *	o  Only valid in EXISTS subqueries
		 *	o  If the AllResultColumn is qualified, then we have to verify
		 *	   that the qualification is a valid exposed name.
		 *	   NOTE: The exposed name can come from an outer query block.
		 */
		resultSet.verifySelectStarSubquery(fromList, subqueryType);

		/* For an EXISTS subquery:
		 *	o  If the SELECT list is a "*", then we convert it to a true.
		 *	   (We need to do the conversion since we don't want the "*" to
		 *	   get expanded.)
		 *  o  We then must bind the expression under the SELECT list to
		 *	   verify that it is a valid expression.  (We must do this as a
		 *	   separate step because we need to validate the expression and
		 *	   we need to handle EXISTS (select * ... union all select 1 ...)
		 *	   without getting a type compatability error.)
		 *	o  Finally, we convert the expression to a SELECT true.
		 */
		if (subqueryType == EXISTS_SUBQUERY)
		{
			/* Transform the * into true (EXISTS). */
			resultSet.setResultToBooleanTrueNode(true);
		}

		/* We need to bind the tables before we can bind the target list
		 * (for exists subqueries).  However, we need to wait until after
		 * any *'s have been replaced, so that they don't get expanded.
		 */
		CompilerContext cc = getCompilerContext();

		resultSet = resultSet.bindNonVTITables(getDataDictionary(), fromList);
		resultSet = resultSet.bindVTITables(fromList);

		/* Set the subquery # for this SubqueryNode */
		if (subqueryNumber == -1)
			subqueryNumber = cc.getNextSubqueryNumber();

		/* reject ? parameters in the select list of subqueries */
		resultSet.rejectParameters();

		if (subqueryType == EXISTS_SUBQUERY)
		{
			/* Bind the expression in the SELECT list */
			resultSet.bindTargetExpressions(fromList);

			/* Transform the ResultColumn into true.
			 * NOTE: This may be a 2nd instance of the same transformation for
			 * an EXISTS (select * ...), since we had to transform the 
			 * AllResultColumn above, but we have to also handle
			 * EXISTS (select r from s ...)
			 */
			resultSet.setResultToBooleanTrueNode(false);
		}

		/* bind the left operand, if there is one */
		if (leftOperand != null)
		{
			leftOperand = leftOperand.bindExpression(fromList, subqueryList,
									   aggregateVector);
		}

		/* bind the expressions in the underlying subquery */
		resultSet.bindExpressions(fromList);

		resultSet.bindResultColumns(fromList);

		/* We need to reset resultColumns since the underlying resultSet may
		 * be a UNION (and UnionNode.bindResultColumns() regens a new RCL).
		 */
		resultColumns = resultSet.getResultColumns();

		/*
		 * A ? parameter to the left of this subquery gets type of the
		 * subquery's sole column.
		 */
		if (leftOperand != null && leftOperand.isParameterNode())
		{
			((ParameterNode) leftOperand).setDescriptor(
				((ResultColumn) resultColumns.elementAt(0)).getTypeServices());
		}

		// Set the DataTypeServices
		setDataTypeServices(resultColumns);

		/* Add this subquery to the subquery list */
		subqueryList.addSubqueryNode(this);

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
		/* Only preprocess this node once.  We may get called multiple times
		 * due to tree transformations.
		 */
		if (preprocessed)
		{
			return this;
		}
		preprocessed = true;

		boolean		flattenable;
		ValueNode	topNode = this;

		resultSet = resultSet.preprocess(numTables, null, (FromList) null);

		// Eliminate any unnecessary DISTINCTs
		if (resultSet instanceof SelectNode)
		{
			if (((SelectNode) resultSet).hasDistinct())
			{
				((SelectNode) resultSet).clearDistinct();
				/* We need to remember to check for single unique value
				 * at execution time for expression subqueries.
				 */
				if  (subqueryType == EXPRESSION_SUBQUERY)
				{
					distinctExpression = true;
				}
			}
		}

		/* Lame transformation - For IN/ANY subqueries, if
		 * result set is guaranteed to return at most 1 row
		 * and it is not correlated
		 * then convert the subquery into the matching expression
		 * subquery type.  For example:
		 *	c1 in (select min(c1) from t2)
		 * becomes:
		 *	c1 = (select min(c1) from t2)
		 * (This actually showed up in an app that a potential customer
		 * was porting from SQL Server.)
		 * The transformed query can then be flattened if appropriate.
		 */
		if ((isIN() || isANY()) &&
			resultSet.returnsAtMostOneRow())
		{
			if (! hasCorrelatedCRs())
			{
				changeToCorrespondingExpressionType();
			}
		}

		/* NOTE: Flattening occurs before the pushing of
		 * the predicate, since the pushing will add a node 
		 * above the SubqueryNode.
		 */

		/* Values subquery is flattenable if:
		 *  o It is not under an OR.
		 *  o It is an expression subquery on the right side
		 *	  of a BinaryComparisonOperatorNode.
		 */
		flattenable = (resultSet instanceof RowResultSetNode) &&
					  underTopAndNode &&
					  parentComparisonOperator instanceof BinaryComparisonOperatorNode;
		if (flattenable)
		{
			/* If we got this far and we are an expression subquery
			 * then we want to set leftOperand to be the left side
			 * of the comparison in case we pull the comparison into
			 * the flattened subquery.
			 */
			leftOperand = parentComparisonOperator.getLeftOperand();
			// Flatten the subquery
			RowResultSetNode rrsn = (RowResultSetNode) resultSet;
			FromList   fl = new FromList();

			// Remove ourselves from the outer subquery list
			outerSubqueryList.removeElement(this);

			/* We only need to add the table from the subquery into 
			 * the outer from list if the subquery itself contains
			 * another subquery.  Otherwise, it just becomes a constant.
			 */
			if (rrsn.subquerys.size() != 0)
			{
				fl.addElement(rrsn);
				outerFromList.destructiveAppend(fl);
			}

			/* Append the subquery's subquery list to the 
			 * outer subquery list.
			 */
			outerSubqueryList.destructiveAppend(rrsn.subquerys);

			/* return the new join condition 
			 * If we are flattening an EXISTS then there is no new join
			 * condition since there is no leftOperand.  Simply return
			 * TRUE.
			 *
			 * NOTE: The outer where clause, etc. has already been normalized,
			 * so we simply return the BinaryComparisonOperatorNode above
			 * the new join condition.
			 */
			ValueNode rightOperand;
			rightOperand = ((ResultColumn) rrsn.getResultColumns().elementAt(0)).
								getExpression();
			return getNewJoinCondition(leftOperand, rightOperand);
		}

		/* Select subquery is flattenable if:
		 *  o It is not under an OR.
		 *  o The subquery type is IN, ANY or EXISTS or
		 *    an expression subquery on the right side
		 *	  of a BinaryComparisonOperatorNode.
		 *  o There are no aggregates in the select list
		 *  o There is no group by clause
		 *  o There is a uniqueness condition that ensures
		 *	  that the flattening of the subquery will not
		 *	  introduce duplicates into the result set.
		 *
		 *	OR,
		 *  o The subquery is NOT EXISTS, NOT IN, ALL (beetle 5173).
		 */
		boolean flattenableNotExists = (isNOT_EXISTS() || canAllBeFlattened());

		flattenable = (resultSet instanceof SelectNode) &&
					  underTopAndNode &&
					  (isIN() || isANY() || isEXISTS() || flattenableNotExists ||
                       parentComparisonOperator != null);

		if (flattenable)
		{
			SelectNode	select = (SelectNode) resultSet;
			if ((select.getAggregateVector(IN_SELECT_LIST).size() == 0) &&
				(! select.getGeneratedForGroupbyClause()))
			{
				ValueNode origLeftOperand = leftOperand;

				/* Check for uniqueness condition. */
				/* Is the column being returned by the subquery
				 * a candidate for an = condition?
				 */
				boolean additionalEQ =
							(subqueryType == IN_SUBQUERY) ||
							(subqueryType == EQ_ANY_SUBQUERY);


				additionalEQ = additionalEQ &&
								((leftOperand instanceof ConstantNode) ||
								 (leftOperand instanceof ColumnReference) ||

								 (leftOperand.isParameterNode()));
				/* If we got this far and we are an expression subquery
				 * then we want to set leftOperand to be the left side
				 * of the comparison in case we pull the comparison into
				 * the flattened subquery.
				 */
				if (parentComparisonOperator instanceof BinaryComparisonOperatorNode)
				{
					leftOperand = parentComparisonOperator.getLeftOperand();
				}
				/* Never flatten to normal join for NOT EXISTS.
				 */
				if ((! flattenableNotExists) && select.uniqueSubquery(additionalEQ))
				{
					// Flatten the subquery
					return flattenToNormalJoin(numTables,
										   outerFromList, outerSubqueryList,
										   outerPredicateList);
				}
				/* We can flatten into an EXISTS join if all of the above
				 * conditions except for a uniqueness condition are true
				 * and:
				 *	o Subquery only has a single entry in its from list
				 *	  and that entry is a FromBaseTable
				 *	o All predicates in the subquery's where clause are
				 *	  pushable.
				 *  o The leftOperand, if non-null, is pushable.
				 * If the subquery meets these conditions then we will flatten
				 * the FBT into an EXISTS FBT, pushd the subquery's
				 * predicates down to the PRN above the EBT and
				 * mark the predicates to say that they cannot be pulled 
				 * above the PRN. (The only way that we can guarantee correctness
				 * is if the predicates do not get pulled up.  If they get pulled
				 * up then the single next logic for an EXISTS join does not work
				 * because that row may get disqualified at a higher level.)
				 */
				else if ( (isIN() || isANY() || isEXISTS() || flattenableNotExists) &&
						  ((leftOperand == null) ? true :
							 leftOperand.categorize(new JBitSet(numTables), false)) &&
						  select.getWherePredicates().allPushable() &&
						  singleFromBaseTable(select.getFromList()))
				{
					return flattenToExistsJoin(numTables,
										   outerFromList, outerSubqueryList,
										   outerPredicateList, flattenableNotExists);
				}

				// restore leftOperand to its original value
				leftOperand = origLeftOperand;
			}
		}

		/* We transform the leftOperand and the select list for quantified 
		 * predicates that have a leftOperand into a new predicate and push it
		 * down to the subquery after we preprocess the subquery's resultSet.
		 * We must do this after preprocessing the underlying subquery so that
		 * we know where to attach the new predicate.
		 * NOTE - If we pushed the predicate before preprocessing the underlying
		 * subquery, then the point of attachment would depend on the form of
		 * that subquery.  (Where clause?  Having clause?)
		 */
		if (leftOperand != null)
		{
			topNode = pushNewPredicate(numTables);
			pushedNewPredicate = true;
		}
		/* Since NOT EXISTS subquery is not flattened, now is good time to create
		 * an IS NULL node on top.  Other cases are taken care of in pushNewPredicate.
		 */
		else if (subqueryType == NOT_EXISTS_SUBQUERY)
		{
			topNode = genIsNullTree();
			subqueryType = EXISTS_SUBQUERY;
		}

		/*
		** Do inVariant and correlated checks now.  We
		** aren't going to use the results here, but they
		** have been stashed away by isInvariant() and hasCorrelatedCRs()
		*/
		isInvariant();
		hasCorrelatedCRs();

		/* If parentComparisonOperator is non-null then we are an
		 * expression subquery that was considered to be a candidate 
		 * for flattening, but we didn't get flattened.  In that case
		 * we are the rightOperand of the parent.  We need to update
		 * the parent's rightOperand with the new topNode and return
		 * the parent because the parent is letting us decide whether
		 * or not to replace the entire comparison, which we can do
		 * if we flatten.  Otherwise we simply return the new top node.
		 */
		if (parentComparisonOperator != null)
		{
			parentComparisonOperator.setRightOperand(topNode);
			return parentComparisonOperator;
		}

		return topNode;
	}

	/**
	 * Does the from list from the subquery contain a
	 * single entry which is a FBT or a PRN/FBT.
	 *
	 * @param fromList	The from list from the subquery
	 *
	 * @return Whether or not the from list from the subquery contains a
	 *			single entry which is a FBT or a PRN/FBT.
	 */
	private boolean singleFromBaseTable(FromList fromList)
	{
		boolean retCode = (fromList.size() == 1);

		if (retCode)
		{
			FromTable ft = (FromTable) fromList.elementAt(0);

			if (((ft instanceof ProjectRestrictNode) &&
				 ((ProjectRestrictNode) ft).getChildResult() instanceof FromBaseTable) ||
				ft instanceof FromBaseTable)
			{
			}
			else
			{
				retCode = false;
			}
		}

		return retCode;
	}

	/**
	 * Can NOT IN, ALL be falttened to NOT EXISTS join?  We can't or the flattening doesn't
	 * easily make sense if either side of the comparison is nullable. (beetle 5173)
	 *
	 * @return Whether or not the NOT IN or ALL subquery can be flattened.
	 */
	private boolean canAllBeFlattened ()
	{
		boolean result = false;
		if (isNOT_IN() || isALL())
		{
			ValueNode rightOperand = ((ResultColumn) resultSet.getResultColumns().elementAt(0)).
									getExpression();
			result = (! leftOperand.getTypeServices().isNullable() &&
						! rightOperand.getTypeServices().isNullable());
		}
		return result;
	}

	/**
	 * Flatten this subquery into the outer query block.  
	 * At this point we are only flattening based on a uniqueness
	 * condition and only flattening non-aggregate subqueries.
	 * So, we promote the subquery's from list, as is, into 
	 * the outer from list.  For EXISTS subquerys, we return a 
	 * TRUE.  Otherwise we return a new comparison between
	 * the leftOperand and the expression in the subquery's
	 * SELECT list.
	 * RESOLVE - we will need to modify this logic to account
	 * for exists joins and aggregates as we support flattening
	 * for them.
	 *
	 * Anyway, here's what we do:
	 *	o We remove ourself from the outer subquery list.
	 *	o We decrement the nesting level for all tables
	 *	  in the subquery tree.
	 *	o We append the subquery's from list to the outer
	 *	  from list.
	 *	o We add the subquery's predicate list to the outer
	 *	  predicate list.  (The subquery has already been
	 *	  preprocessed.)
	 *  o We add the subquery's subquery list to the outer
	 *	  subquery list.
	 *	o For EXISTS, we return a true.
	 *	o Otherwise, we return a new comparison between the
	 *	  leftOperand and the expression in the inner select's
	 *	  RCL.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 *
	 * @return	The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode flattenToNormalJoin(int numTables,
										  FromList outerFromList, 
										  SubqueryList outerSubqueryList,
										  PredicateList outerPredicateList)
		throws StandardException
	{
		SelectNode select = (SelectNode) resultSet;
		FromList   fl = select.getFromList();
		int[] tableNumbers = fl.getTableNumbers();

		// Remove ourselves from the outer subquery list
		outerSubqueryList.removeElement(this);

		/* Decrease the nesting level for all
		 * tables in the subquey tree.
		 */
		select.decrementLevel(1);

		/* Add the table(s) from the subquery into the outer from list */
		outerFromList.destructiveAppend(fl);

		/* Append the subquery's predicate list to the
		 * outer predicate list.
		 */
		outerPredicateList.destructiveAppend(select.getWherePredicates());

		/* Append the subquery's subquery list to the 
		 * outer subquery list.
		 * NOTE: We must propagate any subqueries from both the
		 * SELECT list and WHERE clause of the subquery that's
		 * getting flattened.
		 */
		outerSubqueryList.destructiveAppend(select.getWhereSubquerys());
		outerSubqueryList.destructiveAppend(select.getSelectSubquerys());

		/* return the new join condition 
		 * If we are flattening an EXISTS then there is no new join
		 * condition since there is no leftOperand.  Simply return
		 * TRUE.
		 *
		 * NOTE: The outer where clause, etc. has already been normalized,
		 * so we simply return the BinaryComparisonOperatorNode above
		 * the new join condition.
		 */
		if (leftOperand == null)
		{
			return (ValueNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());
		}
		else
		{
			ValueNode rightOperand;
			rightOperand = ((ResultColumn) select.getResultColumns().elementAt(0)).
								getExpression();
			/* If the right operand is a CR, then we need to decrement
			 * its source level as part of flattening so that
			 * transitive closure will work correctly.
			 */
			if (rightOperand instanceof ColumnReference)
			{
				ColumnReference cr = (ColumnReference) rightOperand;
				int tableNumber = cr.getTableNumber();
				for (int index = 0; index < tableNumbers.length; index++)
				{
					if (tableNumber == tableNumbers[index])
					{
						cr.setSourceLevel(
							cr.getSourceLevel() - 1);
						break;
					}
				}
			}
			return getNewJoinCondition(leftOperand, rightOperand);
		}
	}

	/**
	 * Flatten this subquery into the outer query block
	 * as an exists join.  
	 * At this point we are only flattening non-aggregate subqueries
	 * with a single FBT in the from list.
	 * So, we transform all FBTs in the from list into ExistBaseTables,
	 * update the dependency lists for each of the tables and then
	 * flatten the subquery.
	 * RESOLVE - we will need to modify this logic to account
	 * for aggregates as we support flattening
	 * for them.
	 *
	 * @param	numTables			Number of tables in the DML Statement
	 * @param	outerFromList		FromList from outer query block
	 * @param	outerSubqueryList	SubqueryList from outer query block
	 * @param	outerPredicateList	PredicateList from outer query block
	 * @param	flattenableNotExists Is it a flattening into a NOT EXISTS join
	 *
	 * @return	The modified expression
	 *
	 * @exception StandardException		Thrown on error
	 */
	private ValueNode flattenToExistsJoin(int numTables,
										  FromList outerFromList, 
										  SubqueryList outerSubqueryList,
										  PredicateList outerPredicateList,
										  boolean flattenableNotExists)
		throws StandardException
	{
		SelectNode select = (SelectNode) resultSet;

		// Replace the FromBaseTables in the from list with ExistBaseTables
		select.getFromList().genExistsBaseTables(resultSet.getReferencedTableMap(),
				outerFromList, flattenableNotExists);

		/* NOTE: Because we are currently only flattening single table subqueries
		 * whose predicates are all pushable, we simply follow the rest of the
		 * flattening algorithm for unique subqueries.  Should we decide to 
		 * loosen these restrictions then we need to do more work such as:
		 *
		 * Mark all of the predicates from the subquery as non-pullable. They must
		 * not be pulled so that we can guarantee correctness.  Otherwise, we could
		 * add or subtract rows from the result set.
		 *
		 * Remap all of the non-correlated CRs in the predicate list so that they
		 * point to the correct source.  (We've chopped a level out of the RCL/VCN
		 * chain.)  We then transfer those predicates to the PRN in the subquery's
		 * from list.
		 */

		return flattenToNormalJoin(numTables, outerFromList,
								   outerSubqueryList, outerPredicateList);
	}

	/**
	 * Check to see if we have a Variant value below us.
	 * If so, return true.  Caches the result so multiple
	 * calls are ok.
	 *  
	 * @return boolean whether we have 
	 *
	 * @exception StandardException		Thrown on error
	 */
	private boolean isInvariant() throws StandardException
	{
		if (doneInvariantCheck)
		{
			return !foundVariant;
		}

		doneInvariantCheck = true;
		HasVariantValueNodeVisitor visitor = new HasVariantValueNodeVisitor();
		resultSet.accept(visitor);
		foundVariant = visitor.hasVariant();
		return !foundVariant;
	}

	/**
	 * Check to see if this subquery has correlated
	 * column references.  Only useful results if
	 * called AFTER binding (after CRs have been bound).
	 *
	 * @return whether the subquery has correlated column
	 *	references.
	 * @exception StandardException		Thrown on error
	 */
	public boolean hasCorrelatedCRs() throws StandardException
	{
		if (doneCorrelationCheck)
		{
			return foundCorrelation;
		}
		doneCorrelationCheck = true;

		ResultSetNode realSubquery = resultSet;
		ResultColumnList oldRCL = null;

		/* If we have pushed the new join predicate on top, we want to disregard it
		 * to see if anything under the predicate is correlated.  If nothing correlated
		 * under the new join predicate, we could then materialize the subquery.
		 * See beetle 4373.
		 */
		if (pushedNewPredicate)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(resultSet instanceof ProjectRestrictNode,
					"resultSet expected to be a ProjectRestrictNode!");
			}

			realSubquery = ((ProjectRestrictNode) resultSet).getChildResult();
			oldRCL = realSubquery.getResultColumns();

			/* Only first column matters.
			 */
			if (oldRCL.size() > 1)
			{
				ResultColumnList newRCL = new ResultColumnList();
				newRCL.addResultColumn(oldRCL.getResultColumn(1));
				realSubquery.setResultColumns(newRCL);
			}
		}

		HasCorrelatedCRsVisitor visitor = new HasCorrelatedCRsVisitor();
		realSubquery.accept(visitor);
		foundCorrelation = visitor.hasCorrelatedCRs();

		if (pushedNewPredicate && (oldRCL.size() > 1))
		{
			realSubquery.setResultColumns(oldRCL);
		}

		return foundCorrelation;
	}
				
	/**
	 * Transform:
	 *		expresion QuantifiedOperator (select x from ...)
	 * into
	 *		(select true from .. where expression <BinaryComparisonOperator> x ...)
	 *		IS [NOT] NULL
	 *
	 * or, if we have an aggregate:
	 *		(select true from 
	 *			(select AGG(x) from ...)
	 *		where expression <BinaryComparisonOperator> x ...)
	 *		IS [NOT] NULL
	 *
	 *
	 * For ANY and IN subqueries:
	 *		o  We generate an IS NULL above the SubqueryNode and return the top of
	 *		   the new tree to the caller.
	 *		o  The operator in the new predicate that is added to the subquery
	 *		   will correspond to the operator that modifies the ANY.
	 *		   (eg, = for = ANY, with = for IN.)
	 * For ALL and NOT IN subqueries:
	 *		o  We generate an IS NOT NULL above the SubqueryNode and return the top of
	 *		   the new tree to the caller.
	 *		o  The operator in the new predicate that is added to the subquery
	 *		   will be a BinaryAllOperatorNode whose bcoNodeType corresponds to 
	 *		   the negation of the operator that modifies the ALL.
	 *		   (eg, <> for = ALL, with <> for NOT IN.)
	 *
	 * NOTE: This method is called after the underlying subquery has been
	 * preprocessed, so we build a new Predicate, not just a new expression.
	 *
	 * @param numTables			Number of tables in DML Statement
	 *
	 * @return UnaryComparisonOperatorNode	An IS [NOT] NULL above the 
	 *										transformed subquery.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private UnaryComparisonOperatorNode pushNewPredicate(
				int numTables)
			throws StandardException
	{
		AndNode						andNode;
		BinaryComparisonOperatorNode bcoNode = null;
		JBitSet						tableMap;
		Predicate					predicate;
		ResultColumn				firstRC;
		ResultColumnList			resultColumns;
		UnaryComparisonOperatorNode	ucoNode = null;
		ValueNode					oldWhereClause;
		ValueNode					rightOperand;

		/* We have to ensure that the resultSet immediately under us has
		 * a PredicateList, otherwise we can't push the predicate down.
		 */
		resultSet = resultSet.ensurePredicateList(numTables);

		/* RESOLVE - once we understand how correlated columns will work, 
		 * we probably want to mark leftOperand as a correlated column
		 */
		resultColumns = resultSet.getResultColumns();

		/*
		** Create a new PR node.  Put it over the original subquery.  resulSet
		** is now the new PR.  We give the chance that things under the PR node
		** can be materialized.  See beetle 4373.
		*/
		ResultColumnList newRCL = resultColumns.copyListAndObjects();
		newRCL.genVirtualColumnNodes(resultSet, resultColumns);
		resultSet = (ResultSetNode) getNodeFactory().getNode(
										C_NodeTypes.PROJECT_RESTRICT_NODE,
										resultSet,	// child
										newRCL,			// result columns
										null,			// restriction
										null, 			// restriction list
										null,			// project subqueries
										null,			// restrict subqueries	
										null,
										getContextManager());
		resultColumns = newRCL;
	
		firstRC = (ResultColumn) resultColumns.elementAt(0);
		rightOperand = firstRC.getExpression();

		bcoNode = getNewJoinCondition(leftOperand, rightOperand);

		ValueNode andLeft = bcoNode;

		/* For NOT IN or ALL, and if either side of the comparison is nullable, and the
		 * subquery can not be flattened (because of that), we need to add IS NULL node
		 * on top of the nullables, such that the behavior is (beetle 5173):
		 *
		 *    (1) If we have nulls in right operand, no row is returned.
		 *    (2) If subquery result is empty before applying join predicate, every
		 *		  left row (including NULLs) is returned.
		 *	  (3) Otherwise, return {all left row} - {NULLs}
		 */
		if (isNOT_IN() || isALL())
		{
			boolean leftNullable = leftOperand.getTypeServices().isNullable();
			boolean rightNullable = rightOperand.getTypeServices().isNullable();
			if (leftNullable || rightNullable)
			{
				/* Create a normalized structure.
				 */
				BooleanConstantNode falseNode = (BooleanConstantNode) getNodeFactory().getNode(
												C_NodeTypes.BOOLEAN_CONSTANT_NODE,
												Boolean.FALSE,
												getContextManager());
				OrNode newOr = (OrNode) getNodeFactory().getNode(
												C_NodeTypes.OR_NODE,
												bcoNode,
												falseNode,
												getContextManager());
				newOr.postBindFixup();
				andLeft = newOr;

				if (leftNullable)
				{
					UnaryComparisonOperatorNode leftIsNull = (UnaryComparisonOperatorNode)
									getNodeFactory().getNode(
														C_NodeTypes.IS_NULL_NODE,
														leftOperand,
														getContextManager());
					leftIsNull.bindComparisonOperator();
					newOr = (OrNode) getNodeFactory().getNode(
													C_NodeTypes.OR_NODE,
													leftIsNull,
													andLeft,
													getContextManager());
					newOr.postBindFixup();
					andLeft = newOr;
				}
				if (rightNullable)
				{
					UnaryComparisonOperatorNode rightIsNull = (UnaryComparisonOperatorNode)
									getNodeFactory().getNode(
														C_NodeTypes.IS_NULL_NODE,
														rightOperand,
														getContextManager());
					rightIsNull.bindComparisonOperator();
					newOr = (OrNode) getNodeFactory().getNode(
													C_NodeTypes.OR_NODE,
													rightIsNull,
													andLeft,
													getContextManager());
					newOr.postBindFixup();
					andLeft = newOr;
				}
			}
		}

		/* Place an AndNode above the <BinaryComparisonOperator> */
		andNode = (AndNode) getNodeFactory().getNode(
													C_NodeTypes.AND_NODE,
													andLeft,
													getTrueNode(),
													getContextManager());

		/* Build the referenced table map for the new predicate */
		tableMap = new JBitSet(numTables);
		andNode.postBindFixup();

		/* Put the AndNode under a Predicate */
		predicate = (Predicate) getNodeFactory().getNode(
										C_NodeTypes.PREDICATE,
										andNode,
										tableMap,
										getContextManager());
		predicate.categorize();

		/* Push the new Predicate to the subquery's list */
		resultSet = resultSet.addNewPredicate(predicate);

		/* Clean up the leftOperand and subquery ResultColumn */
		leftOperand = null;
		firstRC.setType(getTypeServices());
		firstRC.setExpression(getTrueNode());

		/* Add the IS [NOT] NULL above the SubqueryNode */
		switch (subqueryType)
		{
			case IN_SUBQUERY:
			case EQ_ANY_SUBQUERY:
			case NE_ANY_SUBQUERY:
			case LE_ANY_SUBQUERY:
			case LT_ANY_SUBQUERY:
			case GE_ANY_SUBQUERY:
			case GT_ANY_SUBQUERY:
				ucoNode = (UnaryComparisonOperatorNode) 
									getNodeFactory().getNode(
												C_NodeTypes.IS_NOT_NULL_NODE,
												this,
												getContextManager());
				break;

			case NOT_IN_SUBQUERY:
			case EQ_ALL_SUBQUERY:
			case NE_ALL_SUBQUERY:
			case LE_ALL_SUBQUERY:
			case LT_ALL_SUBQUERY:
			case GE_ALL_SUBQUERY:
			case GT_ALL_SUBQUERY:
				ucoNode = (UnaryComparisonOperatorNode) 
									getNodeFactory().getNode(
													C_NodeTypes.IS_NULL_NODE,
													this,
													getContextManager());
				break;
		}
		ucoNode.bindComparisonOperator();
		return ucoNode;
	}

	/**
	 * Build a new join condition between the leftOperand
	 * and the rightOperand.  The comparison operator
	 * is dependent on the subquery type.
	 *
	 * @param leftOperand	The left operand for the new condition.
	 * @param rightOperand	The right operand for the new condition.
	 *
	 * @exception StandardException		Thrown on error
	 */
	private BinaryComparisonOperatorNode getNewJoinCondition(
				ValueNode leftOperand, 
				ValueNode rightOperand)
		throws StandardException
	{
		BinaryComparisonOperatorNode bcoNode = null;

		/* NOTE: If we are an expression subquery that's getting
		 * flattened then our subqueryType is EXPRESSION_SUBQUERY.
		 * However, we can get the comparison type from the 
		 * parentComparisonOperator.  In that case we dovetail on
		 * the ANY subquery types.
		 */
		int operatorType = subqueryType;
		if (subqueryType == EXPRESSION_SUBQUERY)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(parentComparisonOperator != null,
					"parentComparisonOperator expected to be non-null");
			}

			int parentOperator = -1;

			if (parentComparisonOperator.isRelationalOperator())
			{
				RelationalOperator ro = (RelationalOperator)parentComparisonOperator;
				parentOperator = ro.getOperator();
			}

			if (parentOperator == RelationalOperator.EQUALS_RELOP)
			{
				operatorType = EQ_ANY_SUBQUERY;
			}
			else if (parentOperator == RelationalOperator.NOT_EQUALS_RELOP)
			{
				operatorType = NE_ANY_SUBQUERY;
			}
			else if (parentOperator == RelationalOperator.LESS_EQUALS_RELOP)
			{
				operatorType = LE_ANY_SUBQUERY;
			}
			else if (parentOperator == RelationalOperator.LESS_THAN_RELOP)
			{
				operatorType = LT_ANY_SUBQUERY;
			}
			else if (parentOperator == RelationalOperator.GREATER_EQUALS_RELOP)
			{
				operatorType = GE_ANY_SUBQUERY;
			}
			else if (parentOperator == RelationalOperator.GREATER_THAN_RELOP)
			{
				operatorType = GT_ANY_SUBQUERY;
			}
		}

		int bcoType = 0;
		int nodeType = 0;

		/* Build the <BinaryComparisonOperator> */
		switch (operatorType)
		{
			case IN_SUBQUERY:
			case EQ_ANY_SUBQUERY:
			case NOT_IN_SUBQUERY:
			case NE_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE;
				break;

			case NE_ANY_SUBQUERY:
			case EQ_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE;
				break;

			case LE_ANY_SUBQUERY:
			case GT_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE;
				break;

			case LT_ANY_SUBQUERY:
			case GE_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE;
				break;

			case GE_ANY_SUBQUERY:
			case LT_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE;
				break;

			case GT_ANY_SUBQUERY:
			case LE_ALL_SUBQUERY:
				nodeType = C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE;
				break;

			default:
				if (SanityManager.DEBUG)
				SanityManager.ASSERT(false,
					"subqueryType (" + subqueryType + ") is an unexpected type");
		}

		bcoNode =  (BinaryComparisonOperatorNode) 
						getNodeFactory().getNode(
							nodeType,
							leftOperand,
							rightOperand,
							getContextManager());

		bcoNode.bindComparisonOperator();
		return bcoNode;
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
		ValueNode result = this;

		if (underNotNode)
		{
			/* Negate the subqueryType. For expression subqueries
			 * we simply return subquery = false
			 */
			/* RESOLVE - This code needs to get cleaned up once there are
			 * more subquery types.  (Consider using arrays.)
			 */
			switch (subqueryType)
			{
				case EXPRESSION_SUBQUERY:
					result = genEqualsFalseTree();
					break;

				case EXISTS_SUBQUERY:
					subqueryType = NOT_EXISTS_SUBQUERY;
					break;

				/* ANY subqueries */
				case IN_SUBQUERY:
				case EQ_ANY_SUBQUERY:
					subqueryType = NOT_IN_SUBQUERY;
					break;

				case NE_ANY_SUBQUERY:
					subqueryType = EQ_ALL_SUBQUERY;
					break;

				case GE_ANY_SUBQUERY:
					subqueryType = LT_ALL_SUBQUERY;
					break;

				case GT_ANY_SUBQUERY:
					subqueryType = LE_ALL_SUBQUERY;
					break;

				case LE_ANY_SUBQUERY:
					subqueryType = GT_ALL_SUBQUERY;
					break;

				case LT_ANY_SUBQUERY:
					subqueryType = GE_ALL_SUBQUERY;
					break;

				/* ALL subqueries - no need for NOT NOT_IN_SUBQUERY, since
				 * NOT IN only comes into existence here.
				 */
				case EQ_ALL_SUBQUERY:
					subqueryType = NE_ANY_SUBQUERY;
					break;

				case NE_ALL_SUBQUERY:
					subqueryType = EQ_ANY_SUBQUERY;
					break;

				case GE_ALL_SUBQUERY:
					subqueryType = LT_ANY_SUBQUERY;
					break;

				case GT_ALL_SUBQUERY:
					subqueryType = LE_ANY_SUBQUERY;
					break;

				case LE_ALL_SUBQUERY:
					subqueryType = GT_ANY_SUBQUERY;
					break;

				case LT_ALL_SUBQUERY:
					subqueryType = GE_ANY_SUBQUERY;
					break;

				default:
					if (SanityManager.DEBUG)
					SanityManager.ASSERT(false,
						"NOT is not supported for this time of subquery");
			}
		}

		/* Halt recursion here, as each query block is preprocessed separately */
		return result;
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
		/* Remember whether or not we are immediately under a top leve
		 * AndNode.  This is important for subquery flattening.
		 * (We can only flatten subqueries under a top level AndNode.)
		 */
		 this.underTopAndNode = underTopAndNode;

		/* Halt recursion here, as each query block is preprocessed separately */
		return this;
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
	 * @return boolean		Whether or not source.expression is a ColumnReference
	 *						or a VirtualColumnNode.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean categorize(JBitSet referencedTabs, boolean simplePredsOnly)
		throws StandardException
	{
		/* We stop here when only considering simple predicates
		 *  as we don't consider method calls when looking
		 * for null invariant predicates.
		 */
		if (simplePredsOnly)
		{
			return false;
		}

		/* RESOLVE - We need to or in a bit map when there are correlation columns */

		/* We categorize a query block at a time, so stop the recursion here */

		/* Predicates with subqueries are not pushable for now */

		/*
		** If we can materialize the subquery, then it is 
		** both invariant and non-correlated.  And so it
		** is pushable.
		*/
		return isMaterializable();

	}

	/*
	** Subquery is materializable if
	** it is an expression subquery that
	** has no correlations and is invariant.
	*/
	boolean isMaterializable() throws StandardException
	{
		boolean retval = (subqueryType == EXPRESSION_SUBQUERY) && 
						  !hasCorrelatedCRs() && 
						  isInvariant();
		/* If we can materialize the subquery, then we set
		 * the level of all of the tables to 0 so that we can
		 * consider bulk fetch for them.
		 */
		if (retval)
		{
			if (resultSet instanceof SelectNode)
			{
				SelectNode select = (SelectNode) resultSet;
				FromList fromList = select.getFromList();
				fromList.setLevel(0);
			}
		}

		return retval;
	}

	/**
	 * Optimize this SubqueryNode.  
	 *
	 * @param dataDictionary	The DataDictionary to use for optimization
	 * @param outerRows			The optimizer's estimate of the number of
	 *							times this subquery will be executed.
	 *
	 * @return	Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void optimize(DataDictionary dataDictionary, double outerRows) 
					throws StandardException
	{
		/* RESOLVE - is there anything else that we need to do for this
		 * node.
		 */

		/* Optimize the underlying result set */
		resultSet = resultSet.optimize(dataDictionary, null, outerRows);
	}

	/**
	 * Make any changes to the access paths, as decided by the optimizer.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void modifyAccessPaths() throws StandardException
	{
		resultSet = resultSet.modifyAccessPaths();
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 *
	 * @exception StandardException		Thrown on error
	 */
	protected int getOrderableVariantType() throws StandardException
	{
		/* 
		 * If the subquery is variant, than return
		 * VARIANT.  Otherwise, if we have an expression
		 * subquery and no correlated CRs we are going
		 * to materialize it, so it is QUERY_INVARIANT.
	  	 * Otherwise, SCAN_INVARIANT.
		 */
		if (isInvariant())
		{
			if (!hasCorrelatedCRs() && 
				(subqueryType == EXPRESSION_SUBQUERY))
			{
				return Qualifier.QUERY_INVARIANT;
			}
			else
			{
				return Qualifier.SCAN_INVARIANT;
			}
		}
		else
		{
			return Qualifier.VARIANT;
		}
	}

	/**
	 * Do code generation for this subquery.
	 *
	 * @param expressionBuilder	The ExpressionClassBuilder for the class being built
	 * @param mbex	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(
									ExpressionClassBuilder expressionBuilder,
									MethodBuilder mbex)
								throws StandardException
	{
		CompilerContext	cc = getCompilerContext();
		String			resultSetString;

		///////////////////////////////////////////////////////////////////////////
		//
		//	Subqueries should not appear in Filter expressions. We should get here
		//	only if we're compiling a query. That means that our class builder
		//	is an activation builder. If we ever allow subqueries in filters, we'll
		//	have to revisit this code.
		//
		///////////////////////////////////////////////////////////////////////////
		
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(expressionBuilder instanceof ActivationClassBuilder,
				"Expecting an ActivationClassBuilder");
		}

		ActivationClassBuilder	acb = (ActivationClassBuilder) expressionBuilder;
		/* Reuse generated code, where possible */

		/* Generate the appropriate (Any or Once) ResultSet */
		if (subqueryType == EXPRESSION_SUBQUERY)
		{
			resultSetString = "getOnceResultSet";
		}
		else
		{
			resultSetString = "getAnyResultSet";
		}

		// Get cost estimate for underlying subquery
		CostEstimate costEstimate = resultSet.getFinalCostEstimate();

		/* Generate a new method.  It's only used within the other
		 * exprFuns, so it could be private. but since we don't
		 * generate the right bytecodes to invoke private methods,
		 * we just make it protected.  This generated class won't
		 * have any subclasses, certainly! (nat 12/97)
		 */
		String subqueryTypeString =
							getTypeCompiler().interfaceName();
		MethodBuilder	mb = acb.newGeneratedFun(subqueryTypeString, Modifier.PROTECTED);

		/* Declare the field to hold the suquery's ResultSet tree */
		LocalField rsFieldLF = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.NoPutResultSet);

		ResultSetNode subNode = null;

		if (!isMaterializable())
		{
            MethodBuilder executeMB = acb.getExecuteMethod();
			if (pushedNewPredicate && (! hasCorrelatedCRs()))
			{
				/* We try to materialize the subquery if it can fit in the memory.  We
				 * evaluate the subquery first.  If the result set fits in the memory,
				 * we replace the resultset with in-memory unions of row result sets.
				 * We do this trick by replacing the child result with a new node --
				 * MaterializeSubqueryNode, which essentially generates the suitable
				 * code to materialize the subquery if possible.  This may have big
				 * performance improvement.  See beetle 4373.
				 */
				if (SanityManager.DEBUG)
				{
					SanityManager.ASSERT(resultSet instanceof ProjectRestrictNode,
						"resultSet expected to be a ProjectRestrictNode!");
				}
				subNode = ((ProjectRestrictNode) resultSet).getChildResult();
				LocalField subRS = acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.NoPutResultSet);
				mb.getField(subRS);
				mb.conditionalIfNull();

				ResultSetNode materialSubNode = new MaterializeSubqueryNode(subRS);
				((ProjectRestrictNode) resultSet).setChildResult(materialSubNode);

				/* Evaluate subquery resultset here first.  Next time when we come to
				 * this subquery it may be replaced by a bunch of unions of rows.
				 */
				subNode.generate(acb, mb);
				mb.startElseCode();
				mb.getField(subRS);
				mb.completeConditional();
		
				mb.putField(subRS);
				mb.endStatement();

                executeMB.pushNull( ClassName.NoPutResultSet);
                executeMB.putField(subRS);
                executeMB.endStatement();
			}

            executeMB.pushNull( ClassName.NoPutResultSet);
            executeMB.putField(rsFieldLF);
            executeMB.endStatement();

			// now we fill in the body of the conditional
			mb.getField(rsFieldLF);
			mb.conditionalIfNull();
		}

		acb.pushGetResultSetFactoryExpression(mb);

		// start of args
		int nargs;

		/* Inside here is where subquery could already have been materialized. 4373
		 */
		resultSet.generate(acb, mb);

		/* Get the next ResultSet #, so that we can number the subquery's 
		 * empty row ResultColumnList and Once/Any ResultSet.
		 */
		int subqResultSetNumber = cc.getNextResultSetNumber();

		/* We will be reusing the RCL from the subquery's ResultSet for the 
		 * empty row function.  We need to reset the resultSetNumber in the
		 * RCL, before we generate that function.  Now that we've called
		 * generate() on the subquery's ResultSet, we can reset that
		 * resultSetNumber.
		 */
		resultSet.getResultColumns().setResultSetNumber(subqResultSetNumber);

		acb.pushThisAsActivation(mb);

		/* Generate code for empty row */
		resultSet.getResultColumns().generateNulls(acb, mb);

		/*
		 *	arg1: suqueryExpress - Expression for subquery's
		 *		  ResultSet
		 *  arg2: Activation
		 *  arg3: Method to generate Row with null(s) if subquery
		 *		  Result Set is empty
		 */
		if (subqueryType == EXPRESSION_SUBQUERY)
		{
			int cardinalityCheck;

			/* No need to do sort if subquery began life as a distinct expression subquery.
			 * (We simply check for a single unique value at execution time.)
			 * No need for cardinality check if we know that underlying
			 * ResultSet can contain at most 1 row.
			 * RESOLVE - Not necessary if we know we
			 * are getting a single row because of a unique index.
			 */
			if (distinctExpression)
			{
				cardinalityCheck = OnceResultSet.UNIQUE_CARDINALITY_CHECK;
			}
			else if (resultSet.returnsAtMostOneRow())
			{
				cardinalityCheck = OnceResultSet.NO_CARDINALITY_CHECK;
			}
			else
			{
				cardinalityCheck = OnceResultSet.DO_CARDINALITY_CHECK;
			}

			/*  arg4: int - whether or not cardinality check is required
			 *				DO_CARDINALITY_CHECK - required
			 *				NO_CARDINALITY_CHECK - not required
			 *				UNIQUE_CARDINALITY_CHECK - verify single
			 *											unique value
			 */
			mb.push(cardinalityCheck);
			nargs = 9;

		} else {
			nargs = 8;
		}

		mb.push(subqResultSetNumber);
		mb.push(subqueryNumber);
		mb.push(pointOfAttachment);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, resultSetString, ClassName.NoPutResultSet, nargs);



		/* Fill in the body of the method
		 * generates the following.
		 * (NOTE: the close() method only generated for
		 * materialized subqueries.  All other subqueries
		 * closed by top result set in the query.):
		 * 
		 *	NoPutResultSet	rsFieldX;
		 *	{
		 *		<Datatype interface> col;
		 *		ExecRow r;
		 *		rsFieldX = (rsFieldX == null) ? outerRSCall() : rsFieldX; // <== NONmaterialized specific
		 *		rsFieldX.openCore();
		 *		r = rsFieldX.getNextRowCore();
		 *		col = (<Datatype interface>) r.getColumn(1);
		 *		return col;
		 *	}
		 *
		 * MATERIALIZED:
		 *	NoPutResultSet	rsFieldX;
		 *	{
		 *		<Datatype interface> col;
		 *		ExecRow r;
		 *		rsFieldX = outerRSCall();
		 *		rsFieldX.openCore();
		 *		r = rsFieldX.getNextRowCore();
		 *		col = (<Datatype interface>) r.getColumn(1);
		 *		rsFieldX.close();								// <== materialized specific
		 *		return col;
		 *	}
		 * and adds it to exprFun
		 */

		/* Generate the declarations */ // PUSHCOMPILE
		//VariableDeclaration colVar = mb.addVariableDeclaration(subqueryTypeString);
		//VariableDeclaration rVar   = mb.addVariableDeclaration(ClassName.ExecRow);

		if (!isMaterializable())
		{
			/* put it back
			 */
			if (pushedNewPredicate && (! hasCorrelatedCRs()))
				((ProjectRestrictNode) resultSet).setChildResult(subNode);

			// now we fill in the body of the conditional
			mb.startElseCode();
			  mb.getField(rsFieldLF);
			mb.completeConditional();
		}
		
		mb.putField(rsFieldLF);
		mb.endStatement();

		/* rs.openCore() */
		mb.getField(rsFieldLF);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "openCore", "void", 0);

		/* r = rs.next() */
		mb.getField(rsFieldLF);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNextRowCore", ClassName.ExecRow, 0);
		//mb.putVariable(rVar);
		//mb.endStatement();

		/* col = (<Datatype interface>) r.getColumn(1) */
		//mb.getVariable(rVar);
		mb.push(1); // both the Row interface and columnId are 1-based
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Row, "getColumn", ClassName.DataValueDescriptor, 1);
		mb.cast(subqueryTypeString);
		//mb.putVariable(colVar);
		//mb.endStatement();

		/* Only generate the close() method for materialized
		 * subqueries.  All others will be closed when the
		 * close() method is called on the top ResultSet.
		 */
		if (isMaterializable())
		{
			/* rs.close() */
			mb.getField(rsFieldLF);
			mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.ResultSet, "close", "void", 0);
		}

		/* return col */
		//mb.getVariable(colVar);
		mb.methodReturn();
		mb.complete();

		/*
		** If we have an expression subquery, then we
		** can materialize it if it has no correlated
		** column references and is invariant.
		*/
		if (isMaterializable())
		{
			LocalField lf = generateMaterialization(acb, mb, subqueryTypeString);
			mbex.getField(lf);

		} else {
			/* Generate the call to the new method */
			mbex.pushThis();
			mbex.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, mb.getName(), subqueryTypeString, 0);
		}
	}

	/*
	** Materialize the subquery in question.  Given the expression
	** that represents the subquery, this returns fieldX where
	** fieldX is set up as follows:
	**
	** private ... fieldX
	**
	** execute()
	** {
	**	fieldX = <subqueryExpression>
	**	...
	** }
	**
	** So we wind up evaluating the subquery when we start
	** execution.  Obviously, it is absolutely necessary that
	** the subquery is invariant and has no correlations
	** for this to work.
	**
	** Ideally we wouldn't evaluate the expression subquery
	** until we know we need to, but because we are marking
	** this expression subquery as pushable, we must evaluate
	** it up front because it might wind up as a qualification,
	** and we cannot execute a subquery in the store as a
	** qualification because the store executes qualifications
	** while holding a latch.  
	**
	** @param acb
	** @param type 
	** @param subqueryExpression
	*/
	private LocalField generateMaterialization(
			ActivationClassBuilder	acb,
			MethodBuilder mbsq,
			String 			type)
	{
		MethodBuilder mb = acb.executeMethod;

		// declare field
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, type);

		/* Generate the call to the new method */
		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, mbsq.getName(), type, 0);

		// generate: field = value (value is on stack)
		mb.putField(field);
		mb.endStatement();

		return field;
	}

	/* Private methods on private variables */
	private BooleanConstantNode getTrueNode()
		throws StandardException
	{
		if (trueNode == null)
		{
			trueNode = (BooleanConstantNode) getNodeFactory().getNode(
											C_NodeTypes.BOOLEAN_CONSTANT_NODE,
											Boolean.TRUE,
											getContextManager());
		}
		return trueNode;
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

		/* shortcut if we've already done it
		 */
		if ((v instanceof HasCorrelatedCRsVisitor) && doneCorrelationCheck) 
		{
			((HasCorrelatedCRsVisitor) v).setHasCorrelatedCRs(foundCorrelation);
			return returnNode;
		}
	
		if (v.skipChildren(this))
		{
			return returnNode;
		}

		if (resultSet != null && !v.stopTraversal())
		{
			resultSet = (ResultSetNode)resultSet.accept(v);
		}

		if (leftOperand != null && !v.stopTraversal())
		{
			leftOperand = (ValueNode)leftOperand.accept(v);
		}
		return returnNode;
	}

	private boolean isIN()
	{
		return subqueryType == IN_SUBQUERY;
	}

	private boolean isNOT_IN()
	{
		return subqueryType == NOT_IN_SUBQUERY;
	}

	private boolean isANY()
	{
		switch (subqueryType)
		{
			case EQ_ANY_SUBQUERY:
			case NE_ANY_SUBQUERY:
			case LE_ANY_SUBQUERY:
			case LT_ANY_SUBQUERY:
			case GE_ANY_SUBQUERY:
			case GT_ANY_SUBQUERY:
				return true;

			default:
				return false;
		}
	}

	private boolean isALL()
	{
		switch (subqueryType)
		{
			case EQ_ALL_SUBQUERY:
			case NE_ALL_SUBQUERY:
			case LE_ALL_SUBQUERY:
			case LT_ALL_SUBQUERY:
			case GE_ALL_SUBQUERY:
			case GT_ALL_SUBQUERY:
				return true;

			default:
				return false;
		}
	}

	private boolean isEXISTS()
	{
		return subqueryType == EXISTS_SUBQUERY;
	}

	private boolean isNOT_EXISTS()
	{
		return subqueryType == NOT_EXISTS_SUBQUERY;
	}

	/**
	 * Convert this IN/ANY subquery, which is known to return at most 1 row,
	 * to an equivalent expression subquery.
	 *
	 * @return Nothing
	 *
	 * @exception StandardException		Thrown on error
	 */
	private void changeToCorrespondingExpressionType()
		throws StandardException
	{
  		BinaryOperatorNode bcon = null;

  		switch (subqueryType)
  		{
  			case EQ_ANY_SUBQUERY:
  			case IN_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  									C_NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
  									leftOperand,
  									this,
  									getContextManager());
  				break;

  			case NE_ANY_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  								C_NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE,
  								leftOperand,
  								this,
  								getContextManager());
  				break;

  			case LE_ANY_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  								C_NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE,
  								leftOperand,
  								this,
  								getContextManager());
  				break;

  			case LT_ANY_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  							C_NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE,
  							leftOperand,
  							this,
  							getContextManager());
  				break;

  			case GE_ANY_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  							C_NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE,
  							leftOperand,
  							this,
  							getContextManager());
  				break;

  			case GT_ANY_SUBQUERY:
  				bcon = (BinaryOperatorNode) getNodeFactory().getNode(
  								C_NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE,
  								leftOperand,
  								this,
  								getContextManager());
  				break;
  		}

  		// clean up the state of the tree to reflect a bound expression subquery
  		subqueryType = EXPRESSION_SUBQUERY;
  		setDataTypeServices(resultSet.getResultColumns());

  		parentComparisonOperator = (BinaryComparisonOperatorNode) bcon;
  		/* Set type info for the operator node */
  		parentComparisonOperator.bindComparisonOperator();
  		leftOperand = null;
   }

	private void setDataTypeServices(ResultColumnList resultColumns)
		throws StandardException
	{
		DataTypeDescriptor dts;

		/* Set the result type for this subquery (must be nullable).
		 * Quantified predicates will return boolean.
		 * However, the return type of the subquery's result list will 
		 * probably not be boolean at this point, because we do not
		 * transform the subquery (other than EXISTS) into 
		 * (select true/false ...) until preprocess().  So, we force 
		 * the return type to boolean.
		 */
		if (subqueryType == EXPRESSION_SUBQUERY)
		{
			dts = ((ResultColumn) resultColumns.elementAt(0)).getTypeServices();
		}
		else
		{
			dts = getTrueNode().getTypeServices();
		}

		/* If datatype of underlying resultSet is nullable, reuse it, otherwise
		 * we need to generate a new one.
		 */
		if (! dts.isNullable())
		{
			dts = new DataTypeDescriptor(dts, true);
		}
		setType(dts);
	}
}
