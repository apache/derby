/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.sql.compile.Visitable; 
import org.apache.derby.iapi.sql.compile.Visitor;

import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;

/**
 * If a RCL (SELECT list) contains an aggregate, then we must verify
 * that the RCL (SELECT list) is valid.  
 * For ungrouped queries,
 * the RCL must be composed entirely of valid aggregate expressions -
 * in this case, no column references outside of an aggregate.
 * For grouped aggregates,
 * the RCL must be composed of grouping columns or valid aggregate
 * expressions - in this case, the only column references allowed outside of
 * an aggregate are grouping columns.
 *
 * @author jamie, from code written by jerry
 */
public class VerifyAggregateExpressionsVisitor implements Visitor
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	private GroupByList groupByList;

	public VerifyAggregateExpressionsVisitor(GroupByList groupByList)
	{
		this.groupByList = groupByList;
	}


	////////////////////////////////////////////////
	//
	// VISITOR INTERFACE
	//
	////////////////////////////////////////////////

	/**
	 * Verify that this expression is ok
	 * for an aggregate query.  
	 *
	 * @param node 	the node to process
	 *
	 * @return me
	 *
	 * @exception StandardException on ColumnReferernce not
	 *	in group by list, ValueNode or 
	 * 	JavaValueNode that isn't under an
	 * 	aggregate
	 */
	public Visitable visit(Visitable node)
		throws StandardException
	{
		if (node instanceof ColumnReference)
		{
			ColumnReference cr = (ColumnReference)node;
		
			if (groupByList == null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_NON_GROUPED_SELECT_LIST, cr.getFullColumnName());
			}

			if (groupByList.containsColumnReference(cr) == null)
			{
				throw StandardException.newException(SQLState.LANG_INVALID_COL_REF_GROUPED_SELECT_LIST, cr.getFullColumnName());
			}
		} 
		/*
		** Subqueries are only valid if they do not have
		** correlations and are expression subqueries.  RESOLVE:
		** this permits VARIANT expressions in the subquery --
		** should this be allowed?  may be confusing to
		** users to complain about:
		**
		**	select max(x), (select sum(y).toString() from y) from x
		*/
		else if (node instanceof SubqueryNode)
		{
			SubqueryNode subq = (SubqueryNode)node;
		
			if ((subq.getSubqueryType() != SubqueryNode.EXPRESSION_SUBQUERY) ||
				 subq.hasCorrelatedCRs())
			{
				throw StandardException.newException( (groupByList == null) ?
							SQLState.LANG_INVALID_NON_GROUPED_SELECT_LIST :
							SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}

			/*
			** TEMPORARY RESTRICTION: we cannot handle an aggregate
			** in the subquery 
			*/
			HasNodeVisitor visitor = new HasNodeVisitor(AggregateNode.class);
			subq.accept(visitor);
			if (visitor.hasNode())
			{	
				throw StandardException.newException( (groupByList == null) ?
							SQLState.LANG_INVALID_NON_GROUPED_SELECT_LIST :
							SQLState.LANG_INVALID_GROUPED_SELECT_LIST);
			}
		}

		return node;
	}

	/**
	 * Don't visit children under an aggregate
	 *
	 * @param node 	the node to process
	 *
	 * @return true/false
	 */
	public boolean skipChildren(Visitable node)
	{
		return (node instanceof AggregateNode) ||
				(node instanceof SubqueryNode);
	}
	
	public boolean stopTraversal()
	{
		return false;
	}
}	
