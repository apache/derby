/*

   Derby - Class org.apache.derby.impl.sql.compile.IndexToBaseRowNode

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

import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;
import java.util.Vector;

/**
 * This node type translates an index row to a base row.  It takes a
 * FromBaseTable as its source ResultSetNode, and generates an
 * IndexRowToBaseRowResultSet that takes a TableScanResultSet on an
 * index conglomerate as its source.
 */
public class IndexToBaseRowNode extends FromTable
{
	protected FromBaseTable	source;
	protected ConglomerateDescriptor	baseCD;
	protected boolean	cursorTargetTable;
	protected PredicateList restrictionList;
	protected boolean	forUpdate;
	private FormatableBitSet	heapReferencedCols;
	private FormatableBitSet	indexReferencedCols;

	public void init(
			Object	source,
			Object	baseCD,
			Object	resultColumns,
			Object	cursorTargetTable,
			Object heapReferencedCols,
			Object indexReferencedCols,
			Object restrictionList,
			Object forUpdate,
			Object tableProperties)
	{
		super.init(null, tableProperties);
		this.source = (FromBaseTable) source;
		this.baseCD = (ConglomerateDescriptor) baseCD;
		this.resultColumns = (ResultColumnList) resultColumns;
		this.cursorTargetTable = ((Boolean) cursorTargetTable).booleanValue();
		this.restrictionList = (PredicateList) restrictionList;
		this.forUpdate = ((Boolean) forUpdate).booleanValue();
		this.heapReferencedCols = (FormatableBitSet) heapReferencedCols;
		this.indexReferencedCols = (FormatableBitSet) indexReferencedCols;
	}

	/** @see Optimizable#forUpdate */
	public boolean forUpdate()
	{
		return source.forUpdate();
	}

	/** @see Optimizable#getTrulyTheBestAccessPath */
	public AccessPath getTrulyTheBestAccessPath()
	{
		// Get AccessPath comes from base table.
		return ((Optimizable) source).getTrulyTheBestAccessPath();
	}

	public CostEstimate getCostEstimate()
	{
		return source.getTrulyTheBestAccessPath().getCostEstimate();
	}

	public CostEstimate getFinalCostEstimate()
	{
		return source.getFinalCostEstimate();
	}

	/**
	 * Return whether or not the underlying ResultSet tree
	 * is ordered on the specified columns.
	 * RESOLVE - This method currently only considers the outermost table 
	 * of the query block.
	 *
	 * @param	crs					The specified ColumnReference[]
	 * @param	permuteOrdering		Whether or not the order of the CRs in the array can be permuted
	 * @param	fbtVector			Vector that is to be filled with the FromBaseTable	
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
	boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, Vector fbtVector)
				throws StandardException
	{
		return source.isOrderedOn(crs, permuteOrdering, fbtVector);
	}

	/**
	 * Generation of an IndexToBaseRowNode creates an
	 * IndexRowToBaseRowResultSet, which uses the RowLocation in the last
	 * column of an index row to get the row from the base conglomerate (heap).
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  for the method to be built
	 *
	 * @return	A compiled Expression that returns a ResultSet that
	 *			gets a base from from an index row.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		ValueNode		restriction = null;

		/*
		** Get the next ResultSet #, so that we can number this ResultSetNode,
		** its ResultColumnList and ResultSet.
		*/
		assignResultSetNumber();

		// Get the CostEstimate info for the underlying scan
		costEstimate = getFinalCostEstimate();

		/* Put the predicates back into the tree */
		if (restrictionList != null)
		{
			restriction = restrictionList.restorePredicates();
			/* Allow the restrictionList to get garbage collected now
			 * that we're done with it.
			 */
			restrictionList = null;
		}

		// for the restriction, we generate an exprFun
		// that evaluates the expression of the clause
		// against the current row of the child's result.
		// if the restriction is empty, simply pass null
		// to optimize for run time performance.

   		// generate the function and initializer:
   		// Note: Boolean lets us return nulls (boolean would not)
   		// private Boolean exprN()
   		// {
   		//   return <<restriction.generate(ps)>>;
   		// }
   		// static Method exprN = method pointer to exprN;



		int heapColRefItem = -1;
		int indexColRefItem = -1;
		if (heapReferencedCols != null)
		{
			heapColRefItem = acb.addItem(heapReferencedCols);
		}
		if (indexReferencedCols != null)
		{
			indexColRefItem = acb.addItem(indexReferencedCols);
		}

		/* Create the ReferencedColumnsDescriptorImpl which tells which columns
		 * come from the index.
		 */
		int indexColMapItem = acb.addItem(new ReferencedColumnsDescriptorImpl(getIndexColMapping()));
		long heapConglomNumber = baseCD.getConglomerateNumber();
		StaticCompiledOpenConglomInfo scoci = getLanguageConnectionContext().
												getTransactionCompile().
													getStaticCompiledConglomInfo(heapConglomNumber);

		acb.pushGetResultSetFactoryExpression(mb);

		mb.push(heapConglomNumber);
		mb.push(acb.addItem(scoci));
		acb.pushThisAsActivation(mb);
		source.generate(acb, mb);
		
		mb.upCast(ClassName.NoPutResultSet);

		resultColumns.generateHolder(acb, mb,  heapReferencedCols, indexReferencedCols);
		mb.push(resultSetNumber);
		mb.push(source.getBaseTableName());
		mb.push(heapColRefItem);
		mb.push(indexColRefItem);
		mb.push(indexColMapItem);

		// if there is no restriction, we just want to pass null.
		if (restriction == null)
		{
		   	mb.pushNull(ClassName.GeneratedMethod);
		}
		else
		{
			// this sets up the method and the static field.
			// generates:
			// 	Object userExprFun { }
			MethodBuilder userExprFun = acb.newUserExprFun();

			// restriction knows it is returning its value;
	
			/* generates:
			 *    return <restriction.generate(acb)>;
			 * and adds it to userExprFun
			 * NOTE: The explicit cast to DataValueDescriptor is required
			 * since the restriction may simply be a boolean column or subquery
			 * which returns a boolean.  For example:
			 *		where booleanColumn
			 */
			restriction.generate(acb, userExprFun);
			userExprFun.methodReturn();

			// we are done modifying userExprFun, complete it.
			userExprFun.complete();

	   		// restriction is used in the final result set as an access of the new static
   			// field holding a reference to this new method.
			// generates:
			//	ActivationClass.userExprFun
			// which is the static field that "points" to the userExprFun
			// that evaluates the where clause.
   			acb.pushMethodReference(mb, userExprFun);
		}

		mb.push(forUpdate);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		closeMethodArgument(acb, mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getIndexRowToBaseRowResultSet",
						ClassName.NoPutResultSet, 15);

		/* The IndexRowToBaseRowResultSet generator is what we return */

		/*
		** Remember if this result set is the cursor target table, so we
		** can know which table to use when doing positioned update and delete.
		*/
		if (cursorTargetTable)
		{
			acb.rememberCursorTarget(mb);
		}
	}

	/**
	 * Return whether or not the underlying ResultSet tree will return
	 * a single row, at most.
	 * This is important for join nodes where we can save the extra next
	 * on the right side if we know that it will return at most 1 row.
	 *
	 * @return Whether or not the underlying ResultSet tree will return a single row.
	 * @exception StandardException		Thrown on error
	 */
	public boolean isOneRowResultSet()	throws StandardException
	{
		// Default is false
		return source.isOneRowResultSet();
	}

	/**
	 * Return whether or not the underlying FBT is for NOT EXISTS.
	 *
	 * @return Whether or not the underlying FBT is for NOT EXISTS.
	 */
	public boolean isNotExists()
	{
		return source.isNotExists();
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
	void decrementLevel(int decrement)
	{
		source.decrementLevel(decrement);
	}

	/**
	 * Get the lock mode for the target of an update statement
	 * (a delete or update).  The update mode will always be row for
	 * CurrentOfNodes.  It will be table if there is no where clause.
	 *
	 * @return	The lock mode
	 */
	public int updateTargetLockMode()
	{
		return source.updateTargetLockMode();
	}

	/**
	 * Notify the underlying result set tree that the result is
	 * ordering dependent.  (For example, no bulk fetch on an index
	 * if under an IndexRowToBaseRow.)
	 *
	 * @return Nothing.
	 */
	void markOrderingDependent()
	{
		/* NOTE: We use a different method to tell a FBT that
		 * it cannot do a bulk fetch as the ordering issues are
		 * specific to a FBT being under an IRTBR as opposed to a
		 * FBT being under a PRN, etc.
		 */
		source.disableBulkFetch();
	}

	/** 
	 * Fill in the column mapping for those columns coming from the index.
	 *
	 * @return The int[] with the mapping.
	 */
	private int[] getIndexColMapping()
	{
		int		rclSize = resultColumns.size();
		int[]	indexColMapping = new int[rclSize];

		for (int index = 0; index < rclSize; index++)
		{
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if (indexReferencedCols != null && rc.getExpression() instanceof VirtualColumnNode)
			{
				// Column is coming from index
				VirtualColumnNode vcn = (VirtualColumnNode) rc.getExpression();
				indexColMapping[index] =
					vcn.getSourceColumn().getVirtualColumnId() - 1;
			}
			else
			{
				// Column is not coming from index
				indexColMapping[index] = -1;
			}
		}

		return indexColMapping;
	}

}
