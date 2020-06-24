/*

   Derby - Class org.apache.derby.impl.sql.compile.IndexToBaseRowNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

package	org.apache.derby.impl.sql.compile;

import java.util.List;
import java.util.Properties;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;

/**
 * This node type translates an index row to a base row.  It takes a
 * FromBaseTable as its source ResultSetNode, and generates an
 * IndexRowToBaseRowResultSet that takes a TableScanResultSet on an
 * index conglomerate as its source.
 */
class IndexToBaseRowNode extends FromTable
{
	protected FromBaseTable	source;
	protected ConglomerateDescriptor	baseCD;
	protected boolean	cursorTargetTable;
	protected PredicateList restrictionList;
	protected boolean	forUpdate;
	private FormatableBitSet	heapReferencedCols;
	private FormatableBitSet	indexReferencedCols;
	private FormatableBitSet	allReferencedCols;
	private FormatableBitSet	heapOnlyReferencedCols;

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    IndexToBaseRowNode(
            FromBaseTable    source,
            ConglomerateDescriptor  baseCD,
            ResultColumnList resultColumns,
            boolean          cursorTargetTable,
            FormatableBitSet heapReferencedCols,
            FormatableBitSet indexReferencedCols,
            PredicateList    restrictionList,
            boolean          forUpdate,
            Properties       tableProperties,
            ContextManager   cm)
	{
        super(null, tableProperties, cm);
        this.source = source;
        this.baseCD = baseCD;
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        setResultColumns( resultColumns );
        this.cursorTargetTable = cursorTargetTable;
        this.restrictionList = restrictionList;
        this.forUpdate = forUpdate;
        this.heapReferencedCols = heapReferencedCols;
        this.indexReferencedCols = indexReferencedCols;

//IC see: https://issues.apache.org/jira/browse/DERBY-2226
		if (this.indexReferencedCols == null) {
			this.allReferencedCols = this.heapReferencedCols;
			heapOnlyReferencedCols = this.heapReferencedCols;
		}
		else {
			this.allReferencedCols =
				new FormatableBitSet(this.heapReferencedCols);
			this.allReferencedCols.or(this.indexReferencedCols);
			heapOnlyReferencedCols =
				new FormatableBitSet(allReferencedCols);
			heapOnlyReferencedCols.xor(this.indexReferencedCols);
		}
	}

	/** @see Optimizable#forUpdate */
    @Override
	public boolean forUpdate()
	{
		return source.forUpdate();
	}

	/** @see Optimizable#getTrulyTheBestAccessPath */
    @Override
	public AccessPath getTrulyTheBestAccessPath()
	{
		// Get AccessPath comes from base table.
		return ((Optimizable) source).getTrulyTheBestAccessPath();
	}

    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    CostEstimate getCostEstimate()
	{
		return source.getTrulyTheBestAccessPath().getCostEstimate();
	}

    @Override
    CostEstimate getFinalCostEstimate()
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
     * @param   fbtHolder           List that is to be filled with the FromBaseTable
	 *
	 * @return	Whether the underlying ResultSet tree
	 * is ordered on the specified column.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    boolean isOrderedOn(ColumnReference[] crs, boolean permuteOrdering, List<FromBaseTable> fbtHolder)
				throws StandardException
	{
        return source.isOrderedOn(crs, permuteOrdering, fbtHolder);
	}

	/**
	 * Generation of an IndexToBaseRowNode creates an
	 * IndexRowToBaseRowResultSet, which uses the RowLocation in the last
	 * column of an index row to get the row from the base conglomerate (heap).
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  for the method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
							throws StandardException
	{
		ValueNode		restriction = null;

		/*
		** Get the next ResultSet #, so that we can number this ResultSetNode,
		** its ResultColumnList and ResultSet.
		*/
		assignResultSetNumber();

		// Get the CostEstimate info for the underlying scan
		setCostEstimate( getFinalCostEstimate() );
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

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
		if (heapReferencedCols != null)
		{
			heapColRefItem = acb.addItem(heapReferencedCols);
		}
		int allColRefItem = -1;
//IC see: https://issues.apache.org/jira/browse/DERBY-2226
		if (allReferencedCols != null)
		{
			allColRefItem = acb.addItem(allReferencedCols);
		}
		int heapOnlyColRefItem = -1;
		if (heapOnlyReferencedCols != null)
		{
			heapOnlyColRefItem = acb.addItem(heapOnlyReferencedCols);
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
		source.generate(acb, mb);
		
		mb.upCast(ClassName.NoPutResultSet);

        // Skip over the index columns that are propagated from the source
        // result set, if there are such columns. We won't pass the SQL NULL
        // wrappers down to store for those columns anyways, so no need to
        // generate them in the row template.
        // NOTE: We have to check for the case where indexReferencedCols is
        // not null, but no bits are set. This can happen when we need to get
        // all of the columns from the heap due to a check constraint.
        boolean skipPropagatedCols =
//IC see: https://issues.apache.org/jira/browse/DERBY-6003
                indexReferencedCols != null &&
                indexReferencedCols.getNumBitsSet() != 0;
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
        mb.push(acb.addItem(getResultColumns()
                .buildRowTemplate(heapReferencedCols, skipPropagatedCols)));

		mb.push(getResultSetNumber());
		mb.push(source.getBaseTableName());
		mb.push(heapColRefItem);

//IC see: https://issues.apache.org/jira/browse/DERBY-2226
		mb.push(allColRefItem);
		mb.push(heapOnlyColRefItem);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6464
		mb.push(getCostEstimate().rowCount());
		mb.push(getCostEstimate().getEstimatedCost());
        mb.push( source.getTableDescriptor().getNumberOfColumns() );

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
    @Override
    boolean isOneRowResultSet() throws StandardException
	{
		// Default is false
		return source.isOneRowResultSet();
	}

	/**
	 * Return whether or not the underlying FBT is for NOT EXISTS.
	 *
	 * @return Whether or not the underlying FBT is for NOT EXISTS.
	 */
    @Override
    boolean isNotExists()
	{
		return source.isNotExists();
	}

	/**
	 * Decrement (query block) level (0-based) for this FromTable.
	 * This is useful when flattening a subquery.
	 *
	 * @param decrement	The amount to decrement by.
	 */
    @Override
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
    @Override
    int updateTargetLockMode()
	{
		return source.updateTargetLockMode();
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination()
	{
		/* NOTE: We use a different method to tell a FBT that
		 * it cannot do a bulk fetch as the ordering issues are
		 * specific to a FBT being under an IRTBR as opposed to a
		 * FBT being under a PRN, etc.
		 */
		source.disableBulkFetch();
	}

	/**
	 * @see ResultSetNode#adjustForSortElimination
	 */
    @Override
	void adjustForSortElimination(RequiredRowOrdering rowOrdering)
		throws StandardException
	{
		/* rowOrdering is not important to this specific node, so
		 * just call the no-arg version of the method.
		 */
		adjustForSortElimination();

		/* Now pass the rowOrdering down to source, which may
		 * need to do additional work. DERBY-3279.
		 */
		source.adjustForSortElimination(rowOrdering);
	}

	/** 
	 * Fill in the column mapping for those columns coming from the index.
	 *
	 * @return The int[] with the mapping.
	 */
	private int[] getIndexColMapping()
	{
		int		rclSize = getResultColumns().size();
		int[]	indexColMapping = new int[rclSize];

		for (int index = 0; index < rclSize; index++)
		{
            ResultColumn rc = getResultColumns().elementAt(index);
//IC see: https://issues.apache.org/jira/browse/DERBY-6464

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

	/**
	 * Accept the visitor for all visitable children of this node.
	 * 
	 * @param v the visitor
	 *
	 * @exception StandardException on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-4421
	void acceptChildren(Visitor v)
		throws StandardException
	{
		super.acceptChildren(v);

//IC see: https://issues.apache.org/jira/browse/DERBY-4421
		if (source != null)
		{
			source = (FromBaseTable)source.accept(v);
		}
	}

}
