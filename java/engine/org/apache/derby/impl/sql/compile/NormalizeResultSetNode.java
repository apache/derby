/*

   Derby - Class org.apache.derby.impl.sql.compile.NormalizeResultSetNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.compile.OptimizableList;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.sql.compile.OptimizablePredicateList;
import org.apache.derby.iapi.sql.compile.Optimizer;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.compile.Visitor;
import org.apache.derby.iapi.sql.compile.RequiredRowOrdering;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import java.util.Properties;

/**
 * A NormalizeResultSetNode represents a normalization result set for any 
 * child result set that needs one.
 *
 */

public class NormalizeResultSetNode extends SingleChildResultSetNode
{
	/**
	 * this indicates if the normalize is being performed for an Update
	 * statement or not. The row passed to update also has
	 * before values of the columns being updated-- we need not 
	 * normalize these values. 
	 */
	private boolean forUpdate;

	/**
	 * Initializer for a NormalizeResultSetNode.
	 ** ColumnReferences must continue to point to the same ResultColumn, so
	 * that ResultColumn must percolate up to the new PRN.  However,
	 * that ResultColumn will point to a new expression, a VirtualColumnNode, 
	 * which points to the FromTable and the ResultColumn that is the source for
	 * the ColumnReference.  
	 * (The new NRSN will have the original of the ResultColumnList and
	 * the ResultColumns from that list.  The FromTable will get shallow copies
	 * of the ResultColumnList and its ResultColumns.  ResultColumn.expression
	 * will remain at the FromTable, with the PRN getting a new 
	 * VirtualColumnNode for each ResultColumn.expression.)
	 *
	 * This is useful for UNIONs, where we want to generate a DistinctNode above
	 * the UnionNode to eliminate the duplicates, because the type going into the
	 * sort has to agree with what the sort expects.
	 * (insert into t1 (smallintcol) values 1 union all values 2;
	 *
	 * @param childResult	The child ResultSetNode
     * @param targetResultColumnList The target resultColumnList from 
     *                          the InsertNode or UpdateNode. These will
     *                          be the types used for the NormalizeResultSetNode.
	 * @param tableProperties	Properties list associated with the table
	 * @param forUpdate 	tells us if the normalize operation is being
	 * performed on behalf of an update statement. 
	 * @throws StandardException 
	 */

	public void init(
							Object childResult,
                            Object targetResultColumnList,
							Object tableProperties,
							Object forUpdate) throws StandardException
	{
		super.init(childResult, tableProperties);
		this.forUpdate = ((Boolean)forUpdate).booleanValue();

		ResultSetNode rsn  = (ResultSetNode) childResult;
		ResultColumnList rcl = rsn.getResultColumns();
		ResultColumnList targetRCL = (ResultColumnList) targetResultColumnList;
        
		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 * 
		 * Setting this.resultColumns to the modified child result column list,
		 * and making a new copy for the child result set node
		 * ensures that the ProjectRestrictNode restrictions still points to 
		 * the same list.  See d3494_npe_writeup-4.html in DERBY-3494 for a
		 * detailed explanation of how this works.
		 */
		ResultColumnList prRCList = rcl;
		rsn.setResultColumns(rcl.copyListAndObjects());
		// Remove any columns that were generated.
		prRCList.removeGeneratedGroupingColumns();

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the NormalizeResultSetNode's ResultColumnList.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, rsn, and source ResultColumn.)
		 */
		prRCList.genVirtualColumnNodes(rsn, rsn.getResultColumns());
        
		this.resultColumns = prRCList;
		// Propagate the referenced table map if it's already been created
		if (rsn.getReferencedTableMap() != null)
		    {
			setReferencedTableMap((JBitSet) getReferencedTableMap().clone());
		    }
        
        
		if (targetResultColumnList != null) {
		    int size = Math.min(targetRCL.size(), resultColumns.size());
		    for (int index = 0; index < size; index++) {
			ResultColumn sourceRC = (ResultColumn) resultColumns.elementAt(index);
			ResultColumn resultColumn = (ResultColumn) targetRCL.elementAt(index);
			sourceRC.setType(resultColumn.getTypeServices());
		    }
		}
	}


    /**
     *
	 *
	 * @exception StandardException		Thrown on error
     */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		int				erdNumber;

		if (SanityManager.DEBUG)
        SanityManager.ASSERT(resultColumns != null, "Tree structure bad");

		/* Get the next ResultSet #, so that we can number this ResultSetNode, its
		 * ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// build up the tree.

		// Generate the child ResultSet

		// Get the cost estimate for the child
		costEstimate = childResult.getFinalCostEstimate();

		erdNumber = acb.addItem(makeResultDescription());

		acb.pushGetResultSetFactoryExpression(mb);
		childResult.generate(acb, mb);
		mb.push(resultSetNumber);
		mb.push(erdNumber);
		mb.push(costEstimate.rowCount());
		mb.push(costEstimate.getEstimatedCost());
		mb.push(forUpdate);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getNormalizeResultSet",
					ClassName.NoPutResultSet, 6);
	}

	/**
	 * set the Information gathered from the parent table that is 
	 * required to peform a referential action on dependent table.
	 */
	public void setRefActionInfo(long fkIndexConglomId, 
								 int[]fkColArray, 
								 String parentResultSetId,
								 boolean dependentScan)
	{
		childResult.setRefActionInfo(fkIndexConglomId,
								   fkColArray,
								   parentResultSetId,
								   dependentScan);
	}


}
