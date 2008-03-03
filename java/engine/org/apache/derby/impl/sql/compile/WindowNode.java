/*
	Derby - Class org.apache.derby.impl.sql.compile.WindowNode
 
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

import java.util.Properties;

import java.util.Vector;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.util.JBitSet;

/**
 * This node type handles window functions. It takes a
 * FromTable as its source ResultSetNode, and generates an
 * WindowResultSet.
 * 
 * The implementation is based on IndexToBaseRowNode.
 */
public class WindowNode extends SingleChildResultSetNode {	
	
	/* The following members define the window properties 
	 * 
	 * NOTE: Named windows, and window partitions except the full ResultSet 
	 *       are not yet supported.
	 */
	private String windowName;
	private ResultColumnList partitionDefinition;
	private OrderByList orderByList;
	private Object frameDefinition; // TODO
		
	/* 
	 * When there are multiple window function columns in a RCL, 
	 * 'windowFunctionLevel' is used to identify which level this WindowNode 
	 * is at in the chain.
	 */
	private int windowFunctionLevel;
	
	private Properties tableProperties;
	private int numTables;
	
	public void init(Object windowName,
		Object partitionDefinition,
		Object orderByList,
		Object frameDefinition)
		throws StandardException {
		this.windowName = (String) windowName;
		this.partitionDefinition = (ResultColumnList) partitionDefinition;
		this.orderByList = (OrderByList) orderByList;
		this.frameDefinition = (Object) frameDefinition; // TODO		
		this.windowFunctionLevel = -1;
	}

	/*
	 *  ResultSet implementation
	 */
	
	/**
	 * Preprocess a WindowNode by calling into its source preprocess.
	 *
	 * RESOLVE: We should probably push predicates down as well?
	 *
	 * @param numTables			The number of tables in the DML Statement	 
	 * @param fromList			The from list, if any
	 * @param subqueryList		The subquery list, if any
	 * @param predicateList		The predicate list, if any
	 *
	 * @return ResultSetNode at top of preprocessed tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode preprocess(int numTables,
		FromList fromList,
		SubqueryList subqueryList,
		PredicateList predicateList)
		throws StandardException {

		/* Set up the referenced table map */
		this.numTables = numTables;
		referencedTableMap = new JBitSet(numTables);
		int flSize = fromList.size();
		for (int index = 0; index < flSize; index++)
		{
			referencedTableMap.or(((FromTable) fromList.elementAt(index)).
													getReferencedTableMap());
		}			
		
		return this;
	}

	/**
	 * Bind this node. 
	 *
	 * @param fromList		The FROM list for the query this
	 *						expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error. Although this class
	 * doesn't throw this exception, it's subclasses do and hence this method
	 * signature here needs to have throws StandardException 
	 */
	public WindowNode bind(
			FromList fromList, 
			SubqueryList subqueryList,
			Vector	aggregateVector)
		throws StandardException
	{
		/*		 
		 * This is simply a stub returning the new top of the querytree, since 
		 * there is nothing to as long as we only support ROW_NUMBER(). It does 
		 * not need any binding to source result columns.		 
		 */
		return this;
	}
	
	/**
	 * Generation of an WindowNode creates an WindowResultSet
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb	the method  for the method to be built
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generate(ActivationClassBuilder acb,	
		MethodBuilder mb)
		throws StandardException {
		int rclSize = resultColumns.size();
		FormatableBitSet referencedCols = new FormatableBitSet(rclSize);
		int erdNumber = -1;
		int numSet = 0;

		/*
		 ** Get the next ResultSet #, so that we can number this ResultSetNode,
		 ** its ResultColumnList and ResultSet.
		 */
		assignResultSetNumber();

		// Get the CostEstimate info for the underlying scan
		costEstimate = getFinalCostEstimate();

		acb.pushGetResultSetFactoryExpression(mb);

		/* 
		 * Build a FormatableBitSet for columns to copy from source. If there are 
		 * multiple window function coulmns, they will be added right to left.		 
		 */
		int skip = 0;
		for (int index = rclSize-1; index >= 0; index--) {		
			ResultColumn rc = (ResultColumn) resultColumns.elementAt(index);
			if ( rc.isWindowFunction() && skip < this.windowFunctionLevel) {
				// Skip this
				skip++;
				continue;
			}
			// if not
			referencedCols.set(index);
			numSet++;
		}

		erdNumber = acb.addItem(referencedCols);

		acb.pushThisAsActivation(mb); // arg 1

		childResult.generate(acb, mb);	  // arg 2
		mb.upCast(ClassName.NoPutResultSet);

		/* row allocator */
		resultColumns.generateHolder(acb, mb); // arg 3		

		mb.push(resultSetNumber); //arg 4
		mb.push(windowFunctionLevel); //arg 5

		/* Pass in the erdNumber for the referenced column FormatableBitSet */
		mb.push(erdNumber); // arg 6		

		/* There is no restriction at this level, we just want to pass null. */
		mb.pushNull(ClassName.GeneratedMethod); // arg 7
		
		mb.push(costEstimate.rowCount()); //arg 8
		mb.push(costEstimate.getEstimatedCost()); // arg 9

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null,			
			"getWindowResultSet", ClassName.NoPutResultSet, 9);

		/*
		 ** Remember if this result set is the cursor target table, so we
		 ** can know which table to use when doing positioned update and delete.
		 */
		if (cursorTargetTable) {
			acb.rememberCursorTarget(mb);
		}
	}

	/**
	 * Consider materialization for this ResultSet tree if it is valid and cost 
	 * effective. It is not valid if incorrect results would be returned.
	 *
	 * @return Top of the new/same ResultSet tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetNode considerMaterialization(JBitSet outerTables)
		throws StandardException {
		/* 
		 * For queries involving window functions like ROW_NUMBER() we should
		 * most likely materialize the ResultSet.
		 * 
		 * Return a reference to ourselves.
		 */
		return this;
	}

	/**
	 * Return whether or not to materialize this ResultSet tree.
	 *
	 * @return Whether or not to materialize this ResultSet tree.
	 *			would return valid results.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean performMaterialization(JBitSet outerTables)
		throws StandardException {
		/* 
		 * Queries involving window functions will most likely benefit from 
		 * materializing the ResultSet. It does not make sense for ROW_NUMBER 
		 * though, so it should probably depend on what function is evaluated.
		 */
		return false;		
	}
	
	/**
	 * Get the windowFunctionLevel of this WindowNode in case there are 
	 * multiple window functions in a RCL.
	 *
	 * @return the windowFunctionLevel for this window function column 
	 */
	public int getWindowFunctionLevel()
	{
		return this.windowFunctionLevel;
	}
	
	/**
	 * Set the windowFunctionLevel of this WindowNode in case there are 
	 * multiple window functions in a RCL.
	 *
	 * @param level The window function level of this window function column 
	 */	
	public void setWindowFunctionLevel(int level)
	{
		this.windowFunctionLevel = level;
	}
}
