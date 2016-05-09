/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealScalarAggregateStatistics

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

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.sql.execute.ResultSetStatistics;
import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for ScalarAggregateResultSet.


*/
public class RealScalarAggregateStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int rowsInput = 0;
	public boolean indexKeyOptimization;
	public ResultSetStatistics childResultSetStatistics;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealScalarAggregateStatistics(
						int numOpens,
						int rowsSeen,
						int rowsFiltered,
						long constructorTime,
						long openTime,
						long nextTime,
						long closeTime,
						int resultSetNumber,
						boolean indexKeyOptimization,
						int rowsInput,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost,
						ResultSetStatistics childResultSetStatistics
						)
	{
		super(
			numOpens,
			rowsSeen,
			rowsFiltered,
			constructorTime,
			openTime,
			nextTime,
			closeTime,
			resultSetNumber,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost
			);
		this.indexKeyOptimization = indexKeyOptimization;
		this.rowsInput = rowsInput;
		this.childResultSetStatistics = childResultSetStatistics;
	}

	// ResultSetStatistics methods

	/**
	 * Return the statement execution plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The statement execution plan as a String.
	 */
	public String getStatementExecutionPlanText(int depth)
	{
		initFormatInfo(depth);

		return
			indent + MessageService.getTextMessage(
												SQLState.RTS_SCALAR_AGG_RS) +
				":\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_INPUT) +
				" = " + rowsInput + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_INDEX_KEY_OPT) +
				" = " + indexKeyOptimization + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
				":\n" +
			childResultSetStatistics.getStatementExecutionPlanText(
																sourceDepth) +
				"\n";
	}

	/**
	 * Return information on the scan nodes from the statement execution 
	 * plan as a String.
	 *
	 * @param depth	Indentation level.
	 * @param tableName if not NULL then print information for this table only
	 *
	 * @return String	The information on the scan nodes from the 
	 *					statement execution plan as a String.
	 */
	public String getScanStatisticsText(String tableName, int depth)
	{
		return childResultSetStatistics.getScanStatisticsText(tableName, depth);
	}

	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}
  public java.util.Vector<ResultSetStatistics> getChildren(){
    java.util.Vector<ResultSetStatistics> children = new java.util.Vector<ResultSetStatistics>();
    children.addElement(childResultSetStatistics);
    return children;
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_SCALAR_AGG);
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        int noChildren = 0;
        if(this.childResultSetStatistics!=null) noChildren++;
        
        //inform the visitor
        visitor.setNumberOfChildren(noChildren);

        // pre-order, depth-first traversal
        // me first
        visitor.visit(this);
        // then my child
        if(childResultSetStatistics!=null){
            childResultSetStatistics.accept(visitor);
		}

    }
    public String getRSXplainType() { return XPLAINUtil.OP_AGGREGATE; }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           this.numOpens,
           null,                              // the number of index updates 
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // the affected rows
           null,                              // the deferred rows
           this.rowsInput,
           this.rowsSeen,
           null,                              // the seen rows right
           this.rowsFiltered,
           this.rowsInput - this.rowsFiltered,
           null,                              // the empty right rows
           this.indexKeyOptimization?"Y":"N",
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,
           (UUID)timingID);
    }
}
