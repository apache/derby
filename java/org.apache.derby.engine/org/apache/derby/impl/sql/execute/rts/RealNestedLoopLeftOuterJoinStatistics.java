/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealNestedLoopLeftOuterJoinStatistics

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
import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for NestedLoopLeftOuterJoinResultSet.


*/
public class RealNestedLoopLeftOuterJoinStatistics 
	extends RealNestedLoopJoinStatistics
{

	/* Leave these fields public for object inspectors */
	public int emptyRightRowsReturned;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealNestedLoopLeftOuterJoinStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								int rowsSeenLeft,
								int rowsSeenRight,
								int rowsReturned,
								long restrictionTime,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
//IC see: https://issues.apache.org/jira/browse/DERBY-573
								String userSuppliedOptimizerOverrides,
								ResultSetStatistics leftResultSetStatistics,
								ResultSetStatistics rightResultSetStatistics,
								int emptyRightRowsReturned
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
			rowsSeenLeft,
			rowsSeenRight,
			rowsReturned,
			restrictionTime,
			false,		// We never do an EXISTS join for an outer join
			optimizerEstimatedRowCount,
			optimizerEstimatedCost,
//IC see: https://issues.apache.org/jira/browse/DERBY-573
			userSuppliedOptimizerOverrides,
			leftResultSetStatistics,
			rightResultSetStatistics
			);
		this.emptyRightRowsReturned = emptyRightRowsReturned;
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
			indent + resultSetName + ":" + "\n" + 
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
					" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_SEEN_LEFT) +
					" = " + rowsSeenLeft + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_SEEN_RIGHT) +
					" = " + rowsSeenRight + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_EMPTY_RIGHT_ROWS) +
					" = " + emptyRightRowsReturned + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
					" = " + rowsFiltered + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_RETURNED) +
					" = " + rowsReturned + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_LEFT_RS) +
					":\n" +
			leftResultSetStatistics.getStatementExecutionPlanText(sourceDepth) +
					"\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_RIGHT_RS) +
					":\n" +
			rightResultSetStatistics.getStatementExecutionPlanText(
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
		return leftResultSetStatistics.getScanStatisticsText(tableName, depth)
			+ rightResultSetStatistics.getScanStatisticsText(tableName, depth);
	}

	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}

	protected void setNames()
	{
		nodeName = MessageService.getTextMessage(
											SQLState.RTS_NESTED_LOOP_LEFT_OJ);
		resultSetName = MessageService.getTextMessage(
										SQLState.RTS_NESTED_LOOP_LEFT_OJ_RS);
	}
    public String getRSXplainType() { return XPLAINUtil.OP_JOIN_NL_LO; }
    public String getRSXplainDetails()
    {
        String op_details = "("+this.resultSetNumber + "), " +
            this.resultSetName;

        // check to see if this NL Join is part of an Exist clause
        if (this.oneRowRightSide) op_details+= ", EXISTS JOIN";
        return op_details;
    }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
           this.numOpens,
           null,                           // index updates
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // affected rows
           null,                              // deferred rows
           null,                              // the input rows
           this.rowsSeenLeft,
           this.rowsSeenRight,
           this.rowsFiltered,
           this.rowsReturned,
           this.emptyRightRowsReturned,
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,
           (UUID)timingID);
    }
}
