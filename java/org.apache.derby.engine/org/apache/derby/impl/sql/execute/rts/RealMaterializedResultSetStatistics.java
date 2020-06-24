/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealMaterializedResultSetStatistics

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
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;


import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;

/**
  ResultSetStatistics implemenation for MaterializedResultSet.


*/
public class RealMaterializedResultSetStatistics 
	extends RealNoPutResultSetStatistics
{


	/* Leave these fields public for object inspectors */
	public ResultSetStatistics childResultSetStatistics;
	public long createTCTime;
	public long fetchTCTime;

	// CONSTRUCTORS
	/**
	 * 
	 *
	 */
    public	RealMaterializedResultSetStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								long createTCTime,
								long fetchTCTime,
								int resultSetNumber,
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
		this.createTCTime = createTCTime;
		this.fetchTCTime = fetchTCTime;
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
												SQLState.RTS_MATERIALIZED_RS) +
					":\n" + 
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
					" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
					" = " + rowsSeen + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			subIndent + MessageService.getTextMessage(
										SQLState.RTS_TEMP_CONGLOM_CREATE_TIME) +
					" = " + createTCTime + "\n" +
			subIndent + MessageService.getTextMessage(
										SQLState.RTS_TEMP_CONGLOM_FETCH_TIME) +
					" = " + fetchTCTime + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
					":\n" +
			childResultSetStatistics.getStatementExecutionPlanText(sourceDepth) + "\n";
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    java.util.Vector<ResultSetStatistics> children = new java.util.Vector<ResultSetStatistics>();
    children.addElement(childResultSetStatistics);
    return children;
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_MATERIALIZED_RS);
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
    public String getRSXplainType() { return XPLAINUtil.OP_MATERIALIZE; }
    public String getRSXplainDetails() { return "("+this.resultSetNumber +")"; }
  
    public Object getResultSetTimingsDescriptor(Object timingID)
    {
        return new XPLAINResultSetTimingsDescriptor(
           (UUID)timingID,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
           this.constructorTime,
           this.openTime,
           this.nextTime,
           this.closeTime,
           this.getNodeTime(),
           XPLAINUtil.getAVGNextTime( (long)this.nextTime, this.rowsSeen),
           null,                          // the projection time
           null,                          // the restriction time
           this.createTCTime,
           this.fetchTCTime
        );
    }
}
