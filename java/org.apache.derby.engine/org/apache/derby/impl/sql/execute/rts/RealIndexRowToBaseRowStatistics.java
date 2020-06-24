/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealIndexRowToBaseRowStatistics

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

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;

/**
  ResultSetStatistics implemenation for IndexRowToBaseRowResultSet.


*/
public class RealIndexRowToBaseRowStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public String tableName;
	public ResultSetStatistics childResultSetStatistics;
	public String colsAccessedFromHeap;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealIndexRowToBaseRowStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								String tableName,
								FormatableBitSet colsAccessedFromHeap,
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
		this.tableName = tableName;
		this.colsAccessedFromHeap = (colsAccessedFromHeap == null) ?
										"{" +
										MessageService.getTextMessage(
															SQLState.RTS_ALL) +
										"}" :
									colsAccessedFromHeap.toString();
		this.childResultSetStatistics = childResultSetStatistics;
	}

	// ResultSetStatistics interface

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
							SQLState.RTS_IRTBR_RS,
							tableName) +
							":" + "\n" + 
			indent + MessageService.getTextMessage(
							SQLState.RTS_NUM_OPENS) +
							" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(
							SQLState.RTS_ROWS_SEEN) +
							" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
							SQLState.RTS_COLS_ACCESSED_FROM_HEAP) +
							" = " + colsAccessedFromHeap + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
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
		if ((tableName == null) || 
			(tableName.equals(this.tableName)))		
			return getStatementExecutionPlanText(depth);
		else
			return (String)"";
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
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    return MessageService.getTextMessage(
									SQLState.RTS_FOR_TAB_NAME,
									tableName);
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_IRTBR);
  }

	/**
	 * Return the ResultSetStatistics for the child of this node.
	 *
	 * @return The ResultSetStatistics for the child of this node.
	 */
	ResultSetStatistics getChildResultSetStatistics()
	{
		return childResultSetStatistics;
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

    public String getRSXplainType() { return XPLAINUtil.OP_ROWIDSCAN; }
    public String getRSXplainDetails()
    {
        return "("+this.resultSetNumber+")," + this.tableName;
    }
}
