/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for MaterializedResultSet.

  @author jerry

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
  public java.util.Vector getChildren(){
    java.util.Vector children = new java.util.Vector();
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
}
