/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
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
  ResultSetStatistics implemenation for NestedLoopJoinResultSet.

  @author jerry

*/
public class RealNestedLoopJoinStatistics 
	extends RealJoinResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/* Leave these fields public for object inspectors */
	public boolean oneRowRightSide;
	public ResultSetStatistics leftResultSetStatistics;
	public ResultSetStatistics rightResultSetStatistics;

	/* KLUDGE - Prior to 2.5, all joins were nested loop in the join node.  
	 * "Make" this a HashJoin if the right child is a HashScan.
	 */
	protected String nodeName ;
	protected String resultSetName;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealNestedLoopJoinStatistics(
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
								boolean oneRowRightSide,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
								ResultSetStatistics leftResultSetStatistics,
								ResultSetStatistics rightResultSetStatistics
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
			optimizerEstimatedRowCount,
			optimizerEstimatedCost
			);
		this.oneRowRightSide = oneRowRightSide;
		this.leftResultSetStatistics = leftResultSetStatistics;
		this.rightResultSetStatistics = rightResultSetStatistics;

		setNames();
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
			indent + resultSetName + ":\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" + 
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_SEEN_LEFT) +
				" = " + rowsSeenLeft + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_SEEN_RIGHT) +
				" = " + rowsSeenRight + "\n" +
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
			indent + MessageService.getTextMessage(SQLState.RTS_RIGHT_RS) +
				":\n" +
			rightResultSetStatistics.getStatementExecutionPlanText(sourceDepth) + "\n";
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
		return  leftResultSetStatistics.getScanStatisticsText(tableName, depth)
			+ rightResultSetStatistics.getScanStatisticsText(tableName, depth);
	}



	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}

	public java.util.Vector getChildren()
	{
		java.util.Vector children = new java.util.Vector();
	    children.addElement(leftResultSetStatistics);
		children.addElement(rightResultSetStatistics);
	    return children;
	}

	/**
     * Format for display, a name for this node.
	 *
	 */
	public String getNodeName()
	{
		return nodeName;
	}

	protected void setNames()
	{
		if (nodeName == null)
		{
			if (oneRowRightSide)
			{
				nodeName = MessageService.getTextMessage(
										SQLState.RTS_NESTED_LOOP_EXISTS_JOIN);
				resultSetName = MessageService.getTextMessage(
									SQLState.RTS_NESTED_LOOP_EXISTS_JOIN_RS);
			}
			else
			{
				nodeName = MessageService.getTextMessage(
										SQLState.RTS_NESTED_LOOP_JOIN);
				resultSetName = MessageService.getTextMessage(
									SQLState.RTS_NESTED_LOOP_JOIN_RS);
			}
		}
	}
}
