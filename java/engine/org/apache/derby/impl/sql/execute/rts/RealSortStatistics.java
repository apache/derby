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
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.iapi.util.PropertyUtil;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Properties;

/**
  ResultSetStatistics implemenation for SortResultSet.

  @author jerry

*/
public class RealSortStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int rowsInput;
	public int rowsReturned;
	public boolean eliminateDuplicates;
	public boolean inSortedOrder;
	public ResultSetStatistics childResultSetStatistics;
	public FormatableProperties sortProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealSortStatistics(
						int numOpens,
						int rowsSeen,
						int rowsFiltered,
						long constructorTime,
						long openTime,
						long nextTime,
						long closeTime,
						int resultSetNumber,
						int rowsInput,
						int rowsReturned,
						boolean eliminateDuplicates,
						boolean inSortedOrder,
						Properties sortProperties,
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
		this.rowsInput = rowsInput;
		this.rowsReturned = rowsReturned;
		this.eliminateDuplicates = eliminateDuplicates;
		this.inSortedOrder = inSortedOrder;
		this.childResultSetStatistics = childResultSetStatistics;
		this.sortProperties = new FormatableProperties();
		for (Enumeration e = sortProperties.keys(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			this.sortProperties.put(key, sortProperties.get(key));
		}
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

		String sortInfo = (inSortedOrder) ? "" :
			indent + MessageService.getTextMessage(SQLState.RTS_SORT_INFO) +
			": \n" + PropertyUtil.sortProperties(sortProperties, subIndent);

		return
			indent + MessageService.getTextMessage(SQLState.RTS_SORT_RS) +
				":\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_INPUT) +
				" = " + rowsInput + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_RETURNED) +
				" = " + rowsReturned + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ELIMINATE_DUPS) +
				" = " + eliminateDuplicates + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_IN_SORTED_ORDER) +
				" = " + inSortedOrder + "\n" +
			sortInfo +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
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
    return MessageService.getTextMessage(SQLState.RTS_SORT);
  }
}
