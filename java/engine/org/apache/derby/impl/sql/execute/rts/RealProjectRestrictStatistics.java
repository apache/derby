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
  ResultSetStatistics implemenation for ProjectRestrictResultSet.

  @author jerry

*/
public class RealProjectRestrictStatistics 
	extends RealNoPutResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/* Leave these fields public for object inspectors */
	public boolean doesProjection;
	public boolean restriction;
	public long restrictionTime;
	public long projectionTime;
	public ResultSetStatistics childResultSetStatistics;
	public ResultSetStatistics[] subqueryTrackingArray;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealProjectRestrictStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								long restrictionTime,
								long projectionTime,
								ResultSetStatistics[] subqueryTrackingArray,
								boolean restriction,
								boolean doesProjection,
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
		this.restriction = restriction;
		this.doesProjection = doesProjection;
		this.restrictionTime = restrictionTime;
		this.projectionTime = projectionTime;
		this.subqueryTrackingArray = subqueryTrackingArray;
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
		String subqueryInfo = "";

		initFormatInfo(depth);

		/* Dump out the statistics for any subqueries */

		if (subqueryTrackingArray != null)
		{
			boolean	foundAttached = false;

			for (int index = 0; index < subqueryTrackingArray.length; index++)
			{
				if (subqueryTrackingArray[index] != null)
				{
					/* Only print attached subqueries message once */
					if (! foundAttached)
					{
						subqueryInfo = indent +
							MessageService.getTextMessage(
												SQLState.RTS_ATTACHED_SQS) +
								":\n";
						foundAttached = true;
					}
					subqueryInfo = subqueryInfo +
						subqueryTrackingArray[index].getStatementExecutionPlanText(sourceDepth);
				}
			}
		}

		return
			subqueryInfo +
			indent + MessageService.getTextMessage(SQLState.RTS_PR_RS) +
				" (" +	resultSetNumber + "):" + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
				" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
				" = " + rowsFiltered + "\n" +
			indent + MessageService.getTextMessage(
													SQLState.RTS_RESTRICTION) +
				" = " + restriction + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_PROJECTION) +
				" = " + doesProjection + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_RESTRICTION_TIME) +
				" = " + restrictionTime + "\n" +
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_PROJECTION_TIME) +
				" = " + projectionTime + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
				":" + "\n" +
			childResultSetStatistics.getStatementExecutionPlanText(sourceDepth);
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
		String subqueryInfo = "";

		/* Dump out the statistics for any subqueries */

		/* RESOLVE - until we externalize RunTimeStats, we just use
		 * this.subqueryTrackingArray since we are currently getting called
		 * on a close() and the StatementContext has changed and doesn't have
		 * a pointer to the top result set.  When we externalize RunTimeStats,
		 * the JDBC Driver will have to push a new context and we will have
		 * to assign the top resultset there.  (Not sure what to do about
		 * insert/update/delete.)
		 *
		NoPutResultSet[] subqueryTrackingArray = sc.getSubqueryTrackingArray();
		 */			

		if (subqueryTrackingArray != null)
		{
			for (int index = 0; index < subqueryTrackingArray.length; index++)
			{
				if (subqueryTrackingArray[index] != null)
				{
					subqueryInfo = subqueryInfo +
								   "\n" +
									MessageService.getTextMessage(
												SQLState.RTS_BEGIN_SQ_NUMBER) +
									" " + index + "\n" +
						subqueryTrackingArray[index].getScanStatisticsText(tableName, depth) +
						MessageService.getTextMessage(
												SQLState.RTS_END_SQ_NUMBER) +
						" " + index + "\n\n";
				}
			}
		}

		return subqueryInfo 
			+ childResultSetStatistics.getScanStatisticsText(tableName, depth);
	}



	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}
  public java.util.Vector getChildren(){
    java.util.Vector children = new java.util.Vector();
    children.addElement(childResultSetStatistics);

	// get all of our subqueries
	if (subqueryTrackingArray != null)
	{
		for (int index = 0; index < subqueryTrackingArray.length; index++)
		{
			if (subqueryTrackingArray[index] != null)
			{
				children.addElement(subqueryTrackingArray[index]);
			}
		}
	}
    return children;
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_PR);
  }
}
