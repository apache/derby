/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.util.PropertyUtil;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableProperties;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Properties;


/**
  ResultSetStatistics implemenation for TableScanResultSet.

  @author jerry

*/
public class RealTableScanStatistics 
	extends RealNoPutResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/* Leave these fields public for object inspectors */
	public boolean isConstraint;
	public boolean coarserLock;
	public int		fetchSize;
	public String isolationLevel;
	public String tableName;
	public String indexName;
	public String lockString;
	public String qualifiers;
	public String startPosition;
	public String stopPosition;
	public FormatableProperties scanProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealTableScanStatistics(int numOpens,
									int rowsSeen,
									int rowsFiltered,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber,
									String tableName,
									String indexName,
									boolean isConstraint,
									String qualifiers,
									Properties scanProperties,
									String startPosition,
									String stopPosition,
									String isolationLevel,
									String lockString,
									int fetchSize,
									boolean coarserLock,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost
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
		this.indexName = indexName;
		this.isConstraint = isConstraint;
		this.qualifiers = qualifiers;
		this.scanProperties = new FormatableProperties();
		for (Enumeration e = scanProperties.keys(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			this.scanProperties.put(key, scanProperties.get(key));
		}
		this.startPosition = startPosition;
		this.stopPosition = stopPosition;
		this.isolationLevel = isolationLevel;
		this.lockString = lockString;
		this.fetchSize = fetchSize;
		this.coarserLock = coarserLock;
	}

	// ResultSetStatistics methods

	/**
	 * Return the statement execution plan as a String.
	 *
	 * @param depth	Indentation level
	 *
	 * @return String	The statement executio plan as a String.
	 */
	public String getStatementExecutionPlanText(int depth)
	{
		String header;
		String isolationString = null;

		initFormatInfo(depth);

		if (indexName != null)
		{
			header =
				indent + MessageService.getTextMessage(
											SQLState.RTS_IS_RS_USING,
											tableName,
											MessageService.getTextMessage(
												(isConstraint) ?
													SQLState.RTS_CONSTRAINT :
													SQLState.RTS_INDEX),
											indexName);
				
		}
		else
		{
			header =
				indent + MessageService.getTextMessage(
											SQLState.RTS_TS_RS_FOR,
											tableName);
		}

		header = header + " " + MessageService.getTextMessage(
											SQLState.RTS_LOCKING_OPTIMIZER,
											isolationLevel,
											lockString);

		/* Did we get (or already have) a coarser lock then requested
		 * due to a covering lock, lock escalation or configuration
		 * for table locking.
		 */
		if (coarserLock)
		{
			header = header + " (" + MessageService.getTextMessage(
													SQLState.RTS_ACTUAL_TABLE) +
								")";
		}

		header = header + "\n";

		String scanInfo =
			indent + MessageService.getTextMessage(SQLState.RTS_SCAN_INFO) +
						": \n" +
						PropertyUtil.sortProperties(scanProperties, subIndent);

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
				" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
				" = " + rowsFiltered + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_FETCH_SIZE) +
				" = " + fetchSize + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			((rowsSeen > 0) 
				?
					subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_TIME) +
								" = " + (nextTime / rowsSeen) + "\n"
				: 
					"") + "\n" +
			scanInfo +
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_START_POSITION) +
				": \n" + startPosition + 
			subIndent + MessageService.getTextMessage(
												SQLState.RTS_STOP_POSITION) +
				": \n" + stopPosition +
			subIndent + MessageService.getTextMessage(SQLState.RTS_QUALS) +
				":\n" + qualifiers + "\n" +
			// RESOLVE - estimated row count and cost will eventually 
			// be displayed for all nodes
			dumpEstimatedCosts(subIndent);
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
		if ((tableName == null) || (tableName.equals(this.tableName)))
			return getStatementExecutionPlanText(depth);
		else
			return "";
	}


	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}

	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
	return MessageService.getTextMessage(
				indexName == null ?
				SQLState.RTS_TABLE_SCAN :
				SQLState.RTS_INDEX_SCAN);
  }

	/**
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    if (indexName == null)
      return MessageService.getTextMessage(SQLState.RTS_ON, tableName);
    else
      return MessageService.getTextMessage(SQLState.RTS_ON_USING,
												tableName,
												indexName);
  }
}
