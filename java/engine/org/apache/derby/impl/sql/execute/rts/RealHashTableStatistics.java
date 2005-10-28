/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashTableStatistics

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;

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
  ResultSetStatistics implemenation for HashTableResultSet.

  @author jerry

*/
public class RealHashTableStatistics
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int hashtableSize;
	public int[] hashKeyColumns;
	public String isolationLevel;
	public String nextQualifiers;
	public FormatableProperties scanProperties;
	public ResultSetStatistics childResultSetStatistics;
	public ResultSetStatistics[] subqueryTrackingArray;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashTableStatistics(
									int numOpens,
									int rowsSeen,
									int rowsFiltered,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber,
									int hashtableSize,
									int[] hashKeyColumns,
									String nextQualifiers,
									Properties scanProperties,
									double optimizerEstimatedRowCount,
									double optimizerEstimatedCost,
									ResultSetStatistics[] subqueryTrackingArray,
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
		this.hashtableSize = hashtableSize;
		this.hashKeyColumns = hashKeyColumns;
		this.nextQualifiers = nextQualifiers;
		this.scanProperties = new FormatableProperties();
		if (scanProperties != null)
		{
			for (Enumeration e = scanProperties.keys(); e.hasMoreElements(); )
			{
				String key = (String)e.nextElement();
				this.scanProperties.put(key, scanProperties.get(key));
			}
		}
		this.subqueryTrackingArray = subqueryTrackingArray;
		this.childResultSetStatistics = childResultSetStatistics;
	}

	// ResultSetStatistics methods

	/**
	 * Return the statement execution plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The statement executio plan as a String.
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

		initFormatInfo(depth);
		
		String hashKeyColumnString;
		if (hashKeyColumns.length == 1)
		{
			hashKeyColumnString =
					MessageService.getTextMessage(SQLState.RTS_HASH_KEY) +
					" " +
					hashKeyColumns[0];
		}
		else
		{
			hashKeyColumnString =
					MessageService.getTextMessage(SQLState.RTS_HASH_KEYS) +
					" (" + hashKeyColumns[0];
			for (int index = 1; index < hashKeyColumns.length; index++)
			{
				hashKeyColumnString = hashKeyColumnString + "," + hashKeyColumns[index];
			}
			hashKeyColumnString = hashKeyColumnString + ")";
		}

		return
			indent + MessageService.getTextMessage(
												SQLState.RTS_HASH_TABLE_RS) +
										" (" +	resultSetNumber + "):" + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_NUM_OPENS) +
										" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_HASH_TABLE_SIZE) +
										" = " + hashtableSize + "\n" +
			indent + hashKeyColumnString + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_SEEN) +
										" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
										" = " + rowsFiltered + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			((rowsSeen > 0) 
				?
					subIndent +
					MessageService.getTextMessage(SQLState.RTS_NEXT_TIME) +
					" = " + (nextTime / rowsSeen) + "\n"
				: 
					"") + "\n" +
			subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_QUALS) +
										":\n" + nextQualifiers + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
										":\n" +
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
		if (tableName == null)
			return getStatementExecutionPlanText(depth);
		else
			return (String)null;
	}


	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}
	/**
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    return "";
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_HASH_TABLE);
  }
}
