/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashScanStatistics

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
import org.apache.derby.iapi.util.PropertyUtil;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableProperties;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;


/**
  ResultSetStatistics implemenation for HashScanResultSet.

  @author jerry

*/
public class RealHashScanStatistics
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public boolean isConstraint;
	public int hashtableSize;
	public int[] hashKeyColumns;
	public String isolationLevel;
	public String lockString;
	public String tableName;
	public String indexName;
	public String nextQualifiers;
	public String scanQualifiers;
	public String startPosition = null;
	public String stopPosition = null;
	public FormatableProperties scanProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashScanStatistics(
									int numOpens,
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
									int hashtableSize,
									int[] hashKeyColumns,
									String scanQualifiers,
									String nextQualifiers,
									Properties scanProperties,
									String startPosition,
									String stopPosition,
									String isolationLevel,
									String lockString,
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
		this.hashtableSize = hashtableSize;
		this.hashKeyColumns = hashKeyColumns;
		this.scanQualifiers = scanQualifiers;
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
		this.startPosition = startPosition;
		this.stopPosition = stopPosition;
		this.isolationLevel = isolationLevel;
		this.lockString = lockString;
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
		String header;
		String isolationString = null;

		initFormatInfo(depth);

		if (indexName != null)
		{
			header =
				indent +
					MessageService.getTextMessage(
										SQLState.RTS_HASH_SCAN_RS_USING,
										tableName,
										MessageService.getTextMessage(
											isConstraint ?
												SQLState.RTS_CONSTRAINT :
												SQLState.RTS_INDEX),
										indexName);
		}
		else
		{
			header =
				indent +
					MessageService.getTextMessage(SQLState.RTS_HASH_SCAN_RS,
														tableName);
		}

		header = header + " " +
					MessageService.getTextMessage(
										SQLState.RTS_LOCKING,
										isolationLevel,
										lockString) +
					": \n";

		String scanInfo =
			indent +
					MessageService.getTextMessage(SQLState.RTS_SCAN_INFO) +
					": \n" +
					PropertyUtil.sortProperties(scanProperties, subIndent);
		
		String hashKeyColumnString;
		if (hashKeyColumns.length == 1)
		{
			hashKeyColumnString = MessageService.getTextMessage(
														SQLState.RTS_HASH_KEY) +
									" " + hashKeyColumns[0];
		}
		else
		{
			hashKeyColumnString = MessageService.getTextMessage(
													SQLState.RTS_HASH_KEYS) +
									" (" + hashKeyColumns[0];
			for (int index = 1; index < hashKeyColumns.length; index++)
			{
				hashKeyColumnString = hashKeyColumnString + "," + hashKeyColumns[index];
			}
			hashKeyColumnString = hashKeyColumnString + ")";
		}

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
						" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_HASH_TABLE_SIZE) +
						" = " + hashtableSize + "\n" +
			indent + hashKeyColumnString + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
						" = " + rowsSeen + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_FILTERED) +
						" = " + rowsFiltered + "\n" +
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
			subIndent + MessageService.getTextMessage(
													SQLState.RTS_SCAN_QUALS) +
					":\n" + scanQualifiers + "\n" +
			subIndent + MessageService.getTextMessage(
													SQLState.RTS_NEXT_QUALS) +
					":\n" + nextQualifiers + "\n" +
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
			return (String)"";
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
    return MessageService.getTextMessage(
										SQLState.RTS_ON_USING,
										tableName,
										indexName);
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_HASH_SCAN);
  }
}
