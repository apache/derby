/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealVTIStatistics

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

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;


/**
  ResultSetStatistics implemenation for VTIResultSet.

  @author jerry

*/
public class RealVTIStatistics 
	extends RealNoPutResultSetStatistics
{


	/* Leave these fields public for object inspectors */
	public String javaClassName;

	// CONSTRUCTORS
	/**
	 * 
	 *
	 */
    public	RealVTIStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								String javaClassName,
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
		this.javaClassName = javaClassName;
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

		initFormatInfo(depth);

		header = indent + MessageService.getTextMessage(
												SQLState.RTS_VTI_RS,
												javaClassName) +
					":\n"; 

		return
			header +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
				" = " + rowsSeen + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
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
		return getStatementExecutionPlanText(depth);
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
											SQLState.RTS_ON,
											javaClassName);
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_VTI);
  }
}
