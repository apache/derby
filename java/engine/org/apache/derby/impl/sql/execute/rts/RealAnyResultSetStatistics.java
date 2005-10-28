/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealAnyResultSetStatistics

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
  ResultSetStatistics implemenation for AnyResultSet.

  @author jerry

*/
public class RealAnyResultSetStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int subqueryNumber;
	public int pointOfAttachment;
	public ResultSetStatistics childResultSetStatistics;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealAnyResultSetStatistics(
								int numOpens,
								int rowsSeen,
								int rowsFiltered,
								long constructorTime,
								long openTime,
								long nextTime,
								long closeTime,
								int resultSetNumber,
								int subqueryNumber,
								int pointOfAttachment,
								double optimizerEstimatedRowCount,
								double optimizerEstimatedCost,
								ResultSetStatistics childResultSetStatistics)
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
		this.subqueryNumber = subqueryNumber;
		this.pointOfAttachment = pointOfAttachment;
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
		String attachmentString = (pointOfAttachment == -1) ? ":" :
				(" (" +
					MessageService.getTextMessage(SQLState.RTS_ATTACHED_TO)
					+ " " + pointOfAttachment + "):");
		initFormatInfo(depth);

		return
			indent +
				MessageService.getTextMessage(SQLState.RTS_BEGIN_SQ_NUMBER) +
				" " + subqueryNumber + "\n" +
			indent +
				MessageService.getTextMessage(SQLState.RTS_ANY_RS) +
				" " + attachmentString + "\n" + 
			indent +
				MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent +
				MessageService.getTextMessage(SQLState.RTS_ROWS_SEEN) +
				" = " + rowsSeen + "\n" +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			indent +
				MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
				":\n" +
			childResultSetStatistics.getStatementExecutionPlanText(sourceDepth) + "\n" +
			indent +
				MessageService.getTextMessage(SQLState.RTS_END_SQ_NUMBER) +
				" " + subqueryNumber + "\n";
	}

	/**
	 * Return information on the scan nodes from the statement execution 
	 * plan as a String.
	 *
	 * @param tableName if not-NULL then return information for this table only
	 * @param depth	Indentation level.
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
    return MessageService.getTextMessage(SQLState.RTS_ANY_RS);
  }
}
