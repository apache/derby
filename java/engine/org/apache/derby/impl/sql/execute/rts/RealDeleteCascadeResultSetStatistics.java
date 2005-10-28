/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealDeleteCascadeResultSetStatistics

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
  ResultSetStatistics implemenation for DeleteCascadeResultSet.

  @author suresht

*/
public class RealDeleteCascadeResultSetStatistics 
	extends RealDeleteResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public 	ResultSetStatistics[] 	dependentTrackingArray;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
	public	RealDeleteCascadeResultSetStatistics(
								int rowCount,
								boolean deferred,
								int indexesUpdated,
								boolean tableLock,
								long executeTime,
								ResultSetStatistics sourceResultSetStatistics,
								ResultSetStatistics[] dependentTrackingArray
								)
	{
		super(rowCount, deferred, indexesUpdated, tableLock, executeTime, sourceResultSetStatistics);
		this.dependentTrackingArray = dependentTrackingArray;
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

		String dependentInfo = "";

		initFormatInfo(depth);

		/* Dump out the statistics for any depedent table for referential actions */
		if (dependentTrackingArray != null)
		{
			boolean	foundAttached = false;

			for (int index = 0; index < dependentTrackingArray.length; index++)
			{
				if (dependentTrackingArray[index] != null)
				{
					/* Only print referential actions on  dependents message once */
					if (! foundAttached)
					{
						dependentInfo = indent  + "\n" +
							MessageService.getTextMessage(
												SQLState.RTS_REFACTION_DEPENDENT) +
								":\n";
						foundAttached = true;
					}
					dependentInfo = dependentInfo +
						dependentTrackingArray[index].getStatementExecutionPlanText(sourceDepth);
				}
			}
		}

		return
			indent +
			  MessageService.getTextMessage(SQLState.RTS_DELETE_CASCADE_RS_USING) +
				" " +
				MessageService.getTextMessage(
					((tableLock) ?
						SQLState.RTS_TABLE_LOCKING : SQLState.RTS_ROW_LOCKING))
				+ ":\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_DEFERRED) +
				": " + deferred + "\n" + 
			indent +
				MessageService.getTextMessage(SQLState.RTS_ROWS_DELETED) +
				" = " + rowCount + "\n" +
			indent +
				MessageService.getTextMessage(SQLState.RTS_INDEXES_UPDATED) +
				" = " + indexesUpdated + "\n" + 
			dumpTimeStats(indent) + ((sourceResultSetStatistics == null) ? "" :
			sourceResultSetStatistics.getStatementExecutionPlanText(1)) +
			dependentInfo;
	}

	/**
	 * Return information on the scan nodes from the statement execution 
	 * plan as a String.
	 *
	 * @param depth	Indentation level.
	 * @param tableName if not NULL then print information for this table only
	 * @return String	The information on the scan nodes from the 
	 *					statement execution plan as a String.
	 */
	public String getScanStatisticsText(String tableName, int depth)
	{
		String dependentInfo = "";


		/* Dump out the statistics for any depedent table scans for referential actions */
		if (dependentTrackingArray != null)
		{
			for (int index = 0; index < dependentTrackingArray.length; index++)
			{
				if (dependentTrackingArray[index] != null)
				{
					dependentInfo = dependentInfo +
								   "\n" +
									MessageService.getTextMessage(
												SQLState.RTS_BEGIN_DEPENDENT_NUMBER) +
									" " + index + "\n" +
						dependentTrackingArray[index].getScanStatisticsText(tableName, depth) +
						MessageService.getTextMessage(
												SQLState.RTS_END_DEPENDENT_NUMBER) +
						" " + index + "\n\n";
				}
			}
		}

		return dependentInfo 
			+ ((sourceResultSetStatistics == null) ? "" :
			   sourceResultSetStatistics.getScanStatisticsText(tableName, depth));
	}


 
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_DELETE_CASCADE);
  }
}






