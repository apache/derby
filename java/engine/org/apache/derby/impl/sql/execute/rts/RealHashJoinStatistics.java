/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashJoinStatistics

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

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
  ResultSetStatistics implemenation for HashJoinResultSet.

  @author jerry

*/
public class RealHashJoinStatistics 
	extends RealNestedLoopJoinStatistics
{

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashJoinStatistics(
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
			oneRowRightSide,
			optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			leftResultSetStatistics,
			rightResultSetStatistics
			);
	}

	// ResultSetStatistics methods



	// Class implementation

	protected void setNames()
	{
		if (oneRowRightSide)
		{
			nodeName = MessageService.getTextMessage(
												SQLState.RTS_HASH_EXISTS_JOIN);
			resultSetName = MessageService.getTextMessage(
											SQLState.RTS_HASH_EXISTS_JOIN_RS);
		}
		else
		{
			nodeName = MessageService.getTextMessage(
												SQLState.RTS_HASH_JOIN);
			resultSetName = MessageService.getTextMessage(
											SQLState.RTS_HASH_JOIN_RS);
		}
	}
}
