/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealJoinResultSetStatistics

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
  ResultSetStatistics implemenation for JoinResultSet.

  @author jerry

*/
public abstract class RealJoinResultSetStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	public long restrictionTime;


	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealJoinResultSetStatistics(
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
		this.rowsSeenLeft = rowsSeenLeft;
		this.rowsSeenRight = rowsSeenRight;
		this.rowsReturned = rowsReturned;
		this.restrictionTime = restrictionTime;
	}

 
	// Class implementation
		/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_JOIN);
  }
}
