/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashJoinStatistics

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.sql.execute.ResultSetStatistics;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;

/**
  ResultSetStatistics implemenation for HashJoinResultSet.


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
								String userSuppliedOptimizerOverrides,
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
			userSuppliedOptimizerOverrides,
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
    public String getRSXplainType() { return XPLAINUtil.OP_JOIN_HASH; }
    public String getRSXplainDetails()
    {
        String op_details = "("+this.resultSetNumber + ")" +
            this.resultSetName       + ", ";

        // check to see if this NL Join is part of an Exist clause
        if (this.oneRowRightSide) op_details+= ", EXISTS JOIN";
        return op_details;
    }
}
