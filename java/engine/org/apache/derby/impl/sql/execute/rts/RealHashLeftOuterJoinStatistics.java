/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealHashLeftOuterJoinStatistics

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

import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;


/**
  ResultSetStatistics implemenation for HashLeftOuterJoinResultSet.


*/
public class RealHashLeftOuterJoinStatistics 
	extends RealNestedLoopLeftOuterJoinStatistics
{


	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealHashLeftOuterJoinStatistics(
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
								double optimizerEstimatedCost,
								String userSuppliedOptimizerOverrides,
								ResultSetStatistics leftResultSetStatistics,
								ResultSetStatistics rightResultSetStatistics,
								int emptyRightRowsReturned
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
			optimizerEstimatedRowCount,
			optimizerEstimatedCost,
			userSuppliedOptimizerOverrides,
			leftResultSetStatistics,
			rightResultSetStatistics,
			emptyRightRowsReturned
			);
	}

	// ResultSetStatistics methods

	// Class implementation
	protected void setNames()
	{
		nodeName = MessageService.getTextMessage(SQLState.RTS_HASH_LEFT_OJ);
		resultSetName =
			MessageService.getTextMessage(SQLState.RTS_HASH_LEFT_OJ_RS);
	}
    public String getRSXplainType() { return XPLAINUtil.OP_JOIN_HASH_LO; }
    public String getRSXplainDetails()
    {
        String op_details = "("+this.resultSetNumber + ")" +
            this.resultSetName       + ", ";

        // check to see if this NL Join is part of an Exist clause
        if (this.oneRowRightSide) op_details+= ", EXISTS JOIN";
        return op_details;
    }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           new Integer(this.numOpens),
           null,                           // index updates
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           new Double(this.optimizerEstimatedRowCount),
           new Double(this.optimizerEstimatedCost),
           null,                              // affected rows
           null,                              // deferred rows
           null,                              // the input rows
           new Integer(this.rowsSeenLeft),
           new Integer(this.rowsSeenRight),
           new Integer(this.rowsFiltered),
           new Integer(this.rowsReturned),
           new Integer(this.emptyRightRowsReturned),
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,
           (UUID)timingID);
    }
}
