/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealJoinResultSetStatistics

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

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for JoinResultSet.


*/
public abstract class RealJoinResultSetStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int rowsSeenLeft;
	public int rowsSeenRight;
	public int rowsReturned;
	public long restrictionTime;
	public String userSuppliedOptimizerOverrides;


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
										double optimizerEstimatedCost,
										String userSuppliedOptimizerOverrides
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
		this.userSuppliedOptimizerOverrides = userSuppliedOptimizerOverrides;	
	}

 
	// Class implementation
		/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_JOIN);
  }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           this.numOpens,
           null,                           // index updates
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // affected rows
           null,                              // deferred rows
           null,                              // the input rows
           this.rowsSeenLeft,
           this.rowsSeenRight,
           this.rowsFiltered,
           this.rowsReturned,
           null,                              // the empty right rows
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,
           (UUID)timingID);
    }
    public Object getResultSetTimingsDescriptor(Object timingID)
    {
        return new XPLAINResultSetTimingsDescriptor(
           (UUID)timingID,
           this.constructorTime,
           this.openTime,
           this.nextTime,
           this.closeTime,
           this.getNodeTime(),
           XPLAINUtil.getAVGNextTime(
               (long)this.nextTime, (this.rowsSeenLeft+this.rowsSeenRight)),
           null,                          // the projection time
           null,                          // the restriction time
           null,                          // the temp_cong_create_time
           null                           // the temo_cong_fetch_time
        );
    }
}
