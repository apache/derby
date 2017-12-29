/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealCurrentOfStatistics

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

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;
import org.apache.derby.shared.common.reference.SQLState;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for CurrentOfResultSet.


*/
public class RealCurrentOfStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealCurrentOfStatistics(
									int numOpens,
									int rowsSeen,
									int rowsFiltered,
									long constructorTime,
									long openTime,
									long nextTime,
									long closeTime,
									int resultSetNumber
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
			0.0d,
			0.0d
			);
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
		initFormatInfo(depth);

		return indent + 
			MessageService.getTextMessage(SQLState.RTS_NOT_IMPL,
						"getStatementExecutionPlanText", "CurrentOfResultSet\n");
			
	}

	/**
	 * Return information on the scan nodes from the statement execution 
	 * plan as a String.
	 *
	 * @param depth	Indentation level.
	 *
	 * @return String	The information on the scan nodes from the 
	 *					statement execution plan as a String.
	 */
	public String getScanStatisticsText(String tableName, int depth)
	{
		return indent + 
			MessageService.getTextMessage(SQLState.RTS_NOT_IMPL,
				"getScanStatisticsText", "CurrentOfResultSet\n");
	}

 	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
	// NOTE: Not internationalizing because "CURRENT OF" are keywords.
    return "Current Of";
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        visitor.setNumberOfChildren(0);
        //
        // Note that visiting this node does nothing in the current XPLAIN
        // implementation. In a future version, we may add XPLAIN support
        // for this node in XPLAINSystemTableVisitor.
        visitor.visit(this);
	}
    public String getRSXplainType() { return XPLAINUtil.OP_CURRENT_OF; }
}
