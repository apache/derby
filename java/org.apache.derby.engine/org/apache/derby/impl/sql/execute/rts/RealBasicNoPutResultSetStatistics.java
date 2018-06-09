/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealBasicNoPutResultSetStatistics

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
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.apache.derby.catalog.UUID;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;


import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Vector;


/**
  ResultSetStatistics implemenation for BasicNoPutResultSetImpl.


*/
abstract class RealBasicNoPutResultSetStatistics
	implements ResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int numOpens;
	public int rowsSeen;
	public int rowsFiltered;
	public long constructorTime;
	public long openTime;
	public long nextTime;
	public long closeTime;
	public long inspectOverall;
	public long inspectNum;
	public String inspectDesc;
	public double optimizerEstimatedRowCount;
	public double optimizerEstimatedCost;

	// CONSTRUCTORS

	/**
	 *
	 *
	 */
    public	RealBasicNoPutResultSetStatistics(
												int numOpens,
												int rowsSeen,
												int rowsFiltered,
												long constructorTime,
												long openTime,
												long nextTime,
												long closeTime,
												double optimizerEstimatedRowCount,
												double optimizerEstimatedCost
											)
	{
		this.numOpens = numOpens;
		this.rowsSeen = rowsSeen;
		this.rowsFiltered = rowsFiltered;
		this.constructorTime = constructorTime;
		this.openTime = openTime;
		this.nextTime = nextTime;
		this.closeTime = closeTime;
		this.optimizerEstimatedRowCount = optimizerEstimatedRowCount;
		this.optimizerEstimatedCost = optimizerEstimatedCost;
	}



	// Class implementation
	/**
	 * Dump out the time information for run time stats.
	 *
	 * @return Nothing.
	 */
	protected final String dumpTimeStats(String indent, String subIndent)
	{
		return
/*
			indent + "time spent in this ResultSet = " +
				getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY) + "\n" +
			indent + "time spent in this ResultSet and below = " +
				getTimeSpent(NoPutResultSet.ENTIRE_RESULTSET_TREE) + "\n" +
				indent + "total time breakdown: " + "\n" +
*/
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CONSTRUCTOR_TIME) +
				" " + constructorTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_OPEN_TIME) +
				" " + openTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_NEXT_TIME) +
				" " + nextTime + "\n" +
			subIndent +
			  MessageService.getTextMessage(SQLState.LANG_CLOSE_TIME) +
				" " + closeTime;
	}

	/**
	 * Dump out the estimated cost information
	 *
	 * @return Nothing.
	 */
	protected final String dumpEstimatedCosts(String subIndent)
	{
		return	subIndent +
            MessageService.getTextMessage(SQLState.RTS_OPT_EST_RC,
                optimizerEstimatedRowCount) + "\n" +
				subIndent +
            MessageService.getTextMessage(SQLState.RTS_OPT_EST_COST,
                 optimizerEstimatedCost);
	}

	/**
	 * Get the objects to be displayed when this tree object is expanded.
	 * <P>
	 * The objects returned can be of any type, including addtional Inspectables.
   *
	 * @return java.util.Vector	A vector of objects.
	 */
  public Vector<ResultSetStatistics> getChildren(){
    return new Vector<ResultSetStatistics>();
  }
	/**
   * Return the time for all operations performed by this node, and the children
   * of this node.  The times included open, next, and close.
	 *
	 */
  public long getTotalTime(){
    //The method below is the original calculation.  However, the constructor
    //time was found to be inaccurate, and was therefore removed from the calculation.
	  //return constructorTime + openTime + nextTime + closeTime;
	  return openTime + nextTime + closeTime;
  }

	/**
   * Return the time for all operations performed by the children of this node.
	 *
	 */
  public long getChildrenTime(){
    long childrenTime = 0;
    java.util.Enumeration e = getChildren().elements();
    while (e.hasMoreElements()){
      childrenTime = childrenTime + ((RealBasicNoPutResultSetStatistics)e.nextElement()).getTotalTime();
    }
    return childrenTime;
  }

	/**
   * Return the time for all operations performed by this node, but not the
   * time for the children of this node.
	 *
	 */
  public long getNodeTime(){
    return getTotalTime() - getChildrenTime();
  }

	/**
   * Format for display, a name for this node.
	 *
	 */
  public abstract String getNodeName();

	/**
	 * If this node is on a database item (like a table or an index), then provide a
   * string that describes the on item.
   *
	 */
  public String getNodeOn(){
    return "";
  }


	/**
	 * Get the estimated row count for the number of rows returned
	 * by the associated query or statement.
	 *
	 * @return	The estimated number of rows returned by the associated
	 * query or statement.
	 */
	public double getEstimatedRowCount()
	{
		return optimizerEstimatedRowCount;
	}

    public String getRSXplainDetails() { return null; }

    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           this.numOpens,
           null,                              // the number of index updates 
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // the affected rows
           null,                              // the deferred rows
           null,                              // the input rows
           this.rowsSeen,
           null,                              // the seen rows right
           this.rowsFiltered,
           this.rowsSeen - this.rowsFiltered,
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
           XPLAINUtil.getAVGNextTime( (long)this.nextTime, this.rowsSeen),
           null,                          // the projection time
           null,                          // the restriction time
           null,                          // the temp_cong_create_time
           null                           // the temo_cong_fetch_time
        );
    }
    public Object getSortPropsDescriptor(Object UUID)
    {
        return null; // Most statistics classes don't have sort props
    }
    public Object getScanPropsDescriptor(Object UUID)
    {
        return null; // Most statistics classes don't have Scan props
    }
}
