/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealSortStatistics

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
import org.apache.derby.catalog.UUID;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import org.apache.derby.impl.sql.catalog.XPLAINSortPropsDescriptor;
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;

import org.apache.derby.iapi.services.io.StoredFormatIds;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;
import org.apache.derby.iapi.services.io.FormatableProperties;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.util.PropertyUtil;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Enumeration;
import java.util.Properties;

/**
  ResultSetStatistics implemenation for SortResultSet.


*/
public class RealSortStatistics 
	extends RealNoPutResultSetStatistics
{

	/* Leave these fields public for object inspectors */
	public int rowsInput;
	public int rowsReturned;
	public boolean eliminateDuplicates;
	public boolean inSortedOrder;
	public ResultSetStatistics childResultSetStatistics;
	public FormatableProperties sortProperties;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealSortStatistics(
						int numOpens,
						int rowsSeen,
						int rowsFiltered,
						long constructorTime,
						long openTime,
						long nextTime,
						long closeTime,
						int resultSetNumber,
						int rowsInput,
						int rowsReturned,
						boolean eliminateDuplicates,
						boolean inSortedOrder,
						Properties sortProperties,
						double optimizerEstimatedRowCount,
						double optimizerEstimatedCost,
						ResultSetStatistics childResultSetStatistics
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
		this.rowsInput = rowsInput;
		this.rowsReturned = rowsReturned;
		this.eliminateDuplicates = eliminateDuplicates;
		this.inSortedOrder = inSortedOrder;
		this.childResultSetStatistics = childResultSetStatistics;
		this.sortProperties = new FormatableProperties();
		for (Enumeration e = sortProperties.keys(); e.hasMoreElements(); )
		{
			String key = (String)e.nextElement();
			this.sortProperties.put(key, sortProperties.get(key));
		}
	}

	// ResultSetStatistics methods

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

		String sortInfo = (inSortedOrder) ? "" :
			indent + MessageService.getTextMessage(SQLState.RTS_SORT_INFO) +
			": \n" + PropertyUtil.sortProperties(sortProperties, subIndent);

		return
			indent + MessageService.getTextMessage(SQLState.RTS_SORT_RS) +
				":\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_NUM_OPENS) +
				" = " + numOpens + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_ROWS_INPUT) +
				" = " + rowsInput + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_RETURNED) +
				" = " + rowsReturned + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_ELIMINATE_DUPS) +
				" = " + eliminateDuplicates + "\n" +
			indent + MessageService.getTextMessage(
												SQLState.RTS_IN_SORTED_ORDER) +
				" = " + inSortedOrder + "\n" +
			sortInfo +
			dumpTimeStats(indent, subIndent) + "\n" +
			dumpEstimatedCosts(subIndent) + "\n" +
			indent + MessageService.getTextMessage(SQLState.RTS_SOURCE_RS) +
				":\n" +
			childResultSetStatistics.getStatementExecutionPlanText(
																sourceDepth) +
				"\n";
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
		return childResultSetStatistics.getScanStatisticsText(tableName, depth);
	}

	// Class implementation
	
	public String toString()
	{
		return getStatementExecutionPlanText(0);
	}
  public java.util.Vector<ResultSetStatistics> getChildren(){
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    java.util.Vector<ResultSetStatistics> children = new java.util.Vector<ResultSetStatistics>();
    children.addElement(childResultSetStatistics);
    return children;
  }
	/**
   * Format for display, a name for this node.
	 *
	 */
  public String getNodeName(){
    return MessageService.getTextMessage(SQLState.RTS_SORT);
  }
  
  // -----------------------------------------------------
  // XPLAINable Implementation
  // -----------------------------------------------------
  
    public void accept(XPLAINVisitor visitor) {
        int noChildren = 0;
        if(this.childResultSetStatistics!=null) noChildren++;
        
        //inform the visitor
        visitor.setNumberOfChildren(noChildren);
        
        // pre-order, depth-first traversal
        // me first
        visitor.visit(this);
        // then my child
        if(childResultSetStatistics!=null){
            childResultSetStatistics.accept(visitor);
        }
    }
  
    public String getRSXplainType() { return XPLAINUtil.OP_SORT; }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
           this.numOpens,
           null,                              // the number of index updates 
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           this.optimizerEstimatedRowCount,
           this.optimizerEstimatedCost,
           null,                              // the affected rows
           null,                              // the deferred rows
           this.rowsInput,
           this.rowsSeen,
           null,                              // the seen rows right
           this.rowsFiltered,
           this.rowsReturned,
           null,                              // the empty right rows
           null,                           // index key optimization
           (UUID)scanID,
           (UUID)sortID,
           (UUID)stmtID,
           (UUID)timingID);
    }
    public Object getSortPropsDescriptor(Object sortPropsID)
    {
        FormatableProperties props = this.sortProperties;
        
        // create new scan info descriptor with some basic information
        XPLAINSortPropsDescriptor sortRSDescriptor =            
          new XPLAINSortPropsDescriptor(
              (UUID)sortPropsID,      // the sort props UUID
              null,             // the sort type, either (C)onstraint, (I)ndex or (T)able
              null,                // the number of input rows
              null,                // the number of output rows
              null,                // the number of merge runs
              null,             // merge run details
              XPLAINUtil.getYesNoCharFromBoolean(
                    this.eliminateDuplicates),// eliminate duplicates
              XPLAINUtil.getYesNoCharFromBoolean(
                    this.inSortedOrder),      // in sorted order
              null              // distinct_aggregate
            );
        
        // fill additional information from scan properties
        return XPLAINUtil.extractSortProps(sortRSDescriptor,props);
    }
}
