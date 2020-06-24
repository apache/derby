/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealNoRowsResultSetStatistics

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
import org.apache.derby.impl.sql.execute.xplain.XPLAINUtil;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Vector;

/**
  ResultSetStatistics implemenation for NoPutResultSetImpl.


*/
abstract class RealNoRowsResultSetStatistics
	implements ResultSetStatistics
{


	/* Leave these fields public for object inspectors */

	/* fields used for formating run time statistics output */
	protected String indent;
	protected String subIndent;
	protected int sourceDepth;
	public	ResultSetStatistics	sourceResultSetStatistics;
	public long executeTime;

	// variables to implement the inspectable interface.
	// Do these have to be public? 
	public long inspectOverall;
	public long inspectNum;
	public String inspectDesc;

	// CONSTRUCTORS

	/**
	 * Initializes the time spent in NoRowsResultSet minus the source
	 * result set.
	 */
	public RealNoRowsResultSetStatistics(long executeTime, 
										 ResultSetStatistics sourceRS)
	{
		if (sourceRS instanceof RealBasicNoPutResultSetStatistics)
			this.executeTime = executeTime -
				((RealBasicNoPutResultSetStatistics)sourceRS).getTotalTime();
	}
 
	/**
	 * Initialize the format info for run time statistics.
	 */
	protected void initFormatInfo(int depth)
	{
		char [] indentchars = new char[depth];
		char [] subIndentchars = new char[depth + 1];
		sourceDepth = depth + 1;

		/*
		** Form an array of tab characters for indentation.
		*/
		subIndentchars[depth] = '\t';
		while (depth > 0)
		{
			subIndentchars[depth - 1] = '\t';
			indentchars[depth - 1] = '\t';
			depth--;
		}
		
		indent = new String(indentchars);
		subIndent = new String(subIndentchars);
	}

	/**
	 * Dump out the time information for run time stats.
	 *
	 * @return String to be printed out.
	 */
	protected String dumpTimeStats(String indent)
	{
		return
			indent + MessageService.getTextMessage(SQLState.RTS_EXECUTE_TIME) +
	                   		" = " + executeTime + "\n";
	}
	/**
	 * Get the objects to be displayed when this tree object is expanded.
	 * <P>
	 * The objects returned can be of any type, including addtional Inspectables.
   *
	 * @return java.util.Vector	A vector of objects.
	 */
  public Vector<ResultSetStatistics> getChildren(){
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
    Vector<ResultSetStatistics> children = new Vector<ResultSetStatistics>();
    children.addElement(sourceResultSetStatistics);
    return children;
  }

	/**
   * Format for display, a name for this node.
	 *
	 */
  public abstract String getNodeName();

	/**
	 * Get the estimated row count for the number of rows returned
	 * by the associated query or statement.
	 *
	 * @return	The estimated number of rows returned by the associated
	 * query or statement.
	 */
	public double getEstimatedRowCount()
	{
		return 0.0;
	}
    public String getRSXplainDetails() { return null; }
    public Object getResultSetDescriptor(Object rsID, Object parentID,
            Object scanID, Object sortID, Object stmtID, Object timingID)
    {
        return new XPLAINResultSetDescriptor(
           (UUID)rsID,
           getRSXplainType(),
           getRSXplainDetails(),
           null,                              // the number of opens
           null,                              // the number of index updates 
           null,                           // lock mode
           null,                           // lock granularity
           (UUID)parentID,
           null,                             // estimated row count
           null,                             // estimated cost
           null,                             // affected rows
           null,                             // deferred rows.
           null,                              // the input rows
           null,                              // the seen rows left
           null,                              // the seen rows right
           null,                              // the filtered rows
           null,                              // the returned rows
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
           null,                                   // the constructor time
           null,                                   // the open time
           null,                                   // the next time
           null,                                   // the close time
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
           this.executeTime,             // the execute time
           null,                                   // the avg next time/row
           null,                                   // the projection time
           null,                                   // the restriction time
           null,                                   // the temp_cong_create_time
           null                                    // the temo_cong_fetch_time
        );
    }
    public Object getSortPropsDescriptor(Object UUID)
    {
        return null; // Most statistics classes don't have sort props
    }
    public Object getScanPropsDescriptor(Object UUID)
    {
        return null; // Most statistics classes don't have sort props
    }
}
