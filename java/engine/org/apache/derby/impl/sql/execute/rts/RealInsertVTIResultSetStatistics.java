/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
  ResultSetStatistics implemenation for InsertVTIResultSet.

  @author jerry

*/
public class RealInsertVTIResultSetStatistics
	extends RealNoRowsResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	/* Leave these fields public for object inspectors */
    public	int 					rowCount;
	public  boolean					deferred;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealInsertVTIResultSetStatistics(
								int rowCount,
								boolean deferred,
								long executeTime,    
								ResultSetStatistics sourceResultSetStatistics
								)
	{
		super(executeTime, sourceResultSetStatistics);
		this.rowCount = rowCount;
		this.deferred = deferred;
		this.sourceResultSetStatistics = sourceResultSetStatistics;
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

		if (sourceResultSetStatistics == null)
			return "";

		return
			indent + MessageService.getTextMessage(
											SQLState.RTS_INSERT_VTI_RESULT_SET) +
					":\n" + 
			indent + MessageService.getTextMessage(SQLState.RTS_DEFERRED) +
					": " + deferred + "\n" + 
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_INSERTED) +
					" = " + rowCount + "\n" +
			dumpTimeStats(indent) + ((sourceResultSetStatistics == null) ? "" :
			sourceResultSetStatistics.getStatementExecutionPlanText(1));
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
		return sourceResultSetStatistics.getScanStatisticsText(tableName, depth);
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
    return MessageService.getTextMessage(SQLState.RTS_INSERT_VTI);
  }
}
