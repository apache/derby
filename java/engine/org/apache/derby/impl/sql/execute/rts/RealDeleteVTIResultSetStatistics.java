/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
  ResultSetStatistics implemenation for DeleteVTIResultSet.

  @author jerry

*/
public class RealDeleteVTIResultSetStatistics
	extends RealNoRowsResultSetStatistics
{

	/* Leave these fields public for object inspectors */
    public	int 					rowCount;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealDeleteVTIResultSetStatistics(
								int rowCount,
								long executeTime,    
								ResultSetStatistics sourceResultSetStatistics
								)
	{
		super(executeTime, sourceResultSetStatistics);
		this.rowCount = rowCount;
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


		return
			indent + MessageService.getTextMessage(SQLState.RTS_DELETE_VTI_RESULT_SET) +
					":\n" + 
			indent + MessageService.getTextMessage(
												SQLState.RTS_ROWS_DELETED) +
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
		if (sourceResultSetStatistics == null)
			return "";
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
    return MessageService.getTextMessage(SQLState.RTS_DELETE_VTI);
  }
}
