/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute.rts
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Vector;

/**
  ResultSetStatistics implemenation for NoPutResultSetImpl.

  @author jerry

*/
abstract class RealNoRowsResultSetStatistics
	implements ResultSetStatistics
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;


	/* Leave these fields public for object inspectors */

	/* fields used for formating run time statistics output */
	protected String indent;
	protected String subIndent;
	protected int sourceDepth;
	public	ResultSetStatistics	sourceResultSetStatistics;
	protected long executeTime;

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
  public Vector getChildren(){
    Vector children = new Vector();
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
}
