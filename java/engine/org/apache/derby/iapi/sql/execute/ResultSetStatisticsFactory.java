/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.sql.execute.RunTimeStatistics;

import org.apache.derby.impl.sql.execute.rts.ResultSetStatistics;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * the result sets statistics objects needed in building the run time statistics.
 *
 * @author jerry
 */
public interface ResultSetStatisticsFactory 
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
		Module name for the monitor's module locating system.
	 */
	String MODULE = "org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory";

	//
	// RunTimeStatistics Object
	//

	/**
	 * RunTimeStatistics creation.
	 *
	 * @param activation	The Activation we are generating the statistics for
	 * @param rs			The top ResultSet for the ResultSet tree
	 * @param subqueryTrackingArray	Array of subqueries, used for finding
	 *								materialized subqueries.
	 *
	 * @exception StandardException on error
	 */
	RunTimeStatistics getRunTimeStatistics(Activation activation, ResultSet rs,
										   NoPutResultSet[] subqueryTrackingArray)
		throws StandardException;


	//
	// ResultSetStatistics Objects
	//

	/**
		Get the matching ResultSetStatistics for the specified ResultSet.
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs);

	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs);

	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs);
}
