/*

   Derby - Class org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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
