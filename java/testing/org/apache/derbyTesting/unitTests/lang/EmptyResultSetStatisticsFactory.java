/*

   Derby - Class org.apache.derbyTesting.unitTests.lang.EmptyResultSetStatisticsFactory

   Copyright 1999, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.unitTests.lang;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.error.StandardException;


import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.PreparedStatement;

import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;

import org.apache.derby.iapi.sql.execute.RunTimeStatistics;

import org.apache.derby.impl.sql.execute.rts.ResultSetStatistics;

import java.util.Properties;

/**
 * ResultSetStatisticsFactory provides a wrapper around all of
 * objects associated with run time statistics.
 * <p>
 * This implementation of the protocol is for stubbing out
 * the RunTimeStatistics feature at execution time..
 *
 * @author jerry
 */
public class EmptyResultSetStatisticsFactory 
		implements ResultSetStatisticsFactory
{
	//
	// ExecutionFactory interface
	//
	//
	// ResultSetStatisticsFactory interface
	//

	/**
		@see ResultSetStatisticsFactory#getRunTimeStatistics
	 */
	public RunTimeStatistics getRunTimeStatistics(
			Activation activation, 
			ResultSet rs,
			NoPutResultSet[] subqueryTrackingArray)
		throws StandardException
	{
		return null;
	}

	/**
		@see ResultSetStatisticsFactory#getResultSetStatistics
	 */
	public ResultSetStatistics getResultSetStatistics(ResultSet rs)
	{
		return null;
	}

	/**
		@see ResultSetStatisticsFactory#getResultSetStatistics
	 */
	public ResultSetStatistics getResultSetStatistics(NoPutResultSet rs)
	{
		return null;
	}

	/**
		@see ResultSetStatisticsFactory#getNoRowsResultSetStatistics
	 */
	public ResultSetStatistics getNoRowsResultSetStatistics(ResultSet rs)
	{
		return null;
	}

	//
	// class interface
	//
	public EmptyResultSetStatisticsFactory() 
	{
	}

}
