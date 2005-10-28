/*

   Derby - Class org.apache.derby.iapi.sql.execute.RunTimeStatistics

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

import java.io.Serializable;

import java.sql.Time;
import java.sql.Timestamp;

/**
	
  * A RunTimeStatistics object is a representation of the query execution plan and run
  * time statistics for a java.sql.ResultSet.
  * 
  * A query execution plan is a tree
  * of execution nodes.  There are a number of possible node types.  Statistics 
  * are accumulated during execution at each node.  The types of statistics include
  * the amount of time spent in specific operations (if STATISTICS TIMING is SET ON),
  * the number of rows passed to the node by its child(ren) and the number of rows
  * returned by the node to its parent.  (The exact statistics are specific to each
  * node type.)
  * <P>
  * RunTimeStatistics is most meaningful for DML statements (SELECT, INSERT, DELETE
  * and UPDATE).
  *
  */
public interface RunTimeStatistics
{
	/**
	 * Get the total compile time for the associated query in milliseconds.
	 * Compile time can be divided into parse, bind, optimize and generate times.
	 * <P>
	 * 0 is returned if STATISTICS TIMING is not SET ON.
	 * 
	 * @return	The total compile time for the associated query in milliseconds.
	 */
	public long getCompileTimeInMillis();

	/**
	 * Get the parse time for the associated query in milliseconds.
	 * <P>
	 * 0 is returned if STATISTICS TIMING is not SET ON.
	 * 
	 * @return	The parse time for the associated query in milliseconds.
	 */
	public long getParseTimeInMillis();

	/**
	 * Get the bind time for the associated query in milliseconds.
	 * 
	 * @return	The bind time for the associated query in milliseconds.
	 */
	public long getBindTimeInMillis();

	/**
	 * Get the optimize time for the associated query in milliseconds.
	 * <P>
	 * 0 is returned if STATISTICS TIMING is not SET ON.
	 * 
	 * @return	The optimize time for the associated query in milliseconds.
	 */
	public long getOptimizeTimeInMillis();

	/**
	 * Get the generate time for the associated query in milliseconds.
	 * <P>
	 * 0 is returned if STATISTICS TIMING is not SET ON.
	 * 
	 * @return	The generate time for the associated query in milliseconds.
	 */
	public long getGenerateTimeInMillis();

	/**
	 * Get the execute time for the associated query in milliseconds.
	 * <P>
	 * 0 is returned if STATISTICS TIMING is not SET ON.
	 * 
	 * @return	The execute time for the associated query in milliseconds.
	 */
	public long getExecuteTimeInMillis();

	/**
	 * Get the timestamp for the beginning of query compilation. 
	 * <P>
	 * A null is returned if STATISTICS TIMING is not SET ON.
	 *
	 * @return	The timestamp for the beginning of query compilation.
	 */
	public Timestamp getBeginCompilationTimestamp(); 

	/**
	 * Get the timestamp for the end of query compilation. 
	 * <P>
	 * A null is returned if STATISTICS TIMING is not SET ON.
	 *
	 * @return	The timestamp for the end of query compilation.
	 */
	public Timestamp getEndCompilationTimestamp(); 

	/**
	 * Get the timestamp for the beginning of query execution. 
	 * <P>
	 * A null is returned if STATISTICS TIMING is not SET ON.
	 *
	 * @return	The timestamp for the beginning of query execution.
	 */
	public Timestamp getBeginExecutionTimestamp(); 

	/**
	 * Get the timestamp for the end of query execution. 
	 * <P>
	 * A null is returned if STATISTICS TIMING is not SET ON.
	 *
	 * @return	The timestamp for the end of query execution.
	 */
	public Timestamp getEndExecutionTimestamp(); 

	/**
	 * Get the name of the associated query or statement.
	 * (This will be an internally generated name if the
	 * user did not assign a name.)
	 *
	 * @return	The name of the associated query or statement.
	 */
	public String getStatementName();

	/**
	 * Get the name of the Stored Prepared Statement used
	 * for the statement.  This method returns
	 * a value only for <i>EXECUTE STATEMENT</i> statements;
	 * otherwise, returns null.
	 * <p>
	 * Note that the name is returned in the schema.name
	 * format (e.g. APP.MYSTMT).
	 *
	 * @return	The Stored Prepared Statement name of 
	 * the associated statement, or null if it is not an EXECUTE 
	 * STATEMENT statement.
	 */
	public String getSPSName();

	/**
	 * Get the text for the associated query or statement.
	 *
	 * @return	The text for the associated query or statement.
	 */
	public String getStatementText();

	/**
	 * Get a String representation of the execution plan 
	 * for the associated query or statement.
	 *
	 * @return	The execution plan for the associated query or statement.
	 */
	public String getStatementExecutionPlanText();

	/**
	 * Get a String representation of the information on the nodes 
	 * relating to table and index scans from the execution plan for 
	 * the associated query or statement.
	 *
	 * @return	The nodes relating to table and index scans
	 * from the execution plan for the associated query or statement.
	 */
	public String getScanStatisticsText();

	/**
	 * Get a String representation of the information on the nodes 
	 * relating to table and index scans from the execution plan for 
	 * the associated query or statement for a particular table.
	 * <P>
	 * @param   tableName The table for which user desires statistics.
	 * <P>
	 * @return	The nodes relating to table and index scans
	 * from the execution plan for the associated query or statement.
	 */
	public String getScanStatisticsText(String tableName);

	/**
	 * Get the estimated row count for the number of rows returned
	 * by the associated query or statement.
	 *
	 * @return	The estimated number of rows returned by the associated
	 * query or statement.
	 */
	public double getEstimatedRowCount();
}
