/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RunTimeStatisticsImpl

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

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import java.util.Vector;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.sql.Timestamp;

/**
  RunTimeStatistics implemenation.

  @author jerry

*/
public final class RunTimeStatisticsImpl implements RunTimeStatistics
{


	/* Leave these fields public for object inspectors */
	public String	statementText;
	public String  statementName;
	public String  spsName;
	public long   	parseTime;
	public long   	bindTime;
	public long   	optimizeTime;
	public long   	generateTime;
	public long	compileTime;
	public long	executeTime;
	public Timestamp beginCompilationTimestamp;
	public Timestamp endCompilationTimestamp;
	public Timestamp beginExecutionTimestamp;
	public Timestamp endExecutionTimestamp;
	public ResultSetStatistics topResultSetStatistics;
	public ResultSetStatistics[] subqueryTrackingArray;

	// CONSTRUCTORS
	/**
	 * 
	 */
    public	RunTimeStatisticsImpl(
									String spsName,
									String statementName,
									String statementText,
									long compileTime,
									long parseTime,
									long bindTime,
									long optimizeTime,
									long generateTime,
									long executeTime,
									Timestamp beginCompilationTimestamp,
									Timestamp endCompilationTimestamp,
									Timestamp beginExecutionTimestamp,
									Timestamp endExecutionTimestamp,
									ResultSetStatistics[] subqueryTrackingArray,
									ResultSetStatistics topResultSetStatistics)
	{
		this.spsName = spsName;
		this.statementName = statementName;
		this.statementText = statementText;
		this.compileTime = compileTime;
		this.parseTime = parseTime;
		this.bindTime = bindTime;
		this.optimizeTime = optimizeTime;
		this.generateTime = generateTime;
		this.executeTime = executeTime;
		this.beginCompilationTimestamp = beginCompilationTimestamp;
		this.endCompilationTimestamp = endCompilationTimestamp;
		this.beginExecutionTimestamp = beginExecutionTimestamp;
		this.endExecutionTimestamp = endExecutionTimestamp;
		this.subqueryTrackingArray = subqueryTrackingArray;
		this.topResultSetStatistics = topResultSetStatistics;
	}

	// RunTimeStatistics methods
	/**
	 * Get the total compile time for the associated query in milliseconds.
	 * Compile time can be divided into parse, bind, optimize and generate times.
	 * 
	 * @return long		The total compile time for the associated query in milliseconds.
	 */
	public long getCompileTimeInMillis()
	{
		return compileTime;
	}

	/**
	 * Get the parse time for the associated query in milliseconds.
	 * 
	 * @return long		The parse time for the associated query in milliseconds.
	 */
	public long getParseTimeInMillis()
	{
		return parseTime;
	}

	/**
	 * Get the bind time for the associated query in milliseconds.
	 * 
	 * @return long		The bind time for the associated query in milliseconds.
	 */
	public long getBindTimeInMillis()
	{
		return bindTime;
	}

	/**
	 * Get the optimize time for the associated query in milliseconds.
	 * 
	 * @return long		The optimize time for the associated query in milliseconds.
	 */
	public long getOptimizeTimeInMillis()
	{
		return optimizeTime;
	}

	/**
	 * Get the generate time for the associated query in milliseconds.
	 * 
	 * @return long		The generate time for the associated query in milliseconds.
	 */
	public long getGenerateTimeInMillis()
	{
		return generateTime;
	}

	/**
	 * Get the execute time for the associated query in milliseconds.
	 * 
	 * @return long		The execute time for the associated query in milliseconds.
	 */
	public long getExecuteTimeInMillis()
	{
		return executeTime;
	}

	/**
	 * Get the timestamp for the beginning of query compilation. 
	 *
	 * @return java.sql.Timestamp	The timestamp for the beginning of query compilation.
	 */
	public Timestamp getBeginCompilationTimestamp()
	{
		return beginCompilationTimestamp;
	}

	/**
	 * Get the timestamp for the end of query compilation. 
	 *
	 * @return java.sql.Timestamp	The timestamp for the end of query compilation.
	 */
	public Timestamp getEndCompilationTimestamp()
	{
		return endCompilationTimestamp;
	}

	/**
	 * Get the timestamp for the beginning of query execution. 
	 *
	 * @return java.sql.Timestamp	The timestamp for the beginning of query execution.
	 */
	public Timestamp getBeginExecutionTimestamp()
	{
		return beginExecutionTimestamp;
	}

	/**
	 * Get the timestamp for the end of query execution. 
	 *
	 * @return java.sql.Timestamp	The timestamp for the end of query execution.
	 */
	public Timestamp getEndExecutionTimestamp()
	{
		return endExecutionTimestamp;
	}

	/**
	 * Get the name of the associated query or statement.
	 * (This will be an internally generated name if the
	 * user did not assign a name.)
	 *
	 * @return java.lang.String	The name of the associated query or statement.
	 */
	public String getStatementName()	
	{
		return statementName;
	}

	/**
	 * Get the name of the Stored Prepared Statement 
	 * for the statement.
	 *
	 * @return java.lang.String	The SPS name of the associated query or statement.
	 */
	public String getSPSName()	
	{
		return spsName;
	}

	/**
	 * Get the text for the associated query or statement.
	 *
	 * @return java.lang.String	The text for the associated query or statement.
	 */
	public String getStatementText()
	{
		return statementText;
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
		if (topResultSetStatistics == null)
		{
			return 0.0;
		}
		return topResultSetStatistics.getEstimatedRowCount();
	}

	/**
	 * Get the execution plan for the associated query or statement as a String.
	 *
	 * @return java.lang.String	The execution plan for the associated query or statement.
	 */
	public String getStatementExecutionPlanText()	
	{
		if (topResultSetStatistics == null)
		{
			return (String) null;
		}

		String subqueryInfo = "";

		/* Dump out the statistics for any subqueries */

		if (subqueryTrackingArray != null)
		{
			boolean	foundAttached = false;

			for (int index = 0; index < subqueryTrackingArray.length; index++)
			{
				if (subqueryTrackingArray[index] != null)
				{
					/* Only print attached subqueries message once */
					if (! foundAttached)
					{
						subqueryInfo = MessageService.getTextMessage(
											SQLState.RTS_MATERIALIZED_SUBQS) +
										":\n";
						foundAttached = true;
					}
					subqueryInfo = subqueryInfo +
						subqueryTrackingArray[index].getStatementExecutionPlanText(1);
				}
			}
		}
		return subqueryInfo +
			topResultSetStatistics.getStatementExecutionPlanText(0);
	}

	/**
	 * Get the information on the nodes relating to table and index scans
	 * from the execution plan for the associated query or statement as a String.
	 *
	 * @return java.lang.String	The nodes relating to table and index scans
	 * from the execution plan for the associated query or statement.
	 */
	public String getScanStatisticsText()
	{
		return (topResultSetStatistics == null) ? 
			(String)null :
			topResultSetStatistics.getScanStatisticsText(null, 0);
	}

	/**
	 * Get the information on the nodes relating to table and index scans
	 * for table tableName from the execution plan for the associated query 
	 * or statement as a String.
	 *
	 * @param tableName table for which user seeks statistics.
	 *
	 * @return java.lang.String	The nodes relating to table and index scans
	 * from the execution plan for the associated query or statement for 
	 * tableName.
	 */
	public String getScanStatisticsText(String tableName)
	{
		if (topResultSetStatistics == null) 
			return (String)null;
		String s = topResultSetStatistics.getScanStatisticsText(tableName, 0);
		return (s.equals("")) ? null : s;
	}



	// Class implementation
	
	public String toString()
	{
		String spstext = 
			(spsName != null) ? 
					("Stored Prepared Statement Name: \n\t" + spsName + "\n") : 
					"";
		return 
			spstext +
			MessageService.getTextMessage(SQLState.RTS_STATEMENT_NAME) +
				": \n\t" + statementName + "\n" +
			MessageService.getTextMessage(SQLState.RTS_STATEMENT_TEXT) +
				": \n\t" + statementText + "\n" +
			MessageService.getTextMessage(SQLState.RTS_PARSE_TIME) +
				": " + parseTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_BIND_TIME) +
				": " + bindTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_OPTIMIZE_TIME) +
				": " + optimizeTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_GENERATE_TIME) +
				": " + generateTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_COMPILE_TIME) +
				": " + compileTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_EXECUTE_TIME) +
				": " + executeTime + "\n" +
			MessageService.getTextMessage(SQLState.RTS_BEGIN_COMP_TS) +
				" : " + beginCompilationTimestamp + "\n" +
			MessageService.getTextMessage(SQLState.RTS_END_COMP_TS) +
				" : " + endCompilationTimestamp + "\n" +
			MessageService.getTextMessage(SQLState.RTS_BEGIN_EXE_TS) +
				" : " + beginExecutionTimestamp + "\n" +
			MessageService.getTextMessage(SQLState.RTS_END_EXE_TS) +
				" : " + endExecutionTimestamp + "\n" +
			MessageService.getTextMessage(SQLState.RTS_STMT_EXE_PLAN_TXT) +
				": \n" + getStatementExecutionPlanText();
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
    children.addElement(topResultSetStatistics);
    return children;
  }

}
