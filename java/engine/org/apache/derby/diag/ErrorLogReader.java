/*

   Derby - Class org.apache.derby.diag.ErrorLogReader

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.diag;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;

import java.util.Hashtable;
import java.util.Enumeration;
import java.util.Properties;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import org.apache.derby.vti.VTITemplate;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;

/**

	ErrorLogReader is a virtual table interface (VTI) which contains all the statements
	of "interest" in db2j.<!-- -->log or a specified file when
	db2j.<!-- -->language.<!-- -->logStatementText=true.
	
	
	<P>One use of this VTI is to determine the active transactions
	and the SQL statements in those transactions at a given point in time, say
	when a deadlock or lock timeout occurred.  In order to do that, you must first
	find the timestamp (timestampConstant) of interest in the error log.  
	The SQL to view the active transactions at a given in time is:
	<PRE>SELECT vti.ts, threadid, cast(xid as int) as xid_int, cast(lccid as int) as lccid_int, logtext 
		 FROM new org.apache.derby.diag.ErrorLogReader() vti, 
			(VALUES timestampConstant) t(ts)
		 WHERE vti.ts <= t.ts AND 
				vti.ts >
					(SELECT MAX(ts) IS NULL ? '2000-01-01 00:00:00.1' : MAX(ts)
					 FROM new org.apache.derby.diag.ErrorLogReader() vti_i
					 WHERE (logtext LIKE 'Committing%' OR
							logtext LIKE 'Rolling%') AND
						   vti.xid = vti_i.xid AND ts < t.ts)
		 ORDER BY xid_int, vti.ts
	</PRE>

	<P>The ErrorLogReader virtual table has the following columns:
	<UL><LI>TS varchar(26) - the timestamp of the statement.</LI>
	<LI>THREADID varchar(40) - the thread name.</LI>
	<LI>XID varchar(15) - the transaction ID.</LI>
	<LI>LCCID varchar(15) - the connection ID.</LI>
	<LI>DATABASE varchar(128) -  Database name
	<LI>DRDAID  varchar(50) - nullable. DRDA ID for network server session.
	<LI>LOGTEXT long varchar - text of the statement or commit or rollback.</LI>
	</UL>

 */
public class ErrorLogReader extends VTITemplate
{
	/*
	** private 
	*/
	private boolean gotFile;
	private InputStreamReader inputFileStreamReader;
	private InputStream inputStream;
	private BufferedReader bufferedReader;
	private String inputFileName;

	// Variables for current row
	private String line;
	private int gmtIndex;
	private int threadIndex;
	private int xidIndex;
	private int lccidIndex;
	private int databaseIndex;
	private int drdaidIndex;


	private static final String GMT_STRING = " GMT";
	private static final String PARAMETERS_STRING = "Parameters:";
	private static final String BEGIN_THREAD_STRING = "[";
	private static final String END_THREAD_STRING = "]";
	private static final String BEGIN_XID_STRING = "= ";
	private static final String END_XID_STRING = ")";
	private static final String BEGIN_DATABASE_STRING = "(DATABASE =";
	private static final String END_DATABASE_STRING = ")";
	private static final String BEGIN_DRDAID_STRING = "(DRDAID =";
	private static final String END_DRDAID_STRING = ")";
	private static final String BEGIN_EXECUTING_STRING = "Executing prepared";
	private static final String END_EXECUTING_STRING = " :End prepared";


	/**
		ErrorLogReader() accesses the derby.log in
		derby.system.home, if set, otherwise it looks in the current directory.
		ErrorLogReader('filename') will access the specified
		file name.
	 */
	public ErrorLogReader()
	{
		String home = System.getProperty("derby.system.home");

		inputFileName = "derby.log";

		if (home != null)
		{
			inputFileName = home + "/" + inputFileName;
		}
	}

	public ErrorLogReader(String inputFileName)
	{
		this.inputFileName = inputFileName;
	}

	/**
		@see java.sql.ResultSet#getMetaData
	 */
	public ResultSetMetaData getMetaData()
	{
		return metadata;
	}

	/**
		@see java.sql.ResultSet#next
		@exception SQLException If database-access error occurs.
	 */
	public boolean next() throws SQLException
	{
		if (! gotFile)
		{
			gotFile = true;
		    try 
			{
		        inputFileStreamReader = new InputStreamReader(new FileInputStream(inputFileName));
				bufferedReader = new BufferedReader(inputFileStreamReader, 32*1024);
			} 
			catch (FileNotFoundException ex) 
			{
				throw new SQLException(ex.getMessage());
			}
		}

		while (true)
		{
			try
			{
				line = bufferedReader.readLine();
			}
			catch (java.io.IOException ioe)
			{
				throw new SQLException(ioe.getMessage());
			}

			if (line == null)
			{
				return false;
			}

			gmtIndex = line.indexOf(GMT_STRING);
			threadIndex = line.indexOf(BEGIN_THREAD_STRING);
			xidIndex = line.indexOf(BEGIN_XID_STRING);
			lccidIndex = line.indexOf(BEGIN_XID_STRING, xidIndex + 1);
			databaseIndex = line.indexOf(BEGIN_DATABASE_STRING, lccidIndex + 1);
			drdaidIndex = line.indexOf(BEGIN_DRDAID_STRING, databaseIndex + 1);

			// Skip parameters
			if (line.indexOf(PARAMETERS_STRING) != -1)
			{
				continue;
			}

			if (gmtIndex != -1 && threadIndex != -1  && xidIndex != -1 && 
				databaseIndex != -1)
			{
				return true;
			}
		}
	}

	/**
		@see java.sql.ResultSet#close
	 */
	public void close()
	{
		if (bufferedReader != null)
		{
			try
			{
				bufferedReader.close();
				inputFileStreamReader.close();
			}
			catch (java.io.IOException ioe)
			{
				// eat exceptions during close;
			}
			finally
			{
				bufferedReader = null;
				inputFileStreamReader = null;
			}
		}
	}

	/**
		All columns in the Db2jLogReader VTI have a of String type.
		@see java.sql.ResultSet#getString
		@exception SQLException If database-access error occurs.
	 */
	public String getString(int columnNumber)
		throws SQLException
	{
		switch (columnNumber)
		{
			case 1:
				return line.substring(0, gmtIndex);

			case 2:
				return line.substring(threadIndex + 1, line.indexOf(END_THREAD_STRING));

			case 3:
				return line.substring(xidIndex + 2, line.indexOf(END_XID_STRING, xidIndex));

			case 4:
				return line.substring(lccidIndex + 2, line.indexOf(END_XID_STRING, lccidIndex));

			case 5:
				return line.substring(databaseIndex + BEGIN_DATABASE_STRING.length(), line.indexOf(END_DATABASE_STRING, databaseIndex));
			case 6:
				return line.substring(drdaidIndex + BEGIN_DRDAID_STRING.length(), line.indexOf(END_DRDAID_STRING, drdaidIndex));
			case 7:
				/* Executing prepared statement is a special case as
				 * it could span multiple lines
				 */
				String output;
				if (line.indexOf(BEGIN_EXECUTING_STRING) == -1)
				{
					output = line.substring(line.indexOf(END_DRDAID_STRING, drdaidIndex) + 3);
				}
				else
				{

				/* We need to build string until we find the end of the text */
				int endIndex = line.indexOf(END_EXECUTING_STRING, drdaidIndex);
				if (endIndex == -1)
				{
					output = line.substring(line.indexOf(END_DRDAID_STRING, drdaidIndex) + 3);
				}
				else
				{
					output = line.substring(line.indexOf(END_XID_STRING, drdaidIndex) + 3,
											endIndex);
				}

				while (endIndex == -1)
				{
					try
					{
						line = bufferedReader.readLine();
					}
					catch (java.io.IOException ioe)
					{
						throw new SQLException("Error reading file " + ioe);
					}
					endIndex = line.indexOf(END_EXECUTING_STRING);
					if (endIndex == -1)
					{
						output = output + line;
					}
					else
					{
						output = output + line.substring(0, endIndex);
					}
				}
				}

				output = StringUtil.truncate(output, DB2Limit.DB2_VARCHAR_MAXWIDTH);

				return output;

			default:
				return "";
		}
	}


	/**
		@see java.sql.ResultSet#wasNull
	 */
	public boolean wasNull()
	{
		return false;
	}

	/* MetaData
	 */
	
	// column1: TS varchar(26) not null
	// column2: THREADID varchar(40) not null
	// column3: XID  varchar(15) not null
	// column4: LCCID  varchar(15) not null
	// column5: DATABASE varchar(128) not null
	// column6: DRDAID varchar(50) nullable
	// column5: LOGTEXT VARCHAR(max) not null
	private static final ResultColumnDescriptor[] columnInfo = {
		EmbedResultSetMetaData.getResultColumnDescriptor("TS", Types.VARCHAR, false, 26),
		EmbedResultSetMetaData.getResultColumnDescriptor("THREADID", Types.VARCHAR, false, 40),
		EmbedResultSetMetaData.getResultColumnDescriptor("XID", Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor("LCCID", Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor("DATABASE", Types.VARCHAR, false, 128),
		EmbedResultSetMetaData.getResultColumnDescriptor("DRDAID", Types.VARCHAR, true, 50),
		EmbedResultSetMetaData.getResultColumnDescriptor("LOGTEXT",Types.VARCHAR, false, DB2Limit.DB2_VARCHAR_MAXWIDTH)
	};
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);

}


