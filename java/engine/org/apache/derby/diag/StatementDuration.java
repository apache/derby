/*

   Derby - Class org.apache.derby.diag.StatementDuration

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
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.derby.vti.VTITemplate;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.util.StringUtil;

/**
	

	StatementDuration is a virtual table which
	can be used to analyze the execution duration of the statements
	of "interest" in db2j.<!-- -->log or a specified file when
	db2j.<!-- -->language.<!-- -->logStatementText=true.
	

	<P>A limitation is that, for each transaction ID,
	a row will not be returned for the last	statement with that
	transaction id.  (Transaction IDs change within a connection after
	a commit or rollback, if the transaction that just ended modified data.)

    <P>The execution duration is the time between the beginning of
	execution of two successive statements.  There are a number of reasons
	why this time may not be accurate.  The duration could include time
	spent in the application waiting for user input, doing other work, etc.
	It may also only include a portion of the actual execution time, if
	the application executes a new statement before draining the previous
	open ResultSet.  StatementDuration can be used to get a rough sense of
	where the bottlenecks in an application's JDBC code are.

	<P>The StatementDuration virtual table has the following columns:
	<UL><LI>TS varchar(26) - not nullable.  The timestamp of the statement.</LI>
	<LI>THREADID varchar(80) - not nullable.  The thread name.</LI>
	<LI>XID varchar(15) - not nullable.  The transaction ID.</LI>
	<LI>LOGTEXT long varchar - nullable.  Text of the statement or commit or rollback.</LI>
	<LI>DURATION varchar(10) - not nullable.  Duration, in milliseconds, of the statement.</LI>
	</UL>

 */
public class StatementDuration extends VTITemplate
{
	/*
	** private 
	*/
	private boolean gotFile;
	private InputStreamReader inputFileStreamReader;
	private InputStream inputStream;
	private BufferedReader bufferedReader;
	private String inputFileName;
	private Hashtable hashTable;

	// Variables for current row
	private String line;
	private int gmtIndex;
	private int threadIndex;
	private int xidIndex;
	private int lccidIndex;
	private String[] currentRow;

	private static final String GMT_STRING = " GMT";
	private static final String BEGIN_THREAD_STRING = "[";
	private static final String END_THREAD_STRING = "]";
	private static final String BEGIN_XID_STRING = "= ";
	private static final String END_XID_STRING = ")";
	private static final String BEGIN_EXECUTING_STRING = "Executing prepared";
	private static final String END_EXECUTING_STRING = " :End prepared";


	/**
		StatementDuration() accesses the error log in
		derby.system.home, if set, otherwise it looks in the current directory.
		StatementDuration('filename') will access the specified
		file name.
	 */
	public StatementDuration()
	{
		String home = System.getProperty("derby.system.home");

		inputFileName = "derby.log";

		if (home != null)
		{
			inputFileName = home + "/" + inputFileName;
		}
	}

	public StatementDuration(String inputFileName)
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
		@exception SQLException If database access error occurs.
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

			hashTable = new Hashtable();
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

			if (gmtIndex != -1 && threadIndex != -1)
			{
				/* Build a row */
				String[] newRow = new String[6];
				for (int index = 1;
					 index <= 5;
					 index++)
				{
					newRow[index - 1] = setupColumn(index);
				}

				/* NOTE: We need to use the LCCID as the key
				 */
				Object previousRow = hashTable.put(newRow[3],
												   newRow);
				if (previousRow == null)
				{
					continue;
				}

				currentRow = (String[]) previousRow;
				
				/* Figure out the duration. */
				Timestamp endTs = Timestamp.valueOf(newRow[0]);
				long end = endTs.getTime() + endTs.getNanos() / 1000000;
				Timestamp startTs = Timestamp.valueOf(currentRow[0]);
				long start = startTs.getTime() + startTs.getNanos() / 1000000;
				currentRow[5] = Long.toString(end - start);

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
		All columns in StatementDuration VTI have String data types.
		@see java.sql.ResultSet#getString
		@exception SQLException If database access error occurs.
	 */
	public String getString(int columnNumber)
		throws SQLException
	{
		return currentRow[columnNumber - 1];
	}

	private String setupColumn(int columnNumber)
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
				/* Executing prepared statement is a special case as
				 * it could span multiple lines
				 */
				String output;
				if (line.indexOf(BEGIN_EXECUTING_STRING) == -1)
				{
					output = line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3);
				}
				else
				{

				/* We need to build string until we find the end of the text */
				int endIndex = line.indexOf(END_EXECUTING_STRING, lccidIndex);
				if (endIndex == -1)
				{
					output = line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3);
				}
				else
				{
					output = line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3,
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
				return null;
		}
	}


	/**
		@see java.sql.ResultSet#wasNull
	 */
	public boolean wasNull()
	{
		return false;
	}

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("TS",        Types.VARCHAR, false, 26),
		EmbedResultSetMetaData.getResultColumnDescriptor("THREADID",  Types.VARCHAR, false, 80),
		EmbedResultSetMetaData.getResultColumnDescriptor("XID",       Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor("LCCID",     Types.VARCHAR, false, 10),
		EmbedResultSetMetaData.getResultColumnDescriptor("LOGTEXT",   Types.VARCHAR, true, DB2Limit.DB2_VARCHAR_MAXWIDTH),
		EmbedResultSetMetaData.getResultColumnDescriptor("DURATION",  Types.VARCHAR, false, 10),
	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
}

