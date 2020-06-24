/*

   Derby - Class org.apache.derby.diag.StatementDuration

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
import java.security.PrivilegedAction;
import java.security.AccessController;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Locale;
import org.apache.derby.vti.VTITemplate;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.shared.common.reference.Limits;
import org.apache.derby.shared.common.reference.Property;
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
	private Hashtable<String,String[]> hashTable;

	// Variables for current row
	private String line;
	private int endTimestampIndex;
	private int threadIndex;
	private int xidIndex;
	private int lccidIndex;
	private String[] currentRow;

	private static final String END_TIMESTAMP = " Thread";
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
	public StatementDuration()  throws StandardException
	{
        DiagUtil.checkAccess();

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        final String home = AccessController.doPrivileged
            (
             new PrivilegedAction<String>()
             {
                 public String run()
                 {
                     return System.getProperty( Property.SYSTEM_HOME_PROPERTY );
                 }
             }
             );

		inputFileName = "derby.log";

		if (home != null)
		{
			inputFileName = home + "/" + inputFileName;
		}
	}

	public StatementDuration(String inputFileName)  throws StandardException
	{
        DiagUtil.checkAccess();

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

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			hashTable = new Hashtable<String,String[]>();
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

            endTimestampIndex = line.indexOf( END_TIMESTAMP );
			threadIndex = line.indexOf(BEGIN_THREAD_STRING);
			xidIndex = line.indexOf(BEGIN_XID_STRING);
			lccidIndex = line.indexOf(BEGIN_XID_STRING, xidIndex + 1);

			if (endTimestampIndex != -1 && threadIndex != -1 && xidIndex != -1)
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
				String[] previousRow = hashTable.put(newRow[3],
												   newRow);
				if (previousRow == null)
				{
					continue;
				}

				currentRow = previousRow;
				
				/* Figure out the duration. */
				Timestamp endTs = stringToTimestamp( newRow[0] );
				long end = endTs.getTime() + endTs.getNanos() / 1000000;
				Timestamp startTs = stringToTimestamp( currentRow[0] );
				long start = startTs.getTime() + startTs.getNanos() / 1000000;
				currentRow[5] = Long.toString(end - start);

				return true;
			}
		}
	}
    // Turn a string into a Timestamp
    private Timestamp   stringToTimestamp( String raw ) throws SQLException
    {
        //
        // We have to handle two timestamp formats.
        //
        // 1) Logged timestamps look like this before 10.7 and the fix introduced by DERBY-4752:
        //
        //     2006-12-15 16:14:58.280 GMT
        //
        // 2) From 10.7 onward, logged timestamps look like this:
        //
        //     Fri Aug 26 09:28:00 PDT 2011
        //
        String  trimmed = raw.trim();

        // if we're dealing with a pre-10.7 timestamp
        if ( !Character.isDigit( trimmed.charAt( trimmed.length() -1 ) ) )
        {
            // strip off the trailing timezone, which Timestamp does not expect

            trimmed = trimmed.substring( 0, trimmed.length() - 4 );
            
            return Timestamp.valueOf( trimmed );
        }
        else
        {
            //
            // From 10.7 onward, the logged timestamp was formatted by
            // Date.toString(), which is always formatted using the pattern
            // specified below, and always in US locale.
            //
//IC see: https://issues.apache.org/jira/browse/DERBY-5414
            SimpleDateFormat sdf =
                new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

            try {
                return new Timestamp( sdf.parse( trimmed ).getTime() );
            }
            catch (Exception e)
            {
                throw new SQLException( e.getMessage() );
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
				return line.substring(0, endTimestampIndex);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5071
				StringBuffer output = new StringBuffer(64);
				if (line.indexOf(BEGIN_EXECUTING_STRING) == -1)
				{
					output.append(line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3));
				}
				else
				{

				/* We need to build string until we find the end of the text */
				int endIndex = line.indexOf(END_EXECUTING_STRING, lccidIndex);
				if (endIndex == -1)
				{
//IC see: https://issues.apache.org/jira/browse/DERBY-5071
					output.append(line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3));
				}
				else
				{
					output.append(line.substring(line.indexOf(END_XID_STRING, lccidIndex) + 3,
											endIndex));
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
//IC see: https://issues.apache.org/jira/browse/DERBY-5071
						output.append(line);
					}
					else
					{
						output.append(line.substring(0, endIndex));
					}
				}
				}

				return StringUtil.truncate(output.toString(), Limits.DB2_VARCHAR_MAXWIDTH);

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

		EmbedResultSetMetaData.getResultColumnDescriptor("TS",        Types.VARCHAR, false, 29),
		EmbedResultSetMetaData.getResultColumnDescriptor("THREADID",  Types.VARCHAR, false, 80),
		EmbedResultSetMetaData.getResultColumnDescriptor("XID",       Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor("LCCID",     Types.VARCHAR, false, 10),
//IC see: https://issues.apache.org/jira/browse/DERBY-104
		EmbedResultSetMetaData.getResultColumnDescriptor("LOGTEXT",   Types.VARCHAR, true, Limits.DB2_VARCHAR_MAXWIDTH),
		EmbedResultSetMetaData.getResultColumnDescriptor("DURATION",  Types.VARCHAR, false, 10),
	};
	
    private static final ResultSetMetaData metadata =
//IC see: https://issues.apache.org/jira/browse/DERBY-1984
        new EmbedResultSetMetaData(columnInfo);
}

