/*

   Derby - Class org.apache.derbyTesting.functionTests.util.StreamUtil

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.db.*;
import java.sql.*;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Methods for stream columns
 */
public class StreamUtil
{

	public static void insertAsciiColumn
	(
		String 			stmtText, 
		int				colNumber,
		String 			value, 
		int 			length
	)
		throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement(stmtText);
		setAsciiColumn(ps, colNumber, value.charAt(0), length);
		ps.setInt(colNumber + 1, length);
		ps.execute();
	}
	
	public static void insertBinaryColumn
	(
		String 			stmtText, 
		int				colNumber,
		String 			value, 
		int 			length
	)
		throws Throwable
	{
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement(stmtText);
		setBinaryColumn(ps, colNumber, value.charAt(0), length);
		ps.setInt(colNumber + 1, length);
		ps.execute();
	}

	/**
	 * Set a particular column to whatever you
	 * wish.  
	 */
	private static void setAsciiColumn
	(
		PreparedStatement 	ps,
		int					colNumber,
		char				value,
		int					length
	) throws SQLException
	{
		byte[] barray = new byte[length];
		for (int i = 0; i < length; i++)
		{
			barray[i] = (byte)value;
		}
		ps.setAsciiStream(colNumber, new ByteArrayInputStream(barray), length);
	}

	/**
	 * Set a particular column to whatever you
	 * wish.  
	 */
	private static void setBinaryColumn
	(
		PreparedStatement 	ps,
		int					colNumber,
		char				value,
		int					length
	) throws SQLException
	{
		byte[] barray = new byte[length];
		for (int i = 0; i < length; i++)
		{
			barray[i] = (byte)value;
		}
		ps.setBinaryStream(colNumber, new ByteArrayInputStream(barray), length);
	}
	
	public static int getAsciiColumn
	(
		int		whichRS, // 0 means old, 1 means new
		int				colNumber,
		String			value
	) throws Throwable
	{
		System.out.println("\ngetAsciiColumn() called");
        ResultSet rs = getRowSet(whichRS);
        if( rs == null)
            return 0;
		while (rs.next())
		{
			InputStream in = rs.getAsciiStream(colNumber);
			int readlen = drainAndValidateStream(in, value.charAt(0));
			if (readlen != rs.getInt(4))
				throw new Exception("INCORRECT READ LENGTH " + readlen + " <> " + rs.getInt(4));
		}
        return 1;
	}

    private static ResultSet getRowSet( int whichRS)
        throws Throwable
    {
        TriggerExecutionContext tec = org.apache.derby.iapi.db.Factory.getTriggerExecutionContext();
        if( tec == null)
        {
            System.out.println( "Not in a trigger.");
            return null;
        }
        
        return (whichRS == 0) ? tec.getOldRowSet() : tec.getNewRowSet();
    }
    
	public static int getBinaryColumn
	(
		int		whichRS, // 0 means old, 1 means new
		int				colNumber,
		String			value
	) throws Throwable
	{
		System.out.println("\ngetBinaryColumn() called");
        ResultSet rs = getRowSet(whichRS);
        if( rs == null)
            return 0;
		while (rs.next())
		{
			InputStream in = rs.getBinaryStream(colNumber);
			int readlen = drainAndValidateStream(in, value.charAt(0));

			if (readlen != rs.getInt(4))
				throw new Exception("INCORRECT READ LENGTH " + readlen + " <> " + rs.getInt(4));
		}
        return 1;
	}

	private static int drainAndValidateStream(InputStream in, char value)
		throws Throwable
	{
		byte[] buf = new byte[1024];
		int inputLength = 0;
		while(true)
		{
			int size = 0;
			try
			{
				size = in.read(buf);
			} catch(Throwable t)
			{
				System.out.println("Got exception on byte "+inputLength+". Rethrowing...");
				throw t;
			}
			if (size == -1)
				break;

			for (int i = 0; i < size; i++)	
			{
				if (buf[i] != (byte)value)
				{
					throw new Throwable("TEST ERROR: byte "+(i+inputLength)+" not what is expected. It is '"+(char)buf[i]+"' rather than '"+value+"'");
				}
			}
			inputLength += size;	
		}
		// System.out.println("...read "+inputLength+" bytes");
		return inputLength;
	}
}
