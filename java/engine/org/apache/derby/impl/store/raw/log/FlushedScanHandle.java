/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.impl.store.raw.log.LogRecord;
import org.apache.derby.impl.store.raw.log.StreamLogScan;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.ScanHandle;
import org.apache.derby.iapi.store.raw.ScannedTransactionHandle;
import org.apache.derby.iapi.store.raw.log.LogFactory;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.store.raw.xact.TransactionId;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;

public class FlushedScanHandle implements ScanHandle
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	LogFactory lf;
	StreamLogScan fs;
	
	LogRecord lr = null;
	boolean readOptionalData = false;
	int groupsIWant;
	
	ArrayInputStream rawInput = new ArrayInputStream(new byte[4096]);
	
	FlushedScanHandle(LogToFile lf, DatabaseInstant start, int groupsIWant)
		 throws StandardException
	{
		this.lf = lf;
		fs = new FlushedScan(lf,((LogCounter)start).getValueAsLong());
		this.groupsIWant = groupsIWant;
	}
	
	public boolean next() throws StandardException
	{
		readOptionalData = false;
		lr = null; 

		// filter the log stream so that only log records that belong to these
		// interesting groups will be returned

		try
		{
			lr = fs.getNextRecord(rawInput,null, groupsIWant);
			if (lr==null) return false; //End of flushed log
			if (SanityManager.DEBUG)
            {
                if ((groupsIWant & lr.group()) == 0)
                    SanityManager.THROWASSERT(groupsIWant + "/" + lr.group());
            }

			return true;
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
	}

	/**
	  Get the group for the current log record.
	  @exception StandardException Oops
	  */
	public int getGroup() throws StandardException
	{
		return lr.group();
	}

	/**
	  Get the Loggable associated with the currentLogRecord
	  @exception StandardException Oops
	  */
	public Loggable getLoggable() throws StandardException
	{
		try {
			return lr.getLoggable();
		}

		catch (IOException ioe)
		{
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}

		catch (ClassNotFoundException cnfe)
		{
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
	}

	//This may be called only once per log record.
    public InputStream getOptionalData()
		 throws StandardException
	{
		if (SanityManager.DEBUG) SanityManager.ASSERT(!readOptionalData);
		if (lr == null) return null;
		try
		{
			int dataLength = rawInput.readInt();
			readOptionalData = true;
			rawInput.setLimit(rawInput.getPosition(), dataLength);
			return rawInput;
		}

		catch (IOException ioe)
		{
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
	}

    public DatabaseInstant getInstant()
		 throws StandardException
	{
		return fs.getLogInstant();
	}

	public Object getTransactionId()
		 throws StandardException
	{  
		try
        {
			return lr.getTransactionId();
		}
		catch (IOException ioe)
		{
			ioe.printStackTrace();
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                    StandardException.newException(SQLState.LOG_IO_ERROR, ioe));
		}
		catch (ClassNotFoundException cnfe)
		{
			fs.close();
			fs = null;
			throw lf.markCorrupt(
                StandardException.newException(SQLState.LOG_CORRUPTED, cnfe));
		}
	}

    public void close()
	{
		if (fs != null) fs.close();
		fs = null;
	}
}
