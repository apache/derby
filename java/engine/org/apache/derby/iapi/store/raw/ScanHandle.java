/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.DatabaseInstant;
import java.io.InputStream;

/**
  Inteface for scanning the log from outside the RawStore.
  */
public interface ScanHandle
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	  Position to the next log record. 
	  @return true if the log contains a next flushed log record and
	           false otherwise. If this returns false it is incorrect
			   to make any of the other calls on this interface.
	  @exception StandardException Oops
	  */
	public boolean next() throws StandardException;

	/**
	  Get the group for the current log record.
	  @exception StandardException Oops
	  */
	public int getGroup() throws StandardException;

	/**
	  Get the Loggable associated with the currentLogRecord
	  @exception StandardException Oops
	  */
	public Loggable getLoggable() throws StandardException;
	/**
	  Get an InputStream for reading the optional data associated with
	  the current log record. This may only be called once per log record.
	  @exception StandardException Oops
	  */
    public InputStream getOptionalData() throws StandardException;
	/**
	  Get the DatabaseInstant for the current log record.
	  @exception StandardException Oops
	  */
    public DatabaseInstant getInstant() throws StandardException;
	/**
	  Get the TransactionId for the current log record.
	  @exception StandardException Oops
	  */
	public Object getTransactionId() throws StandardException;
	/**
	  Close this scan.
	  */
    public void close();
}
