/*

   Derby - Class org.apache.derby.iapi.store.raw.ScanHandle

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
