/*

   Derby - Class org.apache.derby.iapi.store.raw.log.Logger

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.raw.log;

import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Loggable;
import org.apache.derby.iapi.store.raw.Compensation;

import org.apache.derby.iapi.store.raw.xact.RawTransaction;
import org.apache.derby.iapi.store.raw.xact.TransactionFactory;
import org.apache.derby.iapi.store.raw.xact.TransactionId;

import org.apache.derby.iapi.store.raw.data.DataFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.LimitObjectInput;

public interface Logger {

	/**
		Log the loggable operation under the context of the transaction and then
		apply the operation to the RawStore.

		<BR>
		Before you call this method, make sure that the Loggable's doMe
		method will succeed.  This method will go ahead and send the log record
		to disk, and once it does that, then doMe cannot fail or the system
		will be shut down and recovery may fail.  So it is <B> very important 
		</B> to make sure that every resource you need for the loggable's doMe
		method, such as disk space, has be acquired or accounted for before
		calling logAndDo.

		@param xact		the transaction that is affecting the change
		@param operation	the loggable operation that describes the change
		@return LogInstant that is the LogInstant of the loggable operation 

		@exception StandardException	Standard Cloudscape error policy
	   */ 
	public LogInstant logAndDo(RawTransaction xact, Loggable operation)
		 throws StandardException; 

	/**
		Log the compensation operation under the context of the transaction 
        and then apply the undo to the RawStore.

		<BR>
		Before you call this method, make sure that the Compensation's doMe
		method will succeed.  This method will go ahead and send the log record
		to disk, and once it does that, then doMe cannot fail or the system
		will be shut down and recovery may fail.  So it is <B> very important 
		</B> to make sure that every resource you need for the Compensation's 
        doMe method, such as disk space, has be acquired or accounted for before
		calling logAndUnDo.

		@param xact		the transaction that is affecting the undo
		@param operation	the compensation operation
		@param undoInstant	the logInstant of the change that is to be undone
		@param in			optional data

		@return LogInstant that is the LogInstant of the compensation operation

		@exception StandardException	Standard Cloudscape error policy
	   */ 
	public LogInstant logAndUndo(RawTransaction xact,
								 Compensation operation, LogInstant undoInstant,
								 LimitObjectInput in)
		 throws StandardException;

	/**
		Flush all unwritten log record up to the log instance indicated to disk.

		@param where flush log up to here

		@exception StandardException cannot flush due to sync error
	*/
	public void flush(LogInstant where) throws StandardException;


	/**
		Flush all unwritten log to disk

		@exception StandardException cannot flush due to sync error
	*/
	public void flushAll() throws StandardException;

    /**
     * During recovery re-prepare a transaction.
     * <p>
     * After redo() and undo(), this routine is called on all outstanding 
     * in-doubt (prepared) transactions.  This routine re-acquires all 
     * logical write locks for operations in the xact, and then modifies
     * the transaction table entry to make the transaction look as if it
     * had just been prepared following startup after recovery.
     * <p>
     *
     * @param t             is the transaction performing the re-prepare
     * @param undoId        is the transaction ID to be re-prepared
     * @param undoStopAt    is where the log instant (inclusive) where the 
     *                      re-prepare should stop.
     * @param undoStartAt   is the log instant (inclusive) where re-prepare 
     *                      should begin, this is normally the log instant of 
     *                      the last log record of the transaction that is to 
     *                      be re-prepare.  If null, then re-prepare starts 
     *                      from the end of the log.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public void reprepare(
    RawTransaction  t,
    TransactionId   undoId,
    LogInstant      undoStopAt,
    LogInstant      undoStartAt) 
        throws StandardException;

	/**
	  Undo transaction.

	  @param t is the transaction performing the rollback
	  @param undoId is the transaction ID to be rolled back
	  @param undoStopAt is where the log instant (inclusive) where 
				the rollback should stop.
	  @param undoStartAt is the log instant (inclusive) where rollback
				should begin, this is normally the log instant of 
				the last log record of the transaction that is 
				to be rolled back.  
				If null, then rollback starts from the end of the log.

		@exception StandardException	Standard Cloudscape error policy
	  */
	public void undo(RawTransaction t,
					 TransactionId undoId,
					 LogInstant undoStopAt,
					 LogInstant undoStartAt) throws StandardException;
}
