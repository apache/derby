/*

   Derby - Class org.apache.derby.iapi.store.raw.Loggable

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.util.ByteArray;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.derby.iapi.services.io.LimitObjectInput;


/**
	A Loggable is a record of a change of state or an event that happened 
	in the RawStore in the context of a transaction.
	All changes in the RawStore must be logged.

	This is the root class for all log operations.

	@see Transaction#logAndDo
*/

public interface Loggable extends Formatable {

	/**
		Apply the change indicated by this operation and optional data.

		<B>If this method fail, the system will be shut down because the log
		record has already been written to disk.  Moreover, the log record will
		be replayed during recovery and this doMe method will be called on the
		same page again, so if it fails again, recovery will fail and the
		database cannot be started.  So it is very important to make sure that
		every resource you need, such as disk space, has been acquired before
		the logAndDo method is called! </B>

		<BR>This method cannot acquire any resource (like latching of a page)
		since it is called underneath the logging system, ie., the log record has
		already been written to the log stream.

		<P> The available() method of in indicates how much data can be read, i.e.
		how much was originally written.

		@param xact			the Transaction
		@param instant		the log instant of this operation
		@param in			optional data

		@exception IOException Can be thrown by any of the methods of in.
		@exception StandardException Standard Cloudscape policy.
	*/
	public void doMe(Transaction xact, LogInstant instant, LimitObjectInput in)
		 throws StandardException, IOException;

	/**
		The log operations are responsible to create the ByteArray, and the log
		operations should write out any optional data for the change to the ByteArray.
		The ByteArray can be prepared when the log operation is constructed,
		or it can be prepared when getPreparedLog() is called.

		Called by the log manager to allow the log operation to pass the buffer
		which contains optional data that will be available in to doMe() methods.

		@param out Where and how to write to optional data.

		@exception StandardException Standard Cloudscape policy.
	
	*/
	public ByteArray getPreparedLog() throws StandardException;

	/**
	    Determine if the operation should be reapplied in recovery redo.
		If redo is needed, acquire any resource that is necessary for the
		loggable's doMe method.  These need to be released in the
		releaseResource method.

		<P> The sequence of events in recovery redo of a Loggable operation is:
		<NL>
		<LI> Get the loggable operation.  If loggable.needsRedo is false, then
		no need to redo this operation.
		<LI> If loggable.needsRedo is true, all the resources necessary for
		applying the doMe is acquired in needsRedo.
		<LI> If the loggable is actually a compensation operation, then the
		logging system will find the undoable operation that needs to be
		undone, call compensation.setUndoOp with the undoable operation.
		<LI> The recovery system then calls loggable.doMe, which re-applies the
		loggable operation, or re-applies the compensation operation
		<LI> The recovery system then calls loggable.releaseResource.
		</NL>

		@param xact		The transaction trying to redo this operation
		@return true if operation needs redoing, false if not.

		@exception StandardException Standard Cloudscape policy.

		@see Loggable#releaseResource
	*/
	public boolean needsRedo(Transaction xact) throws StandardException;


	/**
		Release any resource that was acquired for doMe for rollback or
		recovery redo.

		This resource is acquired in either generateUndo (if this is a
		compensation operation during run time rollback or recovery rollback)
		or in needsRedo (if this is during recovery redo).  The run time
		transaction context should have all the resource already acquird for
		run time roll forward, so there is no need to releaseResource during
		run time roll forward.

		This method must be safe to be called multiple times.

	*/
	public void releaseResource(Transaction xact);

	/**
		Each loggable belongs to one or more groups of similar functionality.

		Grouping is a way to quickly sort out log records that are interesting
		to different modules or different implementations.

		When a module makes loggable and sent it to the log file, it must mark
		this loggable with one or more of the following group. 
		If none fit, or if the loggable encompasses functionality that is not
		described in existing groups, then a new group should be introduced.  

		Grouping has no effect on how the record is logged or how it is treated
		in rollback or recovery.

		The following groups are defined. This list serves as the registry of
		all loggable groups.
	*/
	public static final int FIRST = 			0x1;	// the first operation of a transaction
	public static final int LAST = 				0x2;	// the last operation of a transaction
	public static final int COMPENSATION = 		0x4;	// a compensation log record
	public static final int BI_LOG = 			0x8;	// a BeforeImage log record
	public static final int COMMIT =		   0x10; 	// the transaction committed
	public static final int ABORT =			   0x20; 	// the transaction aborted
	public static final int PREPARE =		   0x40; 	// the transaction prepared
	public static final int XA_NEEDLOCK =	   0x80; 	// need to reclaim locks associated with theis log record during XA prepared xact recovery


	public static final int RAWSTORE =		  0x100;	// a log record generated by the raw store
	public static final int FILE_RESOURCE =   0x400;    // related to "non-transactional" files.

	/**
		Get the loggable's group value
	*/
	public int group();

}
