/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.io.LimitObjectInput;
import java.io.IOException;

/**
	An Undoable operation is an operation that changed the state of the RawStore
	in the context of a transaction and this change can be rolled back.

	@see Transaction#logAndDo
	@see Compensation
*/

public interface Undoable extends Loggable { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;


	/**
		Generate a loggable which will undo this change, using the optional
		input if necessary.

		<P><B>NOTE</B><BR>Any logical undo logic must be hidden behind generateUndo.
		During recovery redo, it should not depend on any logical undo logic.

		<P>
		There are 3 ways to implement a redo-only log record:
		<NL>
		<LI>Make the log record a Loggable instead of an Undoable, this is the
		cleanest method.
		<LI>If you want to extend a log operation class that is an Undoable,
		you can then either have generateUndo return null - this is preferred -
		(the log operation's undoMe should never be called, so you can put a
		null body there if the super class you are extending does not implement
		a undoMe).
		<LI>Or, have undoMe do nothing - this is least preferred.
		</NL>

		<P>Any resource (e.g., latched page) that is needed for the
		undoable.undoMe() must be acquired in undoable.generateUndo().
		Moreover, that resource must be identified in the compensation
		operation, and reacquired in the compensation.needsRedo() method during
		recovery redo.
		<BR><B>If you do write your own generateUndo or needsRedo, any
		resource you latch or acquire, you must release them in
		Compensation.doMe() or in Compensation.releaseResource().</B>

		<P> To write a generateUndo operation, find the object that needs to be
		rolled back.  Assuming that it is a page, latch it, put together a
		Compensation operation with the undoOp set to this operation, and save
		the page number in the compensation operation, then
		return the Compensation operation to the logging system.

		<P>
		The sequence of events in a rollback of a undoable operation is
		<NL>
		<LI> The logging system calls undoable.generateUndo.  If this returns
		null, then there is nothing to undo.
		<LI> If generateUndo returns a Compensation operation, then the logging
		system will log the Compensation log record and call
		Compenstation.doMe().  (Hopefully, this just calls the undoable's
		undoMe)
		<LI> After the Compensation operation has been applied, the logging
		system will call compensation.releaseResource(). If you do overwrite a
		super class's releaseResource(), it would be prudent to call
		super.releaseResource() first.
		</NL>

		<P> The available() method of in indicates how much data can be read, i.e.
		how much was originally written.

		@param xact	the transaction doing the rollback
		@return the compensation operation that will rollback this change, or
		null if nothing to undo. 

		@exception IOException Can be thrown by any of the methods of ObjectInput.
		@exception StandardException Standard Cloudscape policy.

		@see Loggable#releaseResource
		@see Loggable#needsRedo

	*/
	public Compensation generateUndo(Transaction xact, LimitObjectInput in)
		 throws StandardException, IOException;

}
