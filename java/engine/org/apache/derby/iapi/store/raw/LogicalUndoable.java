/*

   Derby - Class org.apache.derby.iapi.store.raw.LogicalUndoable

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.LimitObjectInput;
import java.io.IOException;

/**
	A LogicalUndoable is a log operation that operates on the content of a page
	and the log operation needs logical undo.  This interface is used by
	LogicalUndo to extract information out of the log record, and to pass back
	to the logging system the real location where the roll back should happen.
	<P>
	It has specific page information such as its segment Id, container Id, page
	number, and it knows how to restore a storable row from the information
	stored in the log record.  

	@see org.apache.derby.iapi.store.access.conglomerate.LogicalUndo
*/
public interface LogicalUndoable extends Undoable
{
	/** 
		Return the containerHandle used by this log operation.  Logical cannot
		change container identity between roll forward and roll back.  This
		method should only be called by LogicalUndo to extract information from
		the log record.

		@exception StandardException Standard Cloudscape error policy
	*/
	public ContainerHandle getContainer() throws StandardException;

	/**
		Return the recordHandle stored in the log operation that correspond to
		the record that was changed in the rollforward.  This method should
		only be called by LogicalUndo to extract information from the log
		record.

	*/
	public RecordHandle getRecordHandle();

	/**
		Restore the row stored in the log operation.   This method should only
		be called by LogicalUndo to extract information from the log record.

		@param row an IN/OUT parameter, caller passed in the row with
		the correct column number and type, the log operation will restore the
		row with the optional data stored in the log record.

		@exception StandardException Standard Cloudscape error policy
		@exception IOException Method may read from in

		@see LogicalUndo
	*/
	public void restoreLoggedRow(Object[] row, LimitObjectInput in)
		throws StandardException, IOException;

	/**
		If the row has moved, reset the record handle that the undo should be applied on.

		@param rh the RecordHandle that represents the row's new location
	*/
	public void resetRecordHandle(RecordHandle rh);
}
