/*

   Derby - Class org.apache.derby.iapi.store.raw.data.RawContainerHandle

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

package org.apache.derby.iapi.store.raw.data;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.log.LogInstant;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.util.ByteArray;

/**
		RawContainerHandle is the form of ContainerHandle that is used within
		the raw store.  This allows the raw store to have a handle on dropped
		container without exposing this to the external interface, which is not
		allowed to get back a handle on a dropped container
*/

public interface RawContainerHandle extends ContainerHandle {

	/** A container can be in 4 states:
		non_existent - this is represented by a null ContainerHandle

		NORMAL - this is the normal case, container has been created and is not dropped.
		DROPPED - container has been dropped, but is not known whether the drop
						has been committed or not
		COMMITTED_DROP - container has been dropped and has committed.  To
						everyone else except recovery, this state is equivalent
						to NON_EXISTENT
	*/
	public static final int NORMAL = 1;
	public static final int DROPPED = 2;
	public static final int COMMITTED_DROP = 4;

	/** 
		Return the status of the container - one of NORMAL, DROPPED, COMMITTED_DROP.
		@exception StandardException  Standard cloudscape exception policy
	*/
	public int getContainerStatus() throws StandardException;

	/**
		Remove the container.

		@exception StandardException  Standard cloudscape exception policy
	*/
	public void removeContainer(LogInstant instant) throws StandardException;

	/**
		If drop is true, drop the container.  if drop is false, un-drop the
		container
		@exception StandardException  Standard cloudscape exception policy
	*/
	public void dropContainer(LogInstant instant, boolean drop) throws StandardException;

	/**
		Get the logged container version
		@exception StandardException  Standard cloudscape exception policy
	*/
	public long getContainerVersion() throws StandardException;

	/**
		Return a Page that represents any page - alloc page, valid page, free page, 
		dealloced page etc.

		@exception StandardException Standard Cloudscape error policy
	*/
	public Page getAnyPage(long pageNumber) throws StandardException;


	/** Backup restore support */

	/**
		ReCreate a page for load tran - called by recovery redo ONLY

		@exception StandardException Standard Cloudscape error policy
	 */
	public Page reCreatePageForLoadTran(int pageFormat, 
										long pageNumber, 
										long pageOffset)
		 throws StandardException;

	/**
		Log all information necessary to recreate the container during a load
		tran.

		@exception StandardException Standard Cloudscape error policy
	 */
	public ByteArray logCreateContainerInfo() throws StandardException;

	 /**
	   The container is about to be modified.
	   Loggable actions use this to make sure the container gets cleaned if a
	   checkpoint is taken after any log record is sent to the log stream but
	   before the container is actually dirtied.

		@exception StandardException Standard Cloudscape error policy
	 */
	public void preDirty(boolean preDirtyOn) throws StandardException;

}
