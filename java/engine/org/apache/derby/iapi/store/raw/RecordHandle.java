/*

   Derby - Class org.apache.derby.iapi.store.raw.RecordHandle

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

import org.apache.derby.iapi.services.locks.Lockable;

/**
	A handle to a record within a container. This interface does not provide
	an information about the data of the record, it is only used to perform
	updates, deletes and allow ordered record manipulation.

	MT - immutable

	@see Page
*/

public interface RecordHandle extends Lockable {

	/*****************************************************************
	 * Special record Identifiers.
	 *
	 * Reserved identifiers that does not represent a row but rather have their
	 * own special meaning.  No real rows will ever have these record
	 * identifiers.
	 *****************************************************************/

	/** An invalid record handle */
	public static final int INVALID_RECORD_HANDLE = 0;
 
	/**
		A lock with this recordHandle protects all the recordIds in the page.
		No recordId can disappear while this lock is held. 
		New recordIds may appear while this lock is held.
	*/
	public static final int RECORD_ID_PROTECTION_HANDLE = 1;

	/**
		A lock with this recordHandle protects this deallocated page from
		being freed and reallocated.  This lock is released when the 
		transaction that deallocated the page terminates, at which point 
		the page can be freed if the transaction committed.
	*/
	public static final int DEALLOCATE_PROTECTION_HANDLE = 2;

	/**
		A lock with this recordHandle is used to lock the range of keys 
        between the first key in a btree and keys previous to it.
	*/
	public static final int PREVIOUS_KEY_HANDLE = 3;

	/**
		Reserve for future use - name it and define it when you have a need to
		use one
	*/
	public static final int RESERVED4_RECORD_HANDLE = 4;
	public static final int RESERVED5_RECORD_HANDLE = 5;
	
	/** 
		First recordId that is used to identify a record.
	*/
	public static final int FIRST_RECORD_ID = 6;

	/**
		Obtain the page-unique identifier for this record.
		This id combined with a page number is guaranteed to be unique
		within a container.
	*/
	public int	getId();

	/**
		Obtain the page number this record lives on.
	*/
	public long getPageNumber();

    /**
     * What slot number might the record be at?
     * <p>
     * The raw store guarantees that the record handle of a record will not
     * change, but it's slot number may.  An implementation of a record handle
     * may provide a hint of the slot number, which may help routines like
     * Page.getSlotNumber() perform better.
     * <p>
     * If an implementation does not track slot numbers at all the 
     * implementation should just always return Page.FIRST_SLOT_NUMBER.
     *
	 * @return The slot number the record handle may be at.
     **/
    public int getSlotNumberHint();

	/**
		Return the identity of my container.
	*/
	public ContainerKey getContainerId();

	/**
		Return the identity of my Page.
	*/
	public Object getPageId();

}
