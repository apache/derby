/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.locks;

import org.apache.derby.iapi.error.StandardException;
import java.util.Enumeration;

/**
	A limit represents a callback on a lock
	group. It is called when the size of
	the group reaches the limit set on the
	call.

	@see LockFactory#setLimit
*/
public interface Limit { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/**
		Called by the lock factory when a limit has been reached.

		@param compatabilitySpace lock space the limit was set for
		@param group lock group the limit was set for
		@param limit the limit's setting
		@param lockList the list of Lockable's in the group
		@param lockCount the number of locks in the group

        @exception StandardException Standard Cloudscape error policy.
	*/
	public void reached(Object compatabilitySpace, Object group, int limit,
		Enumeration lockList, int lockCount)
		throws StandardException;

}
