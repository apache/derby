/*

   Derby - Class org.apache.derby.iapi.services.locks.Limit

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
