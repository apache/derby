/*

   Derby - Class org.apache.derby.iapi.services.locks.C_LockFactory

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

package org.apache.derby.iapi.services.locks;

/**
	Constants for the LockFactory
*/
public interface C_LockFactory {

	/**
		Timeout value that indicates wait for the lock or latch forever.
	*/
	public static final int WAIT_FOREVER = -1;

	/**
		Timeout value that indicates wait for the lock according to
		derby.locks.waitTimeout.
	*/
	public static final int TIMED_WAIT = -2;

	/**
		Timeout value that indicates do not wait for the lock or latch at all
		*/
	public static final int NO_WAIT = 0;
}


