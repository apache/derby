/*

   Derby - Class org.apache.derby.iapi.services.locks.Latch

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

/**
	A Latch represents a latch held in the lock manager.
*/
public interface Latch {

	/**	
		Get the compatability space the latch is held in.
	*/
	public Object getCompatabilitySpace();

	/**
		Gte the object the latch is held on.
	*/
	public Lockable getLockable();

	/**
		Get the qualifier used when the latch was obtained.
	*/
	public Object getQualifier();

	public int getCount();
}
