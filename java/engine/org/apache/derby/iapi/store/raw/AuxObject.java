/*

   Derby - Class org.apache.derby.iapi.store.raw.AuxObject

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

/**

  The interface of objects which can be associated with a page while it's in cache.

  @see Page#setAuxObject

*/
public interface AuxObject
{
	/** 
		This method is called by the page manager when it's about to evict a
		page which is holding an aux object, or when a rollback occurred on the
		page.  The aux object should release its resources.  The aux object can
		assume that no one else has access to it via the raw store during this
		method call. After this method returns the raw store throws away any
		reference to this object. 
	*/
	public void auxObjectInvalidated();
}
