/*

   Derby - Class org.apache.derby.iapi.types.CloneableObject

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

package org.apache.derby.iapi.types;

/**
 * This is a simple interface that is used by the
 * sorter for cloning input rows.  It defines
 * a method that can be used to clone a column.
 */
public interface CloneableObject
{
	/**
	 * Get a shallow copy of the object and return
	 * it.  This is used by the sorter to clone
	 * columns.  It should be cloning the column
	 * holder but not its value.  The only difference
	 * between this method and getClone is this one does
	 * not objectify a stream.
	 *
	 * @return new cloned column as an object
	 */
	public Object cloneObject();
}
