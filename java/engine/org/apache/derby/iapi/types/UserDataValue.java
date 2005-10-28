/*

   Derby - Class org.apache.derby.iapi.types.UserDataValue

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

import org.apache.derby.iapi.error.StandardException;

public interface UserDataValue extends DataValueDescriptor
{
	/**
	 * Set the value of this UserDataValue to the given Object
	 *
	 * @param theValue	The value to set this UserDataValue to
	 *
	 * @return	This UserDataValue
	 *
	 */
	public void setValue(Object theValue) throws StandardException;
}
