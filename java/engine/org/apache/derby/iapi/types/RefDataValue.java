/*

   Derby - Class org.apache.derby.iapi.types.RefDataValue

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

import org.apache.derby.iapi.types.RowLocation;


public interface RefDataValue extends DataValueDescriptor
{

	/**
	 * Set the value of this RefDataValue.
	 *
	 * @param theValue	Contains the boolean value to set this RefDataValue
	 *					to.  Null means set this RefDataValue to null.
	 *
	 * @return	This RefDataValue
	 *
	 */
	public void setValue(RowLocation theValue);
}
