/*

   Derby - Class org.apache.derby.iapi.services.io.Storable

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

package org.apache.derby.iapi.services.io;

import java.lang.ClassNotFoundException;

import java.io.IOException; 

/**
  Formatable for holding SQL data (which may be null).
  @see Formatable
 */
public interface Storable
extends Formatable
{

	/**
	  Return whether the value is null or not.
	  
	  @return true if the value is null and false otherwise.
	**/
	public boolean isNull();

	/**
	  Restore this object to its (SQL)null value.
	**/
	public void restoreToNull();
}
