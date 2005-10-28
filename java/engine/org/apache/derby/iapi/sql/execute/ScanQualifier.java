/*

   Derby - Class org.apache.derby.iapi.sql.execute.ScanQualifier

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.Qualifier;

/**
 * ScanQualifier provides additional methods for the Language layer on
 * top of Qualifier.
 */

public interface ScanQualifier extends Qualifier 
{

	/**
	 * Set the info in a ScanQualifier
	 */
	void setQualifier(
    int                 columnId, 
    DataValueDescriptor orderable, 
    int                 operator,
    boolean             negateCR, 
    boolean             orderedNulls, 
    boolean             unknownRV);
}
