/*

   Derby - Class org.apache.derby.impl.store.raw.xact.EscalateContainerKey

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.raw.xact;

import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.RecordHandle;

import org.apache.derby.iapi.util.Matchable;

public final class EscalateContainerKey implements Matchable
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private ContainerKey container_key;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public EscalateContainerKey(ContainerKey key)
    {
        container_key = key;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
	public boolean match(Object key) 
    {
		if (key instanceof RecordHandle) 
        {
			return(container_key.equals(((RecordHandle) key).getContainerId()));
		}

		return false;
	}
}
