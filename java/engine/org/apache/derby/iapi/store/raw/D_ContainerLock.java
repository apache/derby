/*

   Derby - Class org.apache.derby.iapi.store.raw.D_ContainerLock

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;

import org.apache.derby.iapi.error.StandardException;

/**

The D_ContainerLock class provides diagnostic information about the 
ContainerLock qualifer, and is used for output in lock debugging.

**/

public class D_ContainerLock extends DiagnosticableGeneric
{
    // Names of locks for lock trace print out.
	private static String[] names = { "CIS", "CIX", "CS", "CU", "CX" };

    /**
     * Return string describing id of container.
     * <p>
     *
	 * @return A string of the form: ContainerKey(segment_id, container_id)
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
    public String diag()
        throws StandardException
    {
        ContainerLock mode = (ContainerLock) diag_object;

        return(names[mode.getType()]);
    }
}
