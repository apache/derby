/*

   Derby - Class org.apache.derby.iapi.store.raw.D_ContainerKey

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;

import java.util.Properties;

/**

The D_BaseContainerHandle class provides diagnostic information about the
BaseContainerHandle class.  Currently this info is a single string of the form
    TABLE(conglomerate_id, container_id)
**/

public class D_ContainerKey extends DiagnosticableGeneric
{

    /**
     * Return string identifying the underlying container.
     * <p>
     *
	 * @return A string of the form TABLE(conglomerate_id, container_id).
	 **/
    public String diag()
    {
      return(diag_object.toString());
    }

    /**
     * Return a set of properties describing the the key used to lock container.
     * <p>
     * Used by debugging code to print the lock table on demand.
     *
     **/
    public void diag_detail(Properties prop)
    {
        ContainerKey        key         = (ContainerKey) diag_object;

        prop.put(RowLock.DIAG_CONTAINERID, Long.toString(key.getContainerId()));

        prop.put(RowLock.DIAG_SEGMENTID, Long.toString(key.getSegmentId()));

        // The following 2 don't make sense for container locks, just set
        // them to 0 to make it easier for now to tree container locks and
        // row locks similarly.  
        prop.put(RowLock.DIAG_PAGENUM, Integer.toString(0));
        prop.put(RowLock.DIAG_RECID,   Integer.toString(0));
    }
}
