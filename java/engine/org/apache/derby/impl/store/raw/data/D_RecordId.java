/*

   Derby - Class org.apache.derby.impl.store.raw.data.D_RecordId

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

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.RowLock;

import java.util.Properties;

/**

The D_RecordId class provides diagnostic information about the
BaseContainerHandle class.  Currently this info is a single string of the form
    ROW(conglomerate_id, page_number, record_id)
**/

public class D_RecordId extends DiagnosticableGeneric
{

    /**
     * Return string identifying the underlying container.
     * <p>
     *
	 * @return A string of the form TABLE(conglomerate_id, container_id).
     * @exception StandardException Standard Cloudscape Error
	 **/
    public String diag()
        throws StandardException
    {
        RecordId record_id      = (RecordId) diag_object;
        PageKey  page_key       = (PageKey)record_id.getPageId();
        long     container_id   = page_key.getContainerId().getContainerId(); 
        long     conglom_id     = Long.MIN_VALUE;
        String   str            = null;

        if (conglom_id ==  Long.MIN_VALUE)
        {
            str = "ROW(?, "                 + 
                  container_id              +   ", " + 
                  record_id.getPageNumber() +   ", " +
                  record_id.getId()         +   ")";
        }
        else
        {
            str = "ROW("                    + 
                  conglom_id                +   ", " + 
                  record_id.getPageNumber() +   ", " +
                  record_id.getId()         +   ")";
        }

        return(str);
    }


    /**
     * Return a set of properties describing the the key used to lock container.
     * <p>
     * Used by debugging code to print the lock table on demand.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void diag_detail(Properties prop)
        throws StandardException
    {
        RecordId record_id      = (RecordId) diag_object;
        PageKey  page_key       = (PageKey)record_id.getPageId();

        prop.put(RowLock.DIAG_CONTAINERID, 
            Long.toString(page_key.getContainerId().getContainerId()));

        prop.put(RowLock.DIAG_SEGMENTID, 
            Long.toString(page_key.getContainerId().getSegmentId()));

        prop.put(RowLock.DIAG_PAGENUM, 
            Long.toString(record_id.getPageNumber()));

        prop.put(RowLock.DIAG_RECID, 
            Integer.toString(record_id.getId()));

        return;
    }
}
