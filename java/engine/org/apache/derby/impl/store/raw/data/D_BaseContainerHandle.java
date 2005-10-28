/*

   Derby - Class org.apache.derby.impl.store.raw.data.D_BaseContainerHandle

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
import org.apache.derby.iapi.services.monitor.ModuleControl;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.raw.RowLock;
import org.apache.derby.iapi.store.raw.ContainerKey;

import java.util.Properties;

/**

The D_BaseContainerHandle class provides diagnostic information about the
BaseContainerHandle class.  Currently this info is a single string of the form
    TABLE(conglomerate_id, container_id)
**/

public class D_BaseContainerHandle extends DiagnosticableGeneric
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
        BaseContainerHandle ch  = (BaseContainerHandle) diag_object;
        String              str = null;

        /*
        String str = 
            "BaseContainerHandle:(" + 
            DiagnosticUtil.toDiagString(ch.identity) + 
            ")";
        */

        long container_id = ch.identity.getContainerId();

        long conglom_id   = 
            D_DiagnosticUtil.diag_containerid_to_conglomid(
                    ch.xact.getDataFactory(),
                    container_id);

        if (conglom_id !=  Long.MIN_VALUE)
        {
            str = "TABLE(" + conglom_id + "," + container_id + ")";
        }
        else
        {
            str = "TABLE(Booting..., " + container_id + ")";
        }

        // RESOLVE (mikem) - during boot we can't ask acces to give us the
        // containerid info, since access hasn't booted yet.  For now just
        // assume that is why we got a bad containerid number and don't print
        // the containerid so that we can diff the output.
        /*
        else
        {
            str = "TABLE(?, " +  container_id + ")";

            Thread.dumpStack();
        }
        */

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
        BaseContainerHandle ch          = (BaseContainerHandle) diag_object;
        ContainerKey        key         = ch.getId();

        prop.put(RowLock.DIAG_CONTAINERID, Long.toString(key.getContainerId()));

        prop.put(RowLock.DIAG_SEGMENTID, Long.toString(key.getSegmentId()));

        // The following 2 don't make sense for container locks, just set
        // them to 0 to make it easier for now to tree container locks and
        // row locks similarly.  
        prop.put(RowLock.DIAG_PAGENUM, Integer.toString(0));
        prop.put(RowLock.DIAG_RECID,   Integer.toString(0));

        return;
    }
}
