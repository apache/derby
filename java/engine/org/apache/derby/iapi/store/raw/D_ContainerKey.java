/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
