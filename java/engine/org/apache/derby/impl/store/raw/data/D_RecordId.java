/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
