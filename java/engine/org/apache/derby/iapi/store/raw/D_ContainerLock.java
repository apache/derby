/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
