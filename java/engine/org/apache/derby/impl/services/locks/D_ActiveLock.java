/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.locks
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.locks;

import org.apache.derby.iapi.error.StandardException;

/**
*/

public class D_ActiveLock extends D_Lock  {

	/**
		@exception StandardException Standard cloudscape policy
	*/
    public String diag()
        throws StandardException
    {
		String s = super.diag();

		StringBuffer sb = new StringBuffer(s);

		sb.append(" potentiallyGranted=" + ((ActiveLock) lock).potentiallyGranted + " ");
		sb.append(" wakeUpNow=" + ((ActiveLock) lock).wakeUpNow);

		return sb.toString();
	}
}

