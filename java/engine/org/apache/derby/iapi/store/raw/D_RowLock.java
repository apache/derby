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

The D_RowLock class provides diagnostic information about the 
RowLock qualifer, and is used for output in lock debugging.

**/

public class D_RowLock extends DiagnosticableGeneric
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
    // Names of locks for lock trace print out.
	private static String[] names = { "RS2", "RS3", "RU2", "RU3", "RIP", "RI", "RX2", "RX3" };

    /**
		Return the string for the qualifier.
     *
     * @exception StandardException	Standard Cloudscape error policy
     **/
    public String diag()
        throws StandardException
    {
        RowLock mode = (RowLock) diag_object;

        return(names[mode.getType()]);
    }
}
