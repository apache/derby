/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;

import org.apache.derby.impl.store.raw.log.LogCounter;
import org.apache.derby.iapi.services.diag.DiagnosticUtil;
import org.apache.derby.iapi.services.diag.Diagnosticable;
import org.apache.derby.iapi.services.diag.DiagnosticableGeneric;
import org.apache.derby.iapi.error.StandardException;

public class D_TruncationPoint
extends DiagnosticableGeneric
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	/**
	  @exception StandardException Oops.
	  @see Diagnosticable#diag
	  */
    public String diag()
 		 throws StandardException
    {
		TruncationPoint tp = (TruncationPoint)diag_object;
		StringBuffer r = new StringBuffer();
		r.append("TruncationPoint: ");
		r.append("    name: "+tp.name);
		r.append("    instant: "+
				 DiagnosticUtil.toDiagString(tp.instant));
		return r.toString();
	}
}
