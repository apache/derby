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

public class D_FlushedScan
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
		FlushedScan fs = (FlushedScan)diag_object;
		StringBuffer r = new StringBuffer();
		r.append("FlushedScan: \n");
		r.append("    Open: "+fs.open+"\n");
		r.append("    currentLogFileNumber: "+fs.currentLogFileNumber+"\n");
		r.append("    currentLogFirstUnflushedPosition: "+
				 fs.currentLogFileFirstUnflushedPosition+"\n");
		r.append("    currentInstant: "+fs.currentInstant+"\n");
		r.append("    firstUnflushed: "+fs.firstUnflushed+"\n");
		r.append("    firstUnflushedFileNumber: "+fs.firstUnflushedFileNumber+"\n");
		r.append("    firstUnflushedFilePosition: "+fs.firstUnflushedFilePosition+"\n");
		r.append("    logFactory: \n"+
				 DiagnosticUtil.toDiagString(fs.logFactory));
		r.append("flushedScanEnd\n");
		return r.toString();
	}
}
