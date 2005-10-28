/*

   Derby - Class org.apache.derby.impl.store.raw.log.D_FlushedScan

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
