/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import java.io.InputStream;

public interface ScannedTransactionHandle
{
	Loggable getNextRecord()
		 throws StandardException;

    InputStream getOptionalData()
		 throws StandardException;

    LogInstant getThisInstant()
		 throws StandardException;

    LogInstant getLastInstant()
		 throws StandardException;

    LogInstant getFirstInstant()
		 throws StandardException;

    void close();
}
