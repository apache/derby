/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw.log
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw.log;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.store.access.DatabaseInstant;

/**
	Describes a position in the log.
*/

public interface LogInstant
extends Formatable, DatabaseInstant { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	public static final long INVALID_LOG_INSTANT = 0;
}
