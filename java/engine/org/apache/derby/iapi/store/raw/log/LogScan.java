/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw.log
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw.log;

/**
	LogScan provides methods to read a log record from the log.
*/

public interface LogScan {
	/**
		The how and what a logScan returns is left to the specific
		implementation. 
		This interface is here so that a LogFactory can return it.
	 */
}
