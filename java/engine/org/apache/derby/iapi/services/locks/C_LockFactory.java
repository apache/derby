/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.locks
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.locks;

/**
	Constants for the LockFactory
*/
public interface C_LockFactory {

	/**
		Timeout value that indicates wait for the lock or latch forever.
	*/
	public static final int WAIT_FOREVER = -1;

	/**
		Timeout value that indicates wait for the lock according to
		derby.locks.waitTimeout.
	*/
	public static final int TIMED_WAIT = -2;

	/**
		Timeout value that indicates do not wait for the lock or latch at all
		*/
	public static final int NO_WAIT = 0;
}


