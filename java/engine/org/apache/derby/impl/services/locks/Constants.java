/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.locks
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.locks;

/**
*/

public class Constants {

	/**
		Trace flag to display lock requests, grants and unlocks.
	*/
	public static final String LOCK_TRACE = "LockTrace";

	/**
		Trace flag to display stack trace of lock calls.
	*/
	public static final String LOCK_STACK_TRACE = "LockStackTrace";

	/**
		Trace flag to add thread information to trace info of LockTrace, 
        requires that LockTrace be set to true.
	*/
	public static final String LOCK_TRACE_ADD_THREAD_INFO = "LockTraceAddThreadInfo";


	static final byte WAITING_LOCK_IN_WAIT = 0;
	static final byte WAITING_LOCK_GRANT = 1;
	static final byte WAITING_LOCK_DEADLOCK = 2;
}
