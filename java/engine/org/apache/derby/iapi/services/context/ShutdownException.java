/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.context
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.context;

/**
	A ShutdownException is a runtime exception that is used
	to notify code that the system has/is being shut down.
*/

public final class ShutdownException extends RuntimeException {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	public ShutdownException() {
		super("");
	}
}
