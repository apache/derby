/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.error
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.error;

public final class PassThroughException extends RuntimeException {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private final Exception nested;

	public PassThroughException(Exception e) {
		super(e.getMessage());
		nested = e;
	}

	public Exception getException() {
		return nested;
	}
}
