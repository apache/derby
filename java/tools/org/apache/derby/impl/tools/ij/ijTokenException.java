/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.tools.ij;

public class ijTokenException extends RuntimeException {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	public ijTokenException(String message) {
		super(message);
	}
}
