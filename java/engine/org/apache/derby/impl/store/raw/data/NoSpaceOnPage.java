/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.error.StandardException; 


/**
	An exception used to pass a specfic "error code" through
	various layers of software.
*/
class NoSpaceOnPage extends StandardException {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	private final boolean onOverflowPage;

	/*
	** Constructors
	*/

	protected NoSpaceOnPage(boolean onOverflowPage) {
		super("nospc.U");
		this.onOverflowPage = onOverflowPage;
	}

	protected boolean onOverflowPage() {
		return onOverflowPage;
	}
}
