/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.heap
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.services.io.FormatableInstanceGetter;

public class HeapClassInfo extends FormatableInstanceGetter {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	// only supports a single class at the moment
	public Object getNewInstance() {

		return new HeapRowLocation();
	}
}
