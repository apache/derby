/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.reflect
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.reflect;

import org.apache.derby.iapi.services.loader.GeneratedByteCode;
import org.apache.derby.iapi.services.context.Context;

import org.apache.derby.iapi.error.StandardException;

public abstract class GCInstanceFactory {
	/**
		IBM Copyright &copy notice.
	*/

	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;

	public abstract GeneratedByteCode getNewInstance();
}
