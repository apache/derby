/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.raw
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.error.StandardException;

public interface Corruptable {

	/**
		Mark the module as corrupt.
		It is safe to call this multiple times.

		@return Must always return its parameter.
	*/
	public StandardException markCorrupt(StandardException originalError);
}
