/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.btree
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.btree;

/**

  This error is never seen outside of the btree implementation.  It is raised
  when an operation could not procede because it would have had to wait.

**/

public class WaitError extends Throwable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
}
