/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access.conglomerate
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access.conglomerate;

import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;

/**

  A ScanControllerRowSource is both a RowSource and a ScanController.  This
  interface is internal to Access for use in the case of RowSource which are
  implemented on top of a ScanController.

**/
public interface ScanControllerRowSource 
    extends ScanController, RowLocationRetRowSource
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
}
