/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.btree.index
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.btree.index;

import java.io.IOException;
import java.util.Properties;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;

import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.impl.store.access.btree.BTreeCostController;


/**

  A B2I controller object is the concrete class which corresponds to an open
  b-tree secondary index.

**/

public class B2ICostController extends BTreeCostController
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/*
	** Fields of B2IController.
	*/

	/*
	** Methods of B2IController.
	*/

	B2ICostController()
	{
		// Perform the generic b-tree construction.
		super();
	}

	void init(
    TransactionManager  xact_manager,
    B2I                 conglomerate,
    Transaction         rawtran) 
		throws StandardException
	{
		// Do generic b-tree initialization.
		super.init(xact_manager, conglomerate, rawtran);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(conglomerate != null);
	}
}
