/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.db
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.db;

import org.apache.derby.iapi.services.loader.ClassFactoryContext;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.JarReader;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.services.context.ContextManager;

/**
*/
final class StoreClassFactoryContext extends ClassFactoryContext { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;

	private final AccessFactory store;
	private final JarReader	jarReader;

	StoreClassFactoryContext(ContextManager cm, ClassFactory cf, AccessFactory store, JarReader jarReader) {
		super(cm, cf);
		this.store = store;
		this.jarReader = jarReader;
	}

	public Object getLockSpace() throws StandardException {
		if (store == null)
			return null;
		return store.getTransaction(getContextManager()).getLockObject();	
	}
	public PersistentSet getPersistentSet() throws StandardException {
		if (store == null)
			return null;
		return store.getTransaction(getContextManager());
	}
	public JarReader getJarReader() {

		return jarReader;
	}
}

