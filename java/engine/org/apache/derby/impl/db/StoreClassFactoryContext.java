/*

   Derby - Class org.apache.derby.impl.db.StoreClassFactoryContext

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

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

