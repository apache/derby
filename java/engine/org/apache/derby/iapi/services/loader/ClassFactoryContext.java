/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.iapi.error.StandardException;
/**
*/

public abstract class ClassFactoryContext extends ContextImpl {

	public static final String CONTEXT_ID = "ClassFactoryContext";

	private final ClassFactory cf;

	public ClassFactoryContext(ContextManager cm, ClassFactory cf) {

		super(cm, CONTEXT_ID);

		this.cf = cf;
	}

	public ClassFactory getClassFactory() {
		return cf;
	}

	public abstract Object getLockSpace() throws StandardException;

	public abstract PersistentSet getPersistentSet() throws StandardException;

	/**
		Get the mechanism to rad jar files. The ClassFactory
		may keep the JarReader reference from the first class load.
	*/
	public abstract JarReader getJarReader();

	public void cleanupOnError(Throwable error) {}
}
