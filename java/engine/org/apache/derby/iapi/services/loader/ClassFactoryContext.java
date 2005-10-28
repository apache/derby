/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassFactoryContext

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
