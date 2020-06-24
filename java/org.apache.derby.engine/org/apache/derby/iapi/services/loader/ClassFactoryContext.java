/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassFactoryContext

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

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
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.services.property.PersistentSet;
import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.error.StandardException;

/**
 * Context that provides the correct ClassFactory for the
 * current service. Allows stateless code to obtain the
 * correct class loading scheme.
*/
public abstract class ClassFactoryContext extends ContextImpl {

	public static final String CONTEXT_ID = "ClassFactoryContext";

	private final ClassFactory cf;

	protected ClassFactoryContext(ContextManager cm, ClassFactory cf) {

		super(cm, CONTEXT_ID);

		this.cf = cf;
	}

	public final ClassFactory getClassFactory() {
		return cf;
	}

    /**
     * Get the lock compatibility space to use for the
     * transactional nature of the class loading lock.
     * Used when the classpath changes or a database
     * jar file is installed, removed or replaced.
     *
     * @return the lock compatibility space
     * @throws StandardException on error
     */
    public abstract CompatibilitySpace getLockSpace() throws StandardException;

    /**
     * Get the set of properties stored with this service.
     *
     * @return the set of properties stored with this service
     * @throws StandardException on error
    */
	public abstract PersistentSet getPersistentSet() throws StandardException;

	/**
		Get the mechanism to rad jar files. The ClassFactory
		may keep the JarReader reference from the first class load.

        @return the jar file reader
	*/
	public abstract JarReader getJarReader();

    /**
     * Handle any errors. Only work here is to pop myself
     * on a session or greater severity error.
     */
	public final void cleanupOnError(Throwable error) {
        if (error instanceof StandardException) {
//IC see: https://issues.apache.org/jira/browse/DERBY-1095

            StandardException se = (StandardException) error;
            
            if (se.getSeverity() >= ExceptionSeverity.SESSION_SEVERITY)
                popMe();
        }
    }
}
