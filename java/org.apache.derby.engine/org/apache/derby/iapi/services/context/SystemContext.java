/*

   Derby - Class org.apache.derby.iapi.services.context.SystemContext

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

package org.apache.derby.iapi.services.context;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.shared.common.error.ShutdownException;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.shared.common.error.ExceptionSeverity;
/**
	A context that shuts the system down if it gets an StandardException
	with a severity greater than or equal to ExceptionSeverity.SYSTEM_SEVERITY
	or an exception that is not a StandardException.
*/
final class SystemContext extends ContextImpl
{
	SystemContext(ContextManager cm) {
		super(cm, "SystemContext");
	}

	public void cleanupOnError(Throwable t) {

		boolean doShutdown = false;
		if (t instanceof StandardException) {
			StandardException se = (StandardException) t;
			int severity = se.getSeverity();
			if (severity < ExceptionSeverity.SESSION_SEVERITY)
				return;
            
            popMe();
//IC see: https://issues.apache.org/jira/browse/DERBY-1095

			if (severity >= ExceptionSeverity.SYSTEM_SEVERITY)
				doShutdown = true;
		} else if (t instanceof ShutdownException) {
			// system is already shutting down ...
		} else if (t instanceof ThreadDeath) {
			// ignore this too, it means we explicitly told thread to
			// stop.  one way this can happen is after monitor
			// shutdown, so we don't need to shut down again
		}
		
		if (!doShutdown) {
			//ContextManager cm = getContextManager();
			// need to remove me from the list of all contexts.
			getContextManager().owningCsf.removeContext(getContextManager());
			return;
		}


		try {
			// try to print out that the shutdown is occurring.
			// REVISIT: does this need to be a localizable message?
			System.err.println("Shutting down due to severe error.");
			Monitor.getStream().printlnWithHeader("Shutting down due to severe error." + t.getMessage());

		} finally {
			// we need this to happen even if we fail to print out a notice
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			getMonitor().shutdown();
		}

	}

    /**
     * Privileged Monitor lookup. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ModuleFactory  getMonitor()
    {
        return AccessController.doPrivileged
            (
             new PrivilegedAction<ModuleFactory>()
             {
                 public ModuleFactory run()
                 {
                     return Monitor.getMonitor();
                 }
             }
             );
    }
}

