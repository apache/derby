/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.context
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.context;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.error.ExceptionSeverity;
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
			Monitor.getMonitor().shutdown();
		}

	}

}

