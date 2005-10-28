/*

   Derby - Class org.apache.derby.impl.services.daemon.SingleThreadDaemonFactory

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.services.daemon;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.DaemonFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.impl.services.daemon.BasicDaemon;
import org.apache.derby.iapi.services.monitor.Monitor;

public class SingleThreadDaemonFactory implements DaemonFactory
{
	private final ContextService contextService;
	
	public SingleThreadDaemonFactory()
	{
		contextService = ContextService.getFactory();
	}

	/*
	 * Daemon factory method
	 */

	/* make a daemon service with the default timer */
	public DaemonService createNewDaemon(String name)
	{
		BasicDaemon daemon = new BasicDaemon(contextService);

		Thread daemonThread = Monitor.getMonitor().getDaemonThread(daemon, name, false);
		daemonThread.start();
		return daemon;
	}
}

