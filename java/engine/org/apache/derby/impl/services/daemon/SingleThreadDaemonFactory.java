/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.daemon
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.daemon;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.daemon.DaemonFactory;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.impl.services.daemon.BasicDaemon;
import org.apache.derby.iapi.services.monitor.Monitor;

public class SingleThreadDaemonFactory implements DaemonFactory
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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

