/*

   Derby - Class org.apache.derby.impl.services.daemon.SingleThreadDaemonFactory

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

package org.apache.derby.impl.services.daemon;

import java.security.AccessController;
import java.security.PrivilegedAction;

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		contextService = getContextService();
	}

	/*
	 * Daemon factory method
	 */

	/* make a daemon service with the default timer */
	public DaemonService createNewDaemon(String name)
	{
		BasicDaemon daemon = new BasicDaemon(contextService);

//IC see: https://issues.apache.org/jira/browse/DERBY-6648
		final Thread daemonThread = BasicDaemon.getMonitor().getDaemonThread(daemon, name, false);
		// DERBY-3745.  setContextClassLoader for thread to null to avoid
		// leaking class loaders.
		try {
            AccessController.doPrivileged(
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
             new PrivilegedAction<Object>() {
                public Object run()  {
                    daemonThread.setContextClassLoader(null);
                    return null;
                }
            });
        } catch (SecurityException se) {
            // ignore security exception.  Earlier versions of Derby, before the 
            // DERBY-3745 fix did not require setContextClassloader permissions.
            // We may leak class loaders if we are not able to set this, but 
            // cannot just fail.
        }


		daemonThread.start();
		return daemon;
	}
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getFactory();
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<ContextService>()
                 {
                     public ContextService run()
                     {
                         return ContextService.getFactory();
                     }
                 }
                 );
        }
    }
}

