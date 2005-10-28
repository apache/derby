/*

   Derby - Class org.apache.derby.iapi.services.daemon.DaemonFactory

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

package org.apache.derby.iapi.services.daemon;

import org.apache.derby.iapi.error.StandardException;

/**
	Daemon Factory can create new DaemonService, which runs on seperate
	background threads.  One can use these DaemonService to handle background
	clean up task by implementing Serviceable and subscribing to a DaemonService.

	A DaemonService is a background worker thread which does asynchronous I/O and
	general clean up.  It should not be used as a general worker thread for
	parallel execution.  A DaemonService can be subscribe to by many Serviceable
	objects and a daemon will call that object's performWork from time to
	time.  These performWork method should be well behaved - in other words,
	it should not take too long or hog too many resources or deadlock with 
	anyone else.  And it cannot (should not) error out.

	The best way to use a daemon is to have an existing DaemonService and subscribe to it.
	If you can't find an existing one, then make one thusly:

	DaemonService daemon = DaemonFactory.createNewDaemon();

	After you have a daemon, you can subscribe to it by
	int myClientNumber = daemon.subscribe(serviceableObject);

	and ask it to run performWork for you ASAP by
	daemon.serviceNow(myClientNumber);

	Or, for one time service, you can enqueue a Serviceable Object by
	daemon.enqueue(serviceableObject, true);  - urgent service
	daemon.enqueue(serviceableObject, false); - non-urgent service

	@see DaemonService
	@see Serviceable
*/
public interface DaemonFactory 
{
	/**
		Create a new DaemonService with the default daemon timer delay.

		@exception StandardException Standard cloudscape error policy
	 */
	public DaemonService createNewDaemon(String name) throws StandardException;
}
