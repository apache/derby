/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.daemon
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
		Create a new DaemonService with the default daemon timer delay.

		@exception StandardException Standard cloudscape error policy
	 */
	public DaemonService createNewDaemon(String name) throws StandardException;
}
