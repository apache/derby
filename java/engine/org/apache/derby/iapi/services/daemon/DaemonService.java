/*

   Derby - Class org.apache.derby.iapi.services.daemon.DaemonService

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

import org.apache.derby.iapi.services.sanity.SanityManager;

/** 

  A DaemonService provides a background service which is suitable for
  asynchronous I/O and general clean up.  It should not be used as a general
  worker thread for parallel execution.  A DaemonService can be subscribe to by
  many Serviceable objects and a DaemonService will call that object's
  performWork from time to time.  These performWork method is defined by the
  client object and should be well behaved - in other words, it should not take
  too long or hog too many resources or deadlock with anyone else.  And it
  cannot (should not) error out.
 
  <P>MT - all routines on the interface must be MT-safe.

  @see Serviceable
*/

public interface DaemonService 
{
	public static int TIMER_DELAY = 10000; // wake up once per TIMER_DELAY milli-second


	/**
		Trace flag that can be used by Daemons to print stuff out
	*/
	public static final String DaemonTrace = SanityManager.DEBUG ? "DaemonTrace" : null;

	/**
		Trace flag that can be used to turn off background daemons
		If DaemonOff is set, background Daemon will not attempt to do anything.
	*/
	public static final String DaemonOff = SanityManager.DEBUG ? "DaemonOff" : null;


	/**
		Add a new client that this daemon needs to service

		@param newClient a Serviceable object this daemon will service from time to time
		@param onDemandOnly only service this client when it ask for service with a serviceNow request
		@return a client number that uniquely identifies this client (this subscription) 
	*/
	public int subscribe(Serviceable newClient, boolean onDemandOnly);


	/**
		Get rid of a client from the daemon.

		@param clientNumber the number that uniquely identify the client
	*/
	public void unsubscribe(int clientNumber);


	/**
	    Service this subscription ASAP.  Does not guarantee that the daemon
		will actually do anything about it.

		@param clientNumber the number that uniquely identify the client
	 */
	public void serviceNow(int clientNumber);


	/**
		Request a one time service from the Daemon.  Unless performWork returns
		REQUEUE (see Serviceable), the daemon will service this client once
		and then it will get rid of this client.  Since no client number is
		associated with this client, it cannot request to be serviced or be
		unsubscribed. 

		The work is always added to the deamon, regardless of the
		state it returns.

		@param newClient the object that needs a one time service

		@param serviceNow if true, this client should be serviced ASAP, as if a
		serviceNow has been issued.  If false, then this client will be
		serviced with the normal scheduled.

		@return true if the daemon indicates it is being overloaded,
		false it's happy.
	*/
	public boolean enqueue(Serviceable newClient, boolean serviceNow);

	/**
		Pause.  No new service is performed until a resume is issued.
	*/
	public void pause();
	

	/**
		Resume service after a pause
	*/
	public void resume();
	

	/**
		End this daemon service
	 */
	public void stop();

	/**
		Clear all the queued up work from this daemon.  Subscriptions are not
		affected. 
	 */
	public void clear();

	/*
	 *Wait until work in the high priorty queue is done.
	 */	
	public void waitUntilQueueIsEmpty();
	
}

