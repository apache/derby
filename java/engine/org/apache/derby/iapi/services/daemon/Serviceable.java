/*

   Derby - Class org.apache.derby.iapi.services.daemon.Serviceable

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

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.error.StandardException;

/**
  To use a DaemonService, one implements the Serviceable interface.  Only one
  DaemonService will call this at any given time.  However, if this Serviceable
  object subscribes to or enqueues to the DeamonService multiple times, then
  multiple DaemonService threads may call this Serviceable object at the same
  time.  The Serviceable object must decide what synchronization it needs to
  provide depending on what work it needs to do.

  The Serviceable interface does not provide a way to pass a work object to
  identify what work needs to be done, it is assumed that the Serviceable
  object knows that.  If a Serviceable object has different work for the
  DaemonService to do, it must somehow encapsulate or identify the different
  work by an intermediary object which implements the Serviceable interface and
  which can an identify the different work and pass it along to the object that
  can deal with them.
*/

public interface Serviceable {

	/**
		Do whatever it is that you want the daemon to do for you. There may be
		multiple daemon objects on different thread calling performWork at the
		same time. 

		The DaemonService will always call performWork with a context manager
		set up.  the DaemonService will clean up the context if an exception is
		thrown.  However, it is up to performWork to manage its own
		transaction.  If you start a transaction in performWork, you
		<B>must</B> commit or abort it at the end.  You may leave the
		transaction open so that other serviceable may use the transaction and
		context without starting a new one.  On the same token, there may
		already be an opened transaction on the context.  Serviceable
		performWork should always check the state of the context before use.

		A Serviceable object should be well behaved while it is performing the
		daemon work, i.e., it should not take too many resources or hog the CPU
		for too long or deadlock with anyone else.

		@param context the contextManager set up by the DaemonService.  There
		may or may not be the necessary context on it, depending on which other
		Serviceable object it has done work for.
		@return the return status is only significant if the Serviceable client
		was enqueued instead of subscribed.  For subscribed client, the return
		status is ignored.  For enqueue client, it returns DONE or REQUEUE.  If
		a REQUEUEd is returned, it would be desirable if this should not be
		serviceASAP, although no harm is done if this still maintains that this
		should be serviced ASAP ...

	    @exception StandardException  Standard cloudscape exception policy

		<P>MT - depends on the work.  Be wary of multiple DaemonService thread
		calling at the same time if you subscribe or enqueue multiple times.
	*/
	public int performWork(ContextManager context) throws StandardException;


	/** return status for performWork - only meaningful for enqueued client */
	public static int DONE = 1;	// the daemon work is finished, the
								// DaemonService can get rid of this client
	public static int REQUEUE = 2;// the daemon work is not finished, requeue
								  // the request to be serviced again later.



	/**
		If this work should be done as soon as possible, then return true.  
		If it doesn't make any difference if it is done sooner rather than
		later, then return false.  

		The difference is whether or not the daemon service will be notified to
		work on this when this work is enqueued or subscribed, in case the
		serviceable work is put together but not sent to the daemon service
		directly, like in post commit processing

		<P>MT - MT safe
	*/
	public boolean serviceASAP();


	/**
		If this work should be done immediately on the user thread then return true.  
		If it doesn't make any difference if this work is done on a the user thread
		immediately or if it is performed by another thread asynchronously
		later, then return false.  
	*/
	public boolean serviceImmediately();

}

