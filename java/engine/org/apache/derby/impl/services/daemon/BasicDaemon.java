/*

   Derby - Class org.apache.derby.impl.services.daemon.BasicDaemon

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
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.daemon.DaemonService;
import org.apache.derby.iapi.services.daemon.Serviceable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.monitor.ModuleFactory;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import java.util.Vector;
import java.util.List;

/**
	A BasicDaemon is a background worker thread which does asynchronous I/O and
	general clean up.  It should not be used as a general worker thread for
	parallel execution. 

	One cannot count on the order of request or count on when the daemon will
	wake up, even with serviceNow requests.  Request are not persistent and not
	recoverable, they are all lost when the system crashes or is shutdown.
	System shutdown, even orderly ones, do not wait for daemons to finish its
	work or empty its queue.  Furthermore, any Serviceable subscriptions,
	including onDemandOnly, must tolerate spurrious services.  The BasicDaemon
	will setup a context manager with no context on it.  The Serviceable
	object's performWork must provide useful context on the context manager to
	do its work.  The BasicDaemon will wrap performWork call with try / catch
	block and will use the ContextManager's error handling to clean up any
	error.  The BasicDaemon will guarentee serviceNow request will not be lost
	as long as the jbms does not crash - however, if N serviceNow requests are
	made by the same client, it may only be serviced once, not N times.

	Many Serviceable object will subscribe to the same BasicDaemon.  Their
	performWork method should be well behaved - in other words, it should not
	take too long or hog too many resources or deadlock with anyone else.  And
	it cannot (should not) error out.

	The BasicDaemon implementation manages the DaemonService's data structure,
	handles subscriptions and enqueues requests, and determine the service
	schedule for its Serviceable objects.  The BasicDaemon keeps an array
	(Vector) of Serviceable subscriptions it also keeps 2 queues for clients
	that uses it for one time service - the 1st queue is for a serviceNow
	enqueue request, the 2nd queue is for non serviceNow enqueue request.

	This BasicDaemon services its clients in the following order:
	1. any subscribed client that have made a serviceNow request that has not
				been fulfilled 
	2. serviceable clients on the 1st queue
	3. all subscribed clients that are not onDemandOnly
	4. serviceable clients 2nd queue

*/
public class BasicDaemon implements DaemonService, Runnable
{
	private int numClients;		// number of clients that needs services

	private static final int OPTIMAL_QUEUE_SIZE = 100;

	private final Vector subscription;

	// the context this daemon should run with
	protected final ContextService contextService;
	protected final ContextManager contextMgr;

	/**
		Queues for the work to be done.
		These are synchronized by this object.
	*/
	private final List highPQ;		// high priority queue
	private final List normPQ;		// normal priority queue

	/**
		which subscribed clients to service next?
		only accessed by daemon thread
	*/
	private int nextService;

	/*
	** State for the sleep/wakeup routines.
	*/

	private boolean awakened;			// a wake up call has been issued 
								// MT - synchronized on this

	/**
		true if I'm waiting, if this is false then I am running and a notify is not required.
	*/
	private boolean waiting;

	private boolean inPause;			// if true, don't do anything
	private boolean running;			// I am running now
	private boolean stopRequested;		// thread is requested to die
	private boolean stopped;			// we have stopped

	private long lastServiceTime; // when did I last wake up on a timer
	private int earlyWakeupCount;		// if I am waken up a couple of times, check
								// that lastServiceTime to make sure work
								// scheduled on a timer gets done once in a
								// while

	/**
		make a BasicDaemon

		@param priority the priority of the daemon thread
		@param delay the number of milliseconds between servcies to its clients
	*/
	public BasicDaemon(ContextService contextService)
	{
		this.contextService = contextService;
		this.contextMgr = contextService.newContextManager();

		subscription = new Vector(1, 1);
		highPQ = new java.util.LinkedList();
		normPQ = new java.util.LinkedList();
		
		lastServiceTime = System.currentTimeMillis();
	}

	public int subscribe(Serviceable newClient, boolean onDemandOnly)
	{
		int clientNumber;

		ServiceRecord clientRecord;

		synchronized(this)
		{
			clientNumber = numClients++;

			clientRecord = new ServiceRecord(newClient, onDemandOnly, true);
			subscription.insertElementAt(clientRecord, clientNumber);
		}


		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								"subscribed client # " + clientNumber + " : " +
								clientRecord);
		}

		return clientNumber;
	}

	public void unsubscribe(int clientNumber)
	{
		if (clientNumber < 0 || clientNumber > subscription.size())
			return;

		// client number is never reused.  Just null out the vector entry.
		subscription.setElementAt(null, clientNumber);
	}

	public void serviceNow(int clientNumber)
	{
		if (clientNumber < 0 || clientNumber > subscription.size())
			return;

		ServiceRecord clientRecord = (ServiceRecord)subscription.elementAt(clientNumber);
		if (clientRecord == null)
			return;

		clientRecord.called();
		wakeUp();
	}

	public boolean enqueue(Serviceable newClient, boolean serviceNow)
	{
		ServiceRecord clientRecord = new ServiceRecord(newClient, false, false);

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace,
									"enqueing work, urgent = " + serviceNow + ":" + newClient );
		}


		List queue = serviceNow ? highPQ : normPQ;

		int highPQsize;
		synchronized (this) {
			queue.add(clientRecord);
			highPQsize = highPQ.size();

			if (SanityManager.DEBUG) {

				if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

					if (highPQsize > (OPTIMAL_QUEUE_SIZE * 2))
						System.out.println("memoryLeakTrace:BasicDaemon " + highPQsize);
				}
			}
		}

		if (serviceNow && !awakened)
			wakeUp();

		if (serviceNow) {
			return highPQsize > OPTIMAL_QUEUE_SIZE;
		}
		return false;
	}

	/**
		Get rid of all queued up Serviceable tasks.
	 */
	public synchronized void clear()
	{
		normPQ.clear();
		highPQ.clear();
	}

	/*
	 * class specific methods
	 */

	protected ServiceRecord nextAssignment(boolean urgent)
	{
		// first goes thru the subscription list, then goes thru highPQ;
		ServiceRecord clientRecord;

		while (nextService < subscription.size())
		{
			clientRecord = (ServiceRecord)subscription.elementAt(nextService++);
			if (clientRecord != null && (clientRecord.needImmediateService() || (!urgent && clientRecord.needService())))
				return clientRecord;
		}

		clientRecord = null;

		synchronized(this)
		{
			if (!highPQ.isEmpty())
				clientRecord = (ServiceRecord) highPQ.remove(0);
		}

		if (urgent || clientRecord != null)
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
					SanityManager.DEBUG(DaemonService.DaemonTrace, 
									clientRecord == null ? 
									"No more urgent assignment " : 
									"Next urgent assignment : " + clientRecord);
			}
			
			return clientRecord;
		}

		clientRecord = null;
		synchronized(this)
		{
			if (!normPQ.isEmpty())
			{
				clientRecord = (ServiceRecord)normPQ.remove(0);

				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
						SanityManager.DEBUG(DaemonService.DaemonTrace, 
										"Next normal enqueued : " + clientRecord);
				}
			}

			// else no more work 
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
			{
				if (clientRecord == null)
					SanityManager.DEBUG(DaemonService.DaemonTrace, "No more assignment");
			}
		}

		return clientRecord;
	}

	protected void serviceClient(ServiceRecord clientRecord) 
	{
		clientRecord.serviced();

		Serviceable client = clientRecord.client;

		// client may have unsubscribed while it had items queued
		if (client == null)
			return;

		ContextManager cm = contextMgr;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(cm != null, "Context manager is null");
			SanityManager.ASSERT(client != null, "client is null");
		}

		try
		{
			int status = client.performWork(cm);

			if (clientRecord.subscriber)
				return;

			if (status == Serviceable.REQUEUE)
			{
				List queue = client.serviceASAP() ? highPQ : normPQ;
				synchronized (this) {
					queue.add(clientRecord);

					if (SanityManager.DEBUG) {

						if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

							if (queue.size() > (OPTIMAL_QUEUE_SIZE * 2))
								System.out.println("memoryLeakTrace:BasicDaemon " + queue.size());
						}
					}
				}
			}

			return;
		}
		catch (Throwable e)
		{
			if (SanityManager.DEBUG)
				SanityManager.showTrace(e);
			cm.cleanupOnError(e);
		}
	}

	/*
	 * Runnable methods
	 */
	public void run()
	{
		contextService.setCurrentContextManager(contextMgr);

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonOff))
			{
				SanityManager.DEBUG(DaemonService.DaemonTrace, "DaemonOff is set in properties, background Daemon not run");
				return;
			}
			SanityManager.DEBUG(DaemonService.DaemonTrace, "running");
		}

		// infinite loop of rest and work
		while(true)
		{
			if (stopRequested())
				break;

			// if someone wake me up, only service the urgent requests.
			// if I wake up by my regular schedule, service all clients
			boolean urgentOnly = rest();

			if (stopRequested())
				break;

			if (!inPause())
				work(urgentOnly);
		}

		synchronized(this)
		{
			running = false;
			stopped = true;
		}
		contextMgr.cleanupOnError(StandardException.normalClose());
		contextService.resetCurrentContextManager(contextMgr);
	}

	/*
	 * Daemon Service method
	 */

	/*
	 * pause the daemon.  Wait till it is no running before it returns
	 */
	public void pause()
	{
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, "pausing daemon");
		}

		synchronized(this)
		{
			inPause = true;
			while(running)
			{
				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
						SanityManager.DEBUG(DaemonService.DaemonTrace, 
										"waiting for daemon run to finish");
				}

				try
				{
					wait();
				}
				catch (InterruptedException ie)				
				{
					// someone interrrupt us, done running
				}
			}
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								"daemon paused");
		}
	}

	public void resume()
	{
		synchronized(this)
		{
			inPause = false;
		}

		if (SanityManager.DEBUG) 
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								"daemon resumed");
		}
	}

	/**
		Finish what we are doing and at the next convenient moment, get rid of
		the thread and make the daemon object goes away if possible.

		remember we are calling from another thread
	 */
	public void stop()
	{
		if (stopped)			// already stopped
			return;

		synchronized(this)
		{
			stopRequested = true;
			notifyAll(); // get sleeper to wake up and stop ASAP
		}

		pause(); // finish doing what we are doing first

	}

	/*
	**Wait until the work in the high priority queue is done.
	**Note: Used by tests only to make sure all the work 
	**assigned to the daemon is completed.
	**/
	public void waitUntilQueueIsEmpty()
	{
		while(true){
			synchronized(this)
			{
				boolean noSubscriptionRequests = true; 
				for (int urgentServiced = 0; urgentServiced < subscription.size(); urgentServiced++)
				{
					ServiceRecord clientRecord = (ServiceRecord)subscription.elementAt(urgentServiced);
					if (clientRecord != null &&	clientRecord.needService())
					{
						noSubscriptionRequests = false;
						break;
					}
				}

				if (highPQ.isEmpty() && noSubscriptionRequests &&!running){
					return;
				}else{

					notifyAll(); //wake up the the daemon thread
					//wait for the raw store daemon to wakeus up   
					//when it finihes work.
					try{
						wait();
					}catch (InterruptedException ie)
					{
						// someone interrupt us, see what's going on
					}
				}
			}
		}
	}

	private synchronized boolean stopRequested()
	{
		return stopRequested;
	}

	private synchronized boolean inPause()
	{
		return inPause;
	}

	/*
	 * BasicDaemon method
	 */
	protected synchronized void wakeUp()
	{
		if (!awakened) {
			awakened = true;	// I am being awakened for urgent work.

			if (waiting) {
				notifyAll();
			}
		}
	}

	/**
		Returns true if awakened by some notification, false if wake up by timer
	*/
	private boolean rest()
	{
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								"going back to rest");
		}

		boolean urgentOnly;
		boolean checkWallClock = false;
		synchronized(this)
		{
			try
			{	
				if (!awakened) {
					waiting = true;
					wait(DaemonService.TIMER_DELAY);
					waiting = false;
				}
			}
			catch (InterruptedException ie)
			{
				// someone interrupt us, see what's going on
			}

			nextService = 0;

			urgentOnly = awakened;
			if (urgentOnly)	// check wall clock
			{
				// take a guess that each early request is services every 500ms.
				if (earlyWakeupCount++ > (DaemonService.TIMER_DELAY / 500)) {
					earlyWakeupCount = 0;
					checkWallClock = true;
				}
			}
			awakened = false;			// reset this for next time
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace)) 
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								urgentOnly ?
								"someone wakes me up" : 
								"wakes up by myself");
		}	

		if (checkWallClock)
		{
			long currenttime = System.currentTimeMillis();
			if ((currenttime - lastServiceTime) > DaemonService.TIMER_DELAY)
			{
				lastServiceTime = currenttime;
				urgentOnly = false;

				if (SanityManager.DEBUG)
				{
					if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
						SanityManager.DEBUG(DaemonService.DaemonTrace, 
										"wall clock check says service all");
				}
			}
		}

		return urgentOnly;
	}

	private void work(boolean urgentOnly)
	{
		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
				SanityManager.DEBUG(DaemonService.DaemonTrace, 
								"going back to work");
		}

		ServiceRecord work;


		// while I am working, all serviceNow requests that comes in now will
		// be taken care of when we get the next Assignment.
		int serviceCount = 0;

		int yieldFactor = 10;
		if (urgentOnly && (highPQ.size() > OPTIMAL_QUEUE_SIZE))
			yieldFactor = 2;

		int yieldCount = OPTIMAL_QUEUE_SIZE / yieldFactor;


		for (work = nextAssignment(urgentOnly);
			 work != null;
			 work = nextAssignment(urgentOnly))
		{
			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
					SanityManager.DEBUG(DaemonService.DaemonTrace, 
										"servicing " + work);
			}


			synchronized(this)
			{
				if (inPause || stopRequested)
					break;			// don't do anything more
				running = true;
			}

			// do work
			try
			{
				serviceClient(work);
				serviceCount++;
			}
			finally	
			{
				// catch run time exceptions
				synchronized(this)
				{
					running = false;
					notifyAll();
					if (inPause || stopRequested)
						break;	// don't do anything more
				}
			}

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
					SanityManager.DEBUG(DaemonService.DaemonTrace, 
										"done " + work);
			}

			// ensure the subscribed clients get a look in once in a while
			// when the queues are large.
			if ((serviceCount % (OPTIMAL_QUEUE_SIZE / 2)) == 0) {
				nextService = 0;
			}

			if ((serviceCount % yieldCount) == 0) {

				yield();
			}

			if (SanityManager.DEBUG)
			{
				if (SanityManager.DEBUG_ON(DaemonService.DaemonTrace))
					SanityManager.DEBUG(DaemonService.DaemonTrace,
										"come back from yield");
			}
		}
	}


	/* let everybody else run first */
	private void yield()
	{
		Thread currentThread = Thread.currentThread();
		int oldPriority = currentThread.getPriority();

		if (oldPriority <= Thread.MIN_PRIORITY)
		{
			currentThread.yield();
		}
		else
		{
			ModuleFactory mf = Monitor.getMonitor();
			if (mf != null)
				mf.setThreadPriority(Thread.MIN_PRIORITY);
			currentThread.yield();
			if (mf != null)
				mf.setThreadPriority(oldPriority);
		}
	}
}









