/*

   Derby - Class org.apache.derby.iapi.services.context.ContextService

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

package org.apache.derby.iapi.services.context;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import java.util.Hashtable;
import java.util.Enumeration;

import java.util.HashSet;
import java.util.Iterator;

/**
	A set of static methods to supply easier access to contexts.
*/
public final class ContextService //OLD extends Hashtable
{

	private static ContextService factory;
	private HeaderPrintWriter errorStream;

	/**
		Maintains a list of all the contexts that this thread has created
		and/or used. The object stored in the thread local varys according
		how this thread has been used and will be one of:

		<UL>
		<LI> null - the thread no affiliation with a context manager.

		<LI> ContextManager - the thread created and possibly was used to execute this context manager.
			This is a strong reference as it can be disassociated from the thread when the context is closed.

		<LI> WeakReference containing a ContextManager - the thread was used to execute this context manager.
			This is a weak reference to allow the context to be closed and garbage collected without having
			to track every single thread that used it or having to modify this list when resetting the current
			context manager.

		<LI> WeakHashMap (key = ContextManager, value = Integer) - the thread has created and possibly executed any number of context managers.
		</UL>

		This thread local is used to find the current context manager. Basically it provides
		fast access to a list of candidate contexts. If one of the contexts has its activeThread
		equal to the current thread then it is the current context manager.

		If the thread has pushed multiple contexts (e.g. open a new non-nested Cloudscape connection
		from a server side method) then threadContextList will contain a WeakHashMap. The value for each cm
		will be a push order, with higher numbers being more recently pushed.

		To support the case where a single context manager is pushed twice (nested connection),
		the context manager keeps track of the number of times it has been pushed (set). Note that
		our synchronization requires that a single context can only be accessed by a single thread at a time.
		In the JDBC layer this is enforced by the synchronization on the connection object.

		<P>
		There are two cases we are trying to optimise.
		<UL>
		<LI> Typical JDBC client program where there a Connection is always executed using a single thread.
			In this case this variable will contain the Connection's context manager 
		<LI> Typical application server pooled connection where a single thread may use a connection from a pool
		for the lifetime of the request. In this case this varibale will contain a  WeakReference.
		</UL>
		

		Need to support
			CM1					OPTIMIZE
			CM1,CM1				OPTIMIZE
			CM1,CM1,CM1,CM1		OPTIMIZE
			CM1,CM1,CM2
			CM1,CM2,CM1,CM2
			CM1,CM2,CM3,CM1
			etc.


			STACK for last 3
	*/

	final ThreadLocal	threadContextList = new ThreadLocal();

	private final HashSet allContexts;

	// don't want any instances
	public ContextService() {

		// find the error stream
		errorStream = Monitor.getStream();		

		ContextService.factory = this;

		allContexts = new HashSet();

	}

	/**
		So it can be given to us and taken away...
	 */
	public static void stop() {
		ContextService.factory = null;
	}

	public static ContextService getFactory() {
		ContextService csf = factory;

		if (csf == null)
			throw new ShutdownException();
		return csf;
	}
	/**
		Find the context with the given name in the context service factory
		loaded for the system.

		@return The requested context, null if it doesn't exist.
	*/
	public static Context getContext(String contextId) {

		ContextManager cm = getFactory().getCurrentContextManager();

        if( cm == null)
            return null;
        
		return cm.getContext(contextId);
	}

	/**
		Find the context with the given name in the context service factory
		loaded for the system.

		This version will not do any debug checking, but return null
		quietly if it runs into any problems.

		@return The requested context, null if it doesn't exist.
	*/
	public static Context getContextOrNull(String contextId) {
		ContextService csf = factory;

		if (csf == null)
			return null;
		
		ContextManager cm = csf.getCurrentContextManager();

		if (cm == null)
			return null;

		return cm.getContext(contextId);
	}


	/**
	 * Get current Context Manager
	 * @return ContextManager current Context Manager
	 */
	public ContextManager getCurrentContextManager() {

		Thread me = Thread.currentThread();

		Object list = threadContextList.get();

		if (list instanceof ContextManager) {
			
			ContextManager cm = (ContextManager) list;
			if (cm.activeThread == me)
				return cm;
			return null;
		}

		if (list == null)
			return null;

		java.util.Stack stack = (java.util.Stack) list;
		return (ContextManager) (stack.peek());


	//	if (list == null)
	//		return null;

		/*		Thread me = Thread.currentThread();
		
		synchronized (this) {
			for (Iterator i = allContexts.iterator(); i.hasNext(); ) {

				ContextManager cm = (ContextManager) i.next();
				if (cm.activeThread == me)
					return cm;
			}
		}
		//OLDreturn (ContextManager) get(me);
		return null;
*/	}

	public void resetCurrentContextManager(ContextManager cm) {
		if (SanityManager.DEBUG) {

			if (Thread.currentThread() != cm.activeThread) {
				SanityManager.THROWASSERT("resetCurrentContextManager - mismatch threads - current" + Thread.currentThread() + " - cm's " + cm.activeThread);
			}

			if (getCurrentContextManager() != cm) {
				SanityManager.THROWASSERT("resetCurrentContextManager - mismatch contexts - " + Thread.currentThread());
			}

			if (cm.activeCount < -1) {
				SanityManager.THROWASSERT("resetCurrentContextManager - invalid count - current" + Thread.currentThread() + " - count " + cm.activeCount);
			}

			if (cm.activeCount == 0) {
				SanityManager.THROWASSERT("resetCurrentContextManager - invalid count - current" + Thread.currentThread() + " - count " + cm.activeCount);
			}

			if (cm.activeCount > 0) {
				if (threadContextList.get() != cm)
					SanityManager.THROWASSERT("resetCurrentContextManager - invalid thread local " + Thread.currentThread() + " - object " + threadContextList.get());

			}
		}

		if (cm.activeCount != -1) {
			if (--cm.activeCount == 0)
				cm.activeThread = null;
			return;
		}

		java.util.Stack stack = (java.util.Stack) threadContextList.get();

		Object oldCM = stack.pop();

		ContextManager nextCM = (ContextManager) stack.peek();

		boolean seenMultipleCM = false;
		boolean seenCM = false;
		for (int i = 0; i < stack.size(); i++) {

			Object stackCM = stack.elementAt(i);
			if (stackCM != nextCM)
				seenMultipleCM = true;

			if (stackCM == cm)
				seenCM = true;
		}

		if (!seenCM) {
			cm.activeThread = null;
			cm.activeCount = 0;
		}

		if (!seenMultipleCM)
		{
			// all the context managers on the stack
			// are the same so reduce to a simple count.
			nextCM.activeCount = stack.size();
			threadContextList.set(nextCM);
		}
	}

	private boolean addToThreadList(Thread me, ContextManager associateCM) {

		Object list = threadContextList.get();

		if (associateCM == list)
			return true;

		if (list == null)
		{
			threadContextList.set(associateCM);
			return true;
		}

		java.util.Stack stack;
		if (list instanceof ContextManager) {
			ContextManager threadsCM = (ContextManager) list;
			if (me == null)
				me = Thread.currentThread();
			if (threadsCM.activeThread != me) {
				threadContextList.set(associateCM);
				return true;
			}
			stack = new java.util.Stack();
			threadContextList.set(stack);

			for (int i = 0; i < threadsCM.activeCount; i++)
			{
				stack.push(threadsCM);
			}
			threadsCM.activeCount = -1;
		}
		else
		{
			stack = (java.util.Stack) list;
		}

		stack.push(associateCM);
		associateCM.activeCount = -1;

		if (SanityManager.DEBUG) {

			if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

				if (stack.size() > 10)
					System.out.println("memoryLeakTrace:ContextService:threadLocal " + stack.size());
			}
		}

		return false;
	}

	/**
	 */
	public void setCurrentContextManager(ContextManager cm) {


		if (SanityManager.DEBUG) {
			Thread me = Thread.currentThread();

			if (cm.activeThread != null && me != cm.activeThread) {
				SanityManager.THROWASSERT("setCurrentContextManager - mismatch threads - current " + me + " - cm's " + cm.activeThread);
			}

/*			if ((cm.activeCount <0) || (cm.activeThread == null && cm.activeCount != 0) || (cm.activeThread != null && cm.activeCount == 0)) {
				SanityManager.THROWASSERT("resetCurrentContextManager - invalid count - owner " + cm.activeThread + " - count " + cm.activeCount);
			}
*/
		}

		Thread me = null;

		if (cm.activeThread == null) {
			cm.activeThread = (me = Thread.currentThread());
		}
		if (addToThreadList(me, cm))
			cm.activeCount++;

/*OLD
		if (cm == null)
			// don't bother storing it if it is null
			remove(me);
		else
			put(me, cm);
*/	}

	/**
	 * It's up to the caller to track this context manager and set it
	 * in the context manager list using setCurrentContextManager.
	 * We don't keep track of it due to this call being made.
	 */
	public ContextManager newContextManager()
	{
		ContextManager cm = new ContextManager(this, errorStream);

		// push a context that will shut down the system on
		// a severe error.
		new SystemContext(cm);

		synchronized (this) {
			allContexts.add(cm);

			if (SanityManager.DEBUG) {

				if (SanityManager.DEBUG_ON("memoryLeakTrace")) {

					if (allContexts.size() > 50)
						System.out.println("memoryLeakTrace:ContextService:allContexts " + allContexts.size());
				}
			}
		}

		return cm;
	}

	public void notifyAllActiveThreads(Context c) {
		Thread me = Thread.currentThread();

		synchronized (this) {
			for (Iterator i = allContexts.iterator(); i.hasNext(); ) {

				ContextManager cm = (ContextManager) i.next();

				Thread active = cm.activeThread;

				if (active == me)
					continue;

				if (active == null)
					continue;

				if (cm.setInterrupted(c))
					active.interrupt();
			}
		}

/*
		synchronized (this) {
			for (Enumeration e = keys(); e.hasMoreElements(); ) {
				Thread t = (Thread) e.nextElement();
				if (t == me)
					continue;

				ContextManager him = (ContextManager) get(t);

				if (him.setInterrupted(c))
					t.interrupt();
			}
		} */
	}

    synchronized void removeContext( ContextManager cm)
    {
        allContexts.remove( cm);
    }
}
