/*

   Derby - Class org.apache.derby.iapi.services.context.ContextManager

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;

import org.apache.derby.iapi.error.PassThroughException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import java.io.PrintWriter;

import java.util.Hashtable;
import java.util.Stack;
import java.util.Vector;
import java.util.Locale;

/**
 *
 * The ContextManager collects contexts as they are
 * created. It maintains stacks of contexts by
 * named ids, so that the top context of a given
 * type can be returned. It also maintains a global
 * stack so that contexts can be traversed in the
 * order they were created.
 * <p>
 * The first implementation of the context manager
 * assumes there is only one thread to worry about
 * and that the user(s) of the class only create one
 * instance of ContextManager.
 */

public class ContextManager

{


	private final Stack holder;

	/*
	 * ContextManager interface
	 */

	public void pushContext(Context newContext)
	{
		checkInterrupt();

		String contextId = newContext.getIdName();

		Stack idStack = (Stack) ctxTable.get(contextId);

		// if the stack is null, create a new one.
		if (idStack == null)
		{
			idStack = new Stack();
			ctxTable.put(contextId,idStack);
		}

		// add to top of id's stack
		idStack.push(newContext);

		// add to top of global stack too
		holder.push(newContext);
	}

	/**
	 * @see org.apache.derby.iapi.services.context.ContextManager#getContext
	 */
	public Context getContext(String contextId)
	{
		checkInterrupt();

		Stack idStack = (Stack)ctxTable.get(contextId);

		if (SanityManager.DEBUG)
		SanityManager.ASSERT( idStack == null ||
			idStack.empty() ||
			((Context)idStack.peek()).getIdName().equals(contextId));

		if (idStack == null ||
		    idStack.empty())
		{
			return null;
		}



		return (Context) idStack.peek();
	}

	/**
	 * @see org.apache.derby.iapi.services.context.ContextManager#popContext
	 */
	public void popContext()
	{
		checkInterrupt();

		Context theContext;
		String contextId;
		Stack idStack;

		// no contexts to remove, so we're done.
		if (holder.empty())
		{
			return;
		}

		// remove the top context from the global stack
		theContext = (Context) holder.pop();

		// now find its id and remove it from there, too
		contextId = theContext.getIdName();
		idStack = (Stack)ctxTable.get(contextId);

		if (SanityManager.DEBUG)
		SanityManager.ASSERT( idStack != null &&
			(! idStack.empty()) &&
			((Context)idStack.peek()).getIdName() == contextId);

		idStack.pop();
	}

	void popContext(Context theContext)
	{
		checkInterrupt();

		Stack idStack;

		if (SanityManager.DEBUG)
		SanityManager.ASSERT(!holder.empty());

		// first, remove it from the global stack.
		// to do this we treat it like its vector supertype.
		int index = holder.lastIndexOf(theContext);
		if (index != -1)
			holder.removeElementAt(index);
		else if (SanityManager.DEBUG) {
			//SanityManager.THROWASSERT("Popped non-existent context by id " + theContext + " type " + theContext.getIdName());
		}

		// now remove it from its id's stack.
		idStack = (Stack) ctxTable.get(theContext.getIdName());
		boolean wasThere = idStack.removeElement(theContext);
		if (SanityManager.DEBUG) {
			//if (!wasThere)
			//	SanityManager.THROWASSERT("Popped non-existent stack by id " + theContext + " type " + theContext.getIdName());
		}
	}

	/**
		@return true if the context manager is shutdown, false otherwise.
	 */
	public boolean cleanupOnError(Throwable error)
	{
		if (shutdown)
			return true;

		if (errorStringBuilder == null)
			errorStringBuilder = new ErrorStringBuilder(errorStream.getHeader());

		ThreadDeath seenThreadDeath = null;
		if (error instanceof ThreadDeath)
			seenThreadDeath = (ThreadDeath) error;

		if (error instanceof PassThroughException)
			error = ((PassThroughException) error).getException();

		boolean reportError = reportError(error);

		if (reportError) 
		{
			ContextImpl lcc = null;
			StringBuffer sb = null;
			if (! shutdown)
			{
				// report an id for the message if possible
				lcc = (ContextImpl) getContext(org.apache.derby.iapi.reference.ContextId.LANG_CONNECTION);
				if (lcc != null) {
					sb = lcc.appendErrorInfo();
				}
			}

			String cleanup = "Cleanup action starting";

			if (sb != null) {
				sb.append(cleanup);
				cleanup = sb.toString();
			}

			errorStringBuilder.appendln(cleanup);
			
			if (!shutdown)		// Do this only during normal processing.
			{	
				ContextImpl sc = (ContextImpl) getContext(org.apache.derby.iapi.reference.ContextId.LANG_STATEMENT);
				// Output the SQL statement that failed in the log file.
				if (sc != null)
				{					
					sb = sc.appendErrorInfo();
					if (sb != null)
						errorStringBuilder.appendln(sb.toString());
				}
			}
		}
		
		/*
		  REVISIT RESOLVE
		  Ensure that the traversal of the stack works in all 
		  cases where contexts can  pop themselves *and* 
		  contexts can pop other contexts off the stack.
		*/ 
		

forever: for (;;) {

			int errorSeverity = error instanceof StandardException ?
				((StandardException) error).getSeverity() :
				ExceptionSeverity.NO_APPLICABLE_SEVERITY;
 			if (reportError) {
				errorStringBuilder.stackTrace(error);
				flushErrorString();
			}

			
			boolean	lastHandler = false;


			/*
				Walk down the stack, calling
				cleanup on each context. We use
				the vector interface to do this.
			 */
cleanup:	for (int index = holder.size() - 1; index >= 0; index--) {

				try {
					if (lastHandler)
					{
						break;
					}

					Context ctx = ((Context) holder.elementAt(index));
					lastHandler = ctx.isLastHandler(errorSeverity);

					ctx.cleanupOnError(error);
				}
				catch (StandardException se) {
	
					if (error instanceof StandardException) {
	
						if (se.getSeverity() > ((StandardException) error).getSeverity()) {
							// Ok, error handling raised a more severe error,
							// restart with the more severe error
							error = se;
							reportError = reportError(se);
							if (reportError) {
								errorStream.println("New exception raised during cleanup " + error.getMessage());
								errorStream.flush();
							}
							continue forever;
						}
					}

					if (reportError(se)) {
						errorStringBuilder.appendln("Less severe exception raised during cleanup (ignored) " + se.getMessage());
						errorStringBuilder.stackTrace(se);
						flushErrorString();
					}

					/*
						For a less severe error, keep with the last error
					 */
					continue cleanup;
				}
				catch (Throwable t) {
					reportError = reportError(t);


					if (error instanceof StandardException) {
						/*
							Ok, error handling raised a more severe error,
							restart with the more severe error
							A Throwable after a StandardException is always 
							more severe.
						 */
						error = t;
						if (reportError) {
							errorStream.println("New exception raised during cleanup " + error.getMessage());
							errorStream.flush();
						}
						continue forever;
					}


					if (reportError) {
						errorStringBuilder.appendln("Equally severe exception raised during cleanup (ignored) " + t.getMessage());
						errorStringBuilder.stackTrace(t);
						flushErrorString();
					}

					if (t instanceof ThreadDeath) {
						if (seenThreadDeath != null)
							throw seenThreadDeath;

						seenThreadDeath = (ThreadDeath) t;
					}
	
					/*
						For a less severe error, just continue with the last
						error
					 */
					continue cleanup;
				}
			}

			if (reportError) {
				errorStream.println("Cleanup action completed");
				errorStream.flush();
			}

			if (seenThreadDeath != null)
				throw seenThreadDeath;

			return false;
		}

	}


	synchronized boolean  setInterrupted(Context c) {

		boolean interruptMe = (c == null) || holder.contains(c);

		if (interruptMe) {
			this.shutdown = true;
		}
		return interruptMe;
	}

	/**
		Check to see if we have been interrupted. If we have then
		a ShutdownException will be thrown. This will be either the
		one passed to interrupt or a generic one if some outside
		source interrupted the thread.
	*/
	private void checkInterrupt() {
		if (shutdown) {
			// system must have changed underneath us
			throw new ShutdownException();
		}
	}

	/**
		Set the locale for this context.
	*/
	public void setLocaleFinder(LocaleFinder finder) {
		this.finder = finder;
	}

	private Locale messageLocale;

	public void setMessageLocale(String localeID) throws StandardException {
		this.messageLocale = Monitor.getLocaleFromString(localeID);
	}

	public Locale getMessageLocale()
	{
		if (messageLocale != null)
			return messageLocale;
		else if (finder != null) {
			try {
				return finder.getCurrentLocale();
			} catch (StandardException se) {
				
			}
		}
		return Locale.getDefault();
	}

	/**
	 * Flush the built up error string to whereever
	 * it is supposed to go, and reset the error string
	 */
	private void flushErrorString()
	{
		errorStream.print(errorStringBuilder.get().toString());
		errorStream.flush();
		errorStringBuilder.reset();
	}

	/*
	** Class methods
	*/

	private boolean reportError(Throwable t) {

		if (t instanceof StandardException) {

			StandardException se = (StandardException) t;

			switch (se.report()) {
			case StandardException.REPORT_DEFAULT:
				int level = se.getSeverity();
				return (level >= logSeverityLevel) ||
					(level == ExceptionSeverity.NO_APPLICABLE_SEVERITY);

			case StandardException.REPORT_NEVER:
				return false;

			case StandardException.REPORT_ALWAYS:
			default:
				return true;
			}
		}

		return !(t instanceof ShutdownException);

	}

	/**
	 * constructor specifying the hash table size and load
	 * factor for the hashed-by-id context stacks.
	 */
	ContextManager(ContextService csf, HeaderPrintWriter stream)
	{
		errorStream = stream;
		ctxTable = new Hashtable();
		owningCsf = csf;

		logSeverityLevel = PropertyUtil.getSystemInt(Property.LOG_SEVERITY_LEVEL,
			SanityManager.DEBUG ? 0 : ExceptionSeverity.SESSION_SEVERITY);

		holder = new Stack();
	}

	private final Hashtable ctxTable;

	final ContextService owningCsf;

	private int		logSeverityLevel;

	private HeaderPrintWriter errorStream;
	private ErrorStringBuilder errorStringBuilder;

	private boolean shutdown;
	private LocaleFinder finder;

	final Stack cmStack = new Stack();

	Thread	activeThread;
	int		activeCount;
}
