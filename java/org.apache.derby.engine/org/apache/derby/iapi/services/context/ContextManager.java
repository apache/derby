/*

   Derby - Class org.apache.derby.iapi.services.context.ContextManager

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

package org.apache.derby.iapi.services.context;

import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.stream.HeaderPrintWriter;

import org.apache.derby.shared.common.error.ErrorStringBuilder;
import org.apache.derby.shared.common.error.PassThroughException;
import org.apache.derby.shared.common.error.ShutdownException;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.shared.common.reference.Property;
import org.apache.derby.iapi.services.property.PropertyUtil;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.i18n.LocaleFinder;
import org.apache.derby.shared.common.info.JVMInfo;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Locale;
import org.apache.derby.shared.common.reference.ContextId;

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
	/**
	 * The CtxStack implement a stack on top of an ArrayList (to avoid
	 * the inherent overhead associated with java.util.Stack which is
	 * built on top of java.util.Vector, which is fully
	 * synchronized).
	 */
	private static final class CtxStack {
		/** Internal list with all the elements of the stack. */
		private final ArrayList<Context> stack_ = new ArrayList<Context>();
		/** Read-only view of the internal list. */
		private final List<Context> view_ = Collections.unmodifiableList(stack_);

		// Keeping a reference to the top element on the stack
		// optimizes the frequent accesses to this element. The
		// tradeoff is that pushing and popping becomes more
		// expensive, but those operations are infrequent.
		private Context top_ = null;

//IC see: https://issues.apache.org/jira/browse/DERBY-938
		void push(Context context) { 
			stack_.add(context); 
			top_ = context; 
		}
		void pop() {
			stack_.remove(stack_.size()-1);
			top_ = stack_.isEmpty() ? 
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                null : stack_.get(stack_.size()-1);
		}
		void remove(Context context) {
			if (context == top_) {
				pop();
				return;
			}
			stack_.remove(stack_.lastIndexOf(context)); 
		}
		Context top() { 
			return top_; 
		}
		boolean isEmpty() { return stack_.isEmpty(); }

//IC see: https://issues.apache.org/jira/browse/DERBY-673
        List<Context> getUnmodifiableList() {
//IC see: https://issues.apache.org/jira/browse/DERBY-2884
			return view_;
		}
	}

	/**
	 * Empty ArrayList to use as void value
	 */
	//private final ArrayList voidArrayList_ = new ArrayList(0);

	/**
	 * HashMap that holds the Context objects. The Contexts are stored
	 * with a String key.
	 * @see ContextManager#pushContext(Context)
	 */
	private final HashMap<String,CtxStack> ctxTable = new HashMap<String,CtxStack>();

	/**
	 * List of all Contexts
	 */
	private final ArrayList<Context> holder = new ArrayList<Context>();

	/**
	 * Add a Context object to the ContextManager. The object is added
	 * both to the holder list and to a stack for the specific type of
	 * Context.
	 * @param newContext the new Context object
	 */
	public void pushContext(Context newContext)
	{
		checkInterrupt();
		final String contextId = newContext.getIdName();
		CtxStack idStack = ctxTable.get(contextId);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		// if the stack is null, create a new one.
		if (idStack == null) {
			idStack = new CtxStack();
			ctxTable.put(contextId, idStack);
		}

		// add to top of id's stack
		idStack.push(newContext);

		// add to top of global stack too
		holder.add(newContext);
	}
	
	/**
	 * Obtain the last pushed Context object of the type indicated by
	 * the contextId argument.
	 * @param contextId a String identifying the type of Context
	 * @return The Context object with the corresponding contextId, or null if not found
	 */
	public Context getContext(String contextId) {
		checkInterrupt();
		
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		final CtxStack idStack = ctxTable.get(contextId);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT( idStack == null ||
								  idStack.isEmpty() ||
								  idStack.top().getIdName() == contextId);
		return (idStack==null?null:idStack.top());
	}

	/**
	 * Remove the last pushed Context object, regardless of type. If
	 * there are no Context objects, no action is taken.
	 */
	public void popContext()
	{
		checkInterrupt();
		// no contexts to remove, so we're done.
		if (holder.isEmpty()) {
			return;
		}

		// remove the top context from the global stack
		Context theContext = holder.remove(holder.size()-1);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		// now find its id and remove it from there, too
		final String contextId = theContext.getIdName();
		final CtxStack idStack = ctxTable.get(contextId);

		if (SanityManager.DEBUG) {
			SanityManager.ASSERT( idStack != null &&
								  (! idStack.isEmpty()) &&
								  idStack.top().getIdName() == contextId);
		}
		idStack.pop();
	}

	/**
	 * Removes the specified Context object. If
	 * the specified Context object does not exist, the call will fail.
	 * @param theContext the Context object to remove.
	 */
	void popContext(Context theContext)
	{
		checkInterrupt();
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(!holder.isEmpty());

		// first, remove it from the global stack.
		holder.remove(holder.lastIndexOf(theContext));

		final String contextId = theContext.getIdName();
		final CtxStack idStack = ctxTable.get(contextId);
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		// now remove it from its id's stack.
		idStack.remove(theContext);
	}
    
    /**
     * Is the ContextManager empty containing no Contexts.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-1095
    final boolean isEmpty()
    {
        return holder.isEmpty();
    }
	
	/**
	 * Return an unmodifiable list reference to the ArrayList backing
	 * CtxStack object for this type of Contexts. This method allows
	 * fast traversal of all Contexts on that stack. The first element
	 * in the List corresponds to the bottom of the stack. The
	 * assumption is that the Stack will not be modified while it is
	 * being traversed.
	 * @param contextId the type of Context stack to return.
	 * @return an unmodifiable "view" of the ArrayList backing the stack
	 * @see org.apache.derby.impl.sql.conn.GenericLanguageConnectionContext#resetSavepoints()
	 * @see org.apache.derby.iapi.sql.conn.StatementContext#resetSavePoint()
	 */
    public final List<Context> getContextStack(String contextId) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		final CtxStack cs = ctxTable.get(contextId);
//IC see: https://issues.apache.org/jira/browse/DERBY-673
        return cs == null ?
               Collections.<Context>emptyList() :
               cs.getUnmodifiableList();
	}
    
    /**
     * clean up error and print it to derby.log. Extended diagnosis including
     * thread dump to derby.log and javaDump if available, will print if the
     * database is active and severity is greater than or equals to
     * SESSTION_SEVERITY or as configured by
     * derby.stream.error.extendedDiagSeverityLevel property
     * 
     * @param error the error we want to clean up
     * @param diagActive
     *        true if extended diagnostics should be considered, 
     *        false not interested of extended diagnostic information
     * @return true if the context manager is shutdown, false otherwise.
     */
    public boolean cleanupOnError(Throwable error, boolean diagActive)
	{
		if (shutdown)
			return true;

		if (errorStringBuilder == null)
			errorStringBuilder = new ErrorStringBuilder(errorStream.getHeader());

		ThreadDeath seenThreadDeath = null;
		if (error instanceof ThreadDeath)
			seenThreadDeath = (ThreadDeath) error;

//IC see: https://issues.apache.org/jira/browse/DERBY-4010
        if (error instanceof PassThroughException) {
            error = error.getCause();
        }

		boolean reportError = reportError(error);

		if (reportError) 
		{
			StringBuffer sb = null;
			if (! shutdown)
			{
				// report an id for the message if possible
//IC see: https://issues.apache.org/jira/browse/DERBY-673
                ContextImpl lcc =
                        (ContextImpl)getContext(ContextId.LANG_CONNECTION);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6945
				ContextImpl sc = (ContextImpl) getContext(org.apache.derby.shared.common.reference.ContextId.LANG_STATEMENT);
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

            int errorSeverity = getErrorSeverity(error);
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

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
					Context ctx = (holder.get(index));
					lastHandler = ctx.isLastHandler(errorSeverity);

					ctx.cleanupOnError(error);
                    //When errorSeverity greater or equals Property.EXT_DIAG_SEVERITY_LEVEL,
                    //the threadDump information will be in derby.log and 
                    //the diagnosis information will be prepared.
                    //If Property.EXT_DIAG_SEVERITY_LEVEL is not set in JVM property or
                    //derby property, we will only handle threadDump information and diagnosis
                    //information for errorSeverity = ExceptionSeverity.SESSION_SEVERITY.
                    if (reportError && diagActive
                            && (errorSeverity >= extDiagSeverityLevel)) {
                        threadDump = ExceptionUtil.dumpThreads();
                    } else {
                        threadDump = null;
                    }
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

            if (threadDump != null) {
                errorStream.println(threadDump);
                JVMInfo.javaDump();
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

        @param finder The local finder
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
     * return the severity of the exception. Currently, this method 
     * does not determine a severity that is not StandardException 
     * or SQLException.
     * @param error - Throwable error
     * 
     * @return int vendorcode/severity for the Throwable error
     *            - error/exception to extract vendorcode/severity. 
     *            For error that we can not get severity, 
     *            NO_APPLICABLE_SEVERITY will return.
     */
    public int getErrorSeverity(Throwable error) {
        
//IC see: https://issues.apache.org/jira/browse/DERBY-4856
//IC see: https://issues.apache.org/jira/browse/DERBY-4929
//IC see: https://issues.apache.org/jira/browse/DERBY-289
        if (error instanceof StandardException) {
            return ((StandardException) error).getErrorCode();
        }
        
        if (error instanceof SQLException) {
            return ((SQLException) error).getErrorCode();
        }
        return ExceptionSeverity.NO_APPLICABLE_SEVERITY;
    }

	/**
	 * Constructs a new instance. No CtxStacks are inserted into the
	 * hashMap as they will be allocated on demand.
	 * @param csf the ContextService owning this ContextManager
	 * @param stream error stream for reporting errors
	 */
	ContextManager(ContextService csf, HeaderPrintWriter stream)
	{
		errorStream = stream;
		owningCsf = csf;

		logSeverityLevel = PropertyUtil.getSystemInt(Property.LOG_SEVERITY_LEVEL,
			SanityManager.DEBUG ? 0 : ExceptionSeverity.SESSION_SEVERITY);
        extDiagSeverityLevel = PropertyUtil.getSystemInt(
                Property.EXT_DIAG_SEVERITY_LEVEL,
                ExceptionSeverity.SESSION_SEVERITY);
	}

	final ContextService owningCsf;

	private int		logSeverityLevel;
    // DERBY-4856 track extendedDiagSeverityLevel variable
    private int extDiagSeverityLevel;

	private HeaderPrintWriter errorStream;
	private ErrorStringBuilder errorStringBuilder;
    // DERBY-4856 add thread dump information.
    private String threadDump;

	private boolean shutdown;
	private LocaleFinder finder;

    /**
     * The thread that owns this ContextManager, set by
     * ContextService.setCurrentContextManager and reset
     * by resetCurrentContextManager. Only a single
     * thread can be active in a ContextManager at any time,
     * and the thread only "owns" the ContextManager while
     * it is executing code within Derby. In the JDBC case
     * setCurrentContextManager is called at the start of
     * a JBDC method and resetCurrentContextManager on completion.
     * Nesting within the same thread is supported, such as server-side
     * JDBC calls in a Java routine or procedure. In that case
     * the activeCount will represent the level of nesting, in
     * some situations.
     * <BR>

     * @see ContextService#setCurrentContextManager(ContextManager)
     * @see ContextService#resetCurrentContextManager(ContextManager)
     * @see #activeCount
     */
	Thread	activeThread;
    
    /**
     * Count of the number of setCurrentContextManager calls
     * by a single thread, for nesting situations with a single
     * active Contextmanager. If nesting is occuring with multiple
     * different ContextManagers then this value is set to -1
     * and nesting is represented by entries in a stack in the
     * ThreadLocal variable, threadContextList.
     * 
     * @see ContextService#threadContextList
     */
	int		activeCount;
}
