/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.context
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.context;

/**
 * Contexts are created and used to manage the execution
 * environment. They provide a convenient location for
 * storing globals organized by the module using the
 * globals. 
 * <p>
 * We provide this abstract class for other implementations
 * to use so that they can simply add fields and operations on
 * them. To be usable by the context manager, the subclasses
 * must define CleanupOnError and call super() in any constructor.
 * <p>
 * Contexts assist in cleanup
 * when errors are caught in the outer block.
 * <p>
 * Contexts implement the sanity interface to check and provide
 * information about their contents.
 */
public abstract class ContextImpl 
	implements Context
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	private final String myIdName;
	private final ContextManager myContextManager;

	/*
	 * class interface
	 */
	protected ContextImpl(ContextManager cm, String id) {
		myIdName = id;
		myContextManager = cm;
		cm.pushContext(this);
	}

	/*
	 * Context interface
	 */
	/**
	 * @see org.apache.derby.iapi.services.context.Context#getContextManager
	 */
	final public ContextManager getContextManager()
	{
		return myContextManager;
	}

	/**
	 * @see org.apache.derby.iapi.services.context.Context#getIdName
	 */
	final public String getIdName()
	{
		return myIdName;
	}

	final public void pushMe() {
		getContextManager().pushContext(this);
	}

	/** @see Context#popMe */
	final public void popMe() {
		getContextManager().popContext(this);
	}

	/**
	 * @see Context#isLastHandler
	 */
	public boolean isLastHandler(int severity)
	{
		return false;
	}

	public StringBuffer appendErrorInfo() {
		return null;
	}
}
