/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.sanity
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.sanity;

import java.io.*;

/**
 * AssertFailure is raised when an ASSERT check fails.
 * Because assertions are not used in production code,
 * are never expected to fail, and recovering from their
 * failure is expected to be hard, they are under
 * RuntimeException so that no one needs to list them
 * in their throws clauses.  An AssertFailure at the
 * outermost system level will result in system shutdown.
 **/
public class AssertFailure extends RuntimeException
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	private Throwable nestedException;

	/**
	 * This constructor takes the pieces of information
	 * expected for each error.
	 *
	 * @param messageID the message associated with
	 * the error.
	 *
	 * @param args the arguments and information items
	 * for the error.
	 *
	 * @param nestedError errors can be nested together;
	 * if this error has another error associated with it,
	 * it is specified here. The 'outermost' error should be
	 * the most sever error; inner errors should be providing
	 * additional information about what went wrong.
	 **/
	public AssertFailure(String message, Throwable nestedError)
	{
		super(message);
		nestedException = nestedError;
	}

	/**
	 * This constructor expects no arguments or nested error.
	 **/
	public AssertFailure(String message)
	{
		super(message);
	}

	public void printStackTrace() {
		super.printStackTrace();
		if (nestedException != null)
			nestedException.printStackTrace();
	}
	public void printStackTrace(PrintStream s) {
		super.printStackTrace(s);
		if (nestedException != null)
			nestedException.printStackTrace(s);
	}
	public void printStackTrace(PrintWriter s) {
		super.printStackTrace(s);
		if (nestedException != null)
			nestedException.printStackTrace(s);
	}
}
