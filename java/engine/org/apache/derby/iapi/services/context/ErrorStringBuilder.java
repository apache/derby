/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.context
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.context;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;

import java.io.StringWriter;
import java.io.PrintWriter;

/**
 * Class used to form error messages.  Primary
 * reason for existence is to allow a way to call
 * printStackTrace() w/o automatically writting
 * to a stream.
 */
public class ErrorStringBuilder 
{
	private StringWriter	stringWriter;
	private PrintWriter		printWriter;
	private PrintWriterGetHeader	headerGetter;

	/**
	** Construct an error string builder
	**
	** @param boolean  whether a header string
	**					is included in each message
	*/
	public ErrorStringBuilder(PrintWriterGetHeader headerGetter)
	{
		this.headerGetter = headerGetter;
		this.stringWriter = new StringWriter();
		this.printWriter = new PrintWriter(stringWriter);
	}

	/**
	** Append an error string 
	**
	** @param String 	the string to append
	*/
	public void append(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.print(s);
	}


	/**
	** Append an error string with a newline
	**
	** @param String 	the string to append
	*/
	public void appendln(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.println(s);
	}

	/**
	** Print a stacktrace from the throwable in the error
	** buffer.
	**
	** @param Throwable	the error
	*/
	public void stackTrace(Throwable t)
	{
		int level = 0;
		while(t != null)
		{
			if (level > 0)	
				printWriter.println("============= begin nested exception, level (" +
									level + ") ===========");

			t.printStackTrace(printWriter);


			if (t instanceof StandardException) {
				t = ((StandardException)t).getNestedException();
			}
			else if (t instanceof ExceptionInInitializerError) {
				t = ((ExceptionInInitializerError) t).getException();
			}
			else if (t instanceof java.lang.reflect.InvocationTargetException) {
				t = ((java.lang.reflect.InvocationTargetException) t).getTargetException();
			}
			else if (t instanceof java.sql.SQLException) {
				t = ((java.sql.SQLException)t).getNextException();
			} else {
				t = null;
			}

			if (level > 0)	
				printWriter.println("============= end nested exception, level (" + 
									level + ") ===========");

			level++;

		}

	}

	/**
	** Reset the buffer -- truncate it down to nothing.
	**
	*/
	public void reset()
	{
		// Is this the most effecient way to do this?
		stringWriter.getBuffer().setLength(0);
	}

	/**
	** Get the buffer
	*/
	public StringBuffer get()
	{
		return stringWriter.getBuffer();
	}	
}	
