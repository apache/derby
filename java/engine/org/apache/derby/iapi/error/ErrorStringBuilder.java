/*

   Derby - Class org.apache.derby.iapi.services.context.ErrorStringBuilder

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

package org.apache.derby.iapi.error;

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
	** @param s 	the string to append
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
	** @param s 	the string to append
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
	** @param t	the error
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

			if (t instanceof java.sql.SQLException) {
				Throwable next = ((java.sql.SQLException)t).getNextException();
				t = (next == null) ? t.getCause() : next;
			} else {
				t = t.getCause();
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
