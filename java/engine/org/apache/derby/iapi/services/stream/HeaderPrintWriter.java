/*

   Derby - Class org.apache.derby.iapi.services.stream.HeaderPrintWriter

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

package org.apache.derby.iapi.services.stream;

import java.io.PrintWriter;

/**
 * A HeaderPrintWriter is like a PrintWriter with support for
 * including a header in the output. It is expected users
 * will use HeaderPrintWriters to prepend headings to trace
 * and log messages.
 * 
 */
public interface HeaderPrintWriter
{
	/**
	 * Puts out some setup info for
	 * the current write and the write(s) that will be put out next.
	 * It ends with a \n\r.
	 * <p>
	 * All other writes to the stream use the
	 * PrintStream interface.
	 */
	public void printlnWithHeader(String message);

	/**
	 * Return the header for the stream.
	 */
	public PrintWriterGetHeader getHeader();
	
	/**
	 * Gets a PrintWriter object for writing to this HeaderPrintWriter.
	 * Users may use the HeaderPrintWriter to access methods not included
	 * in this interface or to invoke methods or constructors which require
	 * a PrintWriter. 
	 *
	 * Interleaving calls to a printWriter and its associated HeaderPrintWriter
	 * is not supported.
	 * 
	 */
	public PrintWriter getPrintWriter();

	/*
	 * The routines that mimic java.io.PrintWriter...
	 */
	/**
	 * @see java.io.PrintWriter#print
	 */
	public void print(String message);

	/**
	 * @see java.io.PrintWriter#println
	 */
	public void println(String message);

	/**
	 * @see java.io.PrintWriter#println
	 */
	public void println(Object message);

	/**
	* @see java.io.PrintWriter#flush
	 */
	public void flush();
}

