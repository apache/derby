/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.stream
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
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

