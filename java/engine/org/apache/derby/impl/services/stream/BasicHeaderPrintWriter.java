/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.stream
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.services.stream;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;

import java.io.PrintWriter;
import java.io.Writer;
import java.io.OutputStream;

/**
 * Basic class to print lines with headers. 
 * <p>
 *
 * STUB: Should include code to emit a new line before a header
 *			which is not the first thing on the line.
 *
 */
class BasicHeaderPrintWriter 
	extends PrintWriter
	implements HeaderPrintWriter
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	private final PrintWriterGetHeader headerGetter;
	private final boolean canClose;

	// constructors

	/**
	 * the constructor sets up the HeaderPrintWriter. 
	 * <p>
	 * @param writeTo		Where to write to.
	 * @param headerGetter	Object to get headers for output lines.
	 *
	 * @see	PrintWriterGetHeader
	 */
	BasicHeaderPrintWriter(OutputStream writeTo,
			PrintWriterGetHeader headerGetter,  boolean canClose){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
	}
	BasicHeaderPrintWriter(Writer writeTo,
			PrintWriterGetHeader headerGetter, boolean canClose){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
	}

	/*
	 * HeaderPrintWriter interface (partial; remaining methods
	 * come from the PrintWriter supertype).
	 */
	public synchronized void printlnWithHeader(String message)
	{ 
		print(headerGetter.getHeader());
		println(message);
	}

	public PrintWriterGetHeader getHeader()
	{
		return headerGetter;
	}

	public PrintWriter getPrintWriter(){
		return this;
	}

	void complete() {
		flush();
		if (canClose) {
			close();
		}
	}
}

