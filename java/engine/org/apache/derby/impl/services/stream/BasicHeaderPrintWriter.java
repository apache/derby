/*

   Derby - Class org.apache.derby.impl.services.stream.BasicHeaderPrintWriter

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

