/*

   Derby - Class org.apache.derby.impl.services.stream.BasicHeaderPrintWriter

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

package org.apache.derby.impl.services.stream;

import org.apache.derby.shared.common.stream.HeaderPrintWriter;
import org.apache.derby.shared.common.stream.PrintWriterGetHeader;

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
	private final String name;

	// constructors

	/**
	 * the constructor sets up the HeaderPrintWriter. 
	 * <p>
	 * @param writeTo       Where to write to.
	 * @param headerGetter	Object to get headers for output lines.
	 * @param canClose      If true, {@link #complete} will also close writeTo
	 * @param streamName    Name of writeTo, e.g. a file name
	 *
	 * @see	PrintWriterGetHeader
	 */
	BasicHeaderPrintWriter(OutputStream writeTo,
//IC see: https://issues.apache.org/jira/browse/DERBY-205
			PrintWriterGetHeader headerGetter,  boolean canClose, String streamName){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
		this.name = streamName;
	}

	/**
	 * the constructor sets up the HeaderPrintWriter. 
	 * <p>
	 * @param writeTo       Where to write to.
	 * @param headerGetter	Object to get headers for output lines.
	 * @param canClose      If true, {@link #complete} will also close writeTo
	 * @param writerName    Name of writeTo, e.g. a file name
	 *
	 * @see	PrintWriterGetHeader
	 */
	BasicHeaderPrintWriter(Writer writeTo,
			PrintWriterGetHeader headerGetter, boolean canClose, String writerName){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
		this.name = writerName;
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

	public String getName(){
//IC see: https://issues.apache.org/jira/browse/DERBY-205
		return name;
	}

	/**
	 * Flushes stream, and optionally also closes it if constructed
	 * with canClose equal to true.
	 */

	void complete() {
		flush();
		if (canClose) {
			close();
		}
	}
}

