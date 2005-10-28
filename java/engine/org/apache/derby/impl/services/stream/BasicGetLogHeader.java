/*

   Derby - Class org.apache.derby.impl.services.stream.BasicGetLogHeader

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

import org.apache.derby.iapi.services.stream.PrintWriterGetHeader;
import org.apache.derby.iapi.util.CheapDateFormatter;

/**
 * Get a header to prepend to a line of output. *
 * A HeaderPrintWriter requires an object which implements
 * this interface to construct line headers.
 *
 * @see org.apache.derby.iapi.services.stream.HeaderPrintWriter
 */

class BasicGetLogHeader implements PrintWriterGetHeader
{
	
	private boolean doThreadId;
	private boolean doTimeStamp;
	private String tag;

	/* 
	 * STUB: This should take a header template. Check if
	 *		 the error message facility provides something.
	 *	
	 *		 This should be localizable. How?
	 */
	/**
	 * Constructor for a BasicGetLogHeader object.
	 * <p>
	 * @param doThreadId	true means include the calling thread's
	 *							id in the header.
	 * @param doTimeStamp	true means include the current time in 
	 *							the header.
	 * @param tag			A string to prefix the header. null
	 *						means don't prefix the header with
	 *						a string.
	 */
	BasicGetLogHeader(boolean doThreadId,
				boolean doTimeStamp,
				String tag){
		this.doThreadId = doThreadId;
		this.doTimeStamp = doTimeStamp;
		this.tag = tag;
	}	
	
	public String getHeader()
	{
		StringBuffer header = new StringBuffer(48);

		if (tag != null) {
			header.append(tag);
			header.append(' ');
		}

		if (doTimeStamp) {
			long currentTime = System.currentTimeMillis();

			header.append(CheapDateFormatter.formatDate(currentTime));
			header.append(' ');

		}
		if (doThreadId) {
			header.append(Thread.currentThread().toString());
			header.append(' ');
		}

		return header.toString();
	}
}
	
