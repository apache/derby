/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.services.stream
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	
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
	
