/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.stream
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.stream;

/**
 * Get a header to prepend to a line of output. 
 * A HeaderPrintWriter requires an object which implements
 * this interface to construct headers for output lines.
 *
 * @see org.apache.derby.iapi.services.stream.HeaderPrintWriter
 */

public interface PrintWriterGetHeader
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	 *	getHeader
	 *
	 *  @return	The header for an output line. 
	 *
	 *  @see org.apache.derby.iapi.services.stream.HeaderPrintWriter
	 **/

	public String getHeader();
}
	
