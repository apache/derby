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
	 *	getHeader
	 *
	 *  @return	The header for an output line. 
	 *
	 *  @see org.apache.derby.iapi.services.stream.HeaderPrintWriter
	 **/

	public String getHeader();
}
	
