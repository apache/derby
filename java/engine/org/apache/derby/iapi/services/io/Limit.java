/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.IOException;

/**
	Methods that allow limits to be placed on an input or output stream to
	avoid clients reading or writing too much information.
*/
public interface Limit { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/**
		Set the limit of the data that can be read or written. After this
		call up to and including length bytes can be read from or skipped in
		the stream.
		
		<P> On input classes (e.g. InputStreams) any attempt to read or skip
		beyond the limit will result in an end of file indication
		(e.g. read() methods returning -1 or throwing EOFException).

		<P> On output classes (e.g. OutputStream) any attempt to write
		more beyond the limit will result in an EOFException

		@exception IOException IOException from some underlying stream
		@exception EOFException The set limit would exceed
		the available data in the stream.
	*/
	public void setLimit(int length)
		throws IOException;

	/**
		Clear any limit set by setLimit. After this call no limit checking
		will be made on any read until a setLimit()) call is made.

		@return the number of bytes within the limit that have not been read or written.
	*/
	public int clearLimit();
}
