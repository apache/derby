/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.OutputStream;

/**
	An OutputStream that simply discards all data written to it.
*/

public final class NullOutputStream extends OutputStream {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/*
	** Methods of OutputStream
	*/

	/**
		Discard the data.

		@see OutputStream#write
	*/
	public  void write(int b)  {
	}

	/**
		Discard the data.

		@see OutputStream#write
	*/
	public void write(byte b[]) {
	}

	/**
		Discard the data.

		@see OutputStream#write
	*/
	public void write(byte b[], int off, int len)  {
	}
}
