/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import java.io.InputStream;
import java.io.IOException;

public class ByteHolderInputStream
extends InputStream
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
	protected ByteHolder bh;

	public ByteHolderInputStream(ByteHolder bh) {
		this.bh = bh;
	}

	public int read() throws IOException{
		return bh.read();
	}

	public int read(byte b[], int off, int len)
		 throws IOException {
		return bh.read(b,off,len);
	}

	public long skip(long count) throws IOException {
		return bh.skip(count);
	}

	public int available() throws IOException {
		return bh.available(); 
	}

	public void setByteHolder(ByteHolder bh) {
		this.bh = bh;
	}

	public ByteHolder getByteHolder() {
		return bh;
	}
}
