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

public abstract class BufferedByteHolderInputStream
extends ByteHolderInputStream
{
	public BufferedByteHolderInputStream(ByteHolder bh) {
		super(bh);
	}

	public abstract void fillByteHolder() throws IOException;

	public int read() throws IOException
	{
		fillByteHolder();
		return super.read();
	}

	public int read(byte b[], int off, int len)
		 throws IOException
	{
		fillByteHolder();
		return super.read(b,off,len);
	}

	public long skip(long count) throws IOException
	{
		int bytesSkipped = 0;
		while (bytesSkipped < count) {
			fillByteHolder();
			bytesSkipped += super.skip(count - bytesSkipped);
		}
		return bytesSkipped;
	}

	public int available() throws IOException
	{
		fillByteHolder();
		return super.available(); 
	}
}
