/*

   Derby - Class org.apache.derby.impl.store.raw.data.BufferedByteHolderInputStream

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
