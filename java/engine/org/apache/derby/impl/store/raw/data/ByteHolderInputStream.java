/*

   Derby - Class org.apache.derby.impl.store.raw.data.ByteHolderInputStream

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

public class ByteHolderInputStream
extends InputStream
{
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
