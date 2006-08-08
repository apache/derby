/*

   Derby - Class org.apache.derbyTesting.unitTests.services.MarkedLimitInputStream

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.iapi.services.sanity.SanityManager;
import java.io.DataInputStream;
import java.io.IOException;

/**
  An input stream whose internal data is in blocks, the format of each block is
    (boolean isLastBlock, int blockLength, sequence of blockLength bytes)
  All blocks except for the last block must have isLastBlock set to false.
  The last block must have isLastBlock set to true.

  This class implements an input stream whose length is limited, yet
  the creator (writer) of the stream does not need to know the entire length
  before creating it.
*/

public class MarkedLimitInputStream extends org.apache.derby.iapi.services.io.LimitInputStream
{
    protected boolean isLastBlock;
    protected int blockLength;


	public MarkedLimitInputStream(DataInputStream in)
        throws IOException
    {
		super(in);
        start();
	}

    private void start()
        throws IOException
    {
        isLastBlock = ((DataInputStream) in).readBoolean();
        blockLength = ((DataInputStream) in).readInt();

        if (SanityManager.DEBUG)
        {
            if (!isLastBlock)
            {
                SanityManager.ASSERT(blockLength > 0);
            }
            else
            {
                SanityManager.ASSERT(blockLength >= 0, "blockLength " + blockLength + " is negative");
            }
        }
        setLimit(blockLength);

    }

	public int read()
        throws IOException
    {
        int i = super.read();
        if (i == -1)
        {
            if (isLastBlock)
            {
                return -1;
            }
            else
            {
                start();
                return this.read();
            }
        }
        else
        {
            return i;
        }
	}


	public int read(byte b[], int off, int len)
        throws IOException
    {
		if (isLastBlock)
        {
            // get as many bytes as we can, superclass may return less
            // bytes than we asked for without good reason
	        int m = 0;
	        while (m < len)
            {
	            int count = super.read(b, off + m, len - m);
	            if (count < 0)
                {
		            break;
                }
	            m += count;
	        }
			return m;
        }

        // read until either get back all the bytes we asked for
        // or until we get -1
	    int n = 0;
	    while (n < len)
        {
	        int count = super.read(b, off + n, len - n);
	        if (count < 0)
            {
		        break;
            }
	        n += count;
	    }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(n <= len);
        }
        if (n == len)
        {
            return n;
        }

        // n < len, we didn't finish yet
        // init next block
        start();
        // read rest
        if (n < 0)
        {
            return this.read(b,off,len);
        }

        return n + this.read(b, off+n, len-n);
	}


    public long skip(long count)
        throws IOException
    {
        if (isLastBlock)
        {
            return super.skip(count);
        }

        // long n = super.skip(count);
        // read until either skip all the bytes we asked to
        // or until we get a result which is <= 0
	    long n = 0;
	    while (n < count)
        {
	        long c = super.skip(count-n);
	        if (c <= 0)
            {
		        break;
            }
	        n += c;
	    }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(n <= count);
        }
        if (n == count)
        {
            return n;
        }
        // if n < count, we didn't finish skipping yet
        // init next block
        start();
        // read rest
        if (n < 0)
        {
            return this.skip(count);
        }
        return n + this.skip(count-n);
    }


}
