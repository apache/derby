/*

   Derby - Class org.apache.derby.impl.store.raw.data.MemByteHolder

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Vector;

/**
  A ByteHolder that stores all its bytes in memory.
  */
public class MemByteHolder
implements ByteHolder
{
	int bufSize;

	boolean writing = true;
	
	Vector bufV;
	int curBufVEleAt;

	byte[] curBuf;
	int curBufPos;

	//
	//We use this to determine when we have reached the end
	//of the current buffer whild reading. For the last
	//buffer this may be less than bufSize.
	int curBufDataBytes;

	//
	//We use these to remember the location of the last byte
	//of data we have stored. The read methods use these to
	//avoid reading more bytes than we have stored. These
	//values are set by startReading.
	int lastBufVEleAt = 0;
	int lastBufDataBytes = 0;
	
	/**
	  Create a new MemByteHolder. Store bytes as a list of buffers
	  of size 'bufSize'.
	  */
	public MemByteHolder(int bufSize)
	{
		this.bufSize = bufSize;

		this.curBuf = new byte[bufSize];
		this.curBufPos = 0;

		this.bufV = new Vector(128);
		bufV.addElement(curBuf);
		this.curBufVEleAt = 0;
	}

	/**
	  @see ByteHolder#write
	  @exception IOException		Thrown on error
	  */
	public void write(int b) throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == true,
								 "Writing should be true 1");

		if(curBufPos>=curBuf.length)
			getNextBuffer_w();

		curBuf[curBufPos++] = (byte)b;
	}

	/**
	  @see ByteHolder#write
	  @exception IOException		Thrown on error
	  */
	public void write(byte[] data, int offset, int len) throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == true,
								 "Writing should be true 2");

		while(len > 0)
		{
			if(curBufPos>=curBuf.length)
				getNextBuffer_w();

			int bytesToCopyThisTime = len;
			int bytesInCurBuf = curBuf.length - curBufPos;

			if (bytesToCopyThisTime > bytesInCurBuf)
				bytesToCopyThisTime = bytesInCurBuf;
			System.arraycopy(data,offset,curBuf,curBufPos,bytesToCopyThisTime);
			offset += bytesToCopyThisTime; 
			curBufPos += bytesToCopyThisTime;
			len -= bytesToCopyThisTime;
		}
	}

	/**
	  @see ByteHolder#write
	  @exception IOException		Thrown on error
	  */
	public long write(InputStream is, long count) throws IOException
	{
		long bytesToTransfer = count;
		int bytesTransferredThisTime = 0;
		
		do 
		{
			if(curBufPos>=curBuf.length)
				getNextBuffer_w();
 
			int bytesToTransferThisTime;
			int bytesInCurBuf = curBuf.length - curBufPos;

			if (bytesToTransfer >= bytesInCurBuf)
				bytesToTransferThisTime = bytesInCurBuf;
			else
				 bytesToTransferThisTime = (int)bytesToTransfer;
			//
			//Note read should never return 0. Thus we keep looping
			//transferring bytes from the stream to our buffer until
			//we transfer count bytes or reach the end of the stream.
			//
			bytesTransferredThisTime =
				is.read(curBuf,curBufPos,bytesToTransferThisTime);

			if (bytesTransferredThisTime > 0)
			{
                if (SanityManager.DEBUG)
                    SanityManager.ASSERT(
                        writing == true, "Writing should be true 3");

				bytesToTransfer -= bytesTransferredThisTime;
				curBufPos += bytesTransferredThisTime;
			}
		} while (bytesToTransfer > 0 &&
				 bytesTransferredThisTime > 0);

		return count - bytesToTransfer;
	}

	/**
	  @see ByteHolder#clear

	  @exception IOException		Thrown on error
	  */
	public void clear() throws IOException
	{
		writing = true;
		
		curBuf = (byte[])bufV.elementAt(0);
		this.curBufVEleAt = 0;
		this.curBufPos = 0;
		
		lastBufVEleAt = 0;
		lastBufDataBytes = 0;
	}

	/**
	  @see ByteHolder#startReading
	  */
	public void startReading()
        throws IOException
	{
		if (writing == true)
		{
			//Enter read mode.
			writing = false;
			lastBufDataBytes = curBufPos;
			lastBufVEleAt = curBufVEleAt;
		}
		//
		//Reposition so reads start from the first
		//byte.
		curBuf = (byte[])bufV.elementAt(0);
		this.curBufVEleAt = 0;
		this.curBufPos = 0;
		if (curBufVEleAt == lastBufVEleAt)
			curBufDataBytes = lastBufDataBytes;
		else
			curBufDataBytes = bufSize;
	}

	/**
	  @see ByteHolder#read
	  @exception IOException	Thrown on error
	  */
	public int read() throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == false,
								 "Reading should be true 2");
		
		if (curBufPos >= curBufDataBytes)
			getNextBuffer_r();

		if (curBufPos >= curBufDataBytes)
			return -1;
		else
			return 0xff & curBuf[curBufPos++];
	}

	/**
	  @see ByteHolder#read
	  @exception IOException	Thrown on error
	  */
	public int read(byte b[],
					int off,
					int len)
		throws IOException
	{
		return (read(b, off, (OutputStream) null, len));
	}

	public int read(OutputStream out,
					int len)
		throws IOException
	{
		return(read((byte []) null, 0, out, len));
	}

	/**
	  @see ByteHolder#read
	  @exception IOException	Thrown on error
	  */
	public int read(byte b[],
					int off,
					OutputStream out,
					int len)
		throws IOException
	{
		int bytesIRead = 0;
		boolean eof = false;
		
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == false,
								 "Reading should be true 3");
		
		if (curBufPos >= curBufDataBytes)
			eof = getNextBuffer_r();

		if (eof) return -1;

		while (len > 0 && !eof)
		{
			int bytesInCurBuf = curBufDataBytes - curBufPos;
			int bytesIReadThisTime;
			if (len >= bytesInCurBuf)
				bytesIReadThisTime = bytesInCurBuf;
			else
				 bytesIReadThisTime = len;

			if (out == null) {
				// write the data to the byte array
				System.arraycopy(curBuf,curBufPos,b,off,bytesIReadThisTime);
			} else {
				// write the data to the output stream
				out.write(curBuf, curBufPos, bytesIReadThisTime);
			}
			off+=bytesIReadThisTime;
			curBufPos+=bytesIReadThisTime;
			len -= bytesIReadThisTime;
			bytesIRead+=bytesIReadThisTime;
			if (curBufPos >= curBufDataBytes)
				eof = getNextBuffer_r();
		}

		return bytesIRead;
	}

	/**
	  @see ByteHolder#shiftToFront
	  @exception IOException	Thrown on error
	  */
	public int shiftToFront() throws IOException
	{
		int remainingBytes = available();
		remainingBytes = remainingBytes > 0 ? remainingBytes : (-1) * remainingBytes;

		byte b[] = new byte[remainingBytes + 1];
		int bytesRead = read(b, 0, remainingBytes);

		// clear the buffer
		clear();

		// put the bytes at the beginning of the buffer
		writing = true;
		write(b, 0, bytesRead);

		curBufDataBytes = 0;

		return bytesRead;
	}

	/**
	  @see ByteHolder#available
	  */
	public int available()
	{
		//if (SanityManager.DEBUG)
		//	SanityManager.ASSERT(writing == false,
		//						 "Reading should be true 3");

		int curBufAvailable = curBufDataBytes - curBufPos;
		int lastBufAvailable = 0;
		int middleBuffers = 0;
		if (curBufVEleAt != lastBufVEleAt)
		{			
			middleBuffers = lastBufVEleAt - curBufVEleAt - 1;
			lastBufAvailable = lastBufDataBytes;
		}
		int availableBytes =
			curBufAvailable +
			lastBufAvailable +
			middleBuffers * bufSize;

		return availableBytes;
	}

    /**
 	  Return the number of bytes that have been saved to this byte holder.
      This result is different from available() as it is unaffected by the
      current read position on the ByteHolder.

	  @see ByteHolder#numBytesSaved
	  */
	public int numBytesSaved()
	{
        int ret_val;

        if (writing)
        {
            // still writing, so use the cur* variables
            if (SanityManager.DEBUG)
                SanityManager.ASSERT(
                    lastBufVEleAt == 0 && lastBufDataBytes == 0,
                    "counters were somehow bumped during writing");

            ret_val = (curBufVEleAt * bufSize) + curBufPos;
        }
        else
        {
            ret_val = (lastBufVEleAt * bufSize) + lastBufDataBytes;
        }

        return(ret_val);
	}

	/**
	  @see ByteHolder#skip
	  @exception IOException	Thrown on error
	  */
	public long skip(long count) throws IOException
	{
		long bytesISkipped = 0;
		boolean eof = false;
		
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == false,
								 "Reading should be true 4");
		
		if (curBufPos >= curBufDataBytes)
			eof = getNextBuffer_r();

		while (count > 0 && !eof)
		{
			int bytesInCurBuf = curBufDataBytes - curBufPos;
			int bytesISkippedThisTime;
			
			if (count >= bytesInCurBuf)
				bytesISkippedThisTime = bytesInCurBuf;
			else
				 bytesISkippedThisTime = (int)count;

			curBufPos+=bytesISkippedThisTime;
			count -= bytesISkippedThisTime;
			bytesISkipped+=bytesISkippedThisTime;

			if (count > 0)
				eof = getNextBuffer_r();
		}

		return bytesISkipped;
	}

	/**
	  @see ByteHolder#writingMode
	 */
	public boolean writingMode()
	{
		return writing;
	}

	/**
	  Get the next buffer for writing bytes.
	  @exception IOException	Thrown on error
	  */
	protected void getNextBuffer_w() throws IOException
	{
		if (SanityManager.DEBUG)
		{
			getNextBuffer_w_Sanity();
		}
		
		curBufVEleAt++;

		if (bufV.size() <= curBufVEleAt)
		{
			curBuf = new byte[bufSize];
			bufV.addElement(curBuf);
		}
		else
		{
			curBuf = (byte[])bufV.elementAt(curBufVEleAt);
		}
		
		initBuffer_w();
	}

	/** Do sanity checking when getting the next write buffer */
	protected void getNextBuffer_w_Sanity()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(curBufPos == curBuf.length,
								 "partial write");

			SanityManager.ASSERT(writing == true,
								 "Writing should be true 5");
		}
	}
		
	/** Initialize a buffer for writing */
	protected void initBuffer_w()
	{
		curBufPos = 0;
		
		if (SanityManager.DEBUG)
		{
		    SanityManager.ASSERT(curBuf.length == bufSize,
								 "bad Buf Length "+curBuf.length);
		}
	}

	/**
	  Get the next buffer for reading bytes.

	  @return true if the user has read all the bytes
	  in this ByteHolder.

	  @exception IOException		Thrown on error
	  */
	protected boolean getNextBuffer_r() throws IOException
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(writing == false,
								 "Reading should be true 5");
		if (curBufVEleAt >= lastBufVEleAt) return true;
		curBuf = (byte[])bufV.elementAt(++curBufVEleAt);
		curBufPos = 0;
		if (curBufVEleAt == lastBufVEleAt)
			curBufDataBytes = lastBufDataBytes;
		else
			curBufDataBytes = bufSize;
		return false;
	}

	/**
	  Create a string representation of an internal buffer of bytes.
	  This is useful during debugging.
	  */
	private String dumpBuf(int bufVEleAt)
	{
		StringBuffer sb = new StringBuffer(100);

		byte[] buf = (byte[])bufV.elementAt(bufVEleAt);
		sb.append("(");
		for (int ix = 0;ix<buf.length;ix++)
			sb.append(buf[ix]+".");
		sb.append(")");
		return sb.toString();
	}

	/**
	  Produce a string describing the state of this ByteHolder.
	  This is mainly for debugging.
	  */
	public String toString()
	{
		return
			" writing: "+writing+
			" curBufVEleAt: "+curBufVEleAt+
			" curBufPos: "+curBufPos+
			" curBufDataBytes: "+curBufDataBytes+
			" lastBufVEleAt: "+lastBufVEleAt+
            " lastBufDataBytes: "+lastBufDataBytes+
			" curBuf: "+dumpBuf(curBufVEleAt);
	}
}
