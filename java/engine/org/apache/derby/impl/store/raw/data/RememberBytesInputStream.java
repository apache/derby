/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.services.sanity.SanityManager;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
  A FilterInputStream that remembers read or skipped bytes.

  <P>In record mode this stream remembers all the bytes a
  caller reads or skips. After reading some bytes this
  returns a 'replay' stream to re-read them.

  <P>A caller may call getReplaySteam to get a stream 
  to re-read the the remembered bytes. Any number of calls
  to getReplayStream are supported.

  <P>The clear function causes this stream to forget the remembered
  bytes and re-enter record mode.
  */
public class RememberBytesInputStream extends FilterInputStream
{
	ByteHolder bh;
	boolean recording = true;
	
	/**
	  Construct a RememberBytesInputStream.

	  @param bh for storing the remembered bytes. (must be
	  in writing mode.
	  */
	public RememberBytesInputStream(InputStream in, ByteHolder bh) {
		super(in);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(bh.writingMode());

		this.bh = bh;
	}
	
	/**
	  @see java.io.InputStream#read
	  @exception IOException thrown on an io error spooling rememberd bytes
	             to backing storage.
	  */
	public int read() throws IOException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(recording,
								 "Must be in record mode to perform a read.");
		int value = super.read();
		if (value != -1)
			bh.write(value);
		return value;
	}

	/**
	  @see java.io.InputStream#read
	  @exception IOException thrown on an io error spooling rememberd bytes
	             to backing storage.
	  */
	public int read(byte b[], int off, int len) throws IOException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(recording,
								 "Must be in record mode to perform a read.");
		if ((len + off) > b.length)
			len = b.length - off;
		len = super.read(b,off,len);
		if (len != -1)
			bh.write(b,off,len);
		return len;
	}

	/**
	  read len bytes from the input stream, and store it in the byte holder.

	  @exception IOException thrown on an io error spooling rememberd bytes
	             to backing storage.
	  */
	public long fillBuf(int len) throws IOException{
		return bh.write(this.in, len);
	}

	/**
	  read len bytes from the byte holder, and write it to the output stream.

	  @exception IOException thrown on an io error spooling rememberd bytes
	             to backing storage.
	  */
	public int putBuf(OutputStream out, int len) throws IOException {
		bh.startReading();
		return bh.read(out, len);
	}

	/**
	  @see java.io.InputStream#skip
	  @exception IOException thrown on an io error spooling rememberd bytes
	             to backing storage.
	  */
	public long skip(long count)  throws IOException {
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(recording,
								 "Must be in record mode to perform a read.");
		return bh.write(in,count);
	}

	/**
	  Get an input stream for re-reading the remembered bytes.
	  */
	public InputStream getReplayStream() throws IOException {
		bh.startReading();
		recording = false;
		return new ByteHolderInputStream(bh);
	}

	/**
	  Get the byteHolder.
	  */
	public ByteHolder getByteHolder() throws IOException {
		return bh;
	}

	/**
	  Clear all the remembered bytes. This stream will
	  remember any bytes read after this call.
	  @exception IOException thrown on an io error clearing backing
	             storage.
	  */
	public void clear() throws IOException {
		bh.clear();
		recording = true;
	}

	/**
	  Set the InputStream from which this reads.

	  <P>Please note this does not clear remembered
	  bytes.
	 */
	public void setInput(InputStream in) {
		this.in = in;
	}

	/**
	  Return true iff this RememberBytesInputStream is
	  in recording mode.
	  */
	public boolean recording() {
		return recording;
	}

	/**
	  Return the number of bytes remains in the byteHolder
	  for reading, without setting the write/read mode.
	  */
	public int available() throws IOException {
		// may not have set reading to be true, then,
		// we are getting available in negative numbers.
		int remainingBytes = bh.available();
		remainingBytes = remainingBytes > 0 ? remainingBytes : (-1) * remainingBytes;
		return remainingBytes;
	}

    /**
 	  Return the number of bytes that have been saved to this byte holder.
      This result is different from available() as it is unaffected by the
      current read position on the ByteHolder.
	  */
	public int numBytesSaved() throws IOException 
    {
        return(bh.numBytesSaved());
    }

	/**
	  remove the remaining bytes in the byteHolder to the beginning
	  set the position to start recording just after these bytes.
	  returns how many bytes was transfered to the beginning.
	  */
	public int shiftToFront() throws IOException {
		int bytesShifted = bh.shiftToFront();
		return bytesShifted;
	}

	/**
	  @see java.lang.Object#toString
	  */
	public String toString()
	{
		return
			"RememberBytesInputStream: "+
			" recording: "+recording+" "+bh;
	}
}

