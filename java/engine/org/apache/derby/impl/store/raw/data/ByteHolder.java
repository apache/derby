/*

   Derby - Class org.apache.derby.impl.store.raw.data.ByteHolder

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
  Holder for a growing sequence of bytes. The ByteHolder supports a
  writing phase in which a caller appends bytes to the ByteHolder. 
  Later the caller may read the bytes out of the ByteHolder in
  the order they were written.
  */
public interface ByteHolder
{
	/**
	  Write a byte to this ByteHolder.

	  <P>The ByteHolder must be in writing mode to call this.
	  */
	public void write(int b)
		 throws IOException;
	/**
	  Write len bytes of data starting at 'offset' to this ByteHolder.

	  <P>The ByteHolder must be in writing mode to call this.
	  */
	public void write(byte[] data, int offset, int len)
		 throws IOException;

    /**	
	  Write up to count bytes from an input stream to this
	  ByteHolder. This may write fewer bytes if it encounters
	  an end of file on the input stream.

	  @return the number of bytes written.
	  @exception IOException thrown when reading in causes an
      error.
	  */
	public long write(InputStream in, long count)	
		 throws IOException;

	/**
	  Clear the bytes from the ByteHolder and place it in writing
	  mode. This may not free the memory the ByteHolder uses to
	  store data.
	  */
	public void clear()
		 throws IOException;

	/**
	  Place a ByteHolder in reading mode. After this call,
	  reads scan bytes sequentially in the order they were
	  written to the ByteHolder starting from the first byte.
	  When the ByteHolder is already in readmode this simply
	  arranges for reads to start at the beginning of the
	  sequence of saved bytes.
	  */
	public void startReading()
        throws IOException;

    /**
	  Read a byte from this ByteHolder.

	  <P>The ByteHolder must be in reading mode to call this.

	  @return The byte or -1 if there are no bytes available.
	  */
	public int read()
		 throws IOException;

	/**
	  Read up to 'len' bytes from this ByteHolder and store them in
	  an array at offset 'off'.

	  <P>The ByteHolder must be in reading mode to call this.

	  @return the number of bytes read or -1 if the this ByteHolder
	  has no more bytes.
	  */
	public int read(byte b[],
					int off,
					int len)
		 throws IOException;

	/**
	  Read up to 'len' bytes from this ByteHolder and write them to
	  the OutputStream

	  <P>The ByteHolder must be in reading mode to call this.

	  @return the number of bytes read or -1 if the this ByteHolder
	  has no more bytes.
	  */
	public int read(OutputStream out,
					int len)
		 throws IOException;

	/**
	  shift the remaining unread bytes to the beginning of the byte holder
	  */
	public int shiftToFront()
		throws IOException;

    /**
 	  Return the number of bytes that can be read from this ByteHolder
	  without blocking on an IO.
	  */
	public int available()
		 throws IOException;

    /**
 	  Return the number of bytes that have been saved to this byte holder.
      This result is different from available() as it is unaffected by the
      current read position on the ByteHolder.
	  */
	public int numBytesSaved()
		 throws IOException;

    /**
	  Skip over the specified number of bytes in a ByteHolder.
	  */
	public long skip(long count)
		 throws IOException;

    /**
	  Return true if this is in writing mode.
	  */
	public boolean writingMode();
}
