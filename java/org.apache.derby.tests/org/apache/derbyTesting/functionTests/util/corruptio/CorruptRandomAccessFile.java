/*

   Derby - Class org.apache.derbyTesting.functionTests.util.corruptio.CorruptRandomAccessFile

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

package org.apache.derbyTesting.functionTests.util.corruptio;
import org.apache.derby.io.StorageRandomAccessFile;
import java.io.IOException;
import java.io.File;


/**
 * This class provides a proxy implementation of the StorageRandomAccess File
 * interface.  It is used by CorruptDiskStorageFactory to instrument the database engine 
 * i/o for testing puproses. How the i/o operation are corrupted is based on the values
 * set in the instance of the Singleton CorruptibleIo class by the tests.
 * Methods in this class functon similar to java.io.RandomAccessFile except
 * when modified to perform the corruptios.
 *
 * @version 1.0
 * @see java.io.RandomAccessFile
 * @see StorageRandomAccessFile
 */
public class CorruptRandomAccessFile implements StorageRandomAccessFile
{

	private StorageRandomAccessFile realRaf;
	private CorruptibleIo cbio;
	private File realFile;

    /**
     * Construct a CorruptRandomAccessFile
     *
     * @param raf  The real random access file to which  calls are delegated fro
     *              this proxy class.
     */
    CorruptRandomAccessFile(StorageRandomAccessFile raf, File realFile)
    {
		this.realRaf = raf;
		cbio =  CorruptibleIo.getInstance();
		this.realFile = realFile;
    }

    public  CorruptRandomAccessFile clone()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-590
        return new CorruptRandomAccessFile( realRaf.clone(), realFile );
    }

	
	/**
     * Closes this file.
     */
    public void close() throws IOException
	{
		realRaf.close();
	}

    /**
     * Get the current offset in this file.
     */
    public long getFilePointer() throws IOException
	{
		return realRaf.getFilePointer();
	}

    /**
     * Gets the length of this file.
     */
    public long length() throws IOException
	{
		return realRaf.length();
	}

    /**
     * Set the file pointer. 
     */
    public void seek(long newFilePointer) throws IOException
	{
		realRaf.seek(newFilePointer);
	}

    /**
     * Sets the length of this file, either extending or truncating it.
     */
    public void setLength(long newLength) throws IOException
	{
		realRaf.setLength(newLength);
	}
    
    /**
     * Force any changes out to the persistent store. 
     */
    public void sync() throws IOException
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-4963
//IC see: https://issues.apache.org/jira/browse/DERBY-4963
        realRaf.sync();
	}


	/*** Following functions Implement DataInput interfaces ****/

    /**
     * Reads some bytes from an input  stream into the byte array.
     */
    public void readFully(byte b[]) throws IOException
	{
		realRaf.readFully(b);
	}

    /**
     *
     * Reads the specified number of  bytes from an input stream.
     */
    public void readFully(byte b[], int off, int len) throws IOException
	{
		realRaf.readFully(b , off, len);
	}

    /**
     * skip over <code>nBytes</code> bytes of data 
     */
    public int skipBytes(int nBytes) throws IOException
	{
		return realRaf.skipBytes(nBytes);
	}

    /**
     * Reads a  byte and returns true if the byte is not zero
	 * otherwise false. 
     */
    public boolean readBoolean() throws IOException
	{
		return realRaf.readBoolean();
	}

    /**
     * returns one input byte from the stream.
     */
    public byte readByte() throws IOException
	{
		return realRaf.readByte();
	}

    /**
     * Reads one input byte in the unsigned form. 
     */
    public int readUnsignedByte() throws IOException
	{
		return realRaf.readUnsignedByte();
	}

    /**
     * returns a short  value from the stream. 
     */
    public short readShort() throws IOException
	{
		return realRaf.readShort();
	}

    /**
	 * returns unsigned short.
     */
    public int readUnsignedShort() throws IOException
	{
		return realRaf.readUnsignedShort();
	}

    /**
	 * returns a char value from the stream.
     */
    public char readChar() throws IOException
	{
		return realRaf.readChar();
	}

    /**
	 * returns an Int from the stream.
     */
    public int readInt() throws IOException
	{
		return realRaf.readInt();
	}

    /**
	 * returns a long from the stream.
     */
    public long readLong() throws IOException
	{
		return realRaf.readLong();
	}

    /**
     * returns a float from the stream. 
     */
    public float readFloat() throws IOException
	{
		return realRaf.readFloat();
	}

    /**
     * returns a double from the stream.
     */
    public double readDouble() throws IOException
	{
		return realRaf.readDouble();
	}

    /**
     * returns the next line of text from the input stream.
     */
    public String readLine() throws IOException
	{
		return realRaf.readLine();
	}

    /**
     * returns a string that has been encoded using in the  UTF-8 format.
     */
    public String readUTF() throws IOException
	{
		return realRaf.readUTF();
	}


	/* Proxy Implementation of DataOutput interface */ 	   

	/**
     * Writes an int to the output stream .
     */
    public void write(int b) throws IOException
	{
		realRaf.write(b);
	}

    /**
     * Writes all the bytes in array to the stream.
     */
    public void write(byte b[]) throws IOException
	{
		realRaf.write(b);
	}

    /**
     * Writes specified number bytes from array to the stream.
	 * If the corruption flags are enabled, byte array
	 * is corrupted before doing the real write.
     */
    public void write(byte b[], int off, int len) throws IOException
	{
		if (cbio.isCorruptibleFile(realFile)){
			//corrupt the input byte array
			cbio.corrupt(b , off, len);
		}
		realRaf.write(b, off, len);
	}

    /**
     * Writes a boolean value to this output stream.
     */
    public void writeBoolean(boolean value) throws IOException
	{
		realRaf.writeBoolean(value);
	}

    /**
     * Writes to  the eight low-order bits of ant int.
     *
     */
    public void writeByte(int value) throws IOException
	{
		realRaf.writeByte(value);
	}

    /**
     * Writes a short value to the output stream  
     */
    public void writeShort(int value) throws IOException
	{
		realRaf.writeShort(value);
	}

    /**
     * Writes a char value to the output stream.
	 *
     * @param      value   the <code>char</code> value to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public void writeChar(int value) throws IOException
	{
		realRaf.writeChar(value);
	}

    /**
     * Writes an int value to the output stream.
     */
    public void writeInt(int value) throws IOException
	{
		realRaf.writeInt(value);
	}

    /**
     * Writes a long  value to the output stream.
     */
    public void writeLong(long value) throws IOException
	{
		realRaf.writeLong(value);
	}

    /**
     * Writes a float value to the output stream.
     */
    public void writeFloat(float value) throws IOException
	{
		realRaf.writeFloat(value);
	}

    /**
     * Writes a a double value to the stream.
     */
    public void writeDouble(double value) throws IOException
	{
		realRaf.writeDouble(value);
	}

    /**
     * Writes a string as bytes to the stream.
     */
    public void writeBytes(String str) throws IOException
	{
		realRaf.writeBytes(str);
	}

    /**
     * Writes  the string to the stream.
     */
    public void writeChars(String str) throws IOException
	{
		realRaf.writeChars(str);
	}

    /**
     * Writes the string in the utf format. 
     */
    public void writeUTF(String str) throws IOException
	{
		realRaf.writeUTF(str);
	}

    /**
     * Reads up to <code>len</code> bytes of data from this file into an
     * array of bytes. This method blocks until at least one byte of input
     * is available.
     * <p>
     *
     *
     * @param b     the buffer into which the data is read.
     * @param off   the start offset in array <code>b</code>
     *                   at which the data is written.
     * @param len   the maximum number of bytes read.
     * @return the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception IOException If the first byte cannot be read for any reason
     * other than end of file, or if the random access file has been closed, or
     * if some other I/O error occurs.
     * @exception NullPointerException If <code>b</code> is <code>null</code>.
     * @exception IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     */
    public int read(byte[] b, int off, int len) throws IOException {
        return realRaf.read (b, off, len);
    }
}
